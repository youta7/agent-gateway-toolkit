// Streaming PII redactor for LLM token streams.
//
// Tokens arrive in arbitrary slices, so an email, SSN or card number can be
// split across many deltas. The redactor holds back the smallest suffix of the
// stream that could still grow into a match, releases everything before it,
// and redacts only what it releases. flush() drains whatever is still held.

export const REDACTED = "[REDACTED]";

export interface RedactionStats {
  emails: number;
  ssns: number;
  cards: number;
}

// ---------------------------------------------------------------------------
// Patterns
// ---------------------------------------------------------------------------
const EMAIL_RE = /[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}/g;
const SSN_RE = /(?<!\d)\d{3}-\d{2}-\d{4}(?!\d)/g;
/** Maximal digit runs with at most one space or dash between digits. */
const DIGIT_RUN_RE = /\d(?:[ -]?\d)*/g;

const CARD_MIN_DIGITS = 13;
const CARD_MAX_DIGITS = 19;

/** Characters that may belong to an email, SSN or dash-separated card still in progress. */
const HOLD_CHAR_RE = /[A-Za-z0-9._%+@-]/;
/**
 * A held tail that could still be the continuation of a card: digits, spaces,
 * and the non-letter punctuation the hold class admits. Punctuation is allowed
 * so "1111." (last card group followed by a full stop) still counts and the
 * earlier groups are kept together with it; over-holding costs only latency,
 * under-holding leaks a split card. Letters or "@" mean it is a word or email.
 */
const NUMERIC_ISH_RE = /^[\d ._%+-]*$/;

export function luhn(digits: string): boolean {
  if (digits.length === 0) return false;
  let sum = 0;
  let double = false;
  for (let i = digits.length - 1; i >= 0; i--) {
    let n = digits.charCodeAt(i) - 48;
    if (n < 0 || n > 9) return false;
    if (double) {
      n *= 2;
      if (n > 9) n -= 9;
    }
    sum += n;
    double = !double;
  }
  return sum % 10 === 0;
}

// ---------------------------------------------------------------------------
// Card detection
// ---------------------------------------------------------------------------
interface Group {
  start: number; // offset in the run
  end: number; // exclusive
  digits: string;
}

function splitGroups(run: string): Group[] {
  const groups: Group[] = [];
  let i = 0;
  while (i < run.length) {
    if (run[i] === " " || run[i] === "-") {
      i++;
      continue;
    }
    const start = i;
    while (i < run.length && run[i] !== " " && run[i] !== "-") i++;
    groups.push({ start, end: i, digits: run.slice(start, i) });
  }
  return groups;
}

/**
 * Redacts card numbers inside one maximal digit run.
 *
 * Only windows that start and end on digit-group boundaries are considered.
 * A window that begins or ends mid-group ("1111 1111 1111 111" out of
 * "4111 1111 1111 1111") is almost never what a human wrote as a card, and
 * roughly one in ten random digit strings passes Luhn, so testing unaligned
 * windows would redact phone numbers, order ids and timestamps by accident.
 */
function redactCardsInRun(run: string, stats: RedactionStats): string {
  const groups = splitGroups(run);
  if (groups.length === 0) return run;

  // Prefix sums of digit counts make window length lookups O(1).
  const prefix = [0];
  for (const g of groups) prefix.push((prefix[prefix.length - 1] as number) + g.digits.length);
  const digitCount = (s: number, e: number) => (prefix[e + 1] as number) - (prefix[s] as number);

  const windows: Array<{ s: number; e: number; len: number }> = [];
  for (let s = 0; s < groups.length; s++) {
    for (let e = s; e < groups.length; e++) {
      const len = digitCount(s, e);
      if (len > CARD_MAX_DIGITS) break;
      if (len >= CARD_MIN_DIGITS) windows.push({ s, e, len });
    }
  }
  // Longest first so "4111 1111 1111 1111" beats a 13-digit sub-window that
  // happens to pass Luhn; ties resolve left to right.
  windows.sort((a, b) => b.len - a.len || a.s - b.s);

  for (const w of windows) {
    const digits = groups.slice(w.s, w.e + 1).map((g) => g.digits).join("");
    if (!luhn(digits)) continue;
    const startGroup = groups[w.s] as Group;
    const endGroup = groups[w.e] as Group;
    stats.cards++;
    const left = run.slice(0, startGroup.start);
    const right = run.slice(endGroup.end);
    // The remainder may hold another card ("4111 ... 1111 5500 ... 0004").
    return redactCards(left, stats) + REDACTED + redactCards(right, stats);
  }
  return run;
}

function redactCards(text: string, stats: RedactionStats): string {
  return text.replace(DIGIT_RUN_RE, (run) => redactCardsInRun(run, stats));
}

// ---------------------------------------------------------------------------
// Whole-string redaction
// ---------------------------------------------------------------------------
/**
 * Redacts a complete string. Order matters:
 *  1. emails: they may contain digits that would otherwise feed the card scan
 *  2. SSNs: a rigid 3-2-4 shape with digit lookarounds, so it is far less
 *     ambiguous than a Luhn window. Removing SSNs first keeps "123-45-6789
 *     4111 1111 1111 1111" from being read as one digit run in which a
 *     cross-number window like "6789 4111 1111 1111" can pass Luhn by chance.
 *  3. cards: the Luhn window search over whatever digit runs remain.
 */
export function redactText(text: string, stats: RedactionStats = { emails: 0, ssns: 0, cards: 0 }): string {
  let out = text.replace(EMAIL_RE, () => {
    stats.emails++;
    return REDACTED;
  });
  out = out.replace(SSN_RE, () => {
    stats.ssns++;
    return REDACTED;
  });
  out = redactCards(out, stats);
  return out;
}

// ---------------------------------------------------------------------------
// Streaming
// ---------------------------------------------------------------------------
export class StreamingRedactor {
  private pending = "";
  readonly stats: RedactionStats = { emails: 0, ssns: 0, cards: 0 };
  readonly maxHold: number;

  constructor(options: { maxHold?: number } = {}) {
    this.maxHold = options.maxHold ?? 256;
  }

  /** Text currently held back because it might still grow into a match. */
  get held(): string {
    return this.pending;
  }

  /**
   * Accepts the next delta and returns the text that is now safe to emit.
   *
   * 1. Append the delta to pending.
   * 2. Find the safe cut: everything after it could still be part of a match.
   * 3. CUT FIRST, THEN REDACT only the released head.
   *    Redacting pending before cutting is wrong: with pending equal to
   *    "john@example.co" the email regex already matches (".co" is a valid
   *    TLD), we would emit "[REDACTED]", and the "m" arriving in the next
   *    delta would leak as a stray character while the stats over-count.
   */
  push(delta: string): string {
    this.pending += delta;
    const cut = this.safeCutIndex(this.pending);
    const head = this.pending.slice(0, cut);
    this.pending = this.pending.slice(cut);
    return head.length === 0 ? "" : redactText(head, this.stats);
  }

  /** Redacts and releases everything still held. Call once the stream ends. */
  flush(): string {
    const rest = this.pending;
    this.pending = "";
    return rest.length === 0 ? "" : redactText(rest, this.stats);
  }

  /**
   * Index such that text.slice(0, index) can be released safely.
   *
   * Step A: walk back over characters that can appear in an email, SSN or
   *         dash-separated card. "john@exam" stays held.
   * Step B: while the held tail is numeric-ish, keep extending back over
   *         "<digits><space|dash>" groups so "4111 1111 11" stays together
   *         waiting for the rest of the card. An empty tail counts as
   *         numeric-ish so a delta ending in "4111 1111 " is still held.
   * Cap:    never hold more than maxHold characters, so an endless run of
   *         hold-class characters cannot stall the stream.
   */
  private safeCutIndex(text: string): number {
    let i = text.length;

    // Step A
    while (i > 0 && HOLD_CHAR_RE.test(text[i - 1] as string)) i--;

    // Step B
    for (;;) {
      if (i === 0) break;
      if (!NUMERIC_ISH_RE.test(text.slice(i))) break;
      const sep = text[i - 1];
      if (sep !== " " && sep !== "-") break;
      let j = i - 1;
      while (j > 0 && isDigit(text[j - 1] as string)) j--;
      if (j === i - 1) break; // separator not preceded by digits
      i = j;
    }

    // Cap
    if (text.length - i > this.maxHold) i = text.length - this.maxHold;
    return i;
  }
}

function isDigit(ch: string): boolean {
  return ch >= "0" && ch <= "9";
}
