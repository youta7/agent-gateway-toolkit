// Exact sliding-window token rate limiter on node:sqlite.
//
// Every accepted request inserts a row (tenant, ts_ms, tokens). A tenant's
// usage is the sum of its rows newer than now - windowMs, so the window
// slides continuously instead of resetting on fixed boundaries. Reservations
// are made inside a BEGIN IMMEDIATE transaction so concurrent callers, even
// in other processes sharing the same database file, are serialised.
import { DatabaseSync, type StatementSync } from "node:sqlite";

export interface LimiterOptions {
  /** Maximum tokens a tenant may consume inside any window. */
  limit: number;
  /** Window length in milliseconds. */
  windowMs: number;
  /** How often expired rows are swept globally. 0 disables the timer. Default: min(windowMs, 60s). */
  sweepIntervalMs?: number;
}

export type ReserveResult =
  | { ok: true; reservationId: number; used: number; remaining: number }
  | { ok: false; used: number; limit: number; retryAfterMs: number };

export class SlidingWindowLimiter {
  readonly db: DatabaseSync;
  readonly limit: number;
  readonly windowMs: number;
  private readonly ownsDb: boolean;
  private sweepTimer: NodeJS.Timeout | null = null;

  private readonly stmtDeleteExpiredForTenant: StatementSync;
  private readonly stmtSumForTenant: StatementSync;
  private readonly stmtRowsForTenant: StatementSync;
  private readonly stmtInsert: StatementSync;
  private readonly stmtSettle: StatementSync;
  private readonly stmtRelease: StatementSync;
  private readonly stmtSweep: StatementSync;

  /**
   * @param db  An open DatabaseSync, or a path (":memory:" for tests).
   */
  constructor(db: DatabaseSync | string, options: LimiterOptions) {
    if (!(Number.isFinite(options.limit) && options.limit > 0)) throw new RangeError("limit must be a positive number");
    if (!(Number.isFinite(options.windowMs) && options.windowMs > 0)) throw new RangeError("windowMs must be a positive number");
    this.limit = Math.floor(options.limit);
    this.windowMs = Math.floor(options.windowMs);

    this.ownsDb = typeof db === "string";
    this.db = typeof db === "string" ? new DatabaseSync(db) : db;

    // busy_timeout first: it makes every later statement, including the
    // journal_mode switch and the DDL below, wait for a contending process
    // instead of throwing SQLITE_BUSY. WAL lets readers proceed while a writer
    // holds the reserved lock; synchronous=NORMAL is durable enough for
    // accounting data.
    this.db.exec("PRAGMA busy_timeout = 5000");
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA synchronous = NORMAL");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS token_usage (
        id     INTEGER PRIMARY KEY AUTOINCREMENT,
        tenant TEXT    NOT NULL,
        ts_ms  INTEGER NOT NULL,
        tokens INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_token_usage_tenant_ts ON token_usage (tenant, ts_ms);
    `);

    this.stmtDeleteExpiredForTenant = this.db.prepare("DELETE FROM token_usage WHERE tenant = ? AND ts_ms <= ?");
    this.stmtSumForTenant = this.db.prepare("SELECT COALESCE(SUM(tokens), 0) AS used FROM token_usage WHERE tenant = ? AND ts_ms > ?");
    this.stmtRowsForTenant = this.db.prepare("SELECT ts_ms, tokens FROM token_usage WHERE tenant = ? AND ts_ms > ? ORDER BY ts_ms ASC, id ASC");
    this.stmtInsert = this.db.prepare("INSERT INTO token_usage (tenant, ts_ms, tokens) VALUES (?, ?, ?)");
    this.stmtSettle = this.db.prepare("UPDATE token_usage SET tokens = ? WHERE id = ?");
    this.stmtRelease = this.db.prepare("DELETE FROM token_usage WHERE id = ?");
    this.stmtSweep = this.db.prepare("DELETE FROM token_usage WHERE ts_ms <= ?");

    const sweepEvery = options.sweepIntervalMs ?? Math.min(this.windowMs, 60_000);
    if (sweepEvery > 0) {
      this.sweepTimer = setInterval(() => this.sweep(), sweepEvery);
      this.sweepTimer.unref(); // never keep the process alive just to sweep
    }
  }

  /** Tokens counted against the tenant in the window ending at `now`. */
  used(tenant: string, now: number = Date.now()): number {
    const row = this.stmtSumForTenant.get(tenant, now - this.windowMs) as { used: number | bigint };
    return Number(row.used);
  }

  /**
   * Atomically checks the tenant's window and records `estimate` tokens if
   * it fits.
   *
   * The whole read-check-insert runs inside BEGIN IMMEDIATE. Without the
   * transaction, several concurrent requests for the same tenant would each
   * read "0 used", each conclude they fit, and each insert, letting N
   * requests through where only one should have passed. BEGIN IMMEDIATE
   * takes the write lock up front, so the second caller blocks (up to
   * busy_timeout) until the first has committed and then sees its row.
   */
  reserve(tenant: string, estimate: number, now: number = Date.now()): ReserveResult {
    if (!(Number.isFinite(estimate) && estimate >= 0)) throw new RangeError("estimate must be a non-negative number");
    const tokens = Math.ceil(estimate);
    const cutoff = now - this.windowMs;

    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.stmtDeleteExpiredForTenant.run(tenant, cutoff);
      const used = Number((this.stmtSumForTenant.get(tenant, cutoff) as { used: number | bigint }).used);

      if (used + tokens > this.limit) {
        const retryAfterMs = this.retryAfterMs(tenant, tokens, used, now);
        this.db.exec("ROLLBACK");
        return { ok: false, used, limit: this.limit, retryAfterMs };
      }

      const { lastInsertRowid } = this.stmtInsert.run(tenant, now, tokens);
      this.db.exec("COMMIT");
      const nowUsed = used + tokens;
      return { ok: true, reservationId: Number(lastInsertRowid), used: nowUsed, remaining: this.limit - nowUsed };
    } catch (err) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        /* transaction already gone */
      }
      throw err;
    }
  }

  /**
   * Replaces a reservation's estimate with the actual token count once the
   * response is known. `now` is accepted for signature symmetry; the row keeps
   * its original timestamp so the window still counts from when the request
   * was admitted. Returns false if the row no longer exists (swept or released).
   */
  settle(reservationId: number, actual: number, _now: number = Date.now()): boolean {
    if (!(Number.isFinite(actual) && actual >= 0)) throw new RangeError("actual must be a non-negative number");
    const { changes } = this.stmtSettle.run(Math.ceil(actual), reservationId);
    return Number(changes) > 0;
  }

  /** Drops a reservation entirely (e.g. the upstream call failed before consuming anything). */
  release(reservationId: number, _now: number = Date.now()): boolean {
    const { changes } = this.stmtRelease.run(reservationId);
    return Number(changes) > 0;
  }

  /** Deletes every tenant's expired rows. Returns the number removed. */
  sweep(now: number = Date.now()): number {
    const { changes } = this.stmtSweep.run(now - this.windowMs);
    return Number(changes);
  }

  /** Stops the sweep timer and closes the database if this instance opened it. */
  close(): void {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    if (this.ownsDb) this.db.close();
  }

  /**
   * Milliseconds until `estimate` could fit, assuming no new usage arrives.
   * Walks the tenant's live rows oldest-first; each row stops counting at
   * ts_ms + windowMs, so the answer is the expiry of the first row at which
   * enough tokens have fallen out of the window. An estimate larger than the
   * whole limit can never fit, so the caller is told to wait a full window.
   */
  private retryAfterMs(tenant: string, estimate: number, used: number, now: number): number {
    if (estimate > this.limit) return this.windowMs;
    const need = used + estimate - this.limit;
    if (need <= 0) return 0;
    const rows = this.stmtRowsForTenant.all(tenant, now - this.windowMs) as Array<{ ts_ms: number | bigint; tokens: number | bigint }>;
    let freed = 0;
    for (const row of rows) {
      freed += Number(row.tokens);
      if (freed >= need) {
        return Math.max(1, Number(row.ts_ms) + this.windowMs - now);
      }
    }
    return this.windowMs;
  }
}

// ---------------------------------------------------------------------------
// Request token estimation
// ---------------------------------------------------------------------------
interface ChatLike {
  messages?: unknown;
  max_tokens?: unknown;
  max_completion_tokens?: unknown;
}

function contentChars(content: unknown): number {
  if (typeof content === "string") return content.length;
  if (Array.isArray(content)) {
    // OpenAI content parts: count the text parts, ignore images etc.
    let n = 0;
    for (const part of content) {
      if (part && typeof part === "object" && typeof (part as { text?: unknown }).text === "string") {
        n += ((part as { text: string }).text).length;
      }
    }
    return n;
  }
  return 0;
}

/**
 * Cheap pre-flight estimate for a chat completion request:
 *   ceil(content chars / 4) + 4 overhead per message, plus the completion
 *   budget (max_tokens, else 256).
 */
export function estimateTokens(body: unknown): number {
  const b = (body && typeof body === "object" ? body : {}) as ChatLike;
  let total = 0;
  if (Array.isArray(b.messages)) {
    for (const m of b.messages) {
      const content = m && typeof m === "object" ? (m as { content?: unknown }).content : undefined;
      total += Math.ceil(contentChars(content) / 4) + 4;
    }
  }
  const maxTokens = typeof b.max_tokens === "number" ? b.max_tokens : typeof b.max_completion_tokens === "number" ? b.max_completion_tokens : undefined;
  total += maxTokens !== undefined && Number.isFinite(maxTokens) && maxTokens >= 0 ? Math.ceil(maxTokens) : 256;
  return total;
}
