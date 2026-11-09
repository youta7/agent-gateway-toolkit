// Mock OpenAI-compatible upstream that streams chat.completion.chunk SSE.
//
// The canned reply is tokenised so that PII straddles chunk boundaries on
// purpose: the gateway's redactor has to reassemble "john.d" + "oe@exa" +
// "mple.com" and "4111 11" + "11 1111 1111" before it can see them.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

export const DEFAULT_HOST = "127.0.0.1";
export const DEFAULT_PORT = 5001;
const MAX_DELAY_MS = 5_000;
const MAX_LONG_WORDS = 50_000;

/**
 * Canned tokens. Boundaries are chosen to be hostile:
 *  - the email is split across three tokens
 *  - the spaced card is split mid-group
 *  - the dashed card and a plain 15-digit Amex arrive whole (must still be caught)
 *  - two SSNs, one 13-digit order id that FAILS Luhn (must survive),
 *    a phone number and a ZIP (must survive)
 *  - the final token is an email with nothing after it, so the gateway's
 *    flush-on-finish path is exercised
 */
export const CANNED_TOKENS: readonly string[] = [
  "Hello",
  " there",
  "! Your",
  " account",
  " john.d",
  "oe@exa",
  "mple.com",
  " is",
  " active",
  ".",
  " Card",
  " on",
  " file",
  ":",
  " 4111 11",
  "11 1111 1111",
  ",",
  " backup",
  " 5500-0000-0000-0004",
  ",",
  " amex",
  " 378282246310005",
  ".",
  " SSN",
  " 123-45-6789",
  " and",
  " spouse",
  " 987-65-4321",
  ".",
  " Order",
  " 1234567890123",
  ",",
  " call",
  " 555-0100",
  ",",
  " ZIP",
  " 78701",
  ".",
  " Reach",
  " us",
  " at",
  " support@example.org",
];

export const CANNED_TEXT = CANNED_TOKENS.join("");

/** N words, one per token, with an email in place of every 50th word. */
export function longTokens(wordCount: number): string[] {
  const out: string[] = [];
  for (let i = 1; i <= wordCount; i++) {
    const word = i % 50 === 0 ? `user${i / 50}@example.com` : `word${i}`;
    out.push(i === 1 ? word : ` ${word}`);
  }
  return out;
}

interface ChatMessage {
  role?: unknown;
  content?: unknown;
}

function lastUserContent(messages: unknown): string | null {
  if (!Array.isArray(messages)) return null;
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i] as ChatMessage;
    if (m && m.role === "user" && typeof m.content === "string") return m.content;
  }
  return null;
}

function pickTokens(messages: unknown): string[] {
  const last = lastUserContent(messages);
  const m = last ? /^long:(\d+)/.exec(last.trim()) : null;
  if (m) {
    const n = Math.min(MAX_LONG_WORDS, Math.max(1, Number(m[1])));
    return longTokens(n);
  }
  return [...CANNED_TOKENS];
}

export interface MockUpstream {
  server: Server;
  url: string;
  /** Streams that ended because the client went away before completion. */
  readonly abortedStreams: () => number;
  close: () => Promise<void>;
}

function sendError(res: ServerResponse, status: number, type: string, message: string): void {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify({ error: { type, message } }));
}

export function startMockUpstream(port = DEFAULT_PORT, host = DEFAULT_HOST): Promise<MockUpstream> {
  let aborted = 0;

  const handle = (req: IncomingMessage, res: ServerResponse): void => {
    if (req.url !== "/v1/chat/completions") {
      req.resume();
      sendError(res, 404, "not_found", `No route for ${req.method} ${req.url}`);
      return;
    }
    if (req.method !== "POST") {
      req.resume();
      sendError(res, 405, "method_not_allowed", "Use POST");
      return;
    }

    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      let body: { model?: unknown; messages?: unknown; stream?: unknown };
      try {
        body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as typeof body;
      } catch {
        sendError(res, 400, "invalid_request_error", "Body is not valid JSON");
        return;
      }
      if (typeof body !== "object" || body === null) {
        sendError(res, 400, "invalid_request_error", "Body must be an object");
        return;
      }

      const model = typeof body.model === "string" ? body.model : "mock-gpt";
      const tokens = pickTokens(body.messages);
      const id = `chatcmpl-${randomUUID().replace(/-/g, "").slice(0, 24)}`;
      const created = Math.floor(Date.now() / 1000);
      const delayHeader = req.headers["x-mock-delay-ms"];
      const delayRaw = Number(Array.isArray(delayHeader) ? delayHeader[0] : (delayHeader ?? "0"));
      const delayMs = Number.isFinite(delayRaw) ? Math.min(MAX_DELAY_MS, Math.max(0, delayRaw)) : 0;

      if (body.stream === false) {
        // Non-streaming convenience path; the gateway never uses it.
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            id,
            object: "chat.completion",
            created,
            model,
            choices: [{ index: 0, message: { role: "assistant", content: tokens.join("") }, finish_reason: "stop" }],
          }),
        );
        return;
      }

      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
        "x-mock-upstream": "1",
      });

      const chunk = (delta: Record<string, unknown>, finishReason: string | null) =>
        `data: ${JSON.stringify({
          id,
          object: "chat.completion.chunk",
          created,
          model,
          choices: [{ index: 0, delta, finish_reason: finishReason }],
        })}\n\n`;

      let closed = false;
      res.on("close", () => {
        if (!res.writableFinished) {
          closed = true;
          aborted++;
        }
      });

      // OpenAI sends a role-only frame first, then content frames.
      res.write(chunk({ role: "assistant", content: "" }, null));

      let i = 0;
      const step = (): void => {
        if (closed) return;
        if (i < tokens.length) {
          res.write(chunk({ content: tokens[i] as string }, null));
          i++;
          if (delayMs > 0) setTimeout(step, delayMs);
          else setImmediate(step);
          return;
        }
        res.write(chunk({}, "stop"));
        res.write("data: [DONE]\n\n");
        res.end();
      };
      step();
    });
  };

  const server = createServer(handle);
  return new Promise((resolvePromise, reject) => {
    server.once("error", reject);
    server.listen(port, host, () => {
      server.off("error", reject);
      const addr = server.address();
      const actualPort = typeof addr === "object" && addr ? addr.port : port;
      resolvePromise({
        server,
        url: `http://${host}:${actualPort}`,
        abortedStreams: () => aborted,
        close: () =>
          new Promise<void>((done, failClose) => {
            server.closeAllConnections?.();
            server.close((err) => (err ? failClose(err) : done()));
          }),
      });
    });
  });
}

const isMain = process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isMain) {
  const port = Number(process.env["UPSTREAM_PORT"] ?? DEFAULT_PORT);
  startMockUpstream(port)
    .then((u) => {
      process.stderr.write(JSON.stringify({ ts: new Date().toISOString(), level: "info", event: "upstream_started", url: u.url }) + "\n");
    })
    .catch((err: unknown) => {
      process.stderr.write(
        JSON.stringify({ ts: new Date().toISOString(), level: "error", event: "upstream_failed", message: err instanceof Error ? err.message : String(err) }) + "\n",
      );
      process.exit(1);
    });
}
