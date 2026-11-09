// LLM guardrail gateway: a streaming proxy for POST /v1/chat/completions
// that redacts PII from the assistant's output as it flows past.
//
// Memory discipline: the only things held per request are the SSE line
// buffer (at most one partial event) and the redactor's tail (at most
// maxHold characters). Nothing accumulates the full response.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { StreamingRedactor } from "./redactor.js";

export const DEFAULT_HOST = "127.0.0.1";
export const DEFAULT_PORT = 5000;
export const DEFAULT_UPSTREAM_URL = "http://127.0.0.1:5001";
export const UPSTREAM_HEADERS_TIMEOUT_MS = 10_000;
export const MAX_BODY_BYTES = 1024 * 1024;
const ROUTE = "/v1/chat/completions";

export interface GuardrailOptions {
  port?: number;
  host?: string;
  upstreamUrl?: string;
  upstreamTimeoutMs?: number;
  maxHold?: number;
}

export interface Guardrail {
  server: Server;
  url: string;
  close: () => Promise<void>;
}

// ---------------------------------------------------------------------------
// Errors and logging
// ---------------------------------------------------------------------------
type ErrorType = "invalid_request_error" | "not_found" | "upstream_error" | "payload_too_large";

function sendError(res: ServerResponse, status: number, type: ErrorType, message: string, requestId: string): void {
  res.writeHead(status, { "content-type": "application/json", "x-request-id": requestId });
  res.end(JSON.stringify({ error: { type, message, request_id: requestId } }));
}

function log(level: "info" | "warn" | "error", event: string, fields: Record<string, unknown>): void {
  process.stderr.write(JSON.stringify({ ts: new Date().toISOString(), level, event, ...fields }) + "\n");
}

function headerString(req: IncomingMessage, name: string): string | undefined {
  const v = req.headers[name];
  return Array.isArray(v) ? v[0] : v;
}

// ---------------------------------------------------------------------------
// SSE frame handling
// ---------------------------------------------------------------------------
interface ChunkFrame {
  id?: unknown;
  object?: unknown;
  created?: unknown;
  model?: unknown;
  system_fingerprint?: unknown;
  choices?: Array<{ index?: unknown; delta?: Record<string, unknown>; finish_reason?: unknown }>;
  [k: string]: unknown;
}

/** Splits an SSE event into its data payload (joined data: lines) and the other lines. */
function parseEvent(event: string): { data: string | null; otherLines: string[] } {
  const lines = event.split(/\r?\n/);
  const dataLines: string[] = [];
  const otherLines: string[] = [];
  for (const line of lines) {
    if (line.startsWith("data:")) dataLines.push(line.slice(5).replace(/^ /, ""));
    else otherLines.push(line);
  }
  return { data: dataLines.length > 0 ? dataLines.join("\n") : null, otherLines };
}

function serializeEvent(otherLines: string[], data: string): string {
  return [...otherLines, `data: ${data}`].join("\n") + "\n\n";
}

/** Builds a content frame that looks like it came from upstream. */
function syntheticContentFrame(template: ChunkFrame | null, content: string): string {
  const base: ChunkFrame = template
    ? {
        id: template.id,
        object: template.object ?? "chat.completion.chunk",
        created: template.created,
        model: template.model,
        ...(template.system_fingerprint !== undefined ? { system_fingerprint: template.system_fingerprint } : {}),
      }
    : { id: `chatcmpl-guardrail-${randomUUID().slice(0, 8)}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: "unknown" };
  return `data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta: { content }, finish_reason: null }] })}\n\n`;
}

// ---------------------------------------------------------------------------
// Gateway
// ---------------------------------------------------------------------------
export function startGuardrail(options: GuardrailOptions = {}): Promise<Guardrail> {
  const host = options.host ?? DEFAULT_HOST;
  const port = options.port ?? DEFAULT_PORT;
  const upstreamUrl = options.upstreamUrl ?? DEFAULT_UPSTREAM_URL;
  const upstreamTimeoutMs = options.upstreamTimeoutMs ?? UPSTREAM_HEADERS_TIMEOUT_MS;
  const maxHold = options.maxHold;

  const handle = async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    const requestId = headerString(req, "x-request-id") ?? randomUUID();
    const started = Date.now();
    const url = new URL(req.url ?? "/", "http://localhost");

    if (url.pathname !== ROUTE) {
      req.resume();
      sendError(res, 404, "not_found", `No route for ${req.method} ${url.pathname}`, requestId);
      log("warn", "rejected", { request_id: requestId, status: 404, reason: "not_found", path: url.pathname });
      return;
    }
    if (req.method !== "POST") {
      req.resume();
      sendError(res, 404, "not_found", `${req.method} is not supported on ${ROUTE}; use POST`, requestId);
      log("warn", "rejected", { request_id: requestId, status: 404, reason: "method", method: req.method });
      return;
    }

    // Body
    let rawBody: string;
    try {
      rawBody = await readBody(req, MAX_BODY_BYTES);
    } catch (err) {
      if (err instanceof PayloadTooLarge) {
        sendError(res, 413, "payload_too_large", `Request body exceeds ${MAX_BODY_BYTES} bytes`, requestId);
        log("warn", "rejected", { request_id: requestId, status: 413, reason: "payload_too_large" });
        return;
      }
      sendError(res, 400, "invalid_request_error", "Could not read request body", requestId);
      log("warn", "rejected", { request_id: requestId, status: 400, reason: "body_read_error" });
      return;
    }

    let body: Record<string, unknown>;
    try {
      const parsed: unknown = JSON.parse(rawBody);
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) throw new Error("not an object");
      body = parsed as Record<string, unknown>;
    } catch {
      sendError(res, 400, "invalid_request_error", "Body must be a JSON object", requestId);
      log("warn", "rejected", { request_id: requestId, status: 400, reason: "invalid_json" });
      return;
    }
    if (!Array.isArray(body["messages"]) || body["messages"].length === 0) {
      sendError(res, 400, "invalid_request_error", "messages must be a non-empty array", requestId);
      log("warn", "rejected", { request_id: requestId, status: 400, reason: "missing_messages" });
      return;
    }

    // Force streaming: the redactor only works on a stream, and a client that
    // asked for a non-streamed body still gets a valid SSE response.
    const forwardBody = JSON.stringify({ ...body, stream: true });

    const headers = new Headers({
      "content-type": "application/json",
      accept: "text/event-stream",
      "x-request-id": requestId,
    });
    const auth = headerString(req, "authorization");
    if (auth) headers.set("authorization", auth);
    for (const [name, value] of Object.entries(req.headers)) {
      if (name.startsWith("x-mock-") && typeof value === "string") headers.set(name, value);
    }

    const controller = new AbortController();
    let clientGone = false;
    res.on("close", () => {
      if (!res.writableFinished) {
        clientGone = true;
        controller.abort(new Error("client disconnected"));
      }
    });
    const timer = setTimeout(() => controller.abort(new Error(`upstream headers not received within ${upstreamTimeoutMs}ms`)), upstreamTimeoutMs);

    let upstream: Response;
    try {
      // Append rather than resolve, so an upstream URL with a path prefix
      // (e.g. an Azure deployment or a vendor proxy) keeps that prefix.
      const target = `${upstreamUrl.replace(/\/+$/, "")}${ROUTE}`;
      upstream = await fetch(target, { method: "POST", headers, body: forwardBody, signal: controller.signal });
    } catch (err) {
      clearTimeout(timer);
      const reason = err instanceof Error ? (err.cause instanceof Error ? `${err.message}: ${err.cause.message}` : err.message) : String(err);
      if (!clientGone) sendError(res, 502, "upstream_error", `Upstream unreachable: ${reason}`, requestId);
      log("error", "upstream_error", { request_id: requestId, status: 502, reason, total_ms: Date.now() - started });
      return;
    }
    clearTimeout(timer);

    if (!upstream.ok || !upstream.body) {
      const detail = await upstream.text().catch(() => "");
      sendError(res, 502, "upstream_error", `Upstream returned HTTP ${upstream.status}${detail ? `: ${detail.slice(0, 200)}` : ""}`, requestId);
      log("error", "upstream_error", { request_id: requestId, status: 502, upstream_status: upstream.status, total_ms: Date.now() - started });
      return;
    }

    // Stream
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      "x-request-id": requestId,
    });

    const redactor = new StreamingRedactor(maxHold === undefined ? {} : { maxHold });
    const reader = upstream.body.getReader();
    const decoder = new TextDecoder();
    let lineBuffer = "";
    let template: ChunkFrame | null = null;
    let framesIn = 0;
    let framesOut = 0;
    let synthetic = 0;
    let ttftMs: number | null = null;
    let sawDone = false;
    let finished = false;

    const write = async (text: string): Promise<void> => {
      framesOut++;
      if (!res.write(text)) await once(res, "drain");
    };

    const emitContent = async (content: string): Promise<void> => {
      if (content.length === 0) return;
      if (ttftMs === null) ttftMs = Date.now() - started;
      synthetic++;
      await write(syntheticContentFrame(template, content));
    };

    /** Flushes the redactor and emits whatever it held as a synthetic frame. */
    const flushHeld = async (): Promise<void> => {
      if (finished) return;
      finished = true;
      await emitContent(redactor.flush());
    };

    const handleEvent = async (event: string): Promise<void> => {
      if (event.trim().length === 0) return;
      framesIn++;
      const { data, otherLines } = parseEvent(event);

      // Comments / event-only frames: pass untouched.
      if (data === null) {
        await write(event + "\n\n");
        return;
      }

      if (data.trim() === "[DONE]") {
        sawDone = true;
        await flushHeld();
        await write(serializeEvent(otherLines, data));
        return;
      }

      let frame: ChunkFrame;
      try {
        frame = JSON.parse(data) as ChunkFrame;
      } catch {
        // Not JSON: nothing we can inspect, nothing we can leak. Pass through.
        await write(serializeEvent(otherLines, data));
        return;
      }

      if (frame && typeof frame === "object" && frame.id !== undefined) template = frame;

      const choice = Array.isArray(frame.choices) ? frame.choices[0] : undefined;
      const content = choice?.delta?.["content"];
      const finishReason = choice?.finish_reason;
      const isFinish = finishReason !== undefined && finishReason !== null;

      if (typeof content !== "string" || content.length === 0) {
        // Role-only (OpenAI sends content: "" alongside the role), tool-call,
        // usage or otherwise content-free frame: nothing to redact, nothing
        // that can leak. Pass it through untouched.
        if (isFinish) {
          await flushHeld();
        }
        await write(serializeEvent(otherLines, data));
        return;
      }

      // Content frame.
      const released = redactor.push(content);
      if (isFinish) {
        // Some providers attach the final content to the finish frame. Emit
        // everything (released + flushed) as a synthetic frame, then forward
        // the finish frame with its content removed so nothing bypasses us.
        finished = true;
        await emitContent(released + redactor.flush());
        const delta = { ...(choice?.delta ?? {}) };
        delete delta["content"];
        const stripped = { ...frame, choices: [{ ...choice, delta }] };
        await write(serializeEvent(otherLines, JSON.stringify(stripped)));
        return;
      }

      if (released.length === 0) return; // held entirely; emit nothing for this frame
      if (ttftMs === null) ttftMs = Date.now() - started;
      const rewritten = { ...frame, choices: [{ ...choice, delta: { ...(choice?.delta ?? {}), content: released } }] };
      await write(serializeEvent(otherLines, JSON.stringify(rewritten)));
    };

    let aborted = false;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        lineBuffer += decoder.decode(value, { stream: true });
        // Events are delimited by a blank line. Keep only the trailing partial.
        for (;;) {
          const m = /\r?\n\r?\n/.exec(lineBuffer);
          if (!m) break;
          const event = lineBuffer.slice(0, m.index);
          lineBuffer = lineBuffer.slice(m.index + m[0].length);
          await handleEvent(event);
        }
      }
      lineBuffer += decoder.decode();
      if (lineBuffer.trim().length > 0) await handleEvent(lineBuffer);
      lineBuffer = "";
      // Upstream ended without a finish frame or [DONE]: still release the tail.
      await flushHeld();
      if (!sawDone) await write("data: [DONE]\n\n");
      res.end();
    } catch (err) {
      aborted = true;
      if (!clientGone) {
        log("error", "stream_error", { request_id: requestId, message: err instanceof Error ? err.message : String(err) });
      }
      res.destroy();
    }

    log(aborted ? "warn" : "info", "completion", {
      request_id: requestId,
      ttft_ms: ttftMs,
      total_ms: Date.now() - started,
      frames: { in: framesIn, out: framesOut, synthetic },
      redactions: redactor.stats,
      upstream_status: upstream.status,
      aborted,
      client_disconnected: clientGone,
    });
  };

  const server = createServer((req, res) => {
    handle(req, res).catch((err: unknown) => {
      log("error", "unhandled", { message: err instanceof Error ? err.message : String(err) });
      if (!res.headersSent) sendError(res, 502, "upstream_error", "Internal error", headerString(req, "x-request-id") ?? "unknown");
      else res.destroy();
    });
  });

  return new Promise((resolvePromise, reject) => {
    server.once("error", reject);
    server.listen(port, host, () => {
      server.off("error", reject);
      const addr = server.address();
      const actualPort = typeof addr === "object" && addr ? addr.port : port;
      resolvePromise({
        server,
        url: `http://${host}:${actualPort}`,
        close: () =>
          new Promise<void>((done, failClose) => {
            server.closeAllConnections?.();
            server.close((err) => (err ? failClose(err) : done()));
          }),
      });
    });
  });
}

class PayloadTooLarge extends Error {}

function readBody(req: IncomingMessage, limit: number): Promise<string> {
  return new Promise((resolvePromise, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    req.on("data", (c: Buffer) => {
      total += c.length;
      if (total > limit) {
        reject(new PayloadTooLarge());
        req.removeAllListeners("data");
        req.resume();
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => resolvePromise(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

// ---------------------------------------------------------------------------
// CLI entry
// ---------------------------------------------------------------------------
const isMain = process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isMain) {
  const upstream = process.env["UPSTREAM_URL"] ?? DEFAULT_UPSTREAM_URL;
  startGuardrail({ port: Number(process.env["GUARDRAIL_PORT"] ?? DEFAULT_PORT), upstreamUrl: upstream })
    .then((g) => log("info", "guardrail_started", { url: g.url, upstream }))
    .catch((err: unknown) => {
      log("error", "guardrail_failed", { message: err instanceof Error ? err.message : String(err) });
      process.exit(1);
    });
}
