// Router gateway: per-tenant token rate limiting in front of a primary /
// secondary model router.
//
// Request flow for POST /v1/chat/completions:
//   auth (Bearer -> tenant) -> validate -> estimate -> reserve -> route ->
//   settle with the provider's usage -> respond.
// If anything fails after the reservation was taken, the reservation is
// released so the tenant is not charged for a request that never completed.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { SlidingWindowLimiter, estimateTokens } from "./rate-limiter.js";
import { GatewayError, ModelRouter, type RouterOptions } from "./router.js";
import { PRIMARY_PORT, SECONDARY_PORT } from "./mock-providers.js";

export const DEFAULT_HOST = "127.0.0.1";
export const DEFAULT_PORT = 6100;
export const DEFAULT_TOKENS_PER_MINUTE = 50_000;
export const DEFAULT_WINDOW_MS = 60_000;
export const DEFAULT_PRIMARY_TIMEOUT_MS = 3000;
const ROUTE = "/v1/chat/completions";
const MAX_BODY_BYTES = 1024 * 1024;

/** API key -> tenant. */
export const API_KEYS: ReadonlyMap<string, string> = new Map([
  ["sk-tenant-a", "tenant-a"],
  ["sk-tenant-b", "tenant-b"],
]);

export interface RouterGatewayOptions {
  port?: number;
  host?: string;
  dbPath?: string;
  tokensPerWindow?: number;
  windowMs?: number;
  primaryUrl?: string;
  secondaryUrl?: string;
  primaryTimeoutMs?: number;
  secondaryTimeoutMs?: number;
  log?: RouterOptions["log"];
}

export interface RouterGateway {
  server: Server;
  url: string;
  limiter: SlidingWindowLimiter;
  close: () => Promise<void>;
}

type Log = NonNullable<RouterOptions["log"]>;

const defaultLog: Log = (level, event, fields) => {
  process.stderr.write(JSON.stringify({ ts: new Date().toISOString(), level, event, ...fields }) + "\n");
};

function headerString(req: IncomingMessage, name: string): string | undefined {
  const v = req.headers[name];
  return Array.isArray(v) ? v[0] : v;
}

function sendJson(res: ServerResponse, status: number, payload: unknown, headers: Record<string, string>): void {
  res.writeHead(status, { "content-type": "application/json", ...headers });
  res.end(JSON.stringify(payload));
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

function resolveTenant(authorization: string | undefined): string | null {
  if (!authorization) return null;
  const m = /^\s*Bearer\s+(\S+)\s*$/i.exec(authorization);
  if (!m) return null;
  return API_KEYS.get(m[1] as string) ?? null;
}

export function startRouterGateway(options: RouterGatewayOptions = {}): Promise<RouterGateway> {
  const host = options.host ?? DEFAULT_HOST;
  const port = options.port ?? DEFAULT_PORT;
  const log = options.log ?? defaultLog;
  const limiter = new SlidingWindowLimiter(options.dbPath ?? ":memory:", {
    limit: options.tokensPerWindow ?? DEFAULT_TOKENS_PER_MINUTE,
    windowMs: options.windowMs ?? DEFAULT_WINDOW_MS,
  });
  const router = new ModelRouter({
    primary: { name: "primary", url: options.primaryUrl ?? `http://${DEFAULT_HOST}:${PRIMARY_PORT}` },
    secondary: { name: "secondary", url: options.secondaryUrl ?? `http://${DEFAULT_HOST}:${SECONDARY_PORT}` },
    primaryTimeoutMs: options.primaryTimeoutMs ?? DEFAULT_PRIMARY_TIMEOUT_MS,
    ...(options.secondaryTimeoutMs !== undefined ? { secondaryTimeoutMs: options.secondaryTimeoutMs } : {}),
    log,
  });

  const handle = async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    const requestId = headerString(req, "x-request-id") ?? randomUUID();
    const started = Date.now();
    const baseHeaders: Record<string, string> = { "x-request-id": requestId };
    let tenant: string | null = null;
    let reservationId: number | null = null;
    let settled = false;
    let servedBy: string | null = null;
    let estimate = 0;
    let actual: number | null = null;

    const fail = (err: GatewayError, headers: Record<string, string> = {}): void => {
      sendJson(res, err.status, err.toPayload(requestId), { ...baseHeaders, ...headers });
      log(err.status >= 500 ? "error" : "warn", "request", {
        request_id: requestId,
        tenant,
        status: err.status,
        error_type: err.type,
        served_by: servedBy,
        estimate,
        ms: Date.now() - started,
      });
    };

    try {
      const url = new URL(req.url ?? "/", "http://localhost");
      if (url.pathname !== ROUTE || req.method !== "POST") {
        req.resume();
        throw new GatewayError("bad_request", 404, `No route for ${req.method} ${url.pathname}`);
      }

      // Auth
      tenant = resolveTenant(headerString(req, "authorization"));
      if (!tenant) {
        req.resume();
        fail(new GatewayError("unauthenticated", 401, "Missing or invalid API key"), { "www-authenticate": 'Bearer realm="router-gateway"' });
        return;
      }

      // Body
      let raw: string;
      try {
        raw = await readBody(req, MAX_BODY_BYTES);
      } catch (err) {
        if (err instanceof PayloadTooLarge) throw new GatewayError("bad_request", 413, `Request body exceeds ${MAX_BODY_BYTES} bytes`);
        throw new GatewayError("bad_request", 400, "Could not read request body");
      }
      let body: Record<string, unknown>;
      try {
        const parsed: unknown = JSON.parse(raw);
        if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) throw new Error("not an object");
        body = parsed as Record<string, unknown>;
      } catch {
        throw new GatewayError("bad_request", 400, "Body must be a JSON object");
      }
      const messages = body["messages"];
      if (!Array.isArray(messages) || messages.length === 0) {
        throw new GatewayError("bad_request", 400, "messages must be a non-empty array");
      }
      if (!messages.every((m) => m && typeof m === "object" && !Array.isArray(m))) {
        throw new GatewayError("bad_request", 400, "every message must be an object");
      }

      // Reserve
      estimate = estimateTokens(body);
      const reservation = limiter.reserve(tenant, estimate);
      if (!reservation.ok) {
        const retryAfterSec = Math.max(1, Math.ceil(reservation.retryAfterMs / 1000));
        fail(
          new GatewayError("rate_limited", 429, `Tenant token budget exceeded: ${reservation.used}/${reservation.limit} tokens used in the current window`, {
            retry_after_ms: reservation.retryAfterMs,
            used: reservation.used,
            limit: reservation.limit,
            estimate,
          }),
          {
            "retry-after": String(retryAfterSec),
            "x-ratelimit-limit-tokens": String(reservation.limit),
            "x-ratelimit-remaining-tokens": String(Math.max(0, reservation.limit - reservation.used)),
            "x-ratelimit-reset-ms": String(reservation.retryAfterMs),
          },
        );
        return;
      }
      reservationId = reservation.reservationId;

      // Route
      const result = await router.complete(body, requestId);
      servedBy = result.provider;

      // Settle with what the provider actually billed; fall back to the estimate.
      const total = result.usage && typeof result.usage.total_tokens === "number" && result.usage.total_tokens >= 0 ? result.usage.total_tokens : estimate;
      actual = total;
      limiter.settle(reservationId, total);
      settled = true;
      const remaining = Math.max(0, limiter.limit - limiter.used(tenant));

      sendJson(res, 200, result.body, {
        ...baseHeaders,
        "x-served-by": result.provider,
        "x-ratelimit-limit-tokens": String(limiter.limit),
        "x-ratelimit-remaining-tokens": String(remaining),
      });
      log("info", "request", {
        request_id: requestId,
        tenant,
        status: 200,
        served_by: result.provider,
        attempts: result.attempts,
        estimate,
        actual,
        remaining,
        ms: Date.now() - started,
      });
    } catch (err) {
      // Never charge a tenant for a request that did not complete.
      if (reservationId !== null && !settled) {
        limiter.release(reservationId);
      }
      if (res.headersSent) {
        res.destroy();
        return;
      }
      if (err instanceof GatewayError) {
        fail(err);
        return;
      }
      // Anything else is a bug or an unexpected runtime failure: the client
      // gets a generic 500, the real stack goes to the log under the request id.
      log("error", "internal_error", {
        request_id: requestId,
        tenant,
        message: err instanceof Error ? err.message : String(err),
        stack: err instanceof Error ? err.stack : undefined,
      });
      fail(new GatewayError("internal", 500, "Internal server error"));
    }
  };

  const server = createServer((req, res) => {
    handle(req, res).catch((err: unknown) => {
      log("error", "unhandled", { message: err instanceof Error ? err.message : String(err), stack: err instanceof Error ? err.stack : undefined });
      if (!res.headersSent) sendJson(res, 500, new GatewayError("internal", 500, "Internal server error").toPayload("unknown"), {});
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
        limiter,
        close: () =>
          new Promise<void>((done, failClose) => {
            server.closeAllConnections?.();
            server.close((err) => {
              limiter.close();
              if (err) failClose(err);
              else done();
            });
          }),
      });
    });
  });
}

// ---------------------------------------------------------------------------
// CLI entry
// ---------------------------------------------------------------------------
const isMain = process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isMain) {
  const env = process.env;
  const opts: RouterGatewayOptions = {
    port: Number(env["GATEWAY_PORT"] ?? DEFAULT_PORT),
    dbPath: env["DB_PATH"] ?? "task4-router/router.sqlite",
    tokensPerWindow: Number(env["TOKENS_PER_MINUTE"] ?? DEFAULT_TOKENS_PER_MINUTE),
    windowMs: Number(env["WINDOW_MS"] ?? DEFAULT_WINDOW_MS),
    primaryTimeoutMs: Number(env["PRIMARY_TIMEOUT_MS"] ?? DEFAULT_PRIMARY_TIMEOUT_MS),
    primaryUrl: env["PRIMARY_URL"] ?? `http://${DEFAULT_HOST}:${PRIMARY_PORT}`,
    secondaryUrl: env["SECONDARY_URL"] ?? `http://${DEFAULT_HOST}:${SECONDARY_PORT}`,
  };
  startRouterGateway(opts)
    .then((g) => defaultLog("info", "gateway_started", { url: g.url, ...opts }))
    .catch((err: unknown) => {
      defaultLog("error", "gateway_failed", { message: err instanceof Error ? err.message : String(err) });
      process.exit(1);
    });
}
