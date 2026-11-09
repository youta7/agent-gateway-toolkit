// MCP gateway: authenticating, authorizing reverse proxy for a JSON-RPC-over-
// HTTP MCP server. node:http and fetch only.
//
// Request lifecycle for POST:
//   1. Authenticate the Bearer token              -> 401 / -32000 on failure
//   2. Read the body (1 MiB cap)                   -> 413 on overflow
//   3. Parse JSON                                  -> 400 / -32700 on failure
//   4. Inspect every message (single or batch)     -> 400 / -32600 if malformed
//                                                  -> 403 / -32001 if an admin_*
//                                                     tool is called by a non-admin
//   5. Forward the untouched body downstream with identity headers, pipe the
//      response back (JSON or SSE)                 -> 502 / -32002 if unreachable
//   6. Optionally strip admin_* tools from tools/list results for non-admins
//   7. Emit one audit line per request on stderr
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { resolvePrincipal, type Principal, type Role } from "./auth.js";

export const DEFAULT_HOST = "127.0.0.1";
export const DEFAULT_PORT = 4000;
export const DEFAULT_DOWNSTREAM_URL = "http://127.0.0.1:4001";
export const MAX_BODY_BYTES = 1024 * 1024;
export const UPSTREAM_TIMEOUT_MS = 10_000;

export const RPC = {
  PARSE_ERROR: -32700,
  INVALID_REQUEST: -32600,
  UNAUTHENTICATED: -32000,
  UNAUTHORIZED_TOOL: -32001,
  UPSTREAM_UNAVAILABLE: -32002,
  BATCH_REJECTED: -32003,
} as const;

const ADMIN_TOOL_PREFIX = "admin_";
const ADMIN_ROLE: Role = "admin";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
type JsonRpcId = string | number | null;

interface RpcError {
  jsonrpc: "2.0";
  id: JsonRpcId;
  error: { code: number; message: string; data?: unknown };
}

interface MessageInfo {
  id: JsonRpcId;
  hasId: boolean;
  method: string | null;
  tool: string | null;
}

type Inspection =
  | { kind: "ok"; info: MessageInfo }
  | { kind: "invalid"; info: MessageInfo; message: string }
  | { kind: "denied"; info: MessageInfo; tool: string; requiredRole: Role };

export interface GatewayOptions {
  port?: number;
  host?: string;
  downstreamUrl?: string;
  filterToolList?: boolean;
  upstreamTimeoutMs?: number;
  maxBodyBytes?: number;
}

export interface Gateway {
  server: Server;
  url: string;
  close: () => Promise<void>;
}

interface AuditRecord {
  request_id: string;
  http_method: string;
  path: string;
  principal: Principal | null;
  decision: string;
  http_status: number;
  started: number;
  messages?: MessageInfo[];
  denied?: Array<{ id: JsonRpcId; tool: string }>;
  invalid?: Array<{ id: JsonRpcId; reason: string }>;
  upstream_status?: number;
  upstream_content_type?: string | null;
  tools_filtered?: number;
  bytes_in?: number;
  error?: string;
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------
function rpcError(id: JsonRpcId, code: number, message: string, data?: unknown): RpcError {
  return { jsonrpc: "2.0", id, error: data === undefined ? { code, message } : { code, message, data } };
}

function sendJson(res: ServerResponse, status: number, payload: unknown, headers: Record<string, string> = {}): void {
  res.writeHead(status, { "content-type": "application/json", ...headers });
  res.end(JSON.stringify(payload));
}

function audit(record: AuditRecord): void {
  const { started, ...rest } = record;
  const level = record.http_status >= 500 ? "error" : record.http_status >= 400 ? "warn" : "info";
  const line = {
    ts: new Date().toISOString(),
    level,
    event: "audit",
    ...rest,
    principal: record.principal ? { sub: record.principal.sub, role: record.principal.role, via: record.principal.via } : null,
    duration_ms: Date.now() - started,
  };
  process.stderr.write(JSON.stringify(line) + "\n");
}

function headerString(req: IncomingMessage, name: string): string | undefined {
  const v = req.headers[name];
  if (Array.isArray(v)) return v[0];
  return v;
}

/** Extracts a JSON-RPC id if the value carries a spec-valid one; otherwise null. */
function readId(msg: Record<string, unknown>): { id: JsonRpcId; hasId: boolean } {
  if (!("id" in msg)) return { id: null, hasId: false };
  const id = msg["id"];
  if (typeof id === "string" || typeof id === "number" || id === null) return { id, hasId: true };
  return { id: null, hasId: true };
}

// ---------------------------------------------------------------------------
// Message inspection
// ---------------------------------------------------------------------------
function inspectMessage(raw: unknown, principal: Principal): Inspection {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return {
      kind: "invalid",
      info: { id: null, hasId: false, method: null, tool: null },
      message: "Invalid Request: message must be an object",
    };
  }
  const msg = raw as Record<string, unknown>;
  const { id, hasId } = readId(msg);
  const method = typeof msg["method"] === "string" ? msg["method"] : null;
  const info: MessageInfo = { id, hasId, method, tool: null };

  if (msg["jsonrpc"] !== "2.0") {
    return { kind: "invalid", info, message: 'Invalid Request: jsonrpc must be "2.0"' };
  }
  if (method === null) {
    return { kind: "invalid", info, message: "Invalid Request: method must be a string" };
  }

  if (method === "tools/call") {
    const params = msg["params"];
    const name =
      typeof params === "object" && params !== null && !Array.isArray(params)
        ? (params as Record<string, unknown>)["name"]
        : undefined;
    if (typeof name !== "string" || name.length === 0) {
      return { kind: "invalid", info, message: "Invalid Request: tools/call requires params.name" };
    }
    info.tool = name;
    if (name.startsWith(ADMIN_TOOL_PREFIX) && principal.role !== ADMIN_ROLE) {
      return { kind: "denied", info, tool: name, requiredRole: ADMIN_ROLE };
    }
  }

  return { kind: "ok", info };
}

function errorFor(inspection: Inspection, principal: Principal): RpcError {
  switch (inspection.kind) {
    case "invalid":
      return rpcError(inspection.info.id, RPC.INVALID_REQUEST, inspection.message);
    case "denied":
      return rpcError(inspection.info.id, RPC.UNAUTHORIZED_TOOL, "Unauthorized Tool Call", {
        tool: inspection.tool,
        required_role: inspection.requiredRole,
        role: principal.role,
      });
    case "ok":
      throw new Error("errorFor called on an ok inspection");
  }
}

// ---------------------------------------------------------------------------
// tools/list filtering
// ---------------------------------------------------------------------------
function stripAdminTools(message: unknown, toolListIds: Set<string>, counter: { removed: number }): unknown {
  if (typeof message !== "object" || message === null) return message;
  if (Array.isArray(message)) return message.map((m) => stripAdminTools(m, toolListIds, counter));
  const msg = message as Record<string, unknown>;
  const id = msg["id"];
  if ((typeof id !== "string" && typeof id !== "number") || !toolListIds.has(String(id))) return message;
  const result = msg["result"];
  if (typeof result !== "object" || result === null) return message;
  const tools = (result as Record<string, unknown>)["tools"];
  if (!Array.isArray(tools)) return message;
  const kept = tools.filter((t) => {
    const name = typeof t === "object" && t !== null ? (t as Record<string, unknown>)["name"] : undefined;
    const isAdmin = typeof name === "string" && name.startsWith(ADMIN_TOOL_PREFIX);
    if (isAdmin) counter.removed++;
    return !isAdmin;
  });
  return { ...msg, result: { ...(result as Record<string, unknown>), tools: kept } };
}

/** Rewrites the data: payload of each complete SSE event; returns [output, leftover]. */
function transformSseChunk(buffer: string, toolListIds: Set<string>, counter: { removed: number }): [string, string] {
  let out = "";
  let rest = buffer;
  for (;;) {
    const m = /\r?\n\r?\n/.exec(rest);
    if (!m) break;
    const event = rest.slice(0, m.index);
    const sep = m[0];
    rest = rest.slice(m.index + sep.length);
    out += transformSseEvent(event, toolListIds, counter) + sep;
  }
  return [out, rest];
}

function transformSseEvent(event: string, toolListIds: Set<string>, counter: { removed: number }): string {
  const lines = event.split(/\r?\n/);
  const dataLines = lines.filter((l) => l.startsWith("data:"));
  if (dataLines.length === 0) return event;
  const data = dataLines.map((l) => l.slice(5).replace(/^ /, "")).join("\n");
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    return event;
  }
  const rewritten = stripAdminTools(parsed, toolListIds, counter);
  const others = lines.filter((l) => !l.startsWith("data:"));
  return [...others, `data: ${JSON.stringify(rewritten)}`].join("\n");
}

// ---------------------------------------------------------------------------
// Gateway
// ---------------------------------------------------------------------------
export function startGateway(options: GatewayOptions = {}): Promise<Gateway> {
  const host = options.host ?? DEFAULT_HOST;
  const port = options.port ?? DEFAULT_PORT;
  const downstreamUrl = options.downstreamUrl ?? DEFAULT_DOWNSTREAM_URL;
  const filterToolList = options.filterToolList ?? false;
  const upstreamTimeoutMs = options.upstreamTimeoutMs ?? UPSTREAM_TIMEOUT_MS;
  const maxBodyBytes = options.maxBodyBytes ?? MAX_BODY_BYTES;

  const handle = async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    const requestId = headerString(req, "x-request-id") ?? randomUUID();
    const record: AuditRecord = {
      request_id: requestId,
      http_method: req.method ?? "?",
      path: req.url ?? "/",
      principal: null,
      decision: "pending",
      http_status: 0,
      started: Date.now(),
    };
    const baseHeaders = { "x-request-id": requestId };
    const finish = (decision: string, status: number): void => {
      record.decision = decision;
      record.http_status = status;
      audit(record);
    };

    // 1. Authentication (before touching the body: unauthenticated callers
    //    get no parsing work and no information about what we accept).
    const principal = resolvePrincipal(headerString(req, "authorization"));
    if (!principal) {
      req.resume(); // drain whatever they sent
      sendJson(res, 401, rpcError(null, RPC.UNAUTHENTICATED, "Unauthenticated"), {
        ...baseHeaders,
        "www-authenticate": 'Bearer realm="mcp-gateway", error="invalid_token"',
      });
      finish("unauthenticated", 401);
      return;
    }
    record.principal = principal;

    if (req.method === "GET" || req.method === "DELETE") {
      // Streamable HTTP uses GET to open a server-push SSE stream and DELETE
      // to end a session. Neither carries JSON-RPC to inspect; forward as-is.
      await forward(req, res, principal, requestId, null, new Set(), record, finish);
      return;
    }

    if (req.method !== "POST") {
      req.resume();
      sendJson(res, 405, rpcError(null, RPC.INVALID_REQUEST, "Method Not Allowed"), { ...baseHeaders, allow: "POST, GET, DELETE" });
      finish("method_not_allowed", 405);
      return;
    }

    // 2. Body with a hard cap.
    const rejectTooLarge = (bytes: number): void => {
      record.bytes_in = bytes;
      // Keep draining so the client reliably reads the 413 instead of hitting
      // a TCP reset from unread request data, but stop being generous past a
      // few multiples of the limit.
      drainThenClose(req, maxBodyBytes * 4);
      sendJson(res, 413, rpcError(null, RPC.INVALID_REQUEST, "Payload Too Large"), { ...baseHeaders, connection: "close" });
      finish("payload_too_large", 413);
    };

    const declared = Number(headerString(req, "content-length") ?? "0");
    if (Number.isFinite(declared) && declared > maxBodyBytes) {
      rejectTooLarge(declared);
      return;
    }

    let rawBody: string;
    try {
      rawBody = await readBody(req, maxBodyBytes);
    } catch (err) {
      if (err instanceof PayloadTooLarge) {
        rejectTooLarge(err.bytes);
        return;
      }
      record.error = err instanceof Error ? err.message : String(err);
      sendJson(res, 400, rpcError(null, RPC.INVALID_REQUEST, "Could not read request body"), baseHeaders);
      finish("body_read_error", 400);
      return;
    }
    record.bytes_in = Buffer.byteLength(rawBody);

    // 3. JSON.
    let body: unknown;
    try {
      body = JSON.parse(rawBody);
    } catch {
      sendJson(res, 400, rpcError(null, RPC.PARSE_ERROR, "Parse error"), baseHeaders);
      finish("parse_error", 400);
      return;
    }

    // 4. Inspect every message.
    const isBatch = Array.isArray(body);
    const items: unknown[] = isBatch ? (body as unknown[]) : [body];
    if (isBatch && items.length === 0) {
      sendJson(res, 400, rpcError(null, RPC.INVALID_REQUEST, "Invalid Request: empty batch"), baseHeaders);
      finish("invalid_request", 400);
      return;
    }

    const inspections = items.map((item) => inspectMessage(item, principal));
    record.messages = inspections.map((i) => i.info);
    const denied = inspections.filter((i): i is Extract<Inspection, { kind: "denied" }> => i.kind === "denied");
    const invalid = inspections.filter((i): i is Extract<Inspection, { kind: "invalid" }> => i.kind === "invalid");

    if (denied.length > 0 || invalid.length > 0) {
      record.denied = denied.map((d) => ({ id: d.info.id, tool: d.tool }));
      record.invalid = invalid.map((i) => ({ id: i.info.id, reason: i.message }));
      const status = denied.length > 0 ? 403 : 400;

      // Whole-batch rejection. A batch is forwarded verbatim or not at all:
      //   - Forwarding only the permitted members would mean splicing our
      //     locally generated errors into the downstream response, which may
      //     be an SSE stream we are piping through untouched.
      //   - The client sent the members together, often with ordering or
      //     transactional intent; silently executing a subset changes the
      //     meaning of what it asked for.
      //   - It denies a probing client any partial signal: an admin_* call
      //     cannot be hidden among benign requests to get the rest through.
      // Each member still receives a response so the batch contract holds:
      // denied -> -32001, malformed -> -32600, innocent siblings -> -32003.
      if (isBatch) {
        const payload = inspections.map((insp) => {
          if (insp.kind !== "ok") return errorFor(insp, principal);
          return rpcError(insp.info.id, RPC.BATCH_REJECTED, "Batch Rejected", {
            reason: "a sibling request in this batch was rejected",
            denied: denied.map((d) => d.tool),
            invalid: invalid.length,
          });
        });
        sendJson(res, status, payload, baseHeaders);
      } else {
        sendJson(res, status, errorFor(inspections[0] as Inspection, principal), baseHeaders);
      }
      finish(denied.length > 0 ? "denied" : "invalid_request", status);
      return;
    }

    // 5/6. Forward. Work out which response ids will need tools/list filtering.
    const toolListIds = new Set<string>();
    if (filterToolList && principal.role !== ADMIN_ROLE) {
      for (const insp of inspections) {
        if (insp.info.method === "tools/list" && insp.info.hasId && insp.info.id !== null) {
          toolListIds.add(String(insp.info.id));
        }
      }
    }
    await forward(req, res, principal, requestId, rawBody, toolListIds, record, finish);
  };

  const forward = async (
    req: IncomingMessage,
    res: ServerResponse,
    principal: Principal,
    requestId: string,
    rawBody: string | null,
    toolListIds: Set<string>,
    record: AuditRecord,
    finish: (decision: string, status: number) => void,
  ): Promise<void> => {
    // Outbound headers: identity is asserted by us, never relayed from the client.
    const headers = new Headers();
    headers.set("x-request-id", requestId);
    headers.set("x-forwarded-user", principal.sub);
    headers.set("x-forwarded-role", principal.role);
    headers.set("accept", headerString(req, "accept") ?? "application/json, text/event-stream");
    if (rawBody !== null) headers.set("content-type", "application/json");
    for (const name of ["mcp-session-id", "mcp-protocol-version", "last-event-id"]) {
      const v = headerString(req, name);
      if (v !== undefined) headers.set(name, v);
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error(`upstream did not respond within ${upstreamTimeoutMs}ms`)), upstreamTimeoutMs);
    res.on("close", () => {
      if (!res.writableFinished) controller.abort(new Error("client disconnected"));
    });

    const target = new URL(req.url ?? "/", downstreamUrl);
    let upstream: Response;
    try {
      upstream = await fetch(target, {
        method: req.method ?? "POST",
        headers,
        body: rawBody,
        signal: controller.signal,
      });
    } catch (err) {
      clearTimeout(timer);
      const reason = err instanceof Error ? (err.cause instanceof Error ? `${err.message}: ${err.cause.message}` : err.message) : String(err);
      record.error = reason;
      const ids = (record.messages ?? []).filter((m) => m.hasId).map((m) => m.id);
      const mk = (id: JsonRpcId) => rpcError(id, RPC.UPSTREAM_UNAVAILABLE, "Upstream Unavailable", { reason });
      const payload = Array.isArray(JSON.parse(rawBody ?? "null")) ? ids.map(mk) : mk(ids[0] ?? null);
      sendJson(res, 502, payload, { "x-request-id": requestId });
      finish("upstream_unavailable", 502);
      return;
    }
    clearTimeout(timer); // headers arrived; long-lived SSE bodies are allowed to stream

    record.upstream_status = upstream.status;
    record.upstream_content_type = upstream.headers.get("content-type");
    const outHeaders: Record<string, string> = { "x-request-id": requestId };
    for (const name of ["content-type", "mcp-session-id", "mcp-protocol-version", "cache-control"]) {
      const v = upstream.headers.get(name);
      if (v !== null) outHeaders[name] = v;
    }

    const contentType = upstream.headers.get("content-type") ?? "";
    const wantsFilter = toolListIds.size > 0 && upstream.status === 200;

    if (!upstream.body) {
      res.writeHead(upstream.status, outHeaders);
      res.end();
      finish("forwarded", upstream.status);
      return;
    }

    if (wantsFilter && contentType.includes("application/json")) {
      const counter = { removed: 0 };
      const text = await upstream.text();
      let payload: string = text;
      try {
        payload = JSON.stringify(stripAdminTools(JSON.parse(text), toolListIds, counter));
      } catch {
        // Not JSON after all; pass through untouched.
      }
      record.tools_filtered = counter.removed;
      res.writeHead(upstream.status, outHeaders);
      res.end(payload);
      finish("forwarded", upstream.status);
      return;
    }

    // Stream the body through. getReader() gives us each chunk as it lands,
    // which is what keeps SSE responsive instead of buffering to the end.
    res.writeHead(upstream.status, outHeaders);
    const reader = upstream.body.getReader();
    const sse = wantsFilter && contentType.includes("text/event-stream");
    const decoder = new TextDecoder();
    const counter = { removed: 0 };
    let sseBuffer = "";
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        let chunk: Uint8Array | string = value;
        if (sse) {
          sseBuffer += decoder.decode(value, { stream: true });
          const [out, rest] = transformSseChunk(sseBuffer, toolListIds, counter);
          sseBuffer = rest;
          chunk = out;
          if (chunk.length === 0) continue;
        }
        if (!res.write(chunk)) await once(res, "drain");
      }
      if (sse && sseBuffer.length > 0) res.write(sseBuffer);
      if (sse) record.tools_filtered = counter.removed;
      res.end();
      finish("forwarded", upstream.status);
    } catch (err) {
      record.error = err instanceof Error ? err.message : String(err);
      res.destroy();
      finish("stream_interrupted", upstream.status);
    }
  };

  const server = createServer((req, res) => {
    handle(req, res).catch((err: unknown) => {
      const message = err instanceof Error ? err.message : String(err);
      process.stderr.write(JSON.stringify({ ts: new Date().toISOString(), level: "error", event: "gateway_unhandled", message }) + "\n");
      if (!res.headersSent) sendJson(res, 500, rpcError(null, -32603, "Internal error"));
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

class PayloadTooLarge extends Error {
  constructor(readonly bytes: number) {
    super("Payload Too Large");
  }
}

/** Discards the rest of a request body, destroying the socket if it exceeds `cap` bytes. */
function drainThenClose(req: IncomingMessage, cap: number): void {
  let seen = 0;
  req.removeAllListeners("data");
  req.removeAllListeners("end");
  req.on("data", (c: Buffer) => {
    seen += c.length;
    if (seen > cap) req.destroy();
  });
  req.on("error", () => {
    /* client went away; nothing to do */
  });
  req.resume();
}

function readBody(req: IncomingMessage, limit: number): Promise<string> {
  return new Promise((resolvePromise, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    req.on("data", (c: Buffer) => {
      total += c.length;
      if (total > limit) {
        reject(new PayloadTooLarge(total));
        req.removeAllListeners("data");
        req.removeAllListeners("end");
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
  startGateway({
    port: Number(process.env["GATEWAY_PORT"] ?? DEFAULT_PORT),
    downstreamUrl: process.env["DOWNSTREAM_URL"] ?? DEFAULT_DOWNSTREAM_URL,
    filterToolList: process.env["FILTER_TOOL_LIST"] === "1",
  })
    .then((g) => {
      process.stderr.write(
        JSON.stringify({
          ts: new Date().toISOString(),
          level: "info",
          event: "gateway_started",
          url: g.url,
          downstream: process.env["DOWNSTREAM_URL"] ?? DEFAULT_DOWNSTREAM_URL,
          filter_tool_list: process.env["FILTER_TOOL_LIST"] === "1",
        }) + "\n",
      );
    })
    .catch((err: unknown) => {
      process.stderr.write(
        JSON.stringify({ ts: new Date().toISOString(), level: "error", event: "gateway_failed", message: err instanceof Error ? err.message : String(err) }) + "\n",
      );
      process.exit(1);
    });
}
