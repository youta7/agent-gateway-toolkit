// Mock downstream MCP server: plain JSON-RPC 2.0 over HTTP POST.
//
// Intentionally minimal and trusting. It has no auth of its own; the gateway
// in front of it is responsible for deciding who may call which tool. It
// records every request it receives so tests can assert what the gateway
// forwarded (headers, bodies) via the __debug/received method.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

export const DEFAULT_HOST = "127.0.0.1";
export const DEFAULT_PORT = 4001;
const PROTOCOL_VERSION = "2025-06-18";

// ---------------------------------------------------------------------------
// JSON-RPC shapes (kept local; this file deliberately has no SDK dependency so
// it behaves like an arbitrary third-party backend)
// ---------------------------------------------------------------------------
type JsonRpcId = string | number | null;

interface JsonRpcRequest {
  jsonrpc: "2.0";
  id?: JsonRpcId;
  method: string;
  params?: unknown;
}

interface JsonRpcResponse {
  jsonrpc: "2.0";
  id: JsonRpcId;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
}

export interface RecordedRequest {
  seq: number;
  at: string;
  sessionId: string | null;
  headers: Record<string, string>;
  body: unknown;
}

const ERR = {
  PARSE: -32700,
  INVALID_REQUEST: -32600,
  METHOD_NOT_FOUND: -32601,
  INVALID_PARAMS: -32602,
  INTERNAL: -32603,
} as const;

function ok(id: JsonRpcId, result: unknown): JsonRpcResponse {
  return { jsonrpc: "2.0", id, result };
}

function fail(id: JsonRpcId, code: number, message: string, data?: unknown): JsonRpcResponse {
  return { jsonrpc: "2.0", id, error: data === undefined ? { code, message } : { code, message, data } };
}

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------
interface ToolDef {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
  handler: (args: Record<string, unknown>) => Record<string, unknown>;
}

function requireString(args: Record<string, unknown>, key: string): string {
  const v = args[key];
  if (typeof v !== "string" || v.length === 0) {
    throw new ToolArgError(`${key} must be a non-empty string`);
  }
  return v;
}

class ToolArgError extends Error {}

const TOOLS: ToolDef[] = [
  {
    name: "get_weather",
    description: "Current weather for a city (fake data).",
    inputSchema: {
      type: "object",
      properties: { city: { type: "string" } },
      required: ["city"],
      additionalProperties: false,
    },
    handler: (args) => {
      const city = requireString(args, "city");
      return { city, temperature_c: 21, condition: "partly cloudy", observed_at: new Date().toISOString() };
    },
  },
  {
    name: "search_docs",
    description: "Full-text search over documentation (fake data).",
    inputSchema: {
      type: "object",
      properties: { query: { type: "string" }, limit: { type: "integer", minimum: 1, maximum: 20 } },
      required: ["query"],
      additionalProperties: false,
    },
    handler: (args) => {
      const query = requireString(args, "query");
      const limit = typeof args["limit"] === "number" ? Math.max(1, Math.min(20, Math.trunc(args["limit"]))) : 3;
      const hits = Array.from({ length: limit }, (_, i) => ({
        id: `doc-${i + 1}`,
        title: `Result ${i + 1} for "${query}"`,
        url: `https://docs.example.invalid/${encodeURIComponent(query)}/${i + 1}`,
      }));
      return { query, hits };
    },
  },
  {
    name: "admin_reset_key",
    description: "Rotate a user's API key. Admin only (enforced upstream, not here).",
    inputSchema: {
      type: "object",
      properties: { user_id: { type: "string" } },
      required: ["user_id"],
      additionalProperties: false,
    },
    handler: (args) => {
      const userId = requireString(args, "user_id");
      return { user_id: userId, new_key: `key_${randomUUID().replace(/-/g, "")}`, rotated_at: new Date().toISOString() };
    },
  },
  {
    name: "admin_delete_user",
    description: "Permanently delete a user. Admin only (enforced upstream, not here).",
    inputSchema: {
      type: "object",
      properties: { user_id: { type: "string" } },
      required: ["user_id"],
      additionalProperties: false,
    },
    handler: (args) => {
      const userId = requireString(args, "user_id");
      return { user_id: userId, deleted: true, deleted_at: new Date().toISOString() };
    },
  },
];

const toolsByName = new Map(TOOLS.map((t) => [t.name, t]));

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------
export interface MockDownstream {
  server: Server;
  url: string;
  received: RecordedRequest[];
  close: () => Promise<void>;
}

export function startMockDownstream(port = DEFAULT_PORT, host = DEFAULT_HOST): Promise<MockDownstream> {
  const received: RecordedRequest[] = [];
  let seq = 0;

  const dispatch = (req: JsonRpcRequest, session: { id: string | null }): JsonRpcResponse | null => {
    const id = req.id ?? null;
    const isNotification = req.id === undefined;

    switch (req.method) {
      case "initialize": {
        session.id = randomUUID();
        return ok(id, {
          protocolVersion: PROTOCOL_VERSION,
          capabilities: { tools: {} },
          serverInfo: { name: "mock-downstream", version: "0.1.0" },
        });
      }
      case "notifications/initialized":
      case "notifications/cancelled":
      case "notifications/progress":
        return null;
      case "ping":
        return ok(id, {});
      case "tools/list":
        return ok(id, {
          tools: TOOLS.map(({ name, description, inputSchema }) => ({ name, description, inputSchema })),
        });
      case "tools/call": {
        const params = (req.params ?? {}) as { name?: unknown; arguments?: unknown };
        if (typeof params.name !== "string") {
          return fail(id, ERR.INVALID_PARAMS, "params.name must be a string");
        }
        const tool = toolsByName.get(params.name);
        if (!tool) {
          return fail(id, ERR.INVALID_PARAMS, `Unknown tool: ${params.name}`);
        }
        const args = params.arguments;
        if (args !== undefined && (typeof args !== "object" || args === null || Array.isArray(args))) {
          return fail(id, ERR.INVALID_PARAMS, "params.arguments must be an object");
        }
        try {
          const structured = tool.handler((args ?? {}) as Record<string, unknown>);
          return ok(id, {
            content: [{ type: "text", text: JSON.stringify(structured) }],
            structuredContent: structured,
          });
        } catch (err) {
          if (err instanceof ToolArgError) {
            return fail(id, ERR.INVALID_PARAMS, err.message, { tool: tool.name });
          }
          return fail(id, ERR.INTERNAL, err instanceof Error ? err.message : String(err));
        }
      }
      case "__debug/received":
        return ok(id, { count: received.length, received });
      case "__debug/reset": {
        const cleared = received.length;
        received.length = 0;
        return ok(id, { cleared });
      }
      default:
        if (isNotification) return null;
        return fail(id, ERR.METHOD_NOT_FOUND, `Method not found: ${req.method}`);
    }
  };

  const isRequestShaped = (v: unknown): v is JsonRpcRequest =>
    typeof v === "object" &&
    v !== null &&
    !Array.isArray(v) &&
    (v as { jsonrpc?: unknown }).jsonrpc === "2.0" &&
    typeof (v as { method?: unknown }).method === "string";

  const handle = (req: IncomingMessage, res: ServerResponse): void => {
    if (req.method !== "POST") {
      res.writeHead(405, { allow: "POST", "content-type": "application/json" });
      res.end(JSON.stringify(fail(null, ERR.INVALID_REQUEST, "Only POST is supported")));
      return;
    }

    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      const rawBody = Buffer.concat(chunks).toString("utf8");
      const headers: Record<string, string> = {};
      for (const [k, v] of Object.entries(req.headers)) {
        if (typeof v === "string") headers[k] = v;
        else if (Array.isArray(v)) headers[k] = v.join(", ");
      }
      const incomingSession = headers["mcp-session-id"] ?? null;
      const session = { id: incomingSession };

      let body: unknown;
      try {
        body = JSON.parse(rawBody);
      } catch {
        received.push({ seq: ++seq, at: new Date().toISOString(), sessionId: incomingSession, headers, body: rawBody });
        respond(res, 400, session.id, fail(null, ERR.PARSE, "Parse error"));
        return;
      }

      // Debug introspection is not itself recorded, so the ledger reflects
      // only traffic a gateway forwarded.
      const isDebug = isRequestShaped(body) && body.method.startsWith("__debug/");
      if (!isDebug) {
        received.push({ seq: ++seq, at: new Date().toISOString(), sessionId: incomingSession, headers, body });
      }

      if (Array.isArray(body)) {
        if (body.length === 0) {
          respond(res, 400, session.id, fail(null, ERR.INVALID_REQUEST, "Empty batch"));
          return;
        }
        const responses: JsonRpcResponse[] = [];
        for (const item of body) {
          if (!isRequestShaped(item)) {
            responses.push(fail(null, ERR.INVALID_REQUEST, "Invalid Request"));
            continue;
          }
          const r = dispatch(item, session);
          if (r) responses.push(r);
        }
        if (responses.length === 0) {
          // Batch of notifications only.
          respond(res, 202, session.id, null);
          return;
        }
        respond(res, 200, session.id, responses);
        return;
      }

      if (!isRequestShaped(body)) {
        respond(res, 400, session.id, fail(null, ERR.INVALID_REQUEST, "Invalid Request"));
        return;
      }

      const response = dispatch(body, session);
      if (response === null) {
        respond(res, 202, session.id, null);
        return;
      }
      respond(res, 200, session.id, response);
    });
  };

  const respond = (res: ServerResponse, status: number, sessionId: string | null, payload: unknown): void => {
    const headers: Record<string, string> = {};
    if (sessionId) headers["mcp-session-id"] = sessionId;
    if (payload === null) {
      res.writeHead(status, headers);
      res.end();
      return;
    }
    headers["content-type"] = "application/json";
    res.writeHead(status, headers);
    res.end(JSON.stringify(payload));
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
        received,
        close: () =>
          new Promise<void>((done, failClose) => {
            server.closeAllConnections?.();
            server.close((err) => (err ? failClose(err) : done()));
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
  const port = Number(process.env["DOWNSTREAM_PORT"] ?? DEFAULT_PORT);
  startMockDownstream(port)
    .then((d) => {
      process.stderr.write(
        JSON.stringify({ ts: new Date().toISOString(), level: "info", event: "downstream_started", url: d.url }) + "\n",
      );
    })
    .catch((err: unknown) => {
      process.stderr.write(
        JSON.stringify({
          ts: new Date().toISOString(),
          level: "error",
          event: "downstream_failed",
          message: err instanceof Error ? err.message : String(err),
        }) + "\n",
      );
      process.exit(1);
    });
}
