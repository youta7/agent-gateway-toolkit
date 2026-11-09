// Two mock OpenAI-style completion providers for the router.
//
// Primary (6101) and secondary (6102) both answer POST /v1/chat/completions
// with a non-streaming chat.completion body and tokenizer-like usage. Each
// also has a POST /__mode switch so tests can make it misbehave:
//   ok    -> 200 after ~200ms (so bursts overlap)
//   429   -> rate limited, with Retry-After
//   hang  -> never answers
//   slow  -> 200 after 1500ms
//   500   -> an internal error whose body leaks a fake Java stack trace and
//            an internal hostname, to prove the gateway sanitises it
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

export const DEFAULT_HOST = "127.0.0.1";
export const PRIMARY_PORT = 6101;
export const SECONDARY_PORT = 6102;

export type ProviderMode = "ok" | "429" | "hang" | "slow" | "500";
export const PROVIDER_MODES: readonly ProviderMode[] = ["ok", "429", "hang", "slow", "500"];

export const OK_LATENCY_MS = 200;
export const SLOW_LATENCY_MS = 1500;
export const MAX_COMPLETION_TOKENS = 20;

export interface MockProvider {
  name: string;
  server: Server;
  url: string;
  getMode: () => ProviderMode;
  setMode: (mode: ProviderMode) => void;
  /** Completed (answered) completion requests, by outcome. */
  stats: () => { completions: number; rejected: number; hung: number };
  close: () => Promise<void>;
}

/** Same arithmetic as the gateway's estimator, so settle() sees honest numbers. */
export function billPromptTokens(messages: unknown): number {
  if (!Array.isArray(messages)) return 0;
  let n = 0;
  for (const m of messages) {
    const content = m && typeof m === "object" ? (m as { content?: unknown }).content : undefined;
    const chars = typeof content === "string" ? content.length : 0;
    n += Math.ceil(chars / 4) + 4;
  }
  return n;
}

const FAKE_STACK = [
  "com.example.inference.ModelPoolExhaustedException: no healthy replicas for model gpt-mock",
  "\tat com.example.inference.Scheduler.acquire(Scheduler.java:214)",
  "\tat com.example.inference.CompletionHandler.handle(CompletionHandler.java:88)",
  "\tat io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:379)",
  "host: inference-primary-03.internal.corp.example",
].join("\n");

function sendJson(res: ServerResponse, status: number, payload: unknown, headers: Record<string, string> = {}): void {
  res.writeHead(status, { "content-type": "application/json", ...headers });
  res.end(JSON.stringify(payload));
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolvePromise, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => resolvePromise(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

export function startMockProvider(name: string, port: number, initialMode: ProviderMode = "ok", host = DEFAULT_HOST): Promise<MockProvider> {
  let mode: ProviderMode = initialMode;
  const stats = { completions: 0, rejected: 0, hung: 0 };

  const handle = async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    const url = req.url ?? "/";

    if (url === "/__mode") {
      if (req.method === "GET") {
        sendJson(res, 200, { provider: name, mode });
        return;
      }
      if (req.method === "POST") {
        let next: unknown;
        try {
          next = (JSON.parse(await readBody(req)) as { mode?: unknown }).mode;
        } catch {
          sendJson(res, 400, { error: "body must be JSON {mode}" });
          return;
        }
        if (!PROVIDER_MODES.includes(next as ProviderMode)) {
          sendJson(res, 400, { error: `mode must be one of ${PROVIDER_MODES.join("|")}` });
          return;
        }
        mode = next as ProviderMode;
        sendJson(res, 200, { provider: name, mode });
        return;
      }
    }

    if (url === "/__stats" && req.method === "GET") {
      sendJson(res, 200, { provider: name, mode, ...stats });
      return;
    }

    if (url !== "/v1/chat/completions" || req.method !== "POST") {
      req.resume();
      sendJson(res, 404, { error: { type: "not_found", message: `No route for ${req.method} ${url}` } });
      return;
    }

    const raw = await readBody(req);
    let body: { model?: unknown; messages?: unknown; max_tokens?: unknown };
    try {
      body = JSON.parse(raw) as typeof body;
    } catch {
      stats.rejected++;
      sendJson(res, 400, { error: { type: "invalid_request_error", message: "Body is not valid JSON" } });
      return;
    }

    // A model name starting with "bad-" is the provider's own 400: lets tests
    // prove the gateway does not fail over on a request-shaped error.
    if (typeof body.model === "string" && body.model.startsWith("bad-")) {
      stats.rejected++;
      sendJson(res, 400, { error: { type: "invalid_request_error", message: `Unknown model ${body.model}`, param: "model" } });
      return;
    }

    switch (mode) {
      case "429":
        stats.rejected++;
        sendJson(
          res,
          429,
          { error: { type: "rate_limit_error", message: `${name} is over capacity, retry later` } },
          { "retry-after": "1", "x-provider": name },
        );
        return;
      case "500":
        stats.rejected++;
        res.writeHead(500, { "content-type": "text/plain", "x-provider": name });
        res.end(`Internal Server Error\n${FAKE_STACK}\n`);
        return;
      case "hang":
        stats.hung++;
        // Never respond; just release the handle when the client gives up.
        res.on("close", () => undefined);
        return;
      case "slow":
      case "ok": {
        const latency = mode === "slow" ? SLOW_LATENCY_MS : OK_LATENCY_MS;
        const timer = setTimeout(() => {
          if (res.destroyed) return;
          const promptTokens = billPromptTokens(body.messages);
          const requested = typeof body.max_tokens === "number" && body.max_tokens >= 0 ? Math.floor(body.max_tokens) : MAX_COMPLETION_TOKENS;
          const completionTokens = Math.min(requested, MAX_COMPLETION_TOKENS);
          const content = Array.from({ length: completionTokens }, (_, i) => `tok${i + 1}`).join(" ");
          stats.completions++;
          sendJson(
            res,
            200,
            {
              id: `chatcmpl-${randomUUID().replace(/-/g, "").slice(0, 24)}`,
              object: "chat.completion",
              created: Math.floor(Date.now() / 1000),
              model: typeof body.model === "string" ? body.model : "gpt-mock",
              provider: name,
              choices: [{ index: 0, message: { role: "assistant", content }, finish_reason: completionTokens === requested ? "stop" : "length" }],
              usage: { prompt_tokens: promptTokens, completion_tokens: completionTokens, total_tokens: promptTokens + completionTokens },
            },
            { "x-provider": name },
          );
        }, latency);
        res.on("close", () => clearTimeout(timer));
        return;
      }
    }
  };

  const server = createServer((req, res) => {
    handle(req, res).catch((err: unknown) => {
      if (!res.headersSent) sendJson(res, 500, { error: { type: "internal", message: err instanceof Error ? err.message : String(err) } });
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
        name,
        server,
        url: `http://${host}:${actualPort}`,
        getMode: () => mode,
        setMode: (m) => {
          mode = m;
        },
        stats: () => ({ ...stats }),
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
  const primaryPort = Number(process.env["PRIMARY_PORT"] ?? PRIMARY_PORT);
  const secondaryPort = Number(process.env["SECONDARY_PORT"] ?? SECONDARY_PORT);
  Promise.all([startMockProvider("primary", primaryPort), startMockProvider("secondary", secondaryPort)])
    .then((providers) => {
      for (const p of providers) {
        process.stderr.write(JSON.stringify({ ts: new Date().toISOString(), level: "info", event: "provider_started", provider: p.name, url: p.url }) + "\n");
      }
    })
    .catch((err: unknown) => {
      process.stderr.write(JSON.stringify({ ts: new Date().toISOString(), level: "error", event: "provider_failed", message: err instanceof Error ? err.message : String(err) }) + "\n");
      process.exit(1);
    });
}
