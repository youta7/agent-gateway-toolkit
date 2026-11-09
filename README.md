# agent-gateway-toolkit

Four small, dependency-light TypeScript services that sit between an AI agent and the things it talks to: an MCP tool server, an authorizing MCP gateway, a streaming PII guardrail for LLM output, and a rate-limited model router with failover.

Node 22.5+ (built-in `node:sqlite` and `fetch`, no native modules), ESM, strict TypeScript, run with `tsx`, no build step.

## Install and run

```sh
npm install
npm run check-env     # confirms Node version, node:sqlite, fetch
npm run typecheck
npm run test:all      # runs the four suites in sequence
```

| Task | What it is | Run it | Test it |
|---|---|---|---|
| `task1-mcp-server` | MCP server over stdio with two tools (`get_customer_record`, `trigger_refund`) | `npm run t1:server` | `npm run t1:test` |
| `task2-mcp-gateway` | Authenticating, authorizing reverse proxy for JSON-RPC-over-HTTP MCP servers | `npm run t2:downstream` (port 4001), then `npm run t2:gateway` (port 4000) | `npm run t2:test` |
| `task3-llm-guardrail` | Streaming proxy for `/v1/chat/completions` that redacts emails, SSNs and card numbers from the SSE stream | `npm run t3:upstream` (5001), then `npm run t3:gateway` (5000) | `npm run t3:test` |
| `task4-router` | Per-tenant token rate limiter on SQLite in front of a primary/secondary model router | `npm run t4:providers` (6101, 6102), then `npm run t4:router` (6100) | `npm run t4:test` |

Each test suite spawns the processes it needs on its own ports, prints PASS/FAIL per check, and exits non-zero on any failure. Set `SHOW_SERVER_LOGS=1` to dump the spawned servers' stderr after a run.

Environment variables (all optional): task2 `GATEWAY_PORT`, `DOWNSTREAM_URL`, `GATEWAY_JWT_SECRET`, `FILTER_TOOL_LIST=1`; task3 `GUARDRAIL_PORT`, `UPSTREAM_URL`; task4 `GATEWAY_PORT`, `DB_PATH`, `TOKENS_PER_MINUTE`, `WINDOW_MS`, `PRIMARY_TIMEOUT_MS`, `PRIMARY_URL`, `SECONDARY_URL`.

## How the layers stack

```
                        tool calls (MCP, JSON-RPC)
  +-----------+   ------------------------------->   +------------------+     +-----------------+
  |           |                                      | task2 gateway    |     | task1 server    |
  |   agent   |                                      | authn (JWT/key)  | --> | tools, zod,     |
  |  runtime  |                                      | authz per tool   |     | real RPC errors |
  |           |                                      | batch all-or-none|     +-----------------+
  |           |                                      +------------------+
  |           |
  |           |        LLM calls (OpenAI-style)
  |           |   ------------------------------->   +------------------+     +------------------+     +-----------+
  |           |                                      | task3 guardrail  |     | task4 router     |     | provider  |
  |           |  <-- redacted SSE stream ----------  | cut-before-redact| --> | tenant limiter   | --> | primary   |
  +-----------+                                      | flush on finish  |     | (sqlite, exact)  |     +-----------+
                                                     +------------------+     | failover 429/    | --> | secondary |
                                                                              |   timeout/refused|     +-----------+
                                                                              +------------------+
```

The agent's tool traffic goes through the MCP gateway, which decides who may call what before anything reaches a tool server. The agent's model traffic goes through the guardrail, which scrubs what comes back, and the router, which decides which provider pays for the call and how much of the tenant's budget it burns. Every hop logs one JSON line per request to stderr keyed by `x-request-id`.

## Design decisions

### task1: low-level `Server`, not `McpServer`

The server registers `ListToolsRequestSchema` and `CallToolRequestSchema` handlers directly on the SDK's low-level `Server`. `McpServer` is friendlier, but it catches an `McpError` thrown inside a tool and downgrades it to a tool result with `isError: true`. That erases the distinction this server depends on:

- **Protocol errors** (unknown tool, arguments that fail the Zod schema) are a bug in how the caller invoked us. They go back as JSON-RPC errors with code `-32602` and the Zod issues in `error.data`, so a gateway or agent runtime can see exactly which field was wrong.
- **Domain failures** (customer not found, account suspended, refund over balance) are legitimate outcomes of a well-formed request. They go back as `CallToolResult` with `isError: true` so the model can read them and reason about what to do next.

Input and output schemas come from the same strict Zod objects via `zod-to-json-schema`, with the `$schema` key stripped. Money is integer cents. stdout is locked at startup: `console.*` is rebound to stderr and any non-JSON-RPC write to stdout is diverted with a warning, so nothing can corrupt the transport. A `transport.onerror` hook installed before `connect()` answers unparseable stdin lines with `-32700` and bad envelopes with `-32600`, which the SDK otherwise drops silently.

### task2: authorization inside the JSON-RPC envelope, batches are all-or-nothing

HTTP-level auth only knows the URL. The decision that matters is *which tool* is being called, and that lives in `params.name` inside the JSON-RPC body. The gateway therefore authenticates the Bearer token (static map or HS256 JWT verified with `node:crypto`, alg pinned, constant-time compare, `exp` honored), then parses every message and denies `admin_*` tools to non-admins with a `403` carrying JSON-RPC error `-32001` and the original request id. The downstream is never contacted for a denied call.

A batch is forwarded verbatim or rejected whole. Forwarding only the permitted members would mean splicing locally generated errors into a downstream response that may be an SSE stream we are piping through untouched; it would also change the meaning of a batch the client sent as a unit, and it would let a probing client hide one `admin_*` call among benign ones to get the rest through. Every member still gets a response so the batch contract holds: denied members get `-32001`, malformed ones `-32600`, innocent siblings `-32003`.

The forwarded request has the client's `Authorization` stripped and `x-request-id`, `x-forwarded-user`, `x-forwarded-role` added; `mcp-session-id` and `mcp-protocol-version` pass both ways. Responses are piped with `getReader()` so SSE stays live. `FILTER_TOOL_LIST=1` hides `admin_*` tools from non-admin `tools/list` results, but that is cosmetic; the `tools/call` check is the guard.

### task3: cut before redact, group-aligned Luhn

Tokens arrive in arbitrary slices, so `john.doe@example.com` may show up as `john.d`, `oe@exa`, `mple.com`. The `StreamingRedactor` holds back the smallest suffix that could still grow into a match (a run of email/number characters, extended backwards over `digits<space>` groups while the tail is numeric-ish), releases everything before it, and only then redacts the released head. Redacting the whole pending buffer first is wrong: `john@example.co` already matches (`.co` is a TLD), so the `m` arriving next would leak. The hold is capped at 256 characters so an endless token cannot stall the stream.

Card detection finds maximal digit runs with single space or dash separators, then tries windows that start and end on digit-group boundaries, 13 to 19 digits, longest first, requiring Luhn. Unaligned windows pass Luhn about one time in ten, so testing them would redact phone numbers, order ids and timestamps by accident. SSNs (rigid `3-2-4` with digit lookarounds) are removed before the card scan so a space-joined `ssn card` run cannot produce a cross-number window that passes Luhn by chance.

The gateway re-emits each `chat.completion.chunk` with only the released text, emits nothing for a frame whose text is entirely held, and on `finish_reason` or `[DONE]` flushes the tail as a synthetic content frame placed before the finish frame. Only the SSE line buffer and the redactor's tail are held per request; a 20000-word stream shows no RSS growth.

### task4: reserve / settle / release, failover only on availability signals

The limiter is an exact sliding window on `node:sqlite`: usage is `SUM(tokens)` over rows newer than `now - windowMs`. `reserve()` runs prune, sum, check and insert inside `BEGIN IMMEDIATE`; without the transaction, concurrent requests all read "0 used" and all pass. `settle()` replaces the estimate with the provider's `usage.total_tokens`, and `release()` drops the row if the request never completed, so a failed call never charges the tenant. A rejected request gets `retry_after_ms` computed by walking the tenant's rows oldest-first until enough tokens would have expired. Every method takes `now` so tests use a fake clock. The suite also runs eight processes against one database file and confirms exactly `limit / estimate` reservations are accepted.

The router fails over from primary to secondary only on HTTP 429, a timeout, or a connection failure, because those say "the primary cannot take this right now". A 400 would be rejected identically by the secondary and double the tenant's spend. A 401 is an operator misconfiguration that silent rerouting would hide. A 500 means the primary accepted the request and may already have billed it or caused side effects. Those come back sanitized (`bad_request` or `upstream_error`), with the raw upstream body logged under the request id and never relayed. Both providers failing yields `504 upstream_timeout` or `503 upstream_unavailable` by the last outcome, with an `attempts` list.

## Production gaps

- **Identity**: static keys and a shared HS256 secret. A real deployment needs OIDC with JWKS discovery and key rotation, `aud`/`iss` checks, and tenant mapping from claims rather than a map in code.
- **Rate limiting at scale**: the SQLite limiter is exact but single-node. Multi-node needs Redis with a Lua script that does the same check-and-insert atomically (sorted set keyed by tenant, `ZREMRANGEBYSCORE` + `ZADD` + sum), plus a per-tenant concurrency cap.
- **Observability**: logs are one JSON line per request. Add OpenTelemetry traces propagated on `x-request-id`/`traceparent` across gateway, router and provider, and metrics for TTFT, failover rate, redaction counts and limiter rejections.
- **Redaction coverage**: regex plus Luhn catches structured PII only. Names, addresses and free-text secrets need an NER stage (or a small classifier) with its own hold-back window, and the hold heuristics need per-language tuning.
- **Streaming end to end**: the router handles non-streaming completions; chaining guardrail -> router requires the router to pass SSE through with the same `getReader()` discipline the guardrail uses.
- **Hardening**: body size limits exist, but there is no TLS termination, no connection or per-IP limits, no circuit breaker that keeps the primary out of rotation after repeated failures, and the mock providers obviously stand in for real APIs with real credentials.
