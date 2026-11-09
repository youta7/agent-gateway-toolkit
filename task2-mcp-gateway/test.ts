// Integration test for task2-mcp-gateway.
//
// Spawns mock-downstream.ts and gateway.ts as real child processes (via tsx),
// waits for their ports, and drives the gateway over HTTP exactly as a client
// would. Phase 1 runs with the default configuration; phase 2 restarts the
// gateway with FILTER_TOOL_LIST=1.
//
// Prints PASS/FAIL per check and exits non-zero if anything fails.
import assert from "node:assert/strict";
import { spawn, type ChildProcessByStdio } from "node:child_process";
import { once } from "node:events";
import { connect } from "node:net";
import type { Readable } from "node:stream";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { signJwt } from "./auth.js";

const here = dirname(fileURLToPath(import.meta.url));
const tsxCli = join(here, "..", "node_modules", "tsx", "dist", "cli.mjs");
const HOST = "127.0.0.1";
const DOWNSTREAM_PORT = Number(process.env["TEST_DOWNSTREAM_PORT"] ?? 4101);
const GATEWAY_PORT = Number(process.env["TEST_GATEWAY_PORT"] ?? 4100);
const DOWNSTREAM_URL = `http://${HOST}:${DOWNSTREAM_PORT}`;
const GATEWAY_URL = `http://${HOST}:${GATEWAY_PORT}`;
const PORT_WAIT_MS = 20_000;

const ADMIN = { authorization: "Bearer tok-admin-1" };
const VIEWER = { authorization: "Bearer tok-viewer-1" };

// ---------------------------------------------------------------------------
// Process management
// ---------------------------------------------------------------------------
interface Managed {
  name: string;
  child: ChildProcessByStdio<null, Readable, Readable>;
  stderr: string[];
}

function spawnTsx(name: string, script: string, env: Record<string, string>): Managed {
  const child = spawn(process.execPath, [tsxCli, join(here, script)], {
    env: { ...process.env, ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const managed: Managed = { name, child, stderr: [] };
  child.stderr.on("data", (c: Buffer) => managed.stderr.push(c.toString()));
  child.stdout.on("data", (c: Buffer) => managed.stderr.push(`[stdout] ${c.toString()}`));
  return managed;
}

/** Resolves once something accepts TCP connections on host:port, or rejects on timeout / child exit. */
function waitForPort(port: number, managed: Managed): Promise<void> {
  const deadline = Date.now() + PORT_WAIT_MS;
  return new Promise((resolve, reject) => {
    let settled = false;
    const onExit = (code: number | null) => {
      if (settled) return;
      settled = true;
      reject(new Error(`${managed.name} exited with code ${code} before listening:\n${managed.stderr.join("")}`));
    };
    managed.child.once("exit", onExit);
    const attempt = () => {
      if (settled) return;
      if (Date.now() > deadline) {
        settled = true;
        managed.child.off("exit", onExit);
        reject(new Error(`${managed.name} did not open port ${port} within ${PORT_WAIT_MS}ms:\n${managed.stderr.join("")}`));
        return;
      }
      const sock = connect({ host: HOST, port });
      sock.once("connect", () => {
        sock.destroy();
        settled = true;
        managed.child.off("exit", onExit);
        resolve();
      });
      sock.once("error", () => {
        sock.destroy();
        setTimeout(attempt, 100);
      });
    };
    attempt();
  });
}

/** Kills the child and waits for its exit event so the port is actually free before reuse. */
async function stop(managed: Managed | null): Promise<void> {
  if (!managed) return;
  if (managed.child.exitCode !== null || managed.child.signalCode !== null) return;
  const exited = once(managed.child, "exit");
  managed.child.kill();
  await exited;
}

/** Wait until nothing is listening on the port (belt and braces after exit). */
async function waitForPortFree(port: number): Promise<void> {
  const deadline = Date.now() + PORT_WAIT_MS;
  while (Date.now() < deadline) {
    const open = await new Promise<boolean>((resolve) => {
      const sock = connect({ host: HOST, port });
      sock.once("connect", () => {
        sock.destroy();
        resolve(true);
      });
      sock.once("error", () => {
        sock.destroy();
        resolve(false);
      });
    });
    if (!open) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`port ${port} still in use after ${PORT_WAIT_MS}ms`);
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------
interface HttpResult {
  status: number;
  headers: Headers;
  text: string;
  json: unknown;
}

async function post(url: string, body: unknown, headers: Record<string, string> = {}): Promise<HttpResult> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
  const text = await res.text();
  let json: unknown = null;
  if (text) {
    try {
      json = JSON.parse(text);
    } catch {
      json = null;
    }
  }
  return { status: res.status, headers: res.headers, text, json };
}

interface RpcFrame {
  jsonrpc?: string;
  id?: unknown;
  result?: Record<string, unknown>;
  error?: { code: number; message: string; data?: unknown };
}

function frame(r: HttpResult, label: string): RpcFrame {
  assert.ok(r.json && typeof r.json === "object" && !Array.isArray(r.json), `${label}: expected a single JSON object, got ${r.text}`);
  return r.json as RpcFrame;
}

function frames(r: HttpResult, label: string): RpcFrame[] {
  assert.ok(Array.isArray(r.json), `${label}: expected a JSON array, got ${r.text}`);
  return r.json as RpcFrame[];
}

function expectRpcError(r: HttpResult, status: number, code: number, label: string): RpcFrame {
  assert.equal(r.status, status, `${label}: HTTP ${r.status}, body ${r.text}`);
  const f = frame(r, label);
  assert.ok(f.error, `${label}: expected error frame, got ${r.text}`);
  assert.equal(f.error.code, code, `${label}: expected code ${code}, got ${f.error.code} (${f.error.message})`);
  return f;
}

const callTool = (url: string, id: unknown, name: string, args: Record<string, unknown>, headers: Record<string, string>) =>
  post(url, { jsonrpc: "2.0", id, method: "tools/call", params: { name, arguments: args } }, headers);

interface RecordedRequest {
  seq: number;
  headers: Record<string, string>;
  body: unknown;
}

async function downstreamLedger(): Promise<RecordedRequest[]> {
  const r = await post(DOWNSTREAM_URL, { jsonrpc: "2.0", id: "dbg", method: "__debug/received" });
  return (frame(r, "__debug/received").result as { received: RecordedRequest[] }).received;
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------
interface Check {
  name: string;
  run: () => Promise<void>;
}

const results: Array<{ name: string; ok: boolean; detail?: string }> = [];

async function runChecks(label: string, checks: Check[]): Promise<void> {
  console.log(`\n== ${label} ==`);
  for (const check of checks) {
    try {
      await check.run();
      results.push({ name: check.name, ok: true });
      console.log(`PASS  ${check.name}`);
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      results.push({ name: check.name, ok: false, detail });
      console.log(`FAIL  ${check.name}`);
      console.log(`      ${detail.split("\n").join("\n      ")}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Phase 1 checks (default gateway config)
// ---------------------------------------------------------------------------
function phase1(): Check[] {
  const now = Math.floor(Date.now() / 1000);
  const jwtAdmin = signJwt({ sub: "jwt-admin", role: "admin", exp: now + 300 });
  const jwtViewer = signJwt({ sub: "jwt-viewer", role: "viewer", exp: now + 300 });
  const jwtWrongSecret = signJwt({ sub: "evil", role: "admin", exp: now + 300 }, "not-the-gateway-secret");
  const jwtExpired = signJwt({ sub: "late", role: "admin", exp: now - 3600 });

  return [
    {
      name: "no token -> 401 + -32000 Unauthenticated + WWW-Authenticate",
      run: async () => {
        const r = await post(GATEWAY_URL, { jsonrpc: "2.0", id: 1, method: "tools/list" });
        const f = expectRpcError(r, 401, -32000, "no token");
        assert.equal(f.error?.message, "Unauthenticated");
        assert.match(r.headers.get("www-authenticate") ?? "", /^Bearer\b/);
      },
    },
    {
      name: "bad token -> 401",
      run: async () => {
        expectRpcError(await post(GATEWAY_URL, { jsonrpc: "2.0", id: 1, method: "tools/list" }, { authorization: "Bearer tok-nobody" }), 401, -32000, "bad token");
        expectRpcError(await post(GATEWAY_URL, { jsonrpc: "2.0", id: 1, method: "tools/list" }, { authorization: "Basic dG9rLWFkbWluLTE=" }), 401, -32000, "basic scheme");
      },
    },
    {
      name: "JWT signed with wrong secret -> 401",
      run: async () => {
        expectRpcError(await post(GATEWAY_URL, { jsonrpc: "2.0", id: 1, method: "tools/list" }, { authorization: `Bearer ${jwtWrongSecret}` }), 401, -32000, "wrong secret");
      },
    },
    {
      name: "expired JWT -> 401",
      run: async () => {
        expectRpcError(await post(GATEWAY_URL, { jsonrpc: "2.0", id: 1, method: "tools/list" }, { authorization: `Bearer ${jwtExpired}` }), 401, -32000, "expired");
      },
    },
    {
      name: "viewer tools/list sees all 4 tools (filter off by default)",
      run: async () => {
        const r = await post(GATEWAY_URL, { jsonrpc: "2.0", id: "tl", method: "tools/list" }, VIEWER);
        assert.equal(r.status, 200, r.text);
        const tools = (frame(r, "tools/list").result as { tools: Array<{ name: string }> }).tools;
        assert.deepEqual(
          tools.map((t) => t.name).sort(),
          ["admin_delete_user", "admin_reset_key", "get_weather", "search_docs"],
        );
      },
    },
    {
      name: "mcp-session-id from initialize is passed back, and a client-supplied one is relayed both ways",
      run: async () => {
        const init = await post(GATEWAY_URL, { jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "t", version: "0" } } }, VIEWER);
        assert.equal(init.status, 200, init.text);
        const session = init.headers.get("mcp-session-id");
        assert.ok(session, "initialize response lacks mcp-session-id");

        const ping = await post(GATEWAY_URL, { jsonrpc: "2.0", id: 2, method: "ping" }, { ...VIEWER, "mcp-session-id": session, "mcp-protocol-version": "2025-06-18" });
        assert.equal(ping.status, 200, ping.text);
        assert.equal(ping.headers.get("mcp-session-id"), session, "session id not echoed back through gateway");
        const last = (await downstreamLedger()).at(-1);
        assert.equal(last?.headers["mcp-session-id"], session, "session id not forwarded downstream");
        assert.equal(last?.headers["mcp-protocol-version"], "2025-06-18", "protocol version not forwarded downstream");
      },
    },
    {
      name: "viewer get_weather -> 200 with structuredContent",
      run: async () => {
        const r = await callTool(GATEWAY_URL, "w1", "get_weather", { city: "Reykjavik" }, VIEWER);
        assert.equal(r.status, 200, r.text);
        const f = frame(r, "get_weather");
        assert.equal(f.id, "w1");
        assert.equal((f.result?.["structuredContent"] as { city: string }).city, "Reykjavik");
      },
    },
    {
      name: "viewer admin_reset_key -> 403 + -32001 'Unauthorized Tool Call', id preserved, data filled",
      run: async () => {
        const r = await callTool(GATEWAY_URL, "deny-me", "admin_reset_key", { user_id: "u1" }, VIEWER);
        const f = expectRpcError(r, 403, -32001, "viewer admin call");
        assert.equal(f.error?.message, "Unauthorized Tool Call");
        assert.equal(f.id, "deny-me");
        assert.deepEqual(f.error?.data, { tool: "admin_reset_key", required_role: "admin", role: "viewer" });
      },
    },
    {
      name: "admin static token admin_reset_key -> forwarded, 200, identity headers set, Authorization stripped",
      run: async () => {
        const r = await callTool(GATEWAY_URL, "a1", "admin_reset_key", { user_id: "u1" }, ADMIN);
        assert.equal(r.status, 200, r.text);
        const f = frame(r, "admin static");
        assert.match(String((f.result?.["structuredContent"] as { new_key: string }).new_key), /^key_/);
        const last = (await downstreamLedger()).at(-1);
        assert.equal((last?.body as { params: { name: string } }).params.name, "admin_reset_key");
        assert.equal(last?.headers["x-forwarded-user"], "admin-1");
        assert.equal(last?.headers["x-forwarded-role"], "admin");
        assert.ok(last?.headers["x-request-id"], "x-request-id missing downstream");
        assert.equal(last?.headers["authorization"], undefined, "client Authorization leaked downstream");
      },
    },
    {
      name: "JWT admin admin_delete_user -> forwarded with x-forwarded-user from the JWT sub",
      run: async () => {
        const r = await callTool(GATEWAY_URL, "a2", "admin_delete_user", { user_id: "u2" }, { authorization: `Bearer ${jwtAdmin}` });
        assert.equal(r.status, 200, r.text);
        assert.equal((frame(r, "jwt admin").result?.["structuredContent"] as { deleted: boolean }).deleted, true);
        const last = (await downstreamLedger()).at(-1);
        assert.equal(last?.headers["x-forwarded-user"], "jwt-admin");
        assert.equal(last?.headers["x-forwarded-role"], "admin");
      },
    },
    {
      name: "JWT viewer admin_delete_user -> 403 + -32001",
      run: async () => {
        const f = expectRpcError(
          await callTool(GATEWAY_URL, "v2", "admin_delete_user", { user_id: "u2" }, { authorization: `Bearer ${jwtViewer}` }),
          403,
          -32001,
          "jwt viewer",
        );
        assert.equal(f.id, "v2");
      },
    },
    {
      name: "malformed JSON -> 400 + -32700",
      run: async () => {
        const f = expectRpcError(await post(GATEWAY_URL, '{"jsonrpc":"2.0",', ADMIN), 400, -32700, "malformed json");
        assert.equal(f.id, null);
      },
    },
    {
      name: "tools/call without params.name -> 400 + -32600, id preserved",
      run: async () => {
        const f = expectRpcError(await post(GATEWAY_URL, { jsonrpc: "2.0", id: 44, method: "tools/call", params: { arguments: { city: "x" } } }, ADMIN), 400, -32600, "no name");
        assert.equal(f.id, 44);
        expectRpcError(await post(GATEWAY_URL, { jsonrpc: "2.0", id: 45, method: "tools/call" }, ADMIN), 400, -32600, "no params");
      },
    },
    {
      name: "missing jsonrpc -> 400 + -32600",
      run: async () => {
        const f = expectRpcError(await post(GATEWAY_URL, { id: 46, method: "ping" }, ADMIN), 400, -32600, "missing jsonrpc");
        assert.equal(f.id, 46);
        expectRpcError(await post(GATEWAY_URL, { jsonrpc: "1.0", id: 47, method: "ping" }, ADMIN), 400, -32600, "jsonrpc 1.0");
      },
    },
    {
      name: "'ADMIN_reset_key' from viewer passes the case-sensitive prefix check and downstream answers -32602",
      run: async () => {
        const before = (await downstreamLedger()).length;
        const r = await callTool(GATEWAY_URL, "case", "ADMIN_reset_key", { user_id: "u" }, VIEWER);
        // Gateway did not deny it (that would be 403); downstream has no such tool.
        const f = expectRpcError(r, 200, -32602, "ADMIN_reset_key");
        assert.equal(f.id, "case");
        assert.equal((await downstreamLedger()).length, before + 1, "request should have reached downstream");
      },
    },
    {
      name: "batch with one admin call from viewer -> whole batch 403, denied id carries -32001, nothing forwarded",
      run: async () => {
        const before = (await downstreamLedger()).length;
        const r = await post(
          GATEWAY_URL,
          [
            { jsonrpc: "2.0", id: "b1", method: "tools/call", params: { name: "get_weather", arguments: { city: "x" } } },
            { jsonrpc: "2.0", id: "b2", method: "tools/call", params: { name: "admin_reset_key", arguments: { user_id: "u" } } },
            { jsonrpc: "2.0", id: "b3", method: "tools/list" },
          ],
          VIEWER,
        );
        assert.equal(r.status, 403, r.text);
        const arr = frames(r, "batch");
        assert.equal(arr.length, 3, "every batch member must get a response");
        const denied = arr.find((f) => f.id === "b2");
        assert.equal(denied?.error?.code, -32001);
        assert.equal(denied?.error?.message, "Unauthorized Tool Call");
        for (const sibling of arr.filter((f) => f.id !== "b2")) {
          assert.ok(sibling.error, `sibling ${String(sibling.id)} should be rejected too`);
          assert.notEqual(sibling.error?.code, -32001, "sibling must not be blamed as unauthorized");
        }
        assert.equal((await downstreamLedger()).length, before, "denied batch must not reach downstream");
      },
    },
    {
      name: "clean batch -> 200 array with one response per request",
      run: async () => {
        const r = await post(
          GATEWAY_URL,
          [
            { jsonrpc: "2.0", id: "c1", method: "ping" },
            { jsonrpc: "2.0", id: "c2", method: "tools/call", params: { name: "search_docs", arguments: { query: "gateway", limit: 2 } } },
          ],
          VIEWER,
        );
        assert.equal(r.status, 200, r.text);
        const arr = frames(r, "clean batch");
        assert.deepEqual(arr.map((f) => f.id), ["c1", "c2"]);
        assert.equal(((arr[1]?.result?.["structuredContent"] as { hits: unknown[] }).hits).length, 2);
      },
    },
    {
      name: "notification -> 202 with empty body",
      run: async () => {
        const r = await post(GATEWAY_URL, { jsonrpc: "2.0", method: "notifications/initialized" }, VIEWER);
        assert.equal(r.status, 202);
        assert.equal(r.text, "");
      },
    },
    {
      name: "__debug/received: exactly 2 admin_ calls arrived downstream, both from admins",
      run: async () => {
        const ledger = await downstreamLedger();
        const adminCalls = ledger.filter((rec) => {
          const body = rec.body as { method?: string; params?: { name?: string } };
          return body?.method === "tools/call" && typeof body.params?.name === "string" && body.params.name.startsWith("admin_");
        });
        assert.equal(adminCalls.length, 2, `expected exactly 2 admin_ calls, got ${adminCalls.length}: ${JSON.stringify(adminCalls.map((c) => c.body))}`);
        for (const call of adminCalls) {
          assert.equal(call.headers["x-forwarded-role"], "admin", `non-admin reached an admin tool: ${JSON.stringify(call.headers)}`);
        }
        assert.deepEqual(adminCalls.map((c) => c.headers["x-forwarded-user"]).sort(), ["admin-1", "jwt-admin"]);
        // Sanity: the case-variant call did arrive but is not an admin_ call.
        assert.ok(ledger.some((rec) => (rec.body as { params?: { name?: string } }).params?.name === "ADMIN_reset_key"));
      },
    },
  ];
}

// ---------------------------------------------------------------------------
// Phase 2 checks (FILTER_TOOL_LIST=1)
// ---------------------------------------------------------------------------
function phase2(): Check[] {
  return [
    {
      name: "FILTER_TOOL_LIST=1: viewer tools/list sees only the 2 non-admin tools",
      run: async () => {
        const r = await post(GATEWAY_URL, { jsonrpc: "2.0", id: "f1", method: "tools/list" }, VIEWER);
        assert.equal(r.status, 200, r.text);
        const tools = (frame(r, "filtered viewer").result as { tools: Array<{ name: string }> }).tools;
        assert.deepEqual(tools.map((t) => t.name).sort(), ["get_weather", "search_docs"]);
      },
    },
    {
      name: "FILTER_TOOL_LIST=1: admin tools/list still sees all 4",
      run: async () => {
        const r = await post(GATEWAY_URL, { jsonrpc: "2.0", id: "f2", method: "tools/list" }, ADMIN);
        assert.equal(r.status, 200, r.text);
        const tools = (frame(r, "filtered admin").result as { tools: Array<{ name: string }> }).tools;
        assert.equal(tools.length, 4);
      },
    },
    {
      name: "FILTER_TOOL_LIST=1: filtering also applies inside a batch for a viewer",
      run: async () => {
        const r = await post(GATEWAY_URL, [{ jsonrpc: "2.0", id: "p", method: "ping" }, { jsonrpc: "2.0", id: "t", method: "tools/list" }], VIEWER);
        assert.equal(r.status, 200, r.text);
        const arr = frames(r, "filtered batch");
        assert.equal(((arr[1]?.result as { tools: unknown[] }).tools).length, 2);
      },
    },
    {
      name: "FILTER_TOOL_LIST=1: viewer calling admin_ tool is still denied (filter is cosmetic, authz is the guard)",
      run: async () => {
        expectRpcError(await callTool(GATEWAY_URL, "f3", "admin_reset_key", { user_id: "u" }, VIEWER), 403, -32001, "filtered deny");
      },
    },
  ];
}

// ---------------------------------------------------------------------------
async function main(): Promise<void> {
  let downstream: Managed | null = null;
  let gateway: Managed | null = null;
  const gatewayEnv = { GATEWAY_PORT: String(GATEWAY_PORT), DOWNSTREAM_URL };

  try {
    console.log(`starting downstream on ${DOWNSTREAM_URL} and gateway on ${GATEWAY_URL}`);
    downstream = spawnTsx("downstream", "mock-downstream.ts", { DOWNSTREAM_PORT: String(DOWNSTREAM_PORT) });
    gateway = spawnTsx("gateway", "gateway.ts", { ...gatewayEnv, FILTER_TOOL_LIST: "0" });
    await Promise.all([waitForPort(DOWNSTREAM_PORT, downstream), waitForPort(GATEWAY_PORT, gateway)]);

    await runChecks("Phase 1: default gateway", phase1());

    // Restart the gateway with the filter on. Wait for the old process to
    // actually exit (and the port to be free) before spawning the new one, or
    // the new instance races the old one for the port and fails with EADDRINUSE.
    console.log("\nrestarting gateway with FILTER_TOOL_LIST=1");
    await stop(gateway);
    await waitForPortFree(GATEWAY_PORT);
    gateway = spawnTsx("gateway(filtered)", "gateway.ts", { ...gatewayEnv, FILTER_TOOL_LIST: "1" });
    await waitForPort(GATEWAY_PORT, gateway);

    await runChecks("Phase 2: FILTER_TOOL_LIST=1", phase2());
  } catch (err) {
    results.push({ name: "harness", ok: false, detail: err instanceof Error ? err.message : String(err) });
    console.log(`FAIL  harness\n      ${err instanceof Error ? err.message : String(err)}`);
  } finally {
    await Promise.all([stop(gateway), stop(downstream)]);
  }

  if (process.env["SHOW_SERVER_LOGS"]) {
    for (const m of [downstream, gateway]) {
      if (!m) continue;
      console.log(`\n--- ${m.name} stderr ---\n${m.stderr.join("")}`);
    }
  }

  const failed = results.filter((r) => !r.ok);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  if (failed.length > 0) {
    console.log("Failed:");
    for (const f of failed) console.log(`  - ${f.name}`);
  }
  process.exit(failed.length === 0 ? 0 : 1);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
