// Tests for task4-router.
//
// Part A drives SlidingWindowLimiter in-process with a fake clock and a
// temporary database file.
//
// Part B spawns the mock providers and the router gateway as child
// processes and exercises auth, validation, failover per provider mode,
// sanitisation, a concurrent burst against the tenant budget, and a second
// gateway whose secondary is a dead port.
import assert from "node:assert/strict";
import { spawn, type ChildProcessByStdio } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { connect, createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { SlidingWindowLimiter, estimateTokens } from "./rate-limiter.js";
import { FAKE_INTERNAL_HOST, PRIMARY_PORT, SECONDARY_PORT } from "./mock-providers.js";

const here = dirname(fileURLToPath(import.meta.url));
const tsxCli = join(here, "..", "node_modules", "tsx", "dist", "cli.mjs");
const HOST = "127.0.0.1";
const GATEWAY_PORT = 6100;
const FAILOVER_GATEWAY_PORT = 6110;
const PORT_WAIT_MS = 20_000;

const tmp = mkdtempSync(join(tmpdir(), "task4-router-"));

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------
interface Check {
  name: string;
  run: () => Promise<void> | void;
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
// Part A: limiter with a fake clock
// ---------------------------------------------------------------------------
function partA(): Check[] {
  const T0 = 1_700_000_000_000;
  const WINDOW = 60_000;
  const LIMIT = 1000;
  const dbPath = join(tmp, "limiter.sqlite");
  let limiter: SlidingWindowLimiter;

  return [
    {
      name: "limiter: opens a temp db with table, index and pragmas",
      run: () => {
        limiter = new SlidingWindowLimiter(dbPath, { limit: LIMIT, windowMs: WINDOW, sweepIntervalMs: 0 });
        const cols = (limiter.db.prepare("PRAGMA table_info(token_usage)").all() as Array<{ name: string }>).map((c) => c.name);
        assert.deepEqual(cols, ["id", "tenant", "ts_ms", "tokens"]);
        const idx = (limiter.db.prepare("PRAGMA index_list(token_usage)").all() as Array<{ name: string }>).map((i) => i.name);
        assert.ok(idx.includes("idx_token_usage_tenant_ts"));
        assert.equal((limiter.db.prepare("PRAGMA journal_mode").get() as { journal_mode: string }).journal_mode, "wal");
        assert.equal((limiter.db.prepare("PRAGMA synchronous").get() as { synchronous: number }).synchronous, 1);
        assert.equal((limiter.db.prepare("PRAGMA busy_timeout").get() as { timeout: number }).timeout, 5000);
      },
    },
    {
      name: "limiter: reserve 600 ok -> used 600, remaining 400",
      run: () => {
        const r = limiter.reserve("alpha", 600, T0);
        assert.ok(r.ok, JSON.stringify(r));
        assert.equal(r.used, 600);
        assert.equal(r.remaining, 400);
        assert.ok(r.reservationId > 0);
      },
    },
    {
      name: "limiter: reserve 500 at +1s rejected with used 600 and retryAfterMs 59000",
      run: () => {
        const r = limiter.reserve("alpha", 500, T0 + 1000);
        assert.ok(!r.ok, "should be rejected");
        assert.equal(r.used, 600);
        assert.equal(r.limit, LIMIT);
        assert.equal(r.retryAfterMs, 59_000);
        assert.equal(limiter.used("alpha", T0 + 1000), 600, "rejected reservation must not be counted");
      },
    },
    {
      name: "limiter: other tenant is isolated",
      run: () => {
        assert.equal(limiter.used("beta", T0 + 1000), 0);
        const r = limiter.reserve("beta", 1000, T0 + 1000);
        assert.ok(r.ok && r.remaining === 0);
        assert.equal(limiter.used("alpha", T0 + 1000), 600);
        assert.ok(limiter.release(r.reservationId));
      },
    },
    {
      name: "limiter: settle(100) on the 600 reservation, then 500 fits",
      run: () => {
        const row = limiter.db.prepare("SELECT id FROM token_usage WHERE tenant = 'alpha'").get() as { id: number };
        assert.ok(limiter.settle(row.id, 100, T0 + 2000));
        assert.equal(limiter.used("alpha", T0 + 2000), 100);
        const r = limiter.reserve("alpha", 500, T0 + 2000);
        assert.ok(r.ok, JSON.stringify(r));
        assert.equal(r.used, 600);
        assert.equal(r.remaining, 400);
        assert.ok(limiter.release(r.reservationId), "cleanup");
        assert.equal(limiter.used("alpha", T0 + 2000), 100);
      },
    },
    {
      name: "limiter: release restores the budget; releasing twice reports false",
      run: () => {
        const r = limiter.reserve("alpha", 900, T0 + 3000);
        assert.ok(r.ok);
        assert.equal(limiter.used("alpha", T0 + 3000), 1000);
        assert.ok(!limiter.reserve("alpha", 1, T0 + 3000).ok);
        assert.equal(limiter.release(r.reservationId, T0 + 3000), true);
        assert.equal(limiter.used("alpha", T0 + 3000), 100);
        assert.equal(limiter.release(r.reservationId, T0 + 3000), false);
        assert.ok(limiter.reserve("alpha", 1, T0 + 3000).ok);
      },
    },
    {
      name: "limiter: usage still counted at +59999ms and gone at +60001ms",
      run: () => {
        // alpha has the 100-token row at T0 and a 1-token row at T0+3000.
        assert.equal(limiter.used("alpha", T0 + 59_999), 101);
        assert.equal(limiter.used("alpha", T0 + 60_001), 1, "row at T0 must have left the window");
        assert.equal(limiter.used("alpha", T0 + 63_001), 0);
        // and reserve sees the same thing
        const r = limiter.reserve("alpha", 1000, T0 + 63_001);
        assert.ok(r.ok && r.used === 1000);
        assert.ok(limiter.release(r.reservationId));
      },
    },
    {
      name: "limiter: sweep deletes expired rows for every tenant",
      run: () => {
        // reserve() only prunes its own tenant, so rows for other tenants
        // linger until sweep() runs.
        limiter.reserve("gamma", 5, T0);
        limiter.reserve("eta", 5, T0 + 4000);
        limiter.reserve("delta", 5, T0 + 10_000);
        const count = () => Number((limiter.db.prepare("SELECT COUNT(*) AS n FROM token_usage").get() as { n: number | bigint }).n);
        const before = count();
        assert.ok(before >= 3, `rows before sweep: ${before}`);
        const removed = limiter.sweep(T0 + 65_000); // cutoff T0+5000: gamma and eta expire, delta stays
        assert.ok(removed >= 2, `removed ${removed}`);
        assert.equal(count(), before - removed);
        assert.equal(limiter.used("delta", T0 + 65_000), 5);
        assert.equal(limiter.used("gamma", T0 + 65_000), 0);
        assert.equal(limiter.used("eta", T0 + 65_000), 0);
        assert.equal(limiter.sweep(T0 + 65_000), 0, "second sweep finds nothing");
      },
    },
    {
      name: "limiter: estimate > limit rejected with retryAfterMs == windowMs",
      run: () => {
        const r = limiter.reserve("epsilon", LIMIT + 1, T0 + 70_000);
        assert.ok(!r.ok);
        assert.equal(r.used, 0);
        assert.equal(r.retryAfterMs, WINDOW);
        const r2 = limiter.reserve("alpha", 5000, T0 + 70_000);
        assert.ok(!r2.ok && r2.retryAfterMs === WINDOW);
      },
    },
    {
      name: "limiter: retryAfterMs walks rows oldest-first until enough expires",
      run: () => {
        limiter.reserve("zeta", 300, T0); // expires T0+60000
        limiter.reserve("zeta", 400, T0 + 1000); // expires T0+61000
        limiter.reserve("zeta", 200, T0 + 2000); // expires T0+62000
        const now = T0 + 5000;
        const r = limiter.reserve("zeta", 500, now); // need 400 freed -> second row
        assert.ok(!r.ok && r.retryAfterMs === T0 + 61_000 - now, JSON.stringify(r));
        const r2 = limiter.reserve("zeta", 200, now); // need 100 -> first row
        assert.ok(!r2.ok && r2.retryAfterMs === T0 + 60_000 - now, JSON.stringify(r2));
        assert.ok(limiter.reserve("zeta", 500, now + r.retryAfterMs).ok, "fits exactly at the retry time");
      },
    },
    {
      name: "estimateTokens: ceil(chars/4) + 4 per message + (max_tokens ?? 256)",
      run: () => {
        assert.equal(estimateTokens({ messages: [{ role: "user", content: "hi" }], max_tokens: 50 }), 1 + 4 + 50);
        assert.equal(estimateTokens({ messages: [{ role: "user", content: "hello" }] }), 2 + 4 + 256);
        assert.equal(estimateTokens({ messages: [{ content: "a".repeat(400) }, { content: "" }], max_tokens: 10 }), 100 + 4 + 0 + 4 + 10);
        assert.equal(estimateTokens({ messages: [{ content: [{ type: "text", text: "abcd" }, { type: "image_url" }] }], max_tokens: 0 }), 1 + 4);
        assert.equal(estimateTokens({}), 256);
        assert.equal(estimateTokens(undefined), 256);
        limiter.close();
      },
    },
  ];
}

// ---------------------------------------------------------------------------
// Part B helpers
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
        reject(new Error(`${managed.name} did not open port ${port} in time:\n${managed.stderr.join("")}`));
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

async function stop(managed: Managed | null): Promise<void> {
  if (!managed) return;
  if (managed.child.exitCode !== null || managed.child.signalCode !== null) return;
  const exited = once(managed.child, "exit");
  managed.child.kill();
  await exited;
}

/** A port that nothing listens on: bind, read the number, close. */
function deadPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.once("error", reject);
    srv.listen(0, HOST, () => {
      const port = (srv.address() as { port: number }).port;
      srv.close(() => resolve(port));
    });
  });
}

interface HttpResult {
  status: number;
  headers: Headers;
  text: string;
  json: Record<string, unknown> | null;
}

async function chat(port: number, body: unknown, headers: Record<string, string> = {}): Promise<HttpResult> {
  const res = await fetch(`http://${HOST}:${port}/v1/chat/completions`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
  const text = await res.text();
  let json: Record<string, unknown> | null = null;
  try {
    json = JSON.parse(text) as Record<string, unknown>;
  } catch {
    json = null;
  }
  return { status: res.status, headers: res.headers, text, json };
}

async function setMode(port: number, mode: string): Promise<void> {
  const res = await fetch(`http://${HOST}:${port}/__mode`, { method: "POST", body: JSON.stringify({ mode }) });
  assert.equal(res.status, 200, `failed to set mode ${mode} on ${port}`);
}

function errorOf(r: HttpResult): Record<string, unknown> {
  assert.ok(r.json && typeof r.json["error"] === "object" && r.json["error"] !== null, `expected error payload, got ${r.text}`);
  return r.json["error"] as Record<string, unknown>;
}

function expectStandardError(r: HttpResult, status: number, type: string): Record<string, unknown> {
  assert.equal(r.status, status, `HTTP ${r.status}: ${r.text}`);
  const e = errorOf(r);
  assert.equal(e["type"], type);
  assert.equal(typeof e["message"], "string");
  assert.equal(e["request_id"], r.headers.get("x-request-id"));
  return e;
}

function usageOf(r: HttpResult): number {
  const usage = r.json?.["usage"] as { total_tokens?: unknown } | undefined;
  const total = usage?.total_tokens;
  assert.ok(typeof total === "number", `no usage in ${r.text}`);
  return total;
}

function tenantUsage(dbPath: string, tenant: string, windowMs: number): number {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    const row = db.prepare("SELECT COALESCE(SUM(tokens), 0) AS used FROM token_usage WHERE tenant = ? AND ts_ms > ?").get(tenant, Date.now() - windowMs) as { used: number | bigint };
    return Number(row.used);
  } finally {
    db.close();
  }
}

// ---------------------------------------------------------------------------
// Part B checks
// ---------------------------------------------------------------------------
const A = { authorization: "Bearer sk-tenant-a" };
const B = { authorization: "Bearer sk-tenant-b" };
// "hi" -> ceil(2/4)+4 = 5 prompt tokens; max_tokens 50 -> estimate 55, billed completion min(50,20)=20 -> actual 25.
const BODY = { model: "gpt-mock", messages: [{ role: "user", content: "hi" }], max_tokens: 50 };
const ESTIMATE = 55;
const ACTUAL = 25;
const LIMIT = 1200;

function partB(dbPath: string, failoverDbPath: string, failoverLimit: number): Check[] {
  return [
    {
      name: "gateway: no key -> 401 unauthenticated",
      run: async () => {
        const r = await chat(GATEWAY_PORT, BODY);
        expectStandardError(r, 401, "unauthenticated");
        assert.match(r.headers.get("www-authenticate") ?? "", /^Bearer/);
        expectStandardError(await chat(GATEWAY_PORT, BODY, { authorization: "Bearer sk-nope" }), 401, "unauthenticated");
      },
    },
    {
      name: "gateway: bad JSON -> 400 bad_request",
      run: async () => {
        expectStandardError(await chat(GATEWAY_PORT, '{"model":', A), 400, "bad_request");
      },
    },
    {
      name: "gateway: empty messages -> 400 bad_request",
      run: async () => {
        expectStandardError(await chat(GATEWAY_PORT, { model: "gpt-mock", messages: [] }, A), 400, "bad_request");
        expectStandardError(await chat(GATEWAY_PORT, { model: "gpt-mock" }, A), 400, "bad_request");
      },
    },
    {
      name: `gateway: primary ok -> x-served-by primary, remaining == ${LIMIT - ACTUAL} (settled ${ACTUAL}, not the ${ESTIMATE} estimate)`,
      run: async () => {
        assert.equal(estimateTokens(BODY), ESTIMATE, "fixture estimate");
        const r = await chat(GATEWAY_PORT, BODY, A);
        assert.equal(r.status, 200, r.text);
        assert.equal(r.headers.get("x-served-by"), "primary");
        assert.equal(usageOf(r), ACTUAL);
        assert.equal(r.headers.get("x-ratelimit-remaining-tokens"), String(LIMIT - ACTUAL));
        assert.equal(r.headers.get("x-ratelimit-limit-tokens"), String(LIMIT));
        assert.ok(r.headers.get("x-request-id"));
      },
    },
    {
      name: "gateway: primary mode 429 -> served by secondary",
      run: async () => {
        await setMode(PRIMARY_PORT, "429");
        try {
          const r = await chat(GATEWAY_PORT, BODY, A);
          assert.equal(r.status, 200, r.text);
          assert.equal(r.headers.get("x-served-by"), "secondary");
          assert.equal(r.json?.["provider"], "secondary");
        } finally {
          await setMode(PRIMARY_PORT, "ok");
        }
      },
    },
    {
      name: "gateway: primary mode hang -> secondary in 3000-4500ms",
      run: async () => {
        await setMode(PRIMARY_PORT, "hang");
        try {
          const t0 = Date.now();
          const r = await chat(GATEWAY_PORT, BODY, A);
          const ms = Date.now() - t0;
          assert.equal(r.status, 200, r.text);
          assert.equal(r.headers.get("x-served-by"), "secondary");
          assert.ok(ms >= 3000 && ms <= 4500, `took ${ms}ms`);
          console.log(`      hang failover took ${ms}ms`);
        } finally {
          await setMode(PRIMARY_PORT, "ok");
        }
      },
    },
    {
      name: "gateway: primary mode slow -> primary in 1500-3000ms",
      run: async () => {
        await setMode(PRIMARY_PORT, "slow");
        try {
          const t0 = Date.now();
          const r = await chat(GATEWAY_PORT, BODY, A);
          const ms = Date.now() - t0;
          assert.equal(r.status, 200, r.text);
          assert.equal(r.headers.get("x-served-by"), "primary");
          assert.ok(ms >= 1500 && ms <= 3000, `took ${ms}ms`);
          console.log(`      slow primary took ${ms}ms`);
        } finally {
          await setMode(PRIMARY_PORT, "ok");
        }
      },
    },
    {
      name: "gateway: primary mode 500 -> 502 upstream_error, sanitized (no NullPointer / ModelShard / .java / internal host), not retried",
      run: async () => {
        const statsBefore = (await (await fetch(`http://${HOST}:${SECONDARY_PORT}/__stats`)).json()) as { completions: number; rejected: number };
        await setMode(PRIMARY_PORT, "500");
        try {
          const r = await chat(GATEWAY_PORT, BODY, A);
          const e = expectStandardError(r, 502, "upstream_error");
          assert.equal(e["upstream_status"], 500);
          for (const needle of ["NullPointer", "ModelShard", ".java", FAKE_INTERNAL_HOST, "Scheduler"]) {
            assert.ok(!r.text.includes(needle), `response leaked ${JSON.stringify(needle)}: ${r.text}`);
          }
          const statsAfter = (await (await fetch(`http://${HOST}:${SECONDARY_PORT}/__stats`)).json()) as { completions: number; rejected: number };
          assert.deepEqual(statsAfter, statsBefore, "secondary must not be consulted after a primary 500");
        } finally {
          await setMode(PRIMARY_PORT, "ok");
        }
      },
    },
    {
      name: `gateway: burst on a fresh tenant: warm-up then 40 concurrent -> exactly floor((${LIMIT}-${ACTUAL})/${ESTIMATE}) = 21 pass, rest 429 with Retry-After`,
      run: async () => {
        // tenant-b has no usage yet, so after the warm-up exactly ACTUAL tokens are in its window.
        const warm = await chat(GATEWAY_PORT, BODY, B);
        assert.equal(warm.status, 200, warm.text);
        assert.equal(warm.headers.get("x-ratelimit-remaining-tokens"), String(LIMIT - ACTUAL));

        const expectedPass = Math.floor((LIMIT - ACTUAL) / ESTIMATE);
        assert.equal(expectedPass, 21);
        const results = await Promise.all(Array.from({ length: 40 }, () => chat(GATEWAY_PORT, BODY, B)));
        const passed = results.filter((r) => r.status === 200);
        const limited = results.filter((r) => r.status === 429);
        assert.equal(passed.length, expectedPass, `passed ${passed.length}, statuses ${results.map((r) => r.status).join(",")}`);
        assert.equal(limited.length, 40 - expectedPass);
        for (const r of limited) {
          const e = expectStandardError(r, 429, "rate_limited");
          assert.equal(typeof e["retry_after_ms"], "number");
          assert.equal(e["limit"], LIMIT);
          assert.equal(e["estimate"], ESTIMATE);
          assert.ok(typeof e["used"] === "number" && (e["used"] as number) + ESTIMATE > LIMIT, `used ${String(e["used"])} should not have fit`);
          const retryAfter = Number(r.headers.get("retry-after"));
          assert.ok(Number.isInteger(retryAfter) && retryAfter >= 1 && retryAfter <= 60, `Retry-After ${r.headers.get("retry-after")}`);
          assert.equal(r.headers.get("x-ratelimit-limit-tokens"), String(LIMIT));
          assert.ok(r.headers.get("x-ratelimit-remaining-tokens") !== null);
        }
        for (const r of passed) assert.equal(usageOf(r), ACTUAL);
      },
    },
    {
      name: `gateway: after the burst, settled usage for the tenant == ${1 + 21} * ${ACTUAL} = ${(1 + 21) * ACTUAL}`,
      run: async () => {
        await new Promise((r) => setTimeout(r, 200));
        const used = tenantUsage(dbPath, "tenant-b", 60_000);
        assert.equal(used, 22 * ACTUAL, `settled usage ${used}`);
        // The next request's header agrees with the database.
        const r = await chat(GATEWAY_PORT, BODY, B);
        assert.equal(r.status, 200, r.text);
        assert.equal(r.headers.get("x-ratelimit-remaining-tokens"), String(LIMIT - 23 * ACTUAL));
      },
    },
    {
      name: "failover gateway (6110): primary hang + dead secondary -> 503/504 standardized with attempts, no ECONNREFUSED or IP in body",
      run: async () => {
        await setMode(PRIMARY_PORT, "hang");
        try {
          const r = await chat(FAILOVER_GATEWAY_PORT, BODY, A);
          assert.ok(r.status === 503 || r.status === 504, `HTTP ${r.status}: ${r.text}`);
          const e = errorOf(r);
          assert.ok(e["type"] === "upstream_unavailable" || e["type"] === "upstream_timeout", String(e["type"]));
          assert.equal(e["request_id"], r.headers.get("x-request-id"));
          const attempts = e["attempts"] as Array<{ provider: string; outcome: string }>;
          assert.ok(Array.isArray(attempts) && attempts.length === 2, `attempts: ${JSON.stringify(attempts)}`);
          assert.deepEqual(
            attempts.map((a) => [a.provider, a.outcome]),
            [
              ["primary", "timeout"],
              ["secondary", "unreachable"],
            ],
          );
          assert.ok(!/ECONNREFUSED|\b\d{1,3}(\.\d{1,3}){3}\b|localhost/i.test(r.text), `leaked connection detail: ${r.text}`);
        } finally {
          await setMode(PRIMARY_PORT, "ok");
        }
      },
    },
    {
      name: `failover gateway (6110): a second request is not 429 (reservation released; limit ${failoverLimit} < 2 x estimate)`,
      run: async () => {
        assert.ok(failoverLimit < 2 * ESTIMATE && failoverLimit >= ESTIMATE, "fixture: a leaked reservation would block the second request");
        assert.equal(tenantUsage(failoverDbPath, "tenant-a", 60_000), 0, "failed request must not leave usage behind");
        await setMode(PRIMARY_PORT, "hang");
        try {
          const r = await chat(FAILOVER_GATEWAY_PORT, BODY, A);
          assert.notEqual(r.status, 429, `second request was rate limited: ${r.text}`);
          assert.ok(r.status === 503 || r.status === 504);
        } finally {
          await setMode(PRIMARY_PORT, "ok");
        }
        assert.equal(tenantUsage(failoverDbPath, "tenant-a", 60_000), 0);
        // And with the primary healthy the same gateway serves normally.
        const ok = await chat(FAILOVER_GATEWAY_PORT, BODY, A);
        assert.equal(ok.status, 200, ok.text);
        assert.equal(ok.headers.get("x-served-by"), "primary");
      },
    },
  ];
}

// ---------------------------------------------------------------------------
async function main(): Promise<void> {
  await runChecks("Part A: SlidingWindowLimiter with a fake clock", partA());

  const dbPath = join(tmp, "gateway.sqlite");
  const failoverDbPath = join(tmp, "failover.sqlite");
  const FAILOVER_LIMIT = 100;
  let providers: Managed | null = null;
  let gateway: Managed | null = null;
  let failover: Managed | null = null;

  try {
    const dead = await deadPort();
    console.log(`\nstarting providers on ${PRIMARY_PORT}/${SECONDARY_PORT}, gateway on ${GATEWAY_PORT}, failover gateway on ${FAILOVER_GATEWAY_PORT} (secondary -> dead port ${dead})`);
    providers = spawnTsx("providers", "mock-providers.ts", {});
    gateway = spawnTsx("gateway", "gateway.ts", {
      GATEWAY_PORT: String(GATEWAY_PORT),
      DB_PATH: dbPath,
      TOKENS_PER_MINUTE: String(LIMIT),
      WINDOW_MS: "60000",
    });
    failover = spawnTsx("failover-gateway", "gateway.ts", {
      GATEWAY_PORT: String(FAILOVER_GATEWAY_PORT),
      DB_PATH: failoverDbPath,
      TOKENS_PER_MINUTE: String(FAILOVER_LIMIT),
      WINDOW_MS: "60000",
      SECONDARY_URL: `http://${HOST}:${dead}`,
    });
    await Promise.all([
      waitForPort(PRIMARY_PORT, providers),
      waitForPort(SECONDARY_PORT, providers),
      waitForPort(GATEWAY_PORT, gateway),
      waitForPort(FAILOVER_GATEWAY_PORT, failover),
    ]);

    await runChecks("Part B: providers + gateways over HTTP", partB(dbPath, failoverDbPath, FAILOVER_LIMIT));
  } catch (err) {
    results.push({ name: "harness", ok: false, detail: err instanceof Error ? err.message : String(err) });
    console.log(`FAIL  harness\n      ${err instanceof Error ? err.message : String(err)}`);
  } finally {
    await Promise.all([stop(failover), stop(gateway), stop(providers)]);
  }

  if (process.env["SHOW_SERVER_LOGS"]) {
    for (const m of [providers, gateway, failover]) {
      if (m) console.log(`\n--- ${m.name} stderr ---\n${m.stderr.join("")}`);
    }
  }

  try {
    rmSync(tmp, { recursive: true, force: true });
  } catch {
    /* best effort */
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
