// Tests for task3-llm-guardrail.
//
// Part A exercises StreamingRedactor in-process: every fixture is streamed
// at every single split point and every pair of split points, and the output
// must equal the expected redaction each time.
//
// Part B spawns mock-upstream.ts and gateway.ts as child processes and
// checks the streamed SSE end to end: frame validity, redaction, TTFT under
// per-chunk delay, and memory growth while streaming a long response.
import assert from "node:assert/strict";
import { spawn, execFileSync, type ChildProcessByStdio } from "node:child_process";
import { once } from "node:events";
import { existsSync, readFileSync } from "node:fs";
import { connect } from "node:net";
import { dirname, join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { StreamingRedactor, REDACTED } from "./redactor.js";

const here = dirname(fileURLToPath(import.meta.url));
const tsxCli = join(here, "..", "node_modules", "tsx", "dist", "cli.mjs");
const HOST = "127.0.0.1";
const UPSTREAM_PORT = Number(process.env["TEST_UPSTREAM_PORT"] ?? 5101);
const GATEWAY_PORT = Number(process.env["TEST_GUARDRAIL_PORT"] ?? 5100);
const UPSTREAM_URL = `http://${HOST}:${UPSTREAM_PORT}`;
const GATEWAY_URL = `http://${HOST}:${GATEWAY_PORT}`;
const PORT_WAIT_MS = 20_000;

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
// Part A: redactor
// ---------------------------------------------------------------------------
const R = REDACTED;
const FIXTURES: Array<[input: string, expected: string]> = [
  ["Contact john.doe@example.com for details.", `Contact ${R} for details.`],
  ["Email me at john@example.com", `Email me at ${R}`],
  ["Mail john@example.co.uk. Thanks", `Mail ${R}. Thanks`],
  ["SSN 123-45-6789 on file.", `SSN ${R} on file.`],
  ["card 4111 1111 1111 1111 ok", `card ${R} ok`],
  ["card 5500-0000-0000-0004 ok", `card ${R} ok`],
  ["card 4111111111111111 ok", `card ${R} ok`],
  ["amex 378282246310005 ok", `amex ${R} ok`],
  ["order 1234567890123 shipped", "order 1234567890123 shipped"],
  ["call 555-0100 or 555-123-4567, zip 78701", "call 555-0100 or 555-123-4567, zip 78701"],
  ["id 123-45-67890 is not an ssn", "id 123-45-67890 is not an ssn"],
  ["order 99 4111 1111 1111 1111 end", `order 99 ${R} end`],
  ["two: a@b.io and 987-65-4321.", `two: ${R} and ${R}.`],
  ["card at end 4111 1111 1111 1111", `card at end ${R}`],
  ["back to back 4111 1111 1111 1111 5500 0000 0000 0004 end", `back to back ${R} ${R} end`],
];

function streamAt(text: string, splits: number[]): string {
  const r = new StreamingRedactor();
  let out = "";
  let prev = 0;
  for (const s of splits) {
    out += r.push(text.slice(prev, s));
    prev = s;
  }
  out += r.push(text.slice(prev));
  out += r.flush();
  assert.equal(r.held, "", "flush must leave nothing held");
  return out;
}

function partA(): Check[] {
  const checks: Check[] = FIXTURES.map(([input, expected]) => ({
    name: `redactor: ${JSON.stringify(input)} at every single and double split`,
    run: () => {
      assert.equal(streamAt(input, []), expected, "unsplit");
      const n = input.length;
      let runs = 1;
      for (let i = 1; i < n; i++) {
        const got = streamAt(input, [i]);
        runs++;
        assert.equal(got, expected, `split at ${i}: ${JSON.stringify(input.slice(0, i))} | ${JSON.stringify(input.slice(i))}`);
      }
      for (let i = 1; i < n; i++) {
        for (let j = i + 1; j < n; j++) {
          const got = streamAt(input, [i, j]);
          runs++;
          if (got !== expected) {
            assert.fail(
              `split at ${i},${j}: ${JSON.stringify(input.slice(0, i))} | ${JSON.stringify(input.slice(i, j))} | ${JSON.stringify(input.slice(j))}\n  got      ${JSON.stringify(got)}\n  expected ${JSON.stringify(expected)}`,
            );
          }
        }
      }
      assert.ok(runs > n, "sanity: ran the split matrix");
    },
  }));

  checks.push({
    name: "redactor: a 5000-char token is released except the last maxHold chars",
    run: () => {
      const r = new StreamingRedactor();
      const token = "x".repeat(5000);
      const out = r.push(token);
      assert.equal(out.length, 5000 - r.maxHold);
      assert.equal(r.held.length, r.maxHold);
      assert.equal(out + r.flush(), token);
      const small = new StreamingRedactor({ maxHold: 16 });
      assert.equal(small.push("y".repeat(100)).length, 84);
      assert.equal(small.held.length, 16);
    },
  });

  checks.push({
    name: "redactor: a word is released as soon as the following space arrives",
    run: () => {
      const r = new StreamingRedactor();
      assert.equal(r.push("hello"), "");
      assert.equal(r.held, "hello");
      assert.equal(r.push(" "), "hello ");
      assert.equal(r.held, "");
      assert.equal(r.push("world"), "");
      assert.equal(r.push(" again"), "world ");
      assert.equal(r.held, "again");
      assert.equal(r.flush(), "again");
    },
  });

  checks.push({
    name: "redactor: stats count each category once",
    run: () => {
      const r = new StreamingRedactor();
      let out = "";
      for (const part of ["a@b.io ", "123-45-", "6789 4111 1111 11", "11 1111 378282246310005"]) out += r.push(part);
      out += r.flush();
      assert.equal(out, `${R} ${R} ${R} ${R}`);
      assert.deepEqual(r.stats, { emails: 1, ssns: 1, cards: 2 });
    },
  });

  return checks;
}

// ---------------------------------------------------------------------------
// Part B helpers: processes, SSE, RSS
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

/**
 * Resident set size of a process in bytes. Linux reads /proc/<pid>/status;
 * Windows falls back to tasklist and macOS to ps, since /proc does not exist there.
 */
function readRssBytes(pid: number): number {
  const procPath = `/proc/${pid}/status`;
  if (existsSync(procPath)) {
    const m = /VmRSS:\s+(\d+)\s*kB/.exec(readFileSync(procPath, "utf8"));
    if (!m) throw new Error(`VmRSS not found in ${procPath}`);
    return Number(m[1]) * 1024;
  }
  if (process.platform === "win32") {
    const csv = execFileSync("tasklist", ["/FI", `PID eq ${pid}`, "/FO", "CSV", "/NH"], { encoding: "utf8" });
    const row = csv.split(/\r?\n/).find((l) => l.includes(`"${pid}"`));
    if (!row) throw new Error(`tasklist did not list pid ${pid}: ${csv}`);
    const cells = row.split('","').map((c) => c.replace(/^"|"$/g, ""));
    const mem = cells[cells.length - 1] ?? "";
    const kb = Number(mem.replace(/[^\d]/g, ""));
    if (!Number.isFinite(kb) || kb === 0) throw new Error(`could not parse memory column: ${mem}`);
    return kb * 1024;
  }
  const out = execFileSync("ps", ["-o", "rss=", "-p", String(pid)], { encoding: "utf8" }).trim();
  return Number(out) * 1024;
}

interface Frame {
  raw: string;
  data: string | null;
  json: Record<string, unknown> | null;
  at: number;
}

interface StreamResult {
  status: number;
  headers: Headers;
  frames: Frame[];
  bytes: number;
  startedAt: number;
  endedAt: number;
}

function deltaContent(f: Frame): string | null {
  const choices = f.json?.["choices"];
  if (!Array.isArray(choices)) return null;
  const delta = (choices[0] as { delta?: { content?: unknown } } | undefined)?.delta;
  return typeof delta?.content === "string" ? delta.content : null;
}

async function streamChat(body: unknown, headers: Record<string, string> = {}): Promise<StreamResult> {
  const startedAt = Date.now();
  const res = await fetch(`${GATEWAY_URL}/v1/chat/completions`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  const frames: Frame[] = [];
  let bytes = 0;
  if (!res.body) return { status: res.status, headers: res.headers, frames, bytes, startedAt, endedAt: Date.now() };
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  const drain = () => {
    for (;;) {
      const m = /\r?\n\r?\n/.exec(buf);
      if (!m) break;
      const raw = buf.slice(0, m.index);
      buf = buf.slice(m.index + m[0].length);
      const dataLines = raw.split(/\r?\n/).filter((l) => l.startsWith("data:"));
      const data = dataLines.length > 0 ? dataLines.map((l) => l.slice(5).replace(/^ /, "")).join("\n") : null;
      let json: Record<string, unknown> | null = null;
      if (data !== null && data !== "[DONE]") {
        try {
          json = JSON.parse(data) as Record<string, unknown>;
        } catch {
          json = null;
        }
      }
      frames.push({ raw, data, json, at: Date.now() });
    }
  };
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    bytes += value.byteLength;
    buf += decoder.decode(value, { stream: true });
    drain();
  }
  buf += decoder.decode();
  drain();
  if (buf.trim().length > 0) frames.push({ raw: buf, data: null, json: null, at: Date.now() });
  return { status: res.status, headers: res.headers, frames, bytes, startedAt, endedAt: Date.now() };
}

// ---------------------------------------------------------------------------
// Part B checks
// ---------------------------------------------------------------------------
function partB(gateway: () => Managed): Check[] {
  const BASIC = { model: "mock-gpt", messages: [{ role: "user", content: "hello" }] };
  let canned: StreamResult | null = null;

  return [
    {
      name: "gateway: streamed canned completion -> 200 SSE, every frame valid JSON or [DONE], [DONE] seen last",
      run: async () => {
        canned = await streamChat(BASIC);
        assert.equal(canned.status, 200);
        assert.match(canned.headers.get("content-type") ?? "", /text\/event-stream/);
        assert.ok(canned.frames.length > 3, "too few frames");
        for (const f of canned.frames) {
          assert.ok(f.data !== null, `frame without data: ${JSON.stringify(f.raw)}`);
          if (f.data !== "[DONE]") {
            assert.ok(f.json !== null, `frame is not valid JSON: ${f.raw}`);
            assert.equal(f.json?.["object"], "chat.completion.chunk");
          }
        }
        assert.equal(canned.frames.at(-1)?.data, "[DONE]", "[DONE] must be the final frame");
        assert.equal(canned.frames.filter((f) => f.data === "[DONE]").length, 1);
      },
    },
    {
      name: "gateway: reassembled text leaks no email/SSN/card, has exactly 7 [REDACTED], keeps order id/phone/ZIP",
      run: async () => {
        assert.ok(canned, "previous check must run first");
        const text = canned.frames.map(deltaContent).filter((c): c is string => c !== null).join("");
        assert.ok(!text.includes("@"), `email leaked: ${text}`);
        assert.ok(!/\d{3}-\d{2}-\d{4}/.test(text), `ssn leaked: ${text}`);
        assert.ok(!text.includes("4111") && !text.includes("5500") && !text.includes("378282246310005"), `card leaked: ${text}`);
        assert.equal(text.split(REDACTED).length - 1, 7, `expected exactly 7 ${REDACTED}: ${text}`);
        assert.ok(text.includes("Order 1234567890123,"), "order id altered");
        assert.ok(text.includes("call 555-0100,"), "phone altered");
        assert.ok(text.includes("ZIP 78701."), "ZIP altered");
        assert.ok(text.startsWith("Hello there! Your account [REDACTED] is active."), text);
        assert.ok(text.endsWith(`Reach us at ${REDACTED}`), text);
      },
    },
    {
      name: "gateway: finish frame is preceded by the synthetic flush frame and no empty content frames are emitted",
      run: async () => {
        assert.ok(canned);
        const frames = canned.frames;
        const finishIdx = frames.findIndex((f) => {
          const choices = f.json?.["choices"] as Array<{ finish_reason?: unknown }> | undefined;
          return choices?.[0]?.finish_reason === "stop";
        });
        assert.ok(finishIdx > 0, "no finish frame");
        assert.equal(finishIdx, frames.length - 2, "finish frame should sit right before [DONE]");
        assert.equal(deltaContent(frames[finishIdx - 1] as Frame), REDACTED, "flushed final email should arrive as the frame before finish");
        const first = frames[0] as Frame;
        const firstDelta = (first.json?.["choices"] as Array<{ delta?: { role?: unknown } }>)[0]?.delta;
        assert.equal(firstDelta?.role, "assistant", "role frame must pass through first");
        for (const f of frames.slice(1, finishIdx)) {
          const c = deltaContent(f);
          assert.ok(c !== null && c.length > 0, `empty or non-content frame mid-stream: ${f.raw}`);
        }
      },
    },
    {
      name: "gateway: TTFT < 350ms with x-mock-delay-ms=100 while total > 1s (streaming, not buffering)",
      run: async () => {
        const r = await streamChat(BASIC, { "x-mock-delay-ms": "100" });
        assert.equal(r.status, 200);
        const firstContent = r.frames.find((f) => (deltaContent(f) ?? "").length > 0);
        assert.ok(firstContent, "no content frame");
        const ttft = firstContent.at - r.startedAt;
        const total = r.endedAt - r.startedAt;
        assert.ok(ttft < 350, `TTFT ${ttft}ms should be < 350ms`);
        assert.ok(total > 1000, `total ${total}ms should be > 1s`);
        console.log(`      ttft=${ttft}ms total=${total}ms frames=${r.frames.length}`);
      },
    },
    {
      name: "gateway: long:20000 streams every word, redacts 400 emails, and gateway RSS growth is small relative to response size",
      run: async () => {
        const pid = gateway().child.pid;
        assert.ok(pid, "gateway pid unknown");
        const body = { model: "mock-gpt", messages: [{ role: "user", content: "long:20000" }] };

        // Warm-up so heap sizing and JIT are settled before measuring.
        const warm = await streamChat(body, { "x-mock-delay-ms": "0" });
        assert.equal(warm.status, 200);
        await new Promise((r) => setTimeout(r, 300));

        const before = readRssBytes(pid);
        const r = await streamChat(body, { "x-mock-delay-ms": "0" });
        const after = readRssBytes(pid);
        assert.equal(r.status, 200);

        const text = r.frames.map(deltaContent).filter((c): c is string => c !== null).join("");
        const words = text.split(" ");
        assert.equal(words.length, 20000, `expected 20000 words, got ${words.length}`);
        assert.equal(words.filter((w) => w === REDACTED).length, 400);
        assert.ok(!text.includes("@"), "email leaked in long stream");
        assert.equal(r.frames.at(-1)?.data, "[DONE]");

        const growth = after - before;
        const limit = Math.max(0.5 * r.bytes, 0);
        console.log(`      response=${(r.bytes / 1024).toFixed(0)}KiB rss_before=${(before / 1024 / 1024).toFixed(1)}MiB rss_after=${(after / 1024 / 1024).toFixed(1)}MiB growth=${(growth / 1024).toFixed(0)}KiB`);
        assert.ok(growth < limit, `RSS grew ${growth} bytes while streaming ${r.bytes} bytes; limit ${limit}`);
      },
    },
    {
      name: "gateway: 400 for invalid JSON and missing messages, 404 for unknown route, standardized error shape",
      run: async () => {
        const expectErr = async (res: Response, status: number, type: string) => {
          assert.equal(res.status, status);
          const body = (await res.json()) as { error: { type: string; message: string; request_id: string } };
          assert.equal(body.error.type, type);
          assert.equal(typeof body.error.message, "string");
          assert.equal(body.error.request_id, res.headers.get("x-request-id"));
        };
        await expectErr(
          await fetch(`${GATEWAY_URL}/v1/chat/completions`, { method: "POST", headers: { "content-type": "application/json" }, body: "{broken" }),
          400,
          "invalid_request_error",
        );
        await expectErr(
          await fetch(`${GATEWAY_URL}/v1/chat/completions`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model: "m" }) }),
          400,
          "invalid_request_error",
        );
        await expectErr(await fetch(`${GATEWAY_URL}/v1/models`), 404, "not_found");
      },
    },
    {
      name: "gateway: stderr has a completion log line per stream with ttft_ms, total_ms, frames, redactions",
      run: async () => {
        await new Promise((r) => setTimeout(r, 200));
        const lines = gateway()
          .stderr.join("")
          .split("\n")
          .filter((l) => l.includes('"event":"completion"'))
          .map((l) => JSON.parse(l) as Record<string, unknown>);
        assert.ok(lines.length >= 4, `expected >= 4 completion logs, got ${lines.length}`);
        for (const l of lines) {
          assert.equal(typeof l["ttft_ms"], "number", JSON.stringify(l));
          assert.equal(typeof l["total_ms"], "number");
          const frames = l["frames"] as { in: number; out: number };
          assert.ok(frames.in > 0 && frames.out > 0);
          const red = l["redactions"] as { emails: number; ssns: number; cards: number };
          assert.ok(["emails", "ssns", "cards"].every((k) => typeof (red as Record<string, unknown>)[k] === "number"));
        }
        const cannedLog = lines.find((l) => (l["redactions"] as { cards: number }).cards === 3);
        assert.ok(cannedLog, "canned completion should report 3 cards");
        assert.deepEqual(cannedLog["redactions"], { emails: 2, ssns: 2, cards: 3 });
      },
    },
  ];
}

// ---------------------------------------------------------------------------
async function main(): Promise<void> {
  await runChecks("Part A: StreamingRedactor split matrix", partA());

  let upstream: Managed | null = null;
  let gateway: Managed | null = null;
  try {
    console.log(`\nstarting upstream on ${UPSTREAM_URL} and gateway on ${GATEWAY_URL}`);
    upstream = spawnTsx("upstream", "mock-upstream.ts", { UPSTREAM_PORT: String(UPSTREAM_PORT) });
    gateway = spawnTsx("gateway", "gateway.ts", { GUARDRAIL_PORT: String(GATEWAY_PORT), UPSTREAM_URL });
    await Promise.all([waitForPort(UPSTREAM_PORT, upstream), waitForPort(GATEWAY_PORT, gateway)]);
    const g = gateway;
    await runChecks("Part B: upstream + gateway over HTTP", partB(() => g));
  } catch (err) {
    results.push({ name: "harness", ok: false, detail: err instanceof Error ? err.message : String(err) });
    console.log(`FAIL  harness\n      ${err instanceof Error ? err.message : String(err)}`);
  } finally {
    await Promise.all([stop(gateway), stop(upstream)]);
  }

  if (process.env["SHOW_SERVER_LOGS"]) {
    for (const m of [upstream, gateway]) {
      if (m) console.log(`\n--- ${m.name} stderr ---\n${m.stderr.join("")}`);
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
