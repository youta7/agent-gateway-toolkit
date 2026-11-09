// Test suite for task1-mcp-server.
//
// Part A spawns server.ts with tsx and speaks raw JSON-RPC over stdin so we can
// send things the SDK client refuses to (garbage lines, wrong types, unknown
// methods) and inspect every byte on stdout and stderr.
//
// Part B drives the same server through the official SDK Client to confirm a
// real client sees protocol errors as thrown McpErrors with the right code.
//
// Prints PASS/FAIL per check and exits non-zero if anything fails.
import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { ErrorCode, McpError, type CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { lockStdout, looksLikeJsonRpcFrame } from "./logger.js";

const here = dirname(fileURLToPath(import.meta.url));
const serverPath = join(here, "server.ts");
const tsxCli = join(here, "..", "node_modules", "tsx", "dist", "cli.mjs");
const TIMEOUT_MS = 15_000;

// ---------------------------------------------------------------------------
// Tiny test runner
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
// Part A harness: raw JSON-RPC over a child process
// ---------------------------------------------------------------------------
interface RawFrame {
  jsonrpc?: unknown;
  id?: unknown;
  method?: unknown;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
}

class RawServer {
  readonly child: ChildProcessWithoutNullStreams;
  readonly stdoutLines: string[] = [];
  readonly stderrLines: string[] = [];
  readonly frames: RawFrame[] = [];
  private stdoutPending = "";
  private stderrPending = "";
  private nextId = 1;
  private readonly listeners = new Set<() => void>();

  constructor() {
    this.child = spawn(process.execPath, [tsxCli, serverPath], { stdio: ["pipe", "pipe", "pipe"] });
    this.child.stdout.on("data", (chunk: Buffer) => {
      this.stdoutPending += chunk.toString("utf8");
      let nl: number;
      while ((nl = this.stdoutPending.indexOf("\n")) !== -1) {
        const line = this.stdoutPending.slice(0, nl).replace(/\r$/, "");
        this.stdoutPending = this.stdoutPending.slice(nl + 1);
        this.stdoutLines.push(line);
        try {
          this.frames.push(JSON.parse(line) as RawFrame);
        } catch {
          // Leave it unparsed; the final sweep will flag it.
        }
        for (const l of this.listeners) l();
      }
    });
    this.child.stderr.on("data", (chunk: Buffer) => {
      this.stderrPending += chunk.toString("utf8");
      let nl: number;
      while ((nl = this.stderrPending.indexOf("\n")) !== -1) {
        this.stderrLines.push(this.stderrPending.slice(0, nl).replace(/\r$/, ""));
        this.stderrPending = this.stderrPending.slice(nl + 1);
      }
    });
  }

  get trailingStdout(): string {
    return this.stdoutPending;
  }

  sendRaw(line: string): void {
    this.child.stdin.write(line + "\n");
  }

  notify(method: string, params?: unknown): void {
    this.sendRaw(JSON.stringify(params === undefined ? { jsonrpc: "2.0", method } : { jsonrpc: "2.0", method, params }));
  }

  /** Sends a request and resolves with the matching response frame. */
  async request(method: string, params?: unknown): Promise<RawFrame> {
    const id = this.nextId++;
    const frame: Record<string, unknown> = { jsonrpc: "2.0", id, method };
    if (params !== undefined) frame["params"] = params;
    const wait = this.waitFor((f) => f.id === id, `response to ${method} (id ${id})`);
    this.sendRaw(JSON.stringify(frame));
    return wait;
  }

  /** Sends a raw line and resolves with the next id:null error frame that arrives after it. */
  async sendRawExpectingNullIdError(line: string): Promise<RawFrame> {
    const before = this.frames.length;
    const wait = this.waitFor(
      (f, index) => index >= before && "id" in f && f.id === null && f.error !== undefined,
      `id:null error for line ${JSON.stringify(line)}`,
    );
    this.sendRaw(line);
    return wait;
  }

  private waitFor(pred: (f: RawFrame, index: number) => boolean, what: string): Promise<RawFrame> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.listeners.delete(check);
        reject(new Error(`timed out waiting for ${what}`));
      }, TIMEOUT_MS);
      const check = () => {
        const idx = this.frames.findIndex(pred);
        if (idx !== -1) {
          clearTimeout(timer);
          this.listeners.delete(check);
          resolve(this.frames[idx] as RawFrame);
        }
      };
      this.listeners.add(check);
      check();
    });
  }

  async stop(): Promise<void> {
    this.child.stdin.end();
    this.child.kill();
    await once(this.child, "exit").catch(() => undefined);
  }
}

function expectError(frame: RawFrame, code: number, label: string): NonNullable<RawFrame["error"]> {
  assert.ok(frame.error, `${label}: expected an error frame, got ${JSON.stringify(frame)}`);
  assert.equal(frame.error.code, code, `${label}: expected code ${code}, got ${frame.error.code} (${frame.error.message})`);
  assert.ok(!("result" in frame), `${label}: frame has both result and error`);
  return frame.error;
}

function expectResult(frame: RawFrame, label: string): CallToolResult {
  assert.ok(!frame.error, `${label}: unexpected error ${JSON.stringify(frame.error)}`);
  assert.ok(frame.result, `${label}: missing result`);
  return frame.result as CallToolResult;
}

function zodIssues(err: NonNullable<RawFrame["error"]>, label: string): Array<{ path: unknown[]; code: string }> {
  const data = err.data as { issues?: Array<{ path: unknown[]; code: string }> } | undefined;
  assert.ok(Array.isArray(data?.issues) && data.issues.length > 0, `${label}: expected Zod issues in error.data.issues`);
  return data.issues;
}

function textBody(result: CallToolResult): Record<string, unknown> {
  const first = result.content[0];
  assert.ok(first && first.type === "text", "expected a text content block");
  return JSON.parse(first.text) as Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Part A checks
// ---------------------------------------------------------------------------
function partA(raw: RawServer): Check[] {
  const VALID_REASON = "customer requested a refund for a duplicate charge";
  const callTool = (name: string, args?: unknown) =>
    raw.request("tools/call", args === undefined ? { name } : { name, arguments: args });

  const malformedIds: Array<[label: string, value: unknown]> = [
    ["CUST-1 (too few digits)", "CUST-1"],
    ["cust-00001 (lowercase prefix)", "cust-00001"],
    ["CUST-ABCDE (letters)", "CUST-ABCDE"],
    ["CUST-000011 (six digits)", "CUST-000011"],
    ['"" (empty string)', ""],
    ["12345 (number)", 12345],
    ["null", null],
    ["CUST-00001 with trailing space", "CUST-00001 "],
  ];

  const refundEdges: Array<[label: string, args: Record<string, unknown>, expectPath: string]> = [
    ["negative amount", { customer_id: "CUST-00001", amount: -5, reason: VALID_REASON }, "amount"],
    ["zero amount", { customer_id: "CUST-00001", amount: 0, reason: VALID_REASON }, "amount"],
    ["string amount", { customer_id: "CUST-00001", amount: "10", reason: VALID_REASON }, "amount"],
    ["null amount", { customer_id: "CUST-00001", amount: null, reason: VALID_REASON }, "amount"],
    ["amount above 1e6", { customer_id: "CUST-00001", amount: 1_000_000.01, reason: VALID_REASON }, "amount"],
    ["short reason", { customer_id: "CUST-00001", amount: 5, reason: "too short" }, "reason"],
    ["whitespace-padded short reason", { customer_id: "CUST-00001", amount: 5, reason: "      nope      " }, "reason"],
    ["reason over 500 chars", { customer_id: "CUST-00001", amount: 5, reason: "x".repeat(501) }, "reason"],
    ["missing reason", { customer_id: "CUST-00001", amount: 5 }, "reason"],
    ["extra unknown field", { customer_id: "CUST-00001", amount: 5, reason: VALID_REASON, memo: "hi" }, "memo"],
  ];

  return [
    {
      name: "initialize handshake returns server info and tools capability",
      run: async () => {
        const frame = await raw.request("initialize", {
          protocolVersion: "2025-06-18",
          capabilities: {},
          clientInfo: { name: "raw-test", version: "0.1.0" },
        });
        const result = frame.result as { protocolVersion?: string; capabilities?: { tools?: unknown }; serverInfo?: { name?: string } };
        assert.ok(!frame.error, `initialize failed: ${JSON.stringify(frame.error)}`);
        assert.ok(result.protocolVersion, "missing protocolVersion");
        assert.ok(result.capabilities?.tools, "server did not advertise tools capability");
        assert.equal(result.serverInfo?.name, "agent-gateway-toolkit/task1-mcp-server");
        raw.notify("notifications/initialized");
      },
    },
    {
      name: "tools/list returns exactly get_customer_record and trigger_refund with clean schemas",
      run: async () => {
        const frame = await raw.request("tools/list");
        const tools = (frame.result as { tools: Array<Record<string, unknown>> }).tools;
        assert.deepEqual(tools.map((t) => t["name"]).sort(), ["get_customer_record", "trigger_refund"]);
        for (const tool of tools) {
          const input = tool["inputSchema"] as Record<string, unknown>;
          const output = tool["outputSchema"] as Record<string, unknown> | undefined;
          assert.equal(input["type"], "object", `${tool["name"]} inputSchema.type`);
          assert.equal(input["additionalProperties"], false, `${tool["name"]} should be strict`);
          assert.ok(!("$schema" in input), `${tool["name"]} inputSchema leaked $schema`);
          assert.ok(output && !("$schema" in output), `${tool["name"]} outputSchema missing or leaked $schema`);
        }
        const refund = tools.find((t) => t["name"] === "trigger_refund");
        const required = ((refund?.["inputSchema"] as { required?: string[] }).required ?? []).slice().sort();
        assert.deepEqual(required, ["amount", "customer_id", "reason"]);
      },
    },
    {
      name: "valid get_customer_record returns structuredContent matching the text block",
      run: async () => {
        const result = expectResult(await callTool("get_customer_record", { customer_id: "CUST-00001" }), "get_customer_record");
        assert.notEqual(result.isError, true);
        assert.deepEqual(result.structuredContent, {
          customer_id: "CUST-00001",
          name: "Ada Lovelace",
          email: "ada@example.com",
          status: "active",
          balance: "1250.50",
        });
        assert.deepEqual(textBody(result), result.structuredContent, "text block should mirror structuredContent");
      },
    },
    ...malformedIds.map(([label, value]): Check => ({
      name: `malformed customer_id ${label} -> -32602`,
      run: async () => {
        const err = expectError(await callTool("get_customer_record", { customer_id: value }), -32602, label);
        const issues = zodIssues(err, label);
        assert.ok(issues.some((i) => i.path[0] === "customer_id"), `${label}: issue should point at customer_id`);
      },
    })),
    {
      name: "missing argument (empty object) -> -32602",
      run: async () => {
        const err = expectError(await callTool("get_customer_record", {}), -32602, "empty args");
        assert.ok(zodIssues(err, "empty args").some((i) => i.path[0] === "customer_id"));
      },
    },
    {
      name: "arguments key omitted entirely -> -32602",
      run: async () => {
        expectError(await callTool("get_customer_record"), -32602, "no arguments key");
      },
    },
    {
      name: "unknown tool -> -32602 with known_tools in data",
      run: async () => {
        const err = expectError(await callTool("delete_everything", {}), -32602, "unknown tool");
        assert.match(err.message, /Unknown tool/);
        const data = err.data as { known_tools?: string[] };
        assert.deepEqual(data.known_tools?.slice().sort(), ["get_customer_record", "trigger_refund"]);
      },
    },
    {
      name: "unknown method -> -32601",
      run: async () => {
        expectError(await raw.request("tools/explode", {}), -32601, "unknown method");
        expectError(await raw.request("resources/list"), -32601, "unregistered capability method");
      },
    },
    ...refundEdges.map(([label, args, expectPath]): Check => ({
      name: `trigger_refund ${label} -> -32602`,
      run: async () => {
        const err = expectError(await callTool("trigger_refund", args), -32602, label);
        const issues = zodIssues(err, label);
        const hit = issues.some((i) => i.path[0] === expectPath || (expectPath === "memo" && i.code === "unrecognized_keys"));
        assert.ok(hit, `${label}: expected an issue on ${expectPath}, got ${JSON.stringify(issues.map((i) => i.path))}`);
      },
    })),
    {
      name: "refund over balance -> result.isError true (INSUFFICIENT_BALANCE), not a protocol error",
      run: async () => {
        const result = expectResult(
          await callTool("trigger_refund", { customer_id: "CUST-00002", amount: 50, reason: VALID_REASON }),
          "over balance",
        );
        assert.equal(result.isError, true);
        const body = textBody(result);
        assert.equal(body["error"], "INSUFFICIENT_BALANCE");
        assert.equal(body["available"], "49.99");
      },
    },
    {
      name: "refund for suspended customer -> result.isError true (CUSTOMER_SUSPENDED)",
      run: async () => {
        const result = expectResult(
          await callTool("trigger_refund", { customer_id: "CUST-00003", amount: 1, reason: VALID_REASON }),
          "suspended",
        );
        assert.equal(result.isError, true);
        assert.equal(textBody(result)["error"], "CUSTOMER_SUSPENDED");
      },
    },
    {
      name: "refund for unknown customer -> result.isError true (CUSTOMER_NOT_FOUND)",
      run: async () => {
        const result = expectResult(
          await callTool("trigger_refund", { customer_id: "CUST-99999", amount: 1, reason: VALID_REASON }),
          "unknown customer",
        );
        assert.equal(result.isError, true);
        assert.equal(textBody(result)["error"], "CUSTOMER_NOT_FOUND");
        const lookup = expectResult(await callTool("get_customer_record", { customer_id: "CUST-99999" }), "unknown lookup");
        assert.equal(lookup.isError, true);
      },
    },
    {
      name: "successful refund does exact cent math and persists the new balance",
      run: async () => {
        const result = expectResult(
          await callTool("trigger_refund", { customer_id: "CUST-00002", amount: 0.3, reason: VALID_REASON }),
          "refund 0.30",
        );
        assert.notEqual(result.isError, true);
        const body = result.structuredContent as Record<string, unknown>;
        assert.match(String(body["refund_id"]), /^RF-/);
        assert.equal(body["amount"], "0.30");
        assert.equal(body["remaining_balance"], "49.69");
        const after = expectResult(await callTool("get_customer_record", { customer_id: "CUST-00002" }), "lookup after refund");
        assert.equal((after.structuredContent as Record<string, unknown>)["balance"], "49.69");
      },
    },
    {
      name: "garbage line -> -32700 Parse error with id null",
      run: async () => {
        const frame = await raw.sendRawExpectingNullIdError("this is not json");
        const err = expectError(frame, -32700, "garbage");
        assert.equal(err.message, "Parse error");
        const frame2 = await raw.sendRawExpectingNullIdError("{truncated");
        expectError(frame2, -32700, "truncated json");
      },
    },
    {
      name: "valid JSON but invalid envelope -> -32600 Invalid Request with id null",
      run: async () => {
        const frame = await raw.sendRawExpectingNullIdError(JSON.stringify({ hello: "world" }));
        const err = expectError(frame, -32600, "bad envelope");
        assert.equal(err.message, "Invalid Request");
        assert.ok(Array.isArray((err.data as { issues?: unknown })?.issues), "envelope issues attached");
      },
    },
    {
      name: "server still answers after malformed input",
      run: async () => {
        const frame = await raw.request("tools/list");
        assert.ok(frame.result, "tools/list failed after garbage");
      },
    },
    {
      name: "every stdout line is JSON with jsonrpc 2.0 and nothing is left unterminated",
      run: async () => {
        assert.ok(raw.stdoutLines.length > 0, "no stdout at all");
        for (const line of raw.stdoutLines) {
          let parsed: RawFrame;
          try {
            parsed = JSON.parse(line) as RawFrame;
          } catch {
            assert.fail(`stdout line is not JSON: ${line}`);
          }
          assert.equal(parsed.jsonrpc, "2.0", `stdout line missing jsonrpc 2.0: ${line}`);
          assert.ok(looksLikeJsonRpcFrame(line));
        }
        assert.equal(raw.trailingStdout, "", "partial line left on stdout");
      },
    },
    {
      name: "stderr carries structured log lines (server_started, tools/call, malformed_frame)",
      run: async () => {
        assert.ok(raw.stderrLines.length > 0, "no stderr output");
        const events = new Set<string>();
        for (const line of raw.stderrLines) {
          const parsed = JSON.parse(line) as { ts?: string; level?: string; event?: string };
          assert.ok(parsed.ts && parsed.level && parsed.event, `stderr line missing ts/level/event: ${line}`);
          events.add(parsed.event);
        }
        for (const needed of ["server_started", "tools/call", "invalid_params", "domain_error", "malformed_frame"]) {
          assert.ok(events.has(needed), `expected a ${needed} log line; saw ${[...events].join(", ")}`);
        }
      },
    },
    {
      name: "lockStdout (in-process): console.* and stray writes go to stderr, frames pass",
      run: async () => {
        const stdoutWrites: string[] = [];
        const stderrWrites: string[] = [];
        const realStdout = process.stdout.write;
        const realStderr = process.stderr.write;
        process.stdout.write = ((chunk: unknown) => {
          stdoutWrites.push(String(chunk));
          return true;
        }) as typeof process.stdout.write;
        process.stderr.write = ((chunk: unknown) => {
          stderrWrites.push(String(chunk));
          return true;
        }) as typeof process.stderr.write;
        const unlock = lockStdout();
        try {
          console.log("console-log-line");
          console.info("console-info-line");
          console.debug("console-debug-line");
          console.warn("console-warn-line");
          process.stdout.write("stray banner\n");
          process.stdout.write(Buffer.from("buffer junk\n"));
          let cbCalled = false;
          process.stdout.write("junk with cb\n", () => {
            cbCalled = true;
          });
          const frame = '{"jsonrpc":"2.0","id":1,"result":{}}\n';
          process.stdout.write(frame);
          await new Promise((r) => setImmediate(r));
          assert.deepEqual(stdoutWrites, [frame]);
          assert.ok(cbCalled, "diverted write callback not invoked");
          const all = stderrWrites.join("");
          for (const needle of ["console-log-line", "console-info-line", "console-debug-line", "console-warn-line", "stray banner", "buffer junk", "junk with cb"]) {
            assert.ok(all.includes(needle), `stderr missing ${needle}`);
          }
          assert.equal(stderrWrites.filter((w) => w.includes('"event":"stdout_diverted"')).length, 3);
        } finally {
          unlock();
          process.stdout.write = realStdout;
          process.stderr.write = realStderr;
        }
      },
    },
  ];
}

// ---------------------------------------------------------------------------
// Part B: official SDK client
// ---------------------------------------------------------------------------
async function partB(): Promise<void> {
  const transport = new StdioClientTransport({ command: process.execPath, args: [tsxCli, serverPath], stderr: "pipe" });
  const client = new Client({ name: "task1-sdk-test", version: "0.1.0" });
  await client.connect(transport);

  try {
    await runChecks("Part B: SDK Client over StdioClientTransport", [
      {
        name: "listTools returns both tools with outputSchema",
        run: async () => {
          const { tools } = await client.listTools();
          assert.deepEqual(tools.map((t) => t.name).sort(), ["get_customer_record", "trigger_refund"]);
          assert.ok(tools.every((t) => t.outputSchema), "every tool should expose outputSchema");
        },
      },
      {
        name: "valid callTool returns structuredContent and the client validates it against outputSchema",
        run: async () => {
          const result = (await client.callTool({
            name: "get_customer_record",
            arguments: { customer_id: "CUST-00003" },
          })) as CallToolResult;
          assert.notEqual(result.isError, true);
          assert.equal((result.structuredContent as Record<string, unknown>)["status"], "suspended");
        },
      },
      {
        name: "valid trigger_refund via SDK succeeds",
        run: async () => {
          const result = (await client.callTool({
            name: "trigger_refund",
            arguments: { customer_id: "CUST-00001", amount: 12.34, reason: "goodwill credit after a late delivery" },
          })) as CallToolResult;
          assert.notEqual(result.isError, true);
          assert.equal((result.structuredContent as Record<string, unknown>)["amount"], "12.34");
          assert.equal((result.structuredContent as Record<string, unknown>)["remaining_balance"], "1238.16");
        },
      },
      {
        name: "invalid callTool throws McpError with code -32602 and Zod issues in data",
        run: async () => {
          await assert.rejects(
            client.callTool({ name: "trigger_refund", arguments: { customer_id: "nope", amount: -1, reason: "x" } }),
            (err: unknown) => {
              assert.ok(err instanceof McpError, `expected McpError, got ${String(err)}`);
              assert.equal(err.code, ErrorCode.InvalidParams);
              const issues = (err.data as { issues?: Array<{ path: unknown[] }> }).issues ?? [];
              const paths = issues.map((i) => String(i.path[0]));
              assert.ok(paths.includes("customer_id") && paths.includes("amount") && paths.includes("reason"), `issues: ${paths.join(",")}`);
              return true;
            },
          );
        },
      },
      {
        name: "unknown tool via SDK throws McpError -32602",
        run: async () => {
          await assert.rejects(client.callTool({ name: "nope", arguments: {} }), (err: unknown) => {
            assert.ok(err instanceof McpError);
            assert.equal(err.code, ErrorCode.InvalidParams);
            return true;
          });
        },
      },
      {
        name: "domain failure via SDK resolves (not throws) with isError true",
        run: async () => {
          const result = (await client.callTool({
            name: "trigger_refund",
            arguments: { customer_id: "CUST-00003", amount: 1, reason: "should be blocked by suspension" },
          })) as CallToolResult;
          assert.equal(result.isError, true);
        },
      },
    ]);
  } finally {
    await client.close();
  }
}

// ---------------------------------------------------------------------------
async function main(): Promise<void> {
  const raw = new RawServer();
  try {
    await runChecks("Part A: raw JSON-RPC over stdio", partA(raw));
  } finally {
    await raw.stop();
  }

  await partB();

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
