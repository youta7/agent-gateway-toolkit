// End-to-end test: spawns server.ts over stdio via the SDK client and checks
// that protocol errors and domain failures arrive on the right channel. Also
// covers stdout hardening (lockStdout) and raw-wire malformed input.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
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

interface Check {
  name: string;
  run: (client: Client) => Promise<void>;
}

async function expectProtocolError(
  client: Client,
  name: string,
  args: Record<string, unknown>,
  expectedCode: ErrorCode,
): Promise<McpError> {
  try {
    await client.callTool({ name, arguments: args });
  } catch (err) {
    assert.ok(err instanceof McpError, `expected McpError, got ${String(err)}`);
    assert.equal(err.code, expectedCode, `expected code ${expectedCode}, got ${err.code}: ${err.message}`);
    return err;
  }
  assert.fail(`expected tools/call ${name} to be rejected with ${expectedCode}`);
}

async function callTool(client: Client, name: string, args: Record<string, unknown>): Promise<CallToolResult> {
  return (await client.callTool({ name, arguments: args })) as CallToolResult;
}

function parseText(result: CallToolResult): Record<string, unknown> {
  const first = result.content[0];
  assert.ok(first && first.type === "text", "expected a text content block");
  return JSON.parse(first.text) as Record<string, unknown>;
}

function issuesOf(err: McpError): Array<{ path: unknown[]; code: string }> {
  const data = err.data as { issues?: Array<{ path: unknown[]; code: string }> } | undefined;
  assert.ok(data?.issues && data.issues.length > 0, "expected Zod issues in error.data.issues");
  return data.issues;
}

// ---------------------------------------------------------------------------
// Stdout hardening: in-process unit checks on lockStdout().
// ---------------------------------------------------------------------------
async function checkLockStdout(): Promise<void> {
  const stdoutWrites: string[] = [];
  const stderrWrites: string[] = [];
  const realStdout = process.stdout.write;
  const realStderr = process.stderr.write;

  // Spy on both streams, then lock. lockStdout captures the spy as "original",
  // so anything that passes the guard lands in stdoutWrites.
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
    console.log("hello from console.log");
    console.info("info line");
    console.debug("debug line");
    console.warn("warn line");
    process.stdout.write("stray banner\n");
    process.stdout.write(Buffer.from("buffer junk\n"));
    let cbCalled = false;
    process.stdout.write("junk with cb\n", () => {
      cbCalled = true;
    });
    const frame = '{"jsonrpc":"2.0","id":1,"result":{}}\n';
    const notification = '  {"jsonrpc":"2.0","method":"notifications/x"}\n';
    process.stdout.write(frame);
    process.stdout.write(notification);
    await new Promise((r) => setImmediate(r));

    assert.deepEqual(stdoutWrites, [frame, notification], "only frames reach stdout");
    assert.ok(cbCalled, "diverted write still invokes its callback");
    const stderrText = stderrWrites.join("");
    const needles = ["hello from console.log", "info line", "debug line", "warn line", "stray banner", "buffer junk", "junk with cb"];
    for (const needle of needles) {
      assert.ok(stderrText.includes(needle), `stderr should contain ${JSON.stringify(needle)}`);
    }
    const warnings = stderrWrites.filter((w) => w.includes('"event":"stdout_diverted"'));
    assert.equal(warnings.length, 3, "one structured warning per diverted write");

    assert.equal(looksLikeJsonRpcFrame('{"jsonrpc":"2.0"}'), true);
    assert.equal(looksLikeJsonRpcFrame('\n {"jsonrpc": "2.0"}'), true);
    assert.equal(looksLikeJsonRpcFrame('{"foo":1}'), false);
    assert.equal(looksLikeJsonRpcFrame('jsonrpc {"x":1}'), false);
  } finally {
    unlock();
    process.stdout.write = realStdout;
    process.stderr.write = realStderr;
  }
  assert.equal(process.stdout.write, realStdout, "unlock restores stdout.write");
}

// ---------------------------------------------------------------------------
// Raw wire: feed malformed lines straight into the server's stdin and inspect
// every byte it emits on stdout. The SDK client cannot send these, so we use a
// bare child process.
// ---------------------------------------------------------------------------
interface RawFrame {
  id?: unknown;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
}

async function checkRawWire(): Promise<void> {
  const child = spawn(process.execPath, [tsxCli, serverPath], { stdio: ["pipe", "pipe", "pipe"] });
  const stderr: string[] = [];
  child.stderr.on("data", (c: Buffer) => stderr.push(c.toString()));

  const lines: string[] = [];
  let pending = "";
  child.stdout.on("data", (c: Buffer) => {
    pending += c.toString();
    let nl: number;
    while ((nl = pending.indexOf("\n")) !== -1) {
      lines.push(pending.slice(0, nl));
      pending = pending.slice(nl + 1);
    }
  });

  const gotId = (id: number): Promise<void> =>
    new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`timed out waiting for id ${id}`)), 15_000);
      const check = () => {
        const seen = lines.some((l) => {
          try {
            return (JSON.parse(l) as RawFrame).id === id;
          } catch {
            return false;
          }
        });
        if (seen) {
          clearTimeout(timer);
          child.stdout.off("data", check);
          resolve();
        }
      };
      child.stdout.on("data", check);
      check();
    });

  const send = (line: string) => child.stdin.write(line + "\n");
  try {
    send(
      JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
        params: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "raw", version: "0" } },
      }),
    );
    await gotId(1);
    send(JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }));

    send("this is not json at all");
    send("{not even close");
    send(JSON.stringify({ foo: "bar" })); // valid JSON, invalid envelope
    send(JSON.stringify({ jsonrpc: "1.0", id: 5, method: "tools/list" })); // wrong version
    send(JSON.stringify({ jsonrpc: "2.0", id: 9, method: "tools/list" })); // still alive?
    await gotId(9);

    const frames = lines.map((l) => JSON.parse(l) as RawFrame);
    const parseErrors = frames.filter((f) => f.error?.code === -32700);
    const invalidRequests = frames.filter((f) => f.error?.code === -32600);
    assert.equal(parseErrors.length, 2, `expected two -32700 frames, got ${JSON.stringify(frames)}`);
    assert.equal(invalidRequests.length, 2, `expected two -32600 frames, got ${JSON.stringify(frames)}`);
    for (const f of [...parseErrors, ...invalidRequests]) {
      assert.ok("id" in f && f.id === null, `malformed-frame response must carry id null: ${JSON.stringify(f)}`);
    }
    assert.equal(parseErrors[0]?.error?.message, "Parse error");
    assert.equal(invalidRequests[0]?.error?.message, "Invalid Request");
    const data = invalidRequests[0]?.error?.data as { issues?: unknown } | undefined;
    assert.ok(Array.isArray(data?.issues), "envelope issues attached to -32600 data");

    const alive = frames.find((f) => f.id === 9);
    assert.ok(alive?.result, "server keeps serving after malformed input");

    // Every single stdout line must be a JSON-RPC frame: nothing else leaked.
    for (const l of lines) {
      assert.ok(looksLikeJsonRpcFrame(l), `non-frame on stdout: ${l}`);
    }
    assert.equal(pending, "", "no partial trailing garbage on stdout");

    assert.ok(stderr.join("").includes('"event":"malformed_frame"'), "server logged malformed frames to stderr");
  } finally {
    child.stdin.end();
    child.kill();
    await once(child, "exit").catch(() => undefined);
  }
}

const standalone: Check[] = [
  { name: "lockStdout diverts non-frames to stderr and passes frames", run: () => checkLockStdout() },
  { name: "raw wire: bad JSON -> -32700, bad envelope -> -32600, server survives", run: () => checkRawWire() },
];

const checks: Check[] = [
  {
    name: "tools/list exposes both tools with schemas and no $schema key",
    run: async (client) => {
      const { tools } = await client.listTools();
      const names = tools.map((t) => t.name).sort();
      assert.deepEqual(names, ["get_customer_record", "trigger_refund"]);
      for (const tool of tools) {
        assert.equal(tool.inputSchema.type, "object");
        assert.equal(tool.inputSchema.additionalProperties, false, `${tool.name} input should be strict`);
        assert.ok(!("$schema" in tool.inputSchema), `${tool.name} inputSchema leaked $schema`);
        assert.ok(tool.outputSchema, `${tool.name} missing outputSchema`);
        assert.ok(!("$schema" in tool.outputSchema), `${tool.name} outputSchema leaked $schema`);
      }
      const refund = tools.find((t) => t.name === "trigger_refund");
      assert.deepEqual(refund?.inputSchema.required?.slice().sort(), ["amount", "customer_id", "reason"]);
    },
  },
  {
    name: "unknown tool -> -32602 protocol error",
    run: async (client) => {
      const err = await expectProtocolError(client, "does_not_exist", {}, ErrorCode.InvalidParams);
      assert.match(err.message, /Unknown tool/);
    },
  },
  {
    name: "bad customer_id format -> -32602 with Zod issues in data",
    run: async (client) => {
      const err = await expectProtocolError(
        client,
        "get_customer_record",
        { customer_id: "cust-1" },
        ErrorCode.InvalidParams,
      );
      const issues = issuesOf(err);
      assert.deepEqual(issues[0]?.path, ["customer_id"]);
    },
  },
  {
    name: "extra property rejected by .strict() -> -32602",
    run: async (client) => {
      const err = await expectProtocolError(
        client,
        "get_customer_record",
        { customer_id: "CUST-00001", extra: true },
        ErrorCode.InvalidParams,
      );
      assert.ok(issuesOf(err).some((i) => i.code === "unrecognized_keys"));
    },
  },
  {
    name: "missing arguments entirely -> -32602",
    run: async (client) => {
      await expectProtocolError(client, "trigger_refund", {}, ErrorCode.InvalidParams);
    },
  },
  {
    name: "refund with negative amount, > 1e6, and short reason -> -32602 listing each field",
    run: async (client) => {
      const err = await expectProtocolError(
        client,
        "trigger_refund",
        { customer_id: "CUST-00001", amount: -5, reason: "short" },
        ErrorCode.InvalidParams,
      );
      const paths = issuesOf(err).map((i) => i.path.join("."));
      assert.ok(paths.includes("amount"), `expected amount issue, got ${paths.join(",")}`);
      assert.ok(paths.includes("reason"), `expected reason issue, got ${paths.join(",")}`);

      await expectProtocolError(
        client,
        "trigger_refund",
        { customer_id: "CUST-00001", amount: 1_000_000.01, reason: "a perfectly valid reason" },
        ErrorCode.InvalidParams,
      );
      await expectProtocolError(
        client,
        "trigger_refund",
        { customer_id: "CUST-00001", amount: 10, reason: "         padded         " },
        ErrorCode.InvalidParams,
      );
    },
  },
  {
    name: "get_customer_record happy path returns structuredContent",
    run: async (client) => {
      const result = await callTool(client, "get_customer_record", { customer_id: "CUST-00001" });
      assert.notEqual(result.isError, true);
      assert.deepEqual(result.structuredContent, {
        customer_id: "CUST-00001",
        name: "Ada Lovelace",
        email: "ada@example.com",
        status: "active",
        balance: "1250.50",
      });
    },
  },
  {
    name: "unknown customer -> isError domain result, not a protocol error",
    run: async (client) => {
      const result = await callTool(client, "get_customer_record", { customer_id: "CUST-99999" });
      assert.equal(result.isError, true);
      assert.equal(parseText(result)["error"], "CUSTOMER_NOT_FOUND");
    },
  },
  {
    name: "suspended customer refund -> isError CUSTOMER_SUSPENDED",
    run: async (client) => {
      const result = await callTool(client, "trigger_refund", {
        customer_id: "CUST-00003",
        amount: 1,
        reason: "customer requested a refund",
      });
      assert.equal(result.isError, true);
      assert.equal(parseText(result)["error"], "CUSTOMER_SUSPENDED");
    },
  },
  {
    name: "refund over balance -> isError INSUFFICIENT_BALANCE",
    run: async (client) => {
      const result = await callTool(client, "trigger_refund", {
        customer_id: "CUST-00002",
        amount: 50,
        reason: "duplicate charge on invoice 42",
      });
      assert.equal(result.isError, true);
      const body = parseText(result);
      assert.equal(body["error"], "INSUFFICIENT_BALANCE");
      assert.equal(body["available"], "49.99");
    },
  },
  {
    name: "successful refund uses exact cent math (0.1 + 0.2 style amounts)",
    run: async (client) => {
      const result = await callTool(client, "trigger_refund", {
        customer_id: "CUST-00002",
        amount: 0.3,
        reason: "partial goodwill credit",
      });
      assert.notEqual(result.isError, true);
      const body = result.structuredContent as Record<string, unknown>;
      assert.match(String(body["refund_id"]), /^RF-/);
      assert.equal(body["amount"], "0.30");
      assert.equal(body["remaining_balance"], "49.69");

      // Second refund drains the account to exactly zero.
      const drain = await callTool(client, "trigger_refund", {
        customer_id: "CUST-00002",
        amount: 49.69,
        reason: "closing the account per request",
      });
      assert.notEqual(drain.isError, true);
      assert.equal((drain.structuredContent as Record<string, unknown>)["remaining_balance"], "0.00");
    },
  },
];

async function runCheck(check: Check, client: Client): Promise<boolean> {
  try {
    await check.run(client);
    console.log(`PASS  ${check.name}`);
    return true;
  } catch (err) {
    console.log(`FAIL  ${check.name}`);
    console.log(`      ${err instanceof Error ? err.message : String(err)}`);
    return false;
  }
}

async function main(): Promise<void> {
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [tsxCli, serverPath],
    stderr: "pipe",
  });
  const client = new Client({ name: "task1-test", version: "0.1.0" });

  const serverLogs: string[] = [];
  transport.stderr?.on("data", (chunk: Buffer) => serverLogs.push(chunk.toString()));

  await client.connect(transport);

  let failed = 0;
  for (const check of [...standalone, ...checks]) {
    if (!(await runCheck(check, client))) failed++;
  }

  await client.close();

  if (process.env["SHOW_SERVER_LOGS"]) {
    console.log("\n--- server stderr ---");
    console.log(serverLogs.join(""));
  }

  const total = standalone.length + checks.length;
  console.log(`\n${total - failed}/${total} passed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
