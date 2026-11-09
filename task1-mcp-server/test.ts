// End-to-end test: spawns server.ts over stdio via the SDK client and checks
// that protocol errors and domain failures arrive on the right channel.
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { ErrorCode, McpError, type CallToolResult } from "@modelcontextprotocol/sdk/types.js";

const here = dirname(fileURLToPath(import.meta.url));
const serverPath = join(here, "server.ts");

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

async function main(): Promise<void> {
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [join(here, "..", "node_modules", "tsx", "dist", "cli.mjs"), serverPath],
    stderr: "pipe",
  });
  const client = new Client({ name: "task1-test", version: "0.1.0" });

  const serverLogs: string[] = [];
  transport.stderr?.on("data", (chunk: Buffer) => serverLogs.push(chunk.toString()));

  await client.connect(transport);

  let failed = 0;
  for (const check of checks) {
    try {
      await check.run(client);
      console.log(`PASS  ${check.name}`);
    } catch (err) {
      failed++;
      console.log(`FAIL  ${check.name}`);
      console.log(`      ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  await client.close();

  if (process.env["SHOW_SERVER_LOGS"]) {
    console.log("\n--- server stderr ---");
    console.log(serverLogs.join(""));
  }

  console.log(`\n${checks.length - failed}/${checks.length} passed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
