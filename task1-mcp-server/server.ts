import { randomUUID } from "node:crypto";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
  type CallToolResult,
} from "@modelcontextprotocol/sdk/types.js";
import type { z } from "zod";
import { customers, fromCents, toCents } from "./customers.js";
import { log } from "./logger.js";
import {
  GetCustomerRecordInput,
  TOOL_DEFINITIONS,
  TriggerRefundInput,
  type GetCustomerRecordArgs,
  type TriggerRefundArgs,
} from "./tools.js";

/*
 * Two different failure channels, deliberately kept apart:
 *
 * 1. Protocol errors (thrown McpError -> JSON-RPC `error` object with a code).
 *    These mean the *request itself* was malformed: unknown tool, arguments
 *    that fail schema validation, and so on. The caller (an agent runtime or
 *    gateway) should treat these as bugs in how it called us, not as outcomes
 *    of the operation. Zod issues ride along in `error.data` so the caller can
 *    see exactly which field was wrong. We use the low-level `Server` class
 *    because `McpServer` would catch these and downgrade them to isError
 *    results, losing the code.
 *
 * 2. Domain failures (CallToolResult with isError: true).
 *    The request was well-formed, we executed it, and the business answer is
 *    "no": customer not found, account suspended, refund exceeds balance.
 *    These are legitimate tool outcomes that an LLM should read and reason
 *    about (e.g. tell the user, or try a smaller amount), so they belong in
 *    the tool result, not in the transport-level error channel.
 */

const server = new Server(
  { name: "agent-gateway-toolkit/task1-mcp-server", version: "0.1.0" },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => {
  log("debug", "tools/list");
  return { tools: TOOL_DEFINITIONS };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: rawArgs } = request.params;
  log("info", "tools/call", { tool: name });

  switch (name) {
    case "get_customer_record":
      return getCustomerRecord(validate(GetCustomerRecordInput, rawArgs, name));
    case "trigger_refund":
      return triggerRefund(validate(TriggerRefundInput, rawArgs, name));
    default:
      // Unknown tool is a caller mistake, so it is a protocol error (-32602),
      // not a domain failure.
      log("warn", "unknown_tool", { tool: name });
      throw new McpError(ErrorCode.InvalidParams, `Unknown tool: ${name}`, {
        tool: name,
        known_tools: TOOL_DEFINITIONS.map((t) => t.name),
      });
  }
});

/** Parses arguments with a Zod schema, converting failures into InvalidParams. */
function validate<T extends z.ZodTypeAny>(schema: T, rawArgs: unknown, tool: string): z.infer<T> {
  const parsed = schema.safeParse(rawArgs ?? {});
  if (!parsed.success) {
    log("warn", "invalid_params", { tool, issues: parsed.error.issues });
    throw new McpError(ErrorCode.InvalidParams, `Invalid arguments for tool ${tool}`, {
      tool,
      issues: parsed.error.issues,
    });
  }
  return parsed.data;
}

function domainError(code: string, message: string, extra: Record<string, unknown> = {}): CallToolResult {
  log("info", "domain_error", { code, ...extra });
  return {
    isError: true,
    content: [{ type: "text", text: JSON.stringify({ error: code, message, ...extra }) }],
  };
}

function success(structured: Record<string, unknown>): CallToolResult {
  return {
    content: [{ type: "text", text: JSON.stringify(structured) }],
    structuredContent: structured,
  };
}

function getCustomerRecord(args: GetCustomerRecordArgs): CallToolResult {
  const customer = customers.get(args.customer_id);
  if (!customer) {
    return domainError("CUSTOMER_NOT_FOUND", `No customer with id ${args.customer_id}`, {
      customer_id: args.customer_id,
    });
  }
  return success({
    customer_id: customer.customerId,
    name: customer.name,
    email: customer.email,
    status: customer.status,
    balance: fromCents(customer.balanceCents),
  });
}

function triggerRefund(args: TriggerRefundArgs): CallToolResult {
  const customer = customers.get(args.customer_id);
  if (!customer) {
    return domainError("CUSTOMER_NOT_FOUND", `No customer with id ${args.customer_id}`, {
      customer_id: args.customer_id,
    });
  }
  if (customer.status === "suspended") {
    return domainError("CUSTOMER_SUSPENDED", `Customer ${customer.customerId} is suspended; refunds are blocked`, {
      customer_id: customer.customerId,
    });
  }

  const amountCents = toCents(args.amount);
  if (amountCents <= 0) {
    // e.g. 0.001 rounds to zero cents. Well-formed input, but nothing to refund.
    return domainError("AMOUNT_TOO_SMALL", "Refund amount rounds to zero cents", { amount: args.amount });
  }
  if (amountCents > customer.balanceCents) {
    return domainError("INSUFFICIENT_BALANCE", "Refund exceeds the customer's available balance", {
      customer_id: customer.customerId,
      requested: fromCents(amountCents),
      available: fromCents(customer.balanceCents),
    });
  }

  customer.balanceCents -= amountCents;
  const refundId = `RF-${randomUUID()}`;
  log("info", "refund_issued", {
    refund_id: refundId,
    customer_id: customer.customerId,
    amount_cents: amountCents,
    remaining_cents: customer.balanceCents,
  });
  return success({
    refund_id: refundId,
    customer_id: customer.customerId,
    amount: fromCents(amountCents),
    remaining_balance: fromCents(customer.balanceCents),
  });
}

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  server.onerror = (err) => log("error", "server_error", { message: err.message });
  await server.connect(transport);
  log("info", "server_started", { transport: "stdio", tools: TOOL_DEFINITIONS.map((t) => t.name) });
}

main().catch((err: unknown) => {
  log("error", "fatal", { message: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
