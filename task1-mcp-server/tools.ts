import { z } from "zod";
import { zodToJsonSchema } from "zod-to-json-schema";
import type { Tool } from "@modelcontextprotocol/sdk/types.js";

export const CUSTOMER_ID_PATTERN = /^CUST-\d{5}$/;

const customerId = z
  .string()
  .regex(CUSTOMER_ID_PATTERN, "customer_id must match ^CUST-\\d{5}$")
  .describe("Customer identifier in the form CUST-00000");

export const GetCustomerRecordInput = z
  .object({
    customer_id: customerId,
  })
  .strict();

export const GetCustomerRecordOutput = z
  .object({
    customer_id: customerId,
    name: z.string(),
    email: z.string(),
    status: z.enum(["active", "suspended"]),
    balance: z.string().describe("Current balance in dollars, two decimals, as a string to avoid float drift"),
  })
  .strict();

export const TriggerRefundInput = z
  .object({
    customer_id: customerId,
    amount: z
      .number()
      .finite("amount must be a finite number")
      .positive("amount must be greater than zero")
      .max(1_000_000, "amount must not exceed 1,000,000")
      .describe("Refund amount in dollars"),
    reason: z
      .string()
      .trim()
      .min(10, "reason must be at least 10 characters after trimming")
      .max(500, "reason must be at most 500 characters after trimming")
      .describe("Human-readable justification for the refund"),
  })
  .strict();

export const TriggerRefundOutput = z
  .object({
    refund_id: z.string(),
    customer_id: customerId,
    amount: z.string().describe("Refunded amount in dollars, two decimals"),
    remaining_balance: z.string().describe("Balance after the refund in dollars, two decimals"),
  })
  .strict();

export type GetCustomerRecordArgs = z.infer<typeof GetCustomerRecordInput>;
export type TriggerRefundArgs = z.infer<typeof TriggerRefundInput>;

/**
 * Converts a Zod object schema into the JSON Schema shape MCP expects for
 * inputSchema / outputSchema. zod-to-json-schema emits a top-level "$schema"
 * key that the MCP spec does not include, so it is removed.
 */
function toMcpSchema(schema: z.ZodTypeAny): Tool["inputSchema"] {
  const json = zodToJsonSchema(schema, { $refStrategy: "none" }) as Record<string, unknown>;
  delete json["$schema"];
  return json as Tool["inputSchema"];
}

export const TOOL_DEFINITIONS: Tool[] = [
  {
    name: "get_customer_record",
    description: "Look up a customer by id and return their profile, status, and balance.",
    inputSchema: toMcpSchema(GetCustomerRecordInput),
    outputSchema: toMcpSchema(GetCustomerRecordOutput),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  },
  {
    name: "trigger_refund",
    description:
      "Issue a refund to a customer. Fails if the customer is unknown, suspended, or lacks sufficient balance.",
    inputSchema: toMcpSchema(TriggerRefundInput),
    outputSchema: toMcpSchema(TriggerRefundOutput),
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: false },
  },
];
