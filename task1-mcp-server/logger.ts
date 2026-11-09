// Minimal structured logger. Everything goes to stderr because stdout is the
// MCP stdio transport channel; a stray console.log there would corrupt the
// JSON-RPC stream.
export type LogLevel = "debug" | "info" | "warn" | "error";

export function log(level: LogLevel, event: string, fields: Record<string, unknown> = {}): void {
  const line = JSON.stringify({ ts: new Date().toISOString(), level, event, ...fields });
  process.stderr.write(line + "\n");
}
