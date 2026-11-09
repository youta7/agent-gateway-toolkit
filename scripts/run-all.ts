// Runs the four task test suites in sequence and prints a summary.
// Each suite's own output streams through; this script only adds the
// scoreboard and the overall exit code.
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const tsxCli = join(root, "node_modules", "tsx", "dist", "cli.mjs");

const SUITES = [
  { name: "task1-mcp-server", file: "task1-mcp-server/test.ts" },
  { name: "task2-mcp-gateway", file: "task2-mcp-gateway/test.ts" },
  { name: "task3-llm-guardrail", file: "task3-llm-guardrail/test.ts" },
  { name: "task4-router", file: "task4-router/test.ts" },
] as const;

interface Outcome {
  name: string;
  code: number | null;
  signal: NodeJS.Signals | null;
  ms: number;
}

function runSuite(name: string, file: string): Promise<Outcome> {
  return new Promise((resolve) => {
    const started = Date.now();
    console.log(`\n${"=".repeat(72)}\n${name}  (${file})\n${"=".repeat(72)}`);
    const child = spawn(process.execPath, [tsxCli, join(root, file)], {
      cwd: root,
      env: { ...process.env, NODE_NO_WARNINGS: "1" },
      stdio: "inherit",
    });
    child.on("exit", (code, signal) => resolve({ name, code, signal, ms: Date.now() - started }));
    child.on("error", (err) => {
      console.error(`failed to start ${name}: ${err.message}`);
      resolve({ name, code: -1, signal: null, ms: Date.now() - started });
    });
  });
}

async function main(): Promise<void> {
  const only = process.argv.slice(2);
  const selected = only.length > 0 ? SUITES.filter((s) => only.some((o) => s.name.includes(o))) : [...SUITES];
  if (selected.length === 0) {
    console.error(`no suite matches ${only.join(", ")}; known: ${SUITES.map((s) => s.name).join(", ")}`);
    process.exit(2);
  }

  const outcomes: Outcome[] = [];
  for (const suite of selected) {
    outcomes.push(await runSuite(suite.name, suite.file));
  }

  const width = Math.max(...outcomes.map((o) => o.name.length));
  console.log(`\n${"=".repeat(72)}\nSummary\n${"=".repeat(72)}`);
  for (const o of outcomes) {
    const ok = o.code === 0;
    const detail = ok ? "" : o.signal ? `signal ${o.signal}` : `exit ${o.code}`;
    console.log(`${ok ? "PASS" : "FAIL"}  ${o.name.padEnd(width)}  ${(o.ms / 1000).toFixed(1).padStart(6)}s  ${detail}`);
  }
  const failed = outcomes.filter((o) => o.code !== 0);
  console.log(`\n${outcomes.length - failed.length}/${outcomes.length} suites passed`);
  process.exit(failed.length === 0 ? 0 : 1);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
