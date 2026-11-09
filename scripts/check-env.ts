// Verifies the runtime this repo depends on: Node >= 22.5 with built-in node:sqlite and fetch.
import { DatabaseSync } from "node:sqlite";

const [major, minor] = process.versions.node.split(".").map(Number);
if (major === undefined || minor === undefined || major < 22 || (major === 22 && minor < 5)) {
  console.error(`Node 22.5+ required, found ${process.versions.node}`);
  process.exit(1);
}

const db = new DatabaseSync(":memory:");
const row = db.prepare("select sqlite_version() as version").get() as { version: string };
db.close();

console.log(`node ${process.versions.node}`);
console.log(`node:sqlite ${row.version}`);
console.log(`fetch ${typeof fetch === "function" ? "available" : "MISSING"}`);
