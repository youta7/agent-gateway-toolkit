// Structured stderr logger plus stdout hardening for the stdio transport.
//
// stdout is the JSON-RPC channel. Anything else written there (a stray
// console.log, a library printing a banner, a debug dump) corrupts the stream
// and the client sees a parse error or, worse, silently drops frames. So:
//   - log() always writes to stderr
//   - lockStdout() rebinds console.* to stderr and diverts non-frame writes
//     from process.stdout to stderr with a warning. Transport frames pass.
import { Console } from "node:console";

export type LogLevel = "debug" | "info" | "warn" | "error";

export function log(level: LogLevel, event: string, fields: Record<string, unknown> = {}): void {
  const line = JSON.stringify({ ts: new Date().toISOString(), level, event, ...fields });
  process.stderr.write(line + "\n");
}

/**
 * A chunk "looks like" a JSON-RPC frame if, ignoring leading whitespace, it
 * starts with "{" and mentions the jsonrpc member. Exported so tests can pin
 * the heuristic.
 */
export function looksLikeJsonRpcFrame(chunk: string): boolean {
  const trimmed = chunk.trimStart();
  return trimmed.startsWith("{") && trimmed.includes('"jsonrpc"');
}

type StdoutWrite = typeof process.stdout.write;

/**
 * Locks stdout to JSON-RPC frames. Returns a function that restores the
 * previous state (used by tests; the server never unlocks).
 *
 * Must run before anything else can write to stdout. The SDK transport goes
 * through process.stdout.write at call time, so wrapping the method is enough
 * for its frames to pass through untouched.
 */
export function lockStdout(): () => void {
  const originalWrite: StdoutWrite = process.stdout.write.bind(process.stdout) as StdoutWrite;
  const originalConsole = {
    log: console.log,
    info: console.info,
    debug: console.debug,
    warn: console.warn,
  };

  // Everything console-shaped goes to stderr with normal formatting.
  const stderrConsole = new Console({ stdout: process.stderr, stderr: process.stderr });
  console.log = stderrConsole.log.bind(stderrConsole);
  console.info = stderrConsole.info.bind(stderrConsole);
  console.debug = stderrConsole.debug.bind(stderrConsole);
  console.warn = stderrConsole.warn.bind(stderrConsole);

  const guardedWrite = function (
    this: unknown,
    chunk: Uint8Array | string,
    encodingOrCb?: BufferEncoding | ((err?: Error | null) => void),
    cb?: (err?: Error | null) => void,
  ): boolean {
    const text = typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
    if (looksLikeJsonRpcFrame(text)) {
      // Forward with the exact original argument list so drain/callback
      // semantics stay intact for the transport.
      return (originalWrite as (...args: unknown[]) => boolean)(chunk, encodingOrCb, cb);
    }

    log("warn", "stdout_diverted", {
      reason: "non JSON-RPC write to stdout",
      bytes: text.length,
      preview: text.slice(0, 200),
    });
    process.stderr.write(text);

    const callback = typeof encodingOrCb === "function" ? encodingOrCb : cb;
    if (callback) queueMicrotask(() => callback(null));
    return true;
  } as StdoutWrite;

  process.stdout.write = guardedWrite;

  return () => {
    process.stdout.write = originalWrite;
    console.log = originalConsole.log;
    console.info = originalConsole.info;
    console.debug = originalConsole.debug;
    console.warn = originalConsole.warn;
  };
}
