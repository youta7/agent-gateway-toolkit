// Model router: primary-with-failover for chat completions.
//
// Failover is deliberately narrow. Only outcomes that say "the primary could
// not take this request right now" are retried on the secondary:
//   - HTTP 429            (over capacity)
//   - timeout             (no answer within primaryTimeoutMs)
//   - connection failure  (refused, reset, DNS)
// Anything else the primary says is a verdict, not an availability signal,
// and is returned to the caller sanitised. See `attempt()` for the reasoning.

export type GatewayErrorType =
  | "bad_request"
  | "unauthenticated"
  | "rate_limited"
  | "upstream_timeout"
  | "upstream_unavailable"
  | "upstream_error"
  | "internal";

export class GatewayError extends Error {
  readonly type: GatewayErrorType;
  readonly status: number;
  readonly extra: Record<string, unknown>;

  constructor(type: GatewayErrorType, status: number, message: string, extra: Record<string, unknown> = {}) {
    super(message);
    this.name = "GatewayError";
    this.type = type;
    this.status = status;
    this.extra = extra;
  }

  toPayload(requestId: string): { error: Record<string, unknown> } {
    return { error: { type: this.type, message: this.message, request_id: requestId, ...this.extra } };
  }
}

export interface Provider {
  name: string;
  url: string;
}

export interface RouterOptions {
  primary: Provider;
  secondary: Provider;
  /** Per-attempt timeout for the primary (default 3000ms). */
  primaryTimeoutMs?: number;
  /** Per-attempt timeout for the secondary (default: same as primary). */
  secondaryTimeoutMs?: number;
  /** Extra headers sent to providers (e.g. their API key). */
  providerHeaders?: Record<string, string>;
  log?: (level: "info" | "warn" | "error", event: string, fields: Record<string, unknown>) => void;
}

export interface CompletionUsage {
  prompt_tokens?: number;
  completion_tokens?: number;
  total_tokens?: number;
}

export interface RouteResult {
  provider: string;
  status: number;
  body: Record<string, unknown>;
  usage: CompletionUsage | null;
  attempts: Attempt[];
}

export type AttemptOutcome = "ok" | "rate_limited" | "timeout" | "unreachable" | "upstream_error" | "bad_response";

export interface Attempt {
  provider: string;
  outcome: AttemptOutcome;
  status?: number;
  ms: number;
}

type AttemptResult =
  | { kind: "success"; result: RouteResult }
  | { kind: "failover"; attempt: Attempt }
  | { kind: "final"; attempt: Attempt; error: GatewayError };

const ROUTE = "/v1/chat/completions";
const MAX_LOGGED_BODY = 2000;

function defaultLog(level: "info" | "warn" | "error", event: string, fields: Record<string, unknown>): void {
  process.stderr.write(JSON.stringify({ ts: new Date().toISOString(), level, event, ...fields }) + "\n");
}

export class ModelRouter {
  private readonly primary: Provider;
  private readonly secondary: Provider;
  private readonly primaryTimeoutMs: number;
  private readonly secondaryTimeoutMs: number;
  private readonly providerHeaders: Record<string, string>;
  private readonly log: NonNullable<RouterOptions["log"]>;

  constructor(options: RouterOptions) {
    this.primary = options.primary;
    this.secondary = options.secondary;
    this.primaryTimeoutMs = options.primaryTimeoutMs ?? 3000;
    this.secondaryTimeoutMs = options.secondaryTimeoutMs ?? this.primaryTimeoutMs;
    this.providerHeaders = options.providerHeaders ?? {};
    this.log = options.log ?? defaultLog;
  }

  /**
   * Completes `body` on the primary, failing over to the secondary only for
   * availability-class outcomes. Throws GatewayError on any final failure.
   */
  async complete(body: Record<string, unknown>, requestId: string): Promise<RouteResult> {
    const attempts: Attempt[] = [];
    const payload = JSON.stringify(body);

    const first = await this.attempt(this.primary, payload, this.primaryTimeoutMs, requestId);
    if (first.kind === "success") {
      first.result.attempts = attempts.concat(first.result.attempts);
      return first.result;
    }
    attempts.push(first.attempt);
    if (first.kind === "final") {
      first.error.extra["attempts"] = attempts;
      throw first.error;
    }

    this.log("warn", "failover", { request_id: requestId, from: this.primary.name, to: this.secondary.name, reason: first.attempt.outcome });
    const second = await this.attempt(this.secondary, payload, this.secondaryTimeoutMs, requestId);
    if (second.kind === "success") {
      second.result.attempts = attempts.concat(second.result.attempts);
      return second.result;
    }
    attempts.push(second.attempt);
    if (second.kind === "final") {
      second.error.extra["attempts"] = attempts;
      throw second.error;
    }

    // Both providers were unavailable. Status follows the last outcome.
    const last = second.attempt.outcome;
    if (last === "timeout") {
      throw new GatewayError("upstream_timeout", 504, "All providers timed out", { attempts });
    }
    throw new GatewayError("upstream_unavailable", 503, "All providers are unavailable", { attempts });
  }

  private async attempt(provider: Provider, payload: string, timeoutMs: number, requestId: string): Promise<AttemptResult> {
    const started = Date.now();
    const elapsed = () => Date.now() - started;
    const done = (outcome: AttemptOutcome, status?: number): Attempt =>
      status === undefined ? { provider: provider.name, outcome, ms: elapsed() } : { provider: provider.name, outcome, status, ms: elapsed() };

    let res: Response;
    try {
      res = await fetch(`${provider.url.replace(/\/+$/, "")}${ROUTE}`, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json", "x-request-id": requestId, ...this.providerHeaders },
        body: payload,
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (err) {
      const name = err instanceof Error ? err.name : "";
      const isTimeout = name === "TimeoutError" || name === "AbortError";
      const attempt = done(isTimeout ? "timeout" : "unreachable");
      this.log("warn", "provider_attempt", {
        request_id: requestId,
        ...attempt,
        reason: err instanceof Error ? (err.cause instanceof Error ? `${err.message}: ${err.cause.message}` : err.message) : String(err),
      });
      return { kind: "failover", attempt };
    }

    if (res.status === 429) {
      await res.text().catch(() => "");
      const attempt = done("rate_limited", 429);
      this.log("warn", "provider_attempt", { request_id: requestId, ...attempt, retry_after: res.headers.get("retry-after") });
      return { kind: "failover", attempt };
    }

    if (!res.ok) {
      // A definite answer from a live provider is not an availability signal:
      //  - 400/422: the request itself is unacceptable. The secondary speaks
      //    the same API and would reject it the same way; retrying just
      //    doubles the tenant's spend on a request that cannot succeed.
      //  - 401/403: our credentials for this provider are wrong. Operator
      //    problem, and the secondary has its own credentials; silently
      //    routing around a misconfiguration hides it.
      //  - 500: the provider accepted the request and failed while handling
      //    it. It may already have been billed or had side effects; a blind
      //    retry risks double charging and, during an incident, amplifies load.
      // The upstream body is logged under the request id but never relayed:
      // it routinely contains stack traces and internal hostnames.
      const text = await res.text().catch(() => "");
      const attempt = done("upstream_error", res.status);
      this.log("error", "provider_attempt", { request_id: requestId, ...attempt, upstream_body: text.slice(0, MAX_LOGGED_BODY) });
      const isRequestShaped = res.status === 400 || res.status === 404 || res.status === 413 || res.status === 422;
      const error = isRequestShaped
        ? new GatewayError("bad_request", 400, "The upstream provider rejected the request as invalid", { provider: provider.name, upstream_status: res.status })
        : new GatewayError("upstream_error", 502, "The upstream provider returned an error", { provider: provider.name, upstream_status: res.status });
      return { kind: "final", attempt, error };
    }

    let parsed: unknown;
    const text = await res.text();
    try {
      parsed = JSON.parse(text);
    } catch {
      const attempt = done("bad_response", res.status);
      this.log("error", "provider_attempt", { request_id: requestId, ...attempt, upstream_body: text.slice(0, MAX_LOGGED_BODY) });
      return {
        kind: "final",
        attempt,
        error: new GatewayError("upstream_error", 502, "The upstream provider returned an unreadable response", { provider: provider.name }),
      };
    }
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      const attempt = done("bad_response", res.status);
      this.log("error", "provider_attempt", { request_id: requestId, ...attempt, upstream_body: text.slice(0, MAX_LOGGED_BODY) });
      return {
        kind: "final",
        attempt,
        error: new GatewayError("upstream_error", 502, "The upstream provider returned an unexpected response", { provider: provider.name }),
      };
    }

    const attempt = done("ok", res.status);
    this.log("info", "provider_attempt", { request_id: requestId, ...attempt });
    const bodyObj = parsed as Record<string, unknown>;
    const usage = bodyObj["usage"] && typeof bodyObj["usage"] === "object" ? (bodyObj["usage"] as CompletionUsage) : null;
    return { kind: "success", result: { provider: provider.name, status: res.status, body: bodyObj, usage, attempts: [attempt] } };
  }
}
