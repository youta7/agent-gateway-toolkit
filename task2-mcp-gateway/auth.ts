// Principal resolution for the gateway.
//
// Two credential types are accepted in the Authorization header:
//   1. Static bearer tokens from a fixed map (handy for local testing).
//   2. HS256 JWTs carrying a `role` claim, verified with node:crypto only.
//
// resolvePrincipal never throws: any malformed, tampered, expired, or
// otherwise unacceptable credential resolves to null and the caller treats
// the request as anonymous.
import { createHmac, timingSafeEqual } from "node:crypto";

export type Role = "admin" | "viewer";
export const ROLES: readonly Role[] = ["admin", "viewer"];

export interface Principal {
  sub: string;
  role: Role;
  via: "static" | "jwt";
}

/** Default signing secret for local development. Override with GATEWAY_JWT_SECRET. */
export const DEFAULT_JWT_SECRET = "dev-only-secret-change-me";

export function jwtSecret(): string {
  return process.env["GATEWAY_JWT_SECRET"] ?? DEFAULT_JWT_SECRET;
}

/** Static tokens. Values are the principals they map to. */
export const STATIC_TOKENS: ReadonlyMap<string, Omit<Principal, "via">> = new Map([
  ["tok-admin-1", { sub: "admin-1", role: "admin" }],
  ["tok-viewer-1", { sub: "viewer-1", role: "viewer" }],
]);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function isRole(v: unknown): v is Role {
  return typeof v === "string" && (ROLES as readonly string[]).includes(v);
}

/** Constant-time string equality. Lengths are compared first, which leaks only length. */
function safeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a, "utf8");
  const bb = Buffer.from(b, "utf8");
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}

function base64UrlEncode(input: Buffer | string): string {
  return Buffer.from(input).toString("base64url");
}

function base64UrlDecode(input: string): Buffer | null {
  // Reject anything outside the base64url alphabet so a padded or standard
  // base64 token is not quietly accepted.
  if (!/^[A-Za-z0-9_-]*$/.test(input)) return null;
  return Buffer.from(input, "base64url");
}

function parseJsonObject(buf: Buffer): Record<string, unknown> | null {
  try {
    const parsed: unknown = JSON.parse(buf.toString("utf8"));
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return null;
    return parsed as Record<string, unknown>;
  } catch {
    return null;
  }
}

function hs256(signingInput: string, secret: string): string {
  return createHmac("sha256", secret).update(signingInput).digest("base64url");
}

/** Extracts the bearer token from an Authorization header value, or null. */
export function extractBearer(authorizationHeader: string | undefined | null): string | null {
  if (typeof authorizationHeader !== "string") return null;
  const match = /^\s*Bearer\s+(\S+)\s*$/i.exec(authorizationHeader);
  return match?.[1] ?? null;
}

// ---------------------------------------------------------------------------
// JWT
// ---------------------------------------------------------------------------
export interface JwtPayload {
  sub?: string;
  role?: Role | string;
  exp?: number;
  nbf?: number;
  iat?: number;
  [claim: string]: unknown;
}

/**
 * Signs a payload as an HS256 JWT. Exported for tests and tooling; the
 * gateway itself only verifies.
 */
export function signJwt(payload: JwtPayload, secret: string = jwtSecret()): string {
  const header = base64UrlEncode(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = base64UrlEncode(JSON.stringify(payload));
  const signingInput = `${header}.${body}`;
  return `${signingInput}.${hs256(signingInput, secret)}`;
}

/**
 * Verifies an HS256 JWT and returns its payload, or null on any failure.
 * - alg is pinned to exactly "HS256" (no "none", no RS/ES downgrade tricks)
 * - signature is compared in constant time
 * - exp and nbf are honored when present (seconds since epoch, 30s leeway)
 */
export function verifyJwt(token: string, secret: string = jwtSecret(), nowSeconds = Math.floor(Date.now() / 1000)): JwtPayload | null {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const [h, p, s] = parts as [string, string, string];
    if (!h || !p || !s) return null;

    const headerBuf = base64UrlDecode(h);
    if (!headerBuf) return null;
    const header = parseJsonObject(headerBuf);
    if (!header) return null;
    if (header["alg"] !== "HS256") return null;
    if (header["typ"] !== undefined && header["typ"] !== "JWT") return null;

    // Signature must decode cleanly and match in constant time.
    if (!base64UrlDecode(s)) return null;
    const expected = hs256(`${h}.${p}`, secret);
    if (!safeEqual(expected, s)) return null;

    const payloadBuf = base64UrlDecode(p);
    if (!payloadBuf) return null;
    const payload = parseJsonObject(payloadBuf);
    if (!payload) return null;

    const leeway = 30;
    if (payload["exp"] !== undefined) {
      if (typeof payload["exp"] !== "number" || !Number.isFinite(payload["exp"])) return null;
      if (nowSeconds >= payload["exp"] + leeway) return null;
    }
    if (payload["nbf"] !== undefined) {
      if (typeof payload["nbf"] !== "number" || !Number.isFinite(payload["nbf"])) return null;
      if (nowSeconds + leeway < payload["nbf"]) return null;
    }

    return payload as JwtPayload;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Principal resolution
// ---------------------------------------------------------------------------
/**
 * Resolves an Authorization header to a principal, or null if the header is
 * missing, malformed, or carries an invalid credential. Never throws.
 */
export function resolvePrincipal(authorizationHeader: string | undefined | null): Principal | null {
  try {
    const token = extractBearer(authorizationHeader);
    if (!token) return null;

    // Static tokens. Iterate all entries and compare in constant time so the
    // lookup does not short-circuit on the first matching prefix.
    let matched: Omit<Principal, "via"> | null = null;
    for (const [candidate, principal] of STATIC_TOKENS) {
      if (safeEqual(candidate, token)) matched = principal;
    }
    if (matched) return { ...matched, via: "static" };

    // JWT.
    const payload = verifyJwt(token);
    if (!payload) return null;
    if (typeof payload.sub !== "string" || payload.sub.length === 0) return null;
    if (!isRole(payload.role)) return null;
    return { sub: payload.sub, role: payload.role, via: "jwt" };
  } catch {
    return null;
  }
}
