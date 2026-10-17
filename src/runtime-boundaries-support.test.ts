import { Console } from "node:console";
import { Writable } from "node:stream";
import { inspect } from "node:util";
import type { ClickHouseClientConfig } from "./runtime";

/** Synthetic fixture marker, not an account credential. */
export const dsnMarker = "AUDIT_DSN_SYNTHETIC_SECRET_MARKER";
export const dsnUsername = "audit_fixture_user";
export const validDsn = `http://${dsnUsername}:${dsnMarker}@127.0.0.1:8123/ck_orm_audit_fixture`;
export const invalidDsns = [
  ["invalid-port", `http://${dsnUsername}:${dsnMarker}@127.0.0.1:INVALID/ck_orm_audit_fixture`],
  ["invalid-host", `http://${dsnUsername}:${dsnMarker}@[not-an-ip]/ck_orm_audit_fixture`],
] as const;

export function serializeError(error: unknown) {
  let consoleError = "";
  const sink = new Writable({
    write(chunk: Uint8Array | string, _encoding: string, done: (error?: Error | null) => void) {
      consoleError += chunk.toString();
      done();
    },
  });
  new Console({ stdout: sink, stderr: sink }).error(error);
  sink.end();
  const seen = new WeakSet<object>();
  const detailJson = JSON.stringify(error, (_key, value) => {
    if (value instanceof Error) {
      if (seen.has(value)) {
        return "[Circular]";
      }
      seen.add(value);
      return Object.fromEntries(Object.getOwnPropertyNames(value).map((key) => [key, Reflect.get(value, key)]));
    }
    return value;
  });
  const spread = error instanceof Error ? { ...(error as unknown as Record<string, unknown>) } : {};
  return {
    message: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? (error.stack ?? "") : "",
    stringified: String(error),
    plainJson: JSON.stringify(error) ?? "",
    detailJson: detailJson ?? "",
    inspected: inspect(error, { depth: 8 }),
    consoleError,
    spreadJson: JSON.stringify(spread) ?? "",
  };
}

export function leakingChannels(channels: ReturnType<typeof serializeError>) {
  return Object.entries(channels)
    .filter(([, value]) => value.includes(dsnMarker))
    .map(([name]) => name);
}

export const headerSessionId = "audit_expected_session";
export const headerCredentials = { username: "audit_fixture_user", password: "synthetic_fixture_password" };
export const expectedAuthorization = `Basic ${Buffer.from(
  `${headerCredentials.username}:${headerCredentials.password}`,
).toString("base64")}`;
const canonicalReplica = "X-ClickHouse-Replica-Tag";
const lowerReplica = "x-clickhouse-replica-tag";

export type HeaderCase = {
  label: string;
  kind: "RED" | "CONTROL";
  client?: Record<string, string>;
  scope?: Record<string, string>;
  request?: Record<string, string>;
  plugin?: Record<string, string>;
  shared?: string;
};

export const headerCases: HeaderCase[] = [
  {
    label: "runtime replica tag wins over canonical then lowercase request headers",
    kind: "RED",
    request: { [canonicalReplica]: "synthetic_seed", [lowerReplica]: "synthetic_other_route" },
  },
  {
    label: "runtime replica tag wins over mixed-case client and request headers",
    kind: "RED",
    client: { [canonicalReplica]: "synthetic_seed" },
    request: { [lowerReplica]: "synthetic_other_route" },
  },
  {
    label: "runtime replica tag wins over mixed-case scope and request headers",
    kind: "RED",
    scope: { [canonicalReplica]: "synthetic_seed" },
    request: { [lowerReplica]: "synthetic_other_route" },
  },
  {
    label: "request headers win over client defaults with lowercase plugin headers",
    kind: "RED",
    plugin: { "x-shared": "synthetic_plugin" },
    client: { "X-Shared": "synthetic_client" },
    request: { "x-shared": "synthetic_request" },
    shared: "synthetic_request",
  },
  {
    label: "runtime replica tag wins over a single lowercase override",
    kind: "CONTROL",
    request: { [lowerReplica]: "synthetic_other_route" },
  },
  {
    label: "request precedence works without a plugin",
    kind: "CONTROL",
    client: { "X-Shared": "synthetic_client" },
    request: { "x-shared": "synthetic_request" },
    shared: "synthetic_request",
  },
  {
    label: "request precedence works with consistent header casing",
    kind: "CONTROL",
    plugin: { "x-shared": "synthetic_plugin" },
    client: { "x-shared": "synthetic_client" },
    request: { "x-shared": "synthetic_request" },
    shared: "synthetic_request",
  },
  {
    label: "runtime authentication wins over mixed-case user and plugin headers",
    kind: "CONTROL",
    plugin: { authorization: "synthetic_plugin" },
    client: { Authorization: "synthetic_client" },
    request: { Authorization: "synthetic_request_upper", authorization: "synthetic_request_lower" },
  },
];

export function headerClientConfig(
  host: string,
  item: HeaderCase,
  fetch?: ClickHouseClientConfig["fetch"],
): ClickHouseClientConfig {
  return {
    host,
    ...headerCredentials,
    request_timeout: 2_000,
    fetch,
    http_headers: item.client,
    plugins: item.plugin ? [{ name: "audit-header-fixture", onQueryStart: () => ({ http_headers: item.plugin }) }] : [],
  };
}
