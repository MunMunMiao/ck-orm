import { createClientValidationError } from "../errors";
import { getArrayElementType, getTupleElementTypes } from "../internal/clickhouse-type";
import { formatClickHouseDateTime } from "../internal/datetime";
import type {
  ClickHouseORMInstrumentation,
  ClickHouseORMLogger,
  ClickHouseORMLogLevel,
  ClickHouseORMTracingOptions,
} from "../observability";
import { base64EncodeUtf8, canSetUserAgentHeader } from "../platform";
import type { CompiledQuery, QueryClient } from "../query";
import type { AnyTable } from "../schema";
import { compileSql, type QueryParamTypes, type SQLFragment } from "../sql";
import type { JsonHandling } from "./json-stream";
import type { ClickHouseSettings, ClickHouseSettingValue } from "./settings";

type ClickHouseAuth = { readonly username: string; readonly password: string };

export interface ClickHouseBaseQueryOptions {
  readonly clickhouse_settings?: ClickHouseSettings;
  readonly query_params?: Record<string, unknown>;
  readonly query_id?: string;
  readonly session_id?: string;
  readonly session_timeout?: number;
  readonly session_check?: number;
  /**
   * One or more ClickHouse RBAC role names to apply to this request.
   * Forwarded as one `role=<name>` query parameter per entry. Pass a
   * single string for one role or an array for several. Roles take
   * effect for this request only; they do not mutate the client.
   */
  readonly role?: string | string[];
  /**
   * Per-request override for HTTP Basic auth credentials.
   *
   * Auth precedence (highest first):
   *   1. `databaseUrl` user:password embedded in the URL passed to {@link ClickHouseClientConfig}
   *   2. Per-request `options.auth`
   *   3. Client-level `username`/`password` from {@link ClickHouseClientConfig}
   *      (or `default`/empty when nothing is supplied)
   *
   * Note: `databaseUrl` may not be combined with `username`/`password`
   * fields on the same client config — `normalizeClientConfig` rejects
   * that combination at construction time.
   */
  readonly auth?: ClickHouseAuth;
  /**
   * External `AbortSignal` that cancels this request. Aborting after
   * the response started streaming will also tear down the underlying
   * fetch reader. The signal is composed with the client-level
   * `request_timeout`, so whichever fires first wins. The same signal
   * may be reused across multiple requests; ck-orm registers and
   * removes its listener per-request to avoid leaks.
   */
  readonly abort_signal?: AbortSignal;
  readonly http_headers?: Record<string, string>;
  readonly ignore_error_response?: boolean;
}

export interface ClickHouseQueryOptions extends ClickHouseBaseQueryOptions {
  readonly format?: "JSON";
}

export interface ClickHouseStreamOptions extends ClickHouseBaseQueryOptions {
  readonly format?: "JSONEachRow";
}

export type ClickHouseEndpointOptions = Pick<
  ClickHouseBaseQueryOptions,
  "abort_signal" | "auth" | "http_headers" | "query_id"
>;

export type CreateTemporaryTableOptions = {
  readonly mode?: "create" | "if_not_exists" | "or_replace";
};

export type SessionRunInSessionOptions = Omit<ClickHouseBaseQueryOptions, "session_id"> & {
  session_id?: string;
  /**
   * Optional callback invoked when one or more temporary-table cleanup
   * statements (`DROP TABLE IF EXISTS`) fail at the end of a session.
   *
   * Cleanup errors never override the user callback's error — they are
   * collected after the user callback resolves/rejects and surfaced here.
   *
   * If this hook is **not** supplied:
   * - When the user callback succeeded but cleanup failed, a
   *   `session` error is thrown with the underlying cleanup errors
   *   attached via `cause` (an `AggregateError` when there is more than
   *   one).
   * - When the user callback already threw, cleanup errors are silently
   *   discarded to preserve the original error (matches prior behaviour).
   */
  onCleanupError?: (errors: readonly unknown[], context: { sessionId: string }) => void;
};

export interface Session<TJoinUseNulls extends 0 | 1 = 1>
  extends Pick<QueryClient<TJoinUseNulls>, "ctes" | "select" | "count" | "insert" | "$with" | "with"> {
  readonly sessionId: string;
  execute(query: RawQueryInput, options?: ClickHouseQueryOptions): Promise<Record<string, unknown>[]>;
  stream(
    query: RawQueryInput,
    options?: ClickHouseStreamOptions,
  ): AsyncGenerator<Record<string, unknown>, void, unknown>;
  command(query: RawQueryInput, options?: ClickHouseBaseQueryOptions): Promise<void>;
  ping(options?: ClickHouseEndpointOptions): Promise<void>;
  replicasStatus(options?: ClickHouseEndpointOptions): Promise<void>;
  withSettings<TSettings extends ClickHouseSettings>(
    settings: TSettings,
  ): Session<ResolveJoinUseNulls<TSettings, TJoinUseNulls>>;
  insertJsonEachRow(
    table: AnyTable | string,
    rows: readonly Record<string, unknown>[] | AsyncIterable<Record<string, unknown>>,
    options?: ClickHouseBaseQueryOptions,
  ): Promise<void>;
  registerTempTable(name: string): void;
  createTemporaryTable(table: AnyTable, options?: CreateTemporaryTableOptions): Promise<void>;
  createTemporaryTableRaw(name: string, definition: string): Promise<void>;
  runInSession<TResult>(
    callback: (session: Session<TJoinUseNulls>) => Promise<TResult>,
    options?: SessionRunInSessionOptions,
  ): Promise<TResult>;
}

/**
 * Connection configuration that supplies an embedded `user:password@host`
 * URL. Mutually exclusive with the structured `host`/`username`/`password`
 * variant — passing both forms raises a `client_validation` error.
 *
 * Credentials encoded in the URL take precedence over any per-request
 * `options.auth`. See {@link ClickHouseBaseQueryOptions.auth}.
 */
type DatabaseUrlConnectionConfig = {
  readonly databaseUrl: string | URL;
  readonly host?: never;
  readonly database?: never;
  readonly username?: never;
  readonly password?: never;
  readonly pathname?: never;
};

/**
 * Connection configuration that supplies discrete fields. Mutually
 * exclusive with `databaseUrl`. Passing credentials in `host` (e.g.
 * `http://u:p@srv`) is rejected — use `username`/`password` or switch
 * to `databaseUrl` instead.
 */
type StructuredConnectionConfig = {
  readonly databaseUrl?: never;
  readonly host?: string | URL;
  readonly database?: string;
  readonly username?: string;
  readonly password?: string;
  readonly pathname?: string;
};

type SharedClientConfigOptions = {
  /**
   * Default per-request timeout in milliseconds. Defaults to `30_000`
   * (30 seconds). The timeout starts when the request is dispatched
   * and aborts the underlying fetch (and any in-flight stream reader)
   * if the response is not fully received in time. Per-request
   * `abort_signal` is composed with this; whichever fires first wins.
   */
  readonly request_timeout?: number;
  readonly compression?: {
    readonly response?: boolean;
  };
  readonly application?: string;
  readonly clickhouse_settings?: ClickHouseSettings;
  readonly session_id?: string;
  /**
   * Maximum number of in-flight requests allowed for the same
   * ClickHouse `session_id` within this client instance. Requests
   * without a session id are not throttled by this controller.
   *
   * Defaults to `1`, which preserves request ordering for the same
   * session and avoids overlapping temporary-table operations unless
   * the caller opts into higher concurrency explicitly.
   */
  readonly session_max_concurrent_requests?: number;
  /**
   * Default ClickHouse RBAC role(s) applied to every request. Per-request
   * `options.role` overrides this value. See
   * {@link ClickHouseBaseQueryOptions.role} for semantics.
   */
  readonly role?: string | string[];
  readonly http_headers?: Record<string, string>;
};

export type ClickHouseFetchConfigOptions = SharedClientConfigOptions &
  (DatabaseUrlConnectionConfig | StructuredConnectionConfig);

export type ClickHouseClientConfig = ClickHouseFetchConfigOptions & {
  readonly logger?: ClickHouseORMLogger | false;
  readonly logLevel?: ClickHouseORMLogLevel;
  readonly tracing?: false | ClickHouseORMTracingOptions;
  readonly instrumentation?: ClickHouseORMInstrumentation[];
};

export type ResolveJoinUseNulls<TSettings, TFallback extends 0 | 1> = TSettings extends {
  readonly join_use_nulls?: infer TJoinUseNulls;
}
  ? TJoinUseNulls extends 0 | 1
    ? TJoinUseNulls
    : TFallback
  : TFallback;

export type RawQueryInput = SQLFragment;

export type NormalizedClientConfig = {
  readonly url: URL;
  readonly request_timeout: number;
  readonly compression: {
    readonly response: boolean;
  };
  readonly auth?: ClickHouseAuth;
  readonly authSource: "databaseUrl" | "config";
  readonly application?: string;
  readonly database: string;
  readonly clickhouse_settings: ClickHouseSettings;
  readonly session_id?: string;
  readonly session_max_concurrent_requests: number;
  readonly role?: string | string[];
  readonly http_headers: Record<string, string>;
  readonly json: JsonHandling;
};

export type TransportQueryInput = {
  readonly statement: string;
  readonly query_params?: Record<string, unknown>;
  readonly query_param_types?: QueryParamTypes;
  readonly options: ClickHouseBaseQueryOptions;
  readonly format?: ClickHouseQueryOptions["format"] | ClickHouseStreamOptions["format"];
};

export type RequestHandle = {
  readonly response: Response;
  readonly queryId: string;
  readonly options: ClickHouseBaseQueryOptions;
  finalize(): void;
  readValidatedText(): Promise<string>;
};

export type ResponseParseMode = "json" | "json_each_row" | "text";

export type RequestBody =
  | string
  | URLSearchParams
  | FormData
  | Blob
  | ArrayBuffer
  | Uint8Array
  | ReadableStream<Uint8Array>;

const defaultJsonHandling: JsonHandling = {
  parse: JSON.parse,
  stringify: JSON.stringify,
};

const singleDocumentFormats = new Set(["JSON"]);
const streamingFormats = new Set(["JSONEachRow"]);
const RESERVED_INTERNAL_QUERY_PARAM_PREFIX = "orm_param";

const assertValidUserQueryParams = (queryParams: Record<string, unknown> | undefined): void => {
  if (!queryParams) {
    return;
  }

  for (const key of Object.keys(queryParams)) {
    if (key.startsWith(RESERVED_INTERNAL_QUERY_PARAM_PREFIX)) {
      throw createClientValidationError(
        `query_params key "${key}" uses reserved internal prefix "${RESERVED_INTERNAL_QUERY_PARAM_PREFIX}". ` +
          "This prefix is reserved for ckSql`...` generated parameters.",
      );
    }
  }
};

export const mergeQueryParams = (
  internalParams: Record<string, unknown> | undefined,
  userParams: Record<string, unknown> | undefined,
) => {
  assertValidUserQueryParams(userParams);
  return {
    ...(userParams ?? {}),
    ...(internalParams ?? {}),
  };
};

export const buildQueryParams = (
  compiled: CompiledQuery<Record<string, unknown>>,
  options?: ClickHouseBaseQueryOptions,
) => {
  return {
    query: compiled.statement,
    query_params: mergeQueryParams(compiled.params, options?.query_params),
    query_param_types: compiled.paramTypes ?? {},
  };
};

export const normalizeRawQueryInput = (query: RawQueryInput) => {
  const compiled = compileSql(query);
  return {
    query: compiled.query,
    params: compiled.params,
    paramTypes: compiled.paramTypes,
  };
};

export const toClickHouseTableName = (table: AnyTable | string) => {
  if (typeof table === "string") {
    return table;
  }
  return table.originalName;
};

const resolveRoleEntries = (role: string | string[] | undefined): [string, string][] => {
  if (!role) {
    return [];
  }
  return Array.isArray(role) ? role.map<[string, string]>((value) => ["role", value]) : [["role", role]];
};

const formatQuerySetting = (value: ClickHouseSettingValue) => {
  if (typeof value === "boolean") {
    return value ? "1" : "0";
  }
  return String(value);
};

const RESERVED_CLICKHOUSE_SETTING_PARAM_NAMES = new Set([
  "query",
  "query_id",
  "database",
  "session_id",
  "session_timeout",
  "session_check",
  "role",
  "default_format",
  "user",
  "username",
  "password",
]);

export const assertValidClickHouseSettingKey = (key: string): void => {
  if (RESERVED_CLICKHOUSE_SETTING_PARAM_NAMES.has(key) || key.startsWith("param_")) {
    throw createClientValidationError(
      `clickhouse_settings key "${key}" conflicts with a reserved ClickHouse HTTP parameter`,
    );
  }
};

const VALID_QUERY_PARAM_KEY = /^[a-zA-Z_][a-zA-Z0-9_]{0,99}$/;

export const assertValidQueryParamKey = (key: string): void => {
  if (!VALID_QUERY_PARAM_KEY.test(key)) {
    throw createClientValidationError(
      `Invalid query parameter key: "${key}". Keys must match ${VALID_QUERY_PARAM_KEY.source}`,
    );
  }
};

const VALID_QUERY_ID = /^[a-zA-Z0-9_-]{1,100}$/;

export const assertValidQueryId = (id: string): void => {
  if (!VALID_QUERY_ID.test(id)) {
    throw createClientValidationError(
      `Invalid query_id: "${id}". Must be 1-100 chars of alphanumerics, underscores, or hyphens.`,
    );
  }
};

export const assertValidSessionId = (id: string): void => {
  if (!VALID_QUERY_ID.test(id)) {
    throw createClientValidationError(
      `Invalid session_id: "${id}". Must be 1-100 chars of alphanumerics, underscores, or hyphens.`,
    );
  }
};

export const formatQueryParamValue = (
  value: unknown,
  sqlTypeOrOptions?:
    | string
    | {
        wrapStringInQuotes?: boolean;
        printNullAsKeyword?: boolean;
        nested?: boolean;
      },
  maybeOptions?: {
    wrapStringInQuotes?: boolean;
    printNullAsKeyword?: boolean;
    nested?: boolean;
  },
): string => {
  const sqlType = typeof sqlTypeOrOptions === "string" ? sqlTypeOrOptions : undefined;
  const options = typeof sqlTypeOrOptions === "string" ? maybeOptions : sqlTypeOrOptions;
  const wrapStringInQuotes = options?.wrapStringInQuotes ?? false;
  const printNullAsKeyword = options?.printNullAsKeyword ?? false;
  const nested = options?.nested ?? false;
  const nestedOptions = {
    wrapStringInQuotes: true,
    printNullAsKeyword: true,
    nested: true,
  };

  if (value === null || value === undefined) {
    return printNullAsKeyword ? "NULL" : "\\N";
  }
  if (Number.isNaN(value)) {
    return "nan";
  }
  if (value === Number.POSITIVE_INFINITY) {
    return "+inf";
  }
  if (value === Number.NEGATIVE_INFINITY) {
    return "-inf";
  }

  if (typeof value === "number" || typeof value === "bigint") {
    return String(value);
  }
  if (typeof value === "boolean") {
    if (nested) {
      return value ? "TRUE" : "FALSE";
    }
    return value ? "1" : "0";
  }
  if (typeof value === "string") {
    const escaped = value
      .replaceAll("\\", "\\\\")
      .replaceAll("\0", "\\0")
      .replaceAll("\b", "\\b")
      .replaceAll("\f", "\\f")
      .replaceAll("\t", "\\t")
      .replaceAll("\n", "\\n")
      .replaceAll("\r", "\\r")
      .replaceAll("\v", "\\v")
      .replaceAll("'", "\\'");
    return wrapStringInQuotes ? `'${escaped}'` : escaped;
  }
  if (value instanceof Date) {
    // Use ms precision when present, second precision otherwise — keeps the
    // wire format as terse as possible without losing information.
    return formatClickHouseDateTime(value, value.getUTCMilliseconds() === 0 ? 0 : 3);
  }
  if (Array.isArray(value)) {
    const tupleElementTypes = getTupleElementTypes(sqlType);
    if (tupleElementTypes) {
      if (value.length !== tupleElementTypes.length) {
        throw createClientValidationError(
          `Tuple query parameter expected ${tupleElementTypes.length} items, got ${value.length}`,
        );
      }
      return `(${value
        .map((item, index) => formatQueryParamValue(item, tupleElementTypes[index], nestedOptions))
        .join(",")})`;
    }

    const arrayElementType = getArrayElementType(sqlType);
    return `[${value
      .map((item) =>
        arrayElementType === undefined
          ? formatQueryParamValue(item, nestedOptions)
          : formatQueryParamValue(item, arrayElementType, nestedOptions),
      )
      .join(",")}]`;
  }
  if (value instanceof Map) {
    return `{${[...value.entries()]
      .map(([key, entryValue]) => {
        return `${formatQueryParamValue(key, nestedOptions)}:${formatQueryParamValue(entryValue, nestedOptions)}`;
      })
      .join(",")}}`;
  }
  if (typeof value === "object")
    return `{${Object.entries(value)
      .map(([key, entryValue]) => {
        return `${formatQueryParamValue(key, nestedOptions)}:${formatQueryParamValue(entryValue, nestedOptions)}`;
      })
      .join(",")}}`;

  throw createClientValidationError(`Unsupported query parameter value: ${String(value)}`);
};

export const buildSearchParams = (input: {
  readonly database?: string;
  readonly clickhouse_settings?: ClickHouseSettings;
  readonly query?: string;
  readonly query_id: string;
  readonly session_id?: string;
  readonly session_timeout?: number;
  readonly session_check?: number;
  readonly role?: string | string[];
}) => {
  const entries: [string, string][] = [["query_id", input.query_id]];

  if (input.clickhouse_settings) {
    for (const [key, value] of Object.entries(input.clickhouse_settings)) {
      assertValidClickHouseSettingKey(key);
      entries.push([key, formatQuerySetting(value)]);
    }
  }

  if (input.database && input.database !== "default") {
    entries.push(["database", input.database]);
  }

  if (input.query) {
    entries.push(["query", input.query]);
  }

  if (input.session_id) {
    entries.push(["session_id", input.session_id]);
  }
  if (typeof input.session_timeout === "number") {
    entries.push(["session_timeout", String(input.session_timeout)]);
  }
  if (typeof input.session_check === "number") {
    entries.push(["session_check", String(input.session_check)]);
  }

  entries.push(...resolveRoleEntries(input.role));

  return new URLSearchParams(entries);
};

const normalizePathname = (pathname: string) => {
  return pathname.startsWith("/") ? pathname : `/${pathname}`;
};

const decodeUrlCredential = (value: string) => {
  return decodeURIComponent(value);
};

const resolveUrlAuth = (resolved: URL) => {
  if (resolved.username === "" && resolved.password === "") {
    return undefined;
  }
  return {
    username: resolved.username === "" ? "default" : decodeUrlCredential(resolved.username),
    password: decodeUrlCredential(resolved.password),
  } satisfies ClickHouseAuth;
};

const resolveDatabaseUrlConfig = (databaseUrl: string | URL) => {
  const resolved = new URL(databaseUrl);
  const authFromUrl = resolveUrlAuth(resolved);
  const database = resolved.pathname.trim().length > 1 ? resolved.pathname.slice(1) : "default";

  resolved.pathname = "/";
  resolved.username = "";
  resolved.password = "";

  return {
    url: resolved,
    database,
    auth: authFromUrl ?? {
      username: "default",
      password: "",
    },
    authSource: authFromUrl ? ("databaseUrl" as const) : ("config" as const),
  };
};

const resolveStructuredConfig = (config: StructuredConnectionConfig) => {
  const resolved = new URL(config.host ?? "http://localhost:8123");
  const authFromUrl = resolveUrlAuth(resolved);
  if (authFromUrl) {
    throw createClientValidationError(
      "Structured connection config does not accept credentials in host; use username/password or databaseUrl instead",
    );
  }

  if (config.pathname) {
    resolved.pathname = normalizePathname(config.pathname);
  }

  resolved.username = "";
  resolved.password = "";

  return {
    url: resolved,
    database: config.database ?? "default",
    auth: {
      username: config.username ?? "default",
      password: config.password ?? "",
    } satisfies ClickHouseAuth,
    authSource: "config" as const,
  };
};

const buildAuthHeader = (auth: ClickHouseAuth | undefined) => {
  if (!auth) {
    return undefined;
  }
  const credentials = `${auth.username}:${auth.password}`;
  const encoded = base64EncodeUtf8(credentials);
  return `Basic ${encoded}`;
};

export const normalizeClientConfig = (config: ClickHouseFetchConfigOptions): NormalizedClientConfig => {
  const rawLogConfig = (config as Record<string, unknown>).log;
  if (rawLogConfig !== undefined) {
    throw createClientValidationError(
      "clickhouseClient() does not accept native createClient({ log }) config; use logger and logLevel instead",
    );
  }
  const rawConfig = config as Record<string, unknown>;
  if ("url" in rawConfig) {
    throw createClientValidationError("clickhouseClient() no longer accepts url; use databaseUrl instead");
  }
  if ("access_token" in rawConfig) {
    throw createClientValidationError(
      "clickhouseClient() no longer accepts access_token; use databaseUrl or username/password instead",
    );
  }
  if ("additional_headers" in rawConfig) {
    throw createClientValidationError(
      "clickhouseClient() no longer accepts additional_headers; use http_headers instead",
    );
  }
  if (rawConfig.json !== undefined) {
    throw createClientValidationError(
      "clickhouseClient() no longer accepts json hooks; ck-orm uses built-in JSON handling internally",
    );
  }
  if (rawConfig.session_timeout !== undefined) {
    throw createClientValidationError(
      "clickhouseClient() no longer accepts session_timeout; pass it to a single request or runInSession() instead",
    );
  }
  if (rawConfig.session_check !== undefined) {
    throw createClientValidationError(
      "clickhouseClient() no longer accepts session_check; pass it to a single request or runInSession() instead",
    );
  }
  if (
    config.compression &&
    "request" in config.compression &&
    (config.compression as Record<string, unknown>).request === true
  ) {
    throw createClientValidationError(
      "ck-orm fetch runtime does not support compression.request; only compression.response is supported",
    );
  }

  const enableHttpCompressionSetting = config.clickhouse_settings?.enable_http_compression;
  if (
    enableHttpCompressionSetting !== undefined &&
    config.compression?.response !== undefined &&
    Boolean(Number(enableHttpCompressionSetting)) !== config.compression.response
  ) {
    throw createClientValidationError(
      "compression.response and clickhouse_settings.enable_http_compression must agree. Set only one of them, or set both to the same boolean value (compression.response = true ↔ enable_http_compression = 1).",
    );
  }

  const hasDatabaseUrl = "databaseUrl" in config && config.databaseUrl !== undefined;
  if (hasDatabaseUrl) {
    const conflicts = [
      config.host !== undefined ? "host" : undefined,
      config.database !== undefined ? "database" : undefined,
      config.username !== undefined ? "username" : undefined,
      config.password !== undefined ? "password" : undefined,
      config.pathname !== undefined ? "pathname" : undefined,
    ].filter((value): value is string => value !== undefined);
    if (conflicts.length > 0) {
      throw createClientValidationError(`databaseUrl cannot be combined with ${conflicts.join(", ")}`);
    }
  }

  const resolvedConnection = hasDatabaseUrl
    ? resolveDatabaseUrlConfig(config.databaseUrl)
    : resolveStructuredConfig(config);
  const sessionMaxConcurrentRequests = config.session_max_concurrent_requests ?? 1;
  if (!Number.isInteger(sessionMaxConcurrentRequests) || sessionMaxConcurrentRequests < 1) {
    throw createClientValidationError("clickhouseClient() session_max_concurrent_requests must be a positive integer");
  }
  const requestTimeout = config.request_timeout ?? 30_000;
  if (!Number.isFinite(requestTimeout) || requestTimeout <= 0) {
    throw createClientValidationError("clickhouseClient() request_timeout must be a finite positive number");
  }

  return {
    url: resolvedConnection.url,
    request_timeout: requestTimeout,
    compression: {
      response: config.compression?.response ?? false,
    },
    auth: resolvedConnection.auth,
    authSource: resolvedConnection.authSource,
    application: config.application,
    database: resolvedConnection.database,
    clickhouse_settings: config.clickhouse_settings ?? {},
    session_id: config.session_id,
    session_max_concurrent_requests: sessionMaxConcurrentRequests,
    role: config.role,
    http_headers: {
      ...(config.http_headers ?? {}),
    },
    json: {
      ...defaultJsonHandling,
    },
  };
};

export const buildRequestUrl = (baseUrl: URL, searchParams: URLSearchParams) => {
  const next = new URL(baseUrl);
  next.search = searchParams.toString();
  return next;
};

export const buildEndpointUrl = (baseUrl: URL, path: string) => {
  const next = new URL(baseUrl);
  const basePath = next.pathname === "/" ? "" : next.pathname.replace(/\/+$/g, "");
  const suffix = path.startsWith("/") ? path : `/${path}`;
  next.pathname = `${basePath}${suffix}` || "/";
  next.search = "";
  return next;
};

/**
 * Right-biased shallow merge of `clickhouse_settings`-shaped maps.
 *
 * Centralises the three layers of settings precedence used across the
 * transport: client-config defaults < per-request overrides < forced
 * settings (compression flag, compiled.forcedSettings, etc). The right
 * side wins on key collision and `undefined` sources are skipped.
 */
export const mergeClickHouseSettings = (
  ...sources: ReadonlyArray<ClickHouseSettings | undefined>
): ClickHouseSettings => {
  const merged: Record<string, ClickHouseSettingValue> = {};
  for (const source of sources) {
    if (source) {
      Object.assign(merged, source);
    }
  }
  return merged;
};

let warnedUserAgentRestricted = false;
const warnedForcedSettings = new Set<string>();

export const createHeaders = (input: {
  readonly config: NormalizedClientConfig;
  readonly options: ClickHouseBaseQueryOptions;
  readonly auth?: ClickHouseAuth;
}) => {
  const headers = new Headers();
  for (const [key, value] of Object.entries(input.config.http_headers)) {
    headers.set(key, value);
  }
  for (const [key, value] of Object.entries(input.options.http_headers ?? {})) {
    headers.set(key, value);
  }

  const authHeader = buildAuthHeader(input.auth);
  if (authHeader) {
    headers.set("Authorization", authHeader);
  }
  if (input.config.application) {
    if (canSetUserAgentHeader()) {
      headers.set("User-Agent", `${input.config.application} ck-orm`);
    } else if (!warnedUserAgentRestricted) {
      warnedUserAgentRestricted = true;
      console.warn(
        "[ck-orm] application option is set but this runtime forbids overriding the User-Agent header (e.g. browsers); the value will be ignored.",
      );
    }
  }
  if (input.config.compression.response) {
    headers.set("Accept-Encoding", "gzip");
  }

  return headers;
};

export const validateFormat = (mode: "query" | "stream", format: string) => {
  if (mode === "query") {
    if (!singleDocumentFormats.has(format)) {
      throw createClientValidationError(`Unsupported eager query format: ${format}`);
    }
    return;
  }

  if (!streamingFormats.has(format)) {
    throw createClientValidationError(`Unsupported streaming query format: ${format}`);
  }
};

const isEnabledSetting = (value: string | number | boolean | undefined) => {
  return value === true || value === 1 || value === "1";
};

const isDisabledSetting = (value: string | number | boolean | undefined) => {
  return value === false || value === 0 || value === "0";
};

const warnForcedSettingOverride = (
  key: string,
  configuredValue: ClickHouseSettingValue,
  forcedValue: ClickHouseSettingValue,
): void => {
  if (warnedForcedSettings.has(key)) {
    return;
  }
  warnedForcedSettings.add(key);
  console.warn(
    `[ck-orm] clickhouse_settings.${key}=${String(configuredValue)} is ignored; ck-orm requires ${key}=${String(
      forcedValue,
    )} for its HTTP/JSON wire contract and will use the forced value.`,
  );
};

const forceSetting = (
  settings: Record<string, ClickHouseSettingValue>,
  key: string,
  value: ClickHouseSettingValue,
  accepts: (configured: ClickHouseSettingValue | undefined) => boolean,
): void => {
  const configured = settings[key];
  if (configured !== undefined && !accepts(configured)) {
    warnForcedSettingOverride(key, configured, value);
  }
  settings[key] = value;
};

export const normalizeTransportSettings = (input: {
  readonly settings: ClickHouseSettings;
  readonly parseMode: ResponseParseMode;
}): ClickHouseSettings => {
  const settings: Record<string, ClickHouseSettingValue> = {
    ...input.settings,
  };

  forceSetting(settings, "http_write_exception_in_output_format", 0, isDisabledSetting);
  forceSetting(settings, "output_format_json_quote_64bit_integers", 1, isEnabledSetting);
  forceSetting(settings, "output_format_json_quote_decimals", 1, isEnabledSetting);
  forceSetting(settings, "date_time_output_format", "iso", (value) => value === "iso");

  if (input.parseMode !== "json_each_row" && settings.wait_end_of_query === undefined) {
    settings.wait_end_of_query = 1;
  }

  if (isEnabledSetting(settings.async_insert)) {
    if ("wait_for_async_insert" in settings && isDisabledSetting(settings.wait_for_async_insert)) {
      throw createClientValidationError("ck-orm requires wait_for_async_insert=1 when async_insert=1");
    }
    settings.wait_for_async_insert = 1;
  }

  return settings;
};
export { normalizeQuery, normalizeSingleStatementSql } from "./sql-scan";
