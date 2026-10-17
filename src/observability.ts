import { isClickHouseORMError } from "./errors";
import { createUuid, hashString } from "./platform";

export type ClickHouseORMLogLevel = "trace" | "debug" | "info" | "warn" | "error";
export type ClickHouseORMQueryMode = "query" | "stream" | "command" | "insert";
export type ClickHouseORMQueryKind = "typed" | "raw";

export interface ClickHouseORMLogger {
  trace(message: string, fields?: Record<string, unknown>): void | Promise<void>;
  debug(message: string, fields?: Record<string, unknown>): void | Promise<void>;
  info(message: string, fields?: Record<string, unknown>): void | Promise<void>;
  warn(message: string, fields?: Record<string, unknown>): void | Promise<void>;
  error(message: string, fields?: Record<string, unknown>): void | Promise<void>;
}

export interface ClickHouseORMQueryStatistics {
  readonly serverElapsedMs?: number;
  readonly readRows?: number;
  readonly readBytes?: number;
  readonly resultRows?: number;
  readonly rowsBeforeLimitAtLeast?: number;
}

export interface ClickHouseORMQueryEvent {
  readonly executionId: string;
  readonly system: "clickhouse";
  readonly mode: ClickHouseORMQueryMode;
  readonly queryKind: ClickHouseORMQueryKind;
  readonly statement: string;
  /**
   * Pre-computed `compactStatement(statement)` — same SQL with runs of
   * whitespace collapsed to a single space. Stable for the lifetime of the
   * event; emitted at the source so tracing exporters that expose both
   * `db.statement` and `db.query.text` (and the hash that backs them) don't
   * have to re-normalise.
   */
  readonly compactStatement: string;
  readonly statementHash: string;
  readonly querySummary: string;
  readonly operation: string;
  readonly databaseName?: string;
  readonly serverAddress?: string;
  readonly serverPort?: number;
  readonly requestTimeoutMs?: number;
  /** ClickHouse-assigned (or caller-overridden) query id. Filled when the request actually reaches the server; absent for purely client-side validation failures. */
  readonly queryId?: string;
  /** Set whenever the request runs inside a `runInSession()` block; otherwise undefined. */
  readonly sessionId?: string;
  /** Wire format (`JSON`, `JSONEachRow`, …). Set for query/stream modes; undefined for command/insert. */
  readonly format?: string;
  /** Effective ClickHouse settings forwarded with the request. Only present when at least one setting was supplied. */
  readonly settings?: Record<string, string | number | boolean>;
  readonly startedAt: number;
  /** Backing table name when the operation targets a single table (insert, schema-bound select); undefined for raw or multi-source queries. */
  readonly tableName?: string;
}

export interface ClickHouseORMQueryResultEvent extends ClickHouseORMQueryEvent {
  readonly durationMs: number;
  /** Number of rows produced by the query / yielded by the stream. Undefined for command and insert modes. */
  readonly rowCount?: number;
  readonly serverElapsedMs?: number;
  readonly readRows?: number;
  readonly readBytes?: number;
  readonly resultRows?: number;
  readonly rowsBeforeLimitAtLeast?: number;
}

export interface ClickHouseORMQueryErrorEvent extends ClickHouseORMQueryEvent {
  readonly durationMs: number;
  readonly error: unknown;
  /** @deprecated Prefer `partialRowCount` for error events. Kept for backwards compatibility. */
  readonly rowCount?: number;
  /**
   * Number of rows that were successfully yielded from a streaming query before
   * the error was thrown. Undefined for non-streaming (eager) operations.
   */
  readonly partialRowCount?: number;
}

export interface ClickHouseORMQueryStartResult {
  /**
   * Headers added to this request. Explicit `http_headers` (client-level or
   * per-request) override them, and plugins cannot override the
   * `Authorization` header built from configured credentials or the session
   * `X-ClickHouse-Replica-Tag`.
   */
  readonly http_headers?: Record<string, string>;
}

export interface ClickHouseORMPlugin {
  /** Identifies the plugin in hook error messages. */
  readonly name: string;
  onQueryStart?(
    event: ClickHouseORMQueryEvent,
    // biome-ignore lint/suspicious/noConfusingVoidType: `void` keeps expression-bodied hooks such as `() => log()` assignable
  ): void | ClickHouseORMQueryStartResult | Promise<void | ClickHouseORMQueryStartResult>;
  onQuerySuccess?(event: ClickHouseORMQueryResultEvent): void | Promise<void>;
  onQueryError?(event: ClickHouseORMQueryErrorEvent): void | Promise<void>;
}

export const resolveSafeClickHouseDestination = (url: string | undefined): string | undefined => {
  if (!url) {
    return undefined;
  }

  try {
    const parsed = new URL(url);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return undefined;
    }

    const pathname = parsed.pathname === "/" ? "" : parsed.pathname;
    return `${parsed.protocol}//${parsed.host}${pathname}`;
  } catch {
    return undefined;
  }
};

export const createQueryEvent = (
  event: Omit<
    ClickHouseORMQueryEvent,
    "executionId" | "system" | "statementHash" | "querySummary" | "compactStatement"
  >,
): ClickHouseORMQueryEvent => {
  // Normalise once at the source; downstream consumers (logger, tracer)
  // share the same string instead of re-running the regex per emit.
  const compactedStatement = compactStatement(event.statement);
  return {
    executionId: createUuid(),
    system: "clickhouse",
    compactStatement: compactedStatement,
    statementHash: hashString(compactedStatement),
    querySummary: buildQuerySummary(event.operation, event.tableName),
    ...event,
  };
};

// Local writable view of a `readonly` event shape — used only inside the
// builder, never escapes. Lets us populate optional fields in place instead of
// spreading half a dozen throwaway objects per emit.
type Writable<T> = { -readonly [K in keyof T]: T[K] };

export const createQuerySuccessEvent = (
  event: ClickHouseORMQueryEvent,
  durationMs: number,
  rowCount?: number,
  statistics?: ClickHouseORMQueryStatistics,
): ClickHouseORMQueryResultEvent => {
  const result: Writable<ClickHouseORMQueryResultEvent> = { ...event, durationMs };
  if (rowCount !== undefined) result.rowCount = rowCount;
  if (statistics?.serverElapsedMs !== undefined) result.serverElapsedMs = statistics.serverElapsedMs;
  if (statistics?.readRows !== undefined) result.readRows = statistics.readRows;
  if (statistics?.readBytes !== undefined) result.readBytes = statistics.readBytes;
  if (statistics?.resultRows !== undefined) result.resultRows = statistics.resultRows;
  if (statistics?.rowsBeforeLimitAtLeast !== undefined)
    result.rowsBeforeLimitAtLeast = statistics.rowsBeforeLimitAtLeast;
  return result;
};

export const createQueryErrorEvent = (
  event: ClickHouseORMQueryEvent,
  error: unknown,
  durationMs: number,
  partialRowCount?: number,
): ClickHouseORMQueryErrorEvent => {
  const result: Writable<ClickHouseORMQueryErrorEvent> = { ...event, durationMs, error };
  if (partialRowCount !== undefined) {
    result.rowCount = partialRowCount;
    result.partialRowCount = partialRowCount;
  }
  return result;
};

/**
 * Runs every `onQueryStart` in registration order and returns the headers they
 * contributed. `Headers` lower-cases names, so a later plugin overrides an
 * earlier one regardless of case.
 */
export const emitQueryStart = async (
  plugins: readonly ClickHouseORMPlugin[],
  event: ClickHouseORMQueryEvent,
): Promise<Record<string, string>> => {
  const headers: Record<string, string> = {};
  for (const plugin of plugins) {
    if (!plugin.onQueryStart) {
      continue;
    }
    await invokePluginHook(plugin, "onQueryStart", async () => {
      const result = await plugin.onQueryStart?.(event);
      if (!result?.http_headers) {
        return;
      }
      // `new Headers` rejects malformed names and values before anything is
      // merged, so a bad plugin loses its own headers instead of failing the request.
      new Headers(result.http_headers).forEach((value, key) => {
        headers[key] = value;
      });
    });
  }
  return headers;
};

export const emitQuerySuccess = async (
  plugins: readonly ClickHouseORMPlugin[],
  event: ClickHouseORMQueryResultEvent,
): Promise<void> => {
  for (let index = plugins.length - 1; index >= 0; index -= 1) {
    const plugin = plugins[index];
    if (!plugin.onQuerySuccess) {
      continue;
    }
    await invokePluginHook(plugin, "onQuerySuccess", () => plugin.onQuerySuccess?.(event));
  }
};

export const emitQueryError = async (
  plugins: readonly ClickHouseORMPlugin[],
  event: ClickHouseORMQueryErrorEvent,
): Promise<void> => {
  for (let index = plugins.length - 1; index >= 0; index -= 1) {
    const plugin = plugins[index];
    if (!plugin.onQueryError) {
      continue;
    }
    await invokePluginHook(plugin, "onQueryError", () => plugin.onQueryError?.(event));
  }
};

type PluginHookName = "onQueryStart" | "onQuerySuccess" | "onQueryError";

const invokePluginHook = async (plugin: ClickHouseORMPlugin, hook: PluginHookName, operation: () => unknown) => {
  try {
    await operation();
  } catch (hookError) {
    try {
      console.error(`[ck-orm] plugin "${plugin.name}" ${hook} threw:`, hookError);
    } catch {
      // ignore secondary failures from console
    }
  }
};

const logLevelPriority: Record<ClickHouseORMLogLevel, number> = {
  trace: 10,
  debug: 20,
  info: 30,
  warn: 40,
  error: 50,
};

const shouldWriteLog = (minimumLevel: ClickHouseORMLogLevel, eventLevel: ClickHouseORMLogLevel) => {
  return logLevelPriority[eventLevel] >= logLevelPriority[minimumLevel];
};

export const createLoggerPlugin = (
  logger: ClickHouseORMLogger,
  minimumLevel: ClickHouseORMLogLevel = "warn",
): ClickHouseORMPlugin => {
  return {
    name: "logger",
    onQueryStart(event) {
      if (!shouldWriteLog(minimumLevel, "debug")) {
        return;
      }
      return logger.debug("[clickhouse] orm", {
        ...buildBaseLogFields(event),
        outcome: "start",
      });
    },
    onQuerySuccess(event) {
      if (!shouldWriteLog(minimumLevel, "debug")) {
        return;
      }
      return logger.debug("[clickhouse] orm", {
        ...buildBaseLogFields(event),
        durationMs: event.durationMs,
        outcome: "success",
        rowCount: event.rowCount,
        serverElapsedMs: event.serverElapsedMs,
        readRows: event.readRows,
        readBytes: event.readBytes,
        resultRows: event.resultRows,
        rowsBeforeLimitAtLeast: event.rowsBeforeLimitAtLeast,
      });
    },
    onQueryError(event) {
      return logger.error("[clickhouse] orm", {
        ...buildBaseLogFields(event),
        durationMs: event.durationMs,
        outcome: "error",
        rowCount: event.rowCount,
        partialRowCount: event.partialRowCount,
        errorKind: isClickHouseORMError(event.error) ? event.error.kind : undefined,
        executionState: isClickHouseORMError(event.error) ? event.error.executionState : undefined,
        httpStatus: isClickHouseORMError(event.error) ? event.error.httpStatus : undefined,
        clickhouseCode: isClickHouseORMError(event.error) ? event.error.clickhouseCode : undefined,
        clickhouseName: isClickHouseORMError(event.error) ? event.error.clickhouseName : undefined,
        error: event.error,
      });
    },
  };
};

const buildBaseLogFields = (event: ClickHouseORMQueryEvent) => {
  return {
    executionId: event.executionId,
    provider: "clickhouse",
    system: event.system,
    mode: event.mode,
    queryKind: event.queryKind,
    operation: event.operation,
    databaseName: event.databaseName,
    serverAddress: event.serverAddress,
    serverPort: event.serverPort,
    requestTimeoutMs: event.requestTimeoutMs,
    statement: event.statement,
    statementHash: event.statementHash,
    querySummary: event.querySummary,
    queryId: event.queryId,
    sessionId: event.sessionId,
    format: event.format,
    settings: event.settings,
    tableName: event.tableName,
    startedAt: event.startedAt,
  };
};

const buildQuerySummary = (operation: string, tableName: string | undefined): string => {
  return tableName ? `${operation} ${tableName}` : operation;
};

export const compactStatement = (statement: string): string => {
  return statement.replace(/\s+/g, " ").trim();
};

export const hashStatement = (statement: string): string => {
  const normalized = compactStatement(statement);
  return hashString(normalized);
};

const LEADING_KEYWORD_PATTERN = /^[A-Z]+/;
// Match the first DML keyword that is *not* nested inside a CTE body — both
// sides must be whitespace or string boundary, so `(SELECT` from an inner CTE
// like `with t as (select 1) insert ...` is skipped while the trailing
// `INSERT` wins.
const WITH_INNER_DML_PATTERN = /(?:^|\s)(SELECT|INSERT|UPDATE|DELETE)(?=\s|$)/;
const KNOWN_LEADING_OPERATIONS = new Set([
  "SELECT",
  "INSERT",
  "UPDATE",
  "DELETE",
  "MERGE",
  "CALL",
  "CREATE",
  "ALTER",
  "DROP",
  "TRUNCATE",
]);

export const resolveSqlOperation = (statement: string): string => {
  const normalized = compactStatement(stripLeadingSqlComments(statement)).toUpperCase();
  if (!normalized) {
    return "QUERY";
  }

  const firstToken = LEADING_KEYWORD_PATTERN.exec(normalized)?.[0] ?? "";
  if (firstToken === "WITH") {
    // Only WITH-prefixed queries need a deeper scan to find the inner DML; the
    // common path skips the full-statement split entirely.
    return WITH_INNER_DML_PATTERN.exec(normalized)?.[1] ?? "QUERY";
  }
  return KNOWN_LEADING_OPERATIONS.has(firstToken) ? firstToken : "QUERY";
};

const stripLeadingSqlComments = (statement: string): string => {
  let remaining = statement.trimStart();
  while (remaining.startsWith("/*") || remaining.startsWith("--")) {
    if (remaining.startsWith("/*")) {
      const endIndex = remaining.indexOf("*/");
      if (endIndex < 0) {
        return "";
      }
      remaining = remaining.slice(endIndex + 2).trimStart();
      continue;
    }

    const endOfLineIndex = remaining.indexOf("\n");
    if (endOfLineIndex < 0) {
      return "";
    }
    remaining = remaining.slice(endOfLineIndex + 1).trimStart();
  }
  return remaining;
};
