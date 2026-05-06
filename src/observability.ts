import { type Span, SpanKind, SpanStatusCode, type Tracer, trace } from "@opentelemetry/api";
import { isClickHouseORMError } from "./errors";
import { createUuid, hashString } from "./platform";

export type ClickHouseORMLogLevel = "trace" | "debug" | "info" | "warn" | "error";
export type ClickHouseORMQueryMode = "query" | "stream" | "command" | "insert";
export type ClickHouseORMQueryKind = "typed" | "raw";

export interface ClickHouseORMLogger {
  trace(message: string, fields?: Record<string, unknown>): void;
  debug(message: string, fields?: Record<string, unknown>): void;
  info(message: string, fields?: Record<string, unknown>): void;
  warn(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
}

export interface ClickHouseORMTracingOptions {
  readonly tracer?: Tracer;
  readonly attributes?: Record<string, string | number | boolean>;
  readonly includeStatement?: boolean;
  readonly includeRowCount?: boolean;
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

export interface ClickHouseORMInstrumentation {
  onQueryStart?(event: ClickHouseORMQueryEvent): void | Promise<void>;
  onQuerySuccess?(event: ClickHouseORMQueryResultEvent): void | Promise<void>;
  onQueryError?(event: ClickHouseORMQueryErrorEvent): void | Promise<void>;
}

const RESERVED_TRACING_ATTRIBUTE_PREFIX = "db.";

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

export const emitQueryStart = async (
  instrumentations: readonly ClickHouseORMInstrumentation[],
  event: ClickHouseORMQueryEvent,
): Promise<void> => {
  for (const instrumentation of instrumentations) {
    if (!instrumentation.onQueryStart) {
      continue;
    }
    await invokeInstrumentation(() => instrumentation.onQueryStart?.(event));
  }
};

export const emitQuerySuccess = async (
  instrumentations: readonly ClickHouseORMInstrumentation[],
  event: ClickHouseORMQueryResultEvent,
): Promise<void> => {
  for (let index = instrumentations.length - 1; index >= 0; index -= 1) {
    const instrumentation = instrumentations[index];
    if (!instrumentation.onQuerySuccess) {
      continue;
    }
    await invokeInstrumentation(() => instrumentation.onQuerySuccess?.(event));
  }
};

export const emitQueryError = async (
  instrumentations: readonly ClickHouseORMInstrumentation[],
  event: ClickHouseORMQueryErrorEvent,
): Promise<void> => {
  for (let index = instrumentations.length - 1; index >= 0; index -= 1) {
    const instrumentation = instrumentations[index];
    if (!instrumentation.onQueryError) {
      continue;
    }
    await invokeInstrumentation(() => instrumentation.onQueryError?.(event));
  }
};

const invokeInstrumentation = async (operation: () => void | Promise<void>) => {
  try {
    await operation();
  } catch (instrumentationError) {
    try {
      console.error("[ck-orm] instrumentation hook threw:", instrumentationError);
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

export const createLoggerInstrumentation = (
  logger: ClickHouseORMLogger,
  minimumLevel: ClickHouseORMLogLevel = "warn",
): ClickHouseORMInstrumentation => {
  return {
    onQueryStart(event) {
      if (!shouldWriteLog(minimumLevel, "debug")) {
        return;
      }
      logger.debug("[clickhouse] orm", {
        ...buildBaseLogFields(event),
        outcome: "start",
      });
    },
    onQuerySuccess(event) {
      if (!shouldWriteLog(minimumLevel, "debug")) {
        return;
      }
      logger.debug("[clickhouse] orm", {
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
      logger.error("[clickhouse] orm", {
        ...buildBaseLogFields(event),
        durationMs: event.durationMs,
        outcome: "error",
        rowCount: event.rowCount,
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

export const createTracingInstrumentation = (
  options: ClickHouseORMTracingOptions = {},
): ClickHouseORMInstrumentation => {
  const tracer = options.tracer ?? trace.getTracer("ck-orm");
  const includeStatement = options.includeStatement ?? true;
  const spanByExecutionId = new Map<string, Span>();

  // Common epilogue shared by `onQuerySuccess` / `onQueryError`: pull the span,
  // detach it from the lookup map, write the per-result attributes that don't
  // depend on outcome, then let the caller stamp status + close.
  const closeSpan = (
    event: ClickHouseORMQueryResultEvent | ClickHouseORMQueryErrorEvent,
    finalize: (span: Span) => void,
  ): void => {
    const span = spanByExecutionId.get(event.executionId);
    if (!span) return;
    spanByExecutionId.delete(event.executionId);

    if (event.format) {
      span.setAttribute("db.response.format", event.format);
    }
    if (shouldIncludeRowCount(options.includeRowCount, event.mode) && typeof event.rowCount === "number") {
      span.setAttribute("db.response.row_count", event.rowCount);
      span.setAttribute("db.response.returned_rows", event.rowCount);
    }
    span.setAttribute("db.query.duration_ms", event.durationMs);
    span.setAttribute("ck_orm.duration_ms", event.durationMs);

    finalize(span);
    span.end();
  };

  return {
    onQueryStart(event) {
      const span = tracer.startSpan(buildClickHouseSpanName(event.operation), {
        kind: SpanKind.CLIENT,
        attributes: buildSpanAttributes(event, {
          attributes: options.attributes,
          includeStatement,
        }),
      });
      spanByExecutionId.set(event.executionId, span);
    },
    onQuerySuccess(event) {
      closeSpan(event, (span) => {
        setOptionalNumberAttribute(span, "ck_orm.server.elapsed_ms", event.serverElapsedMs);
        setOptionalNumberAttribute(span, "ck_orm.read.rows", event.readRows);
        setOptionalNumberAttribute(span, "ck_orm.read.bytes", event.readBytes);
        setOptionalNumberAttribute(span, "ck_orm.result.rows", event.resultRows);
        setOptionalNumberAttribute(span, "ck_orm.rows_before_limit_at_least", event.rowsBeforeLimitAtLeast);
        span.setStatus({ code: SpanStatusCode.OK });
      });
    },
    onQueryError(event) {
      closeSpan(event, (span) => {
        setErrorAttributes(span, event.error);
        span.recordException(toSpanException(event.error));
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: event.error instanceof Error ? event.error.message : String(event.error),
        });
      });
    },
  };
};

const shouldIncludeRowCount = (includeRowCount: boolean | undefined, mode: ClickHouseORMQueryMode) => {
  if (includeRowCount !== undefined) {
    return includeRowCount;
  }
  return mode === "query";
};

const toSpanException = (error: unknown): Error | string => {
  if (error instanceof Error) {
    return error;
  }
  if (typeof error === "string") return error;
  return String(error);
};

const setOptionalNumberAttribute = (span: Span, key: string, value: number | undefined) => {
  if (typeof value === "number") {
    span.setAttribute(key, value);
  }
};

const setErrorAttributes = (span: Span, error: unknown) => {
  if (!isClickHouseORMError(error)) {
    span.setAttribute("error.type", error instanceof Error ? error.name : typeof error);
    return;
  }

  span.setAttribute("error.type", error.clickhouseName ?? error.kind);
  span.setAttribute("ck_orm.error.kind", error.kind);
  span.setAttribute("ck_orm.execution_state", error.executionState);
  if (typeof error.clickhouseCode === "number") {
    span.setAttribute("db.response.status_code", String(error.clickhouseCode));
  } else if (typeof error.httpStatus === "number") {
    span.setAttribute("db.response.status_code", String(error.httpStatus));
  }
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

const buildSpanAttributes = (
  event: ClickHouseORMQueryEvent,
  options: {
    attributes?: Record<string, string | number | boolean>;
    includeStatement: boolean;
  },
) => {
  const attributes: Record<string, string | number | boolean> = {
    ...filterCustomTracingAttributes(options.attributes),
    "db.system": "clickhouse",
    "db.system.name": "clickhouse",
    "db.operation": event.operation,
    "db.operation.name": event.operation,
    "db.query.summary": event.querySummary,
    "db.query.kind": event.queryKind,
    "db.query.mode": event.mode,
    "db.statement.hash": event.statementHash,
    "ck_orm.execution_id": event.executionId,
    "ck_orm.query.kind": event.queryKind,
    "ck_orm.query.mode": event.mode,
    "ck_orm.statement.hash": event.statementHash,
  };

  if (event.databaseName !== undefined) {
    attributes["db.name"] = event.databaseName;
    attributes["db.namespace"] = event.databaseName;
  }
  if (event.queryId !== undefined) {
    attributes["db.query.id"] = event.queryId;
    attributes["ck_orm.query_id"] = event.queryId;
  }
  if (event.sessionId !== undefined) {
    attributes["db.session.id"] = event.sessionId;
    attributes["ck_orm.session_id"] = event.sessionId;
  }
  if (event.tableName !== undefined) {
    attributes["db.table"] = event.tableName;
    attributes["db.collection.name"] = event.tableName;
  }
  if (event.format !== undefined) {
    attributes["db.response.format"] = event.format;
    attributes["ck_orm.format"] = event.format;
  }
  if (event.serverAddress !== undefined) attributes["server.address"] = event.serverAddress;
  if (event.serverPort !== undefined) attributes["server.port"] = event.serverPort;
  if (event.requestTimeoutMs !== undefined) attributes["ck_orm.request_timeout_ms"] = event.requestTimeoutMs;

  if (options.includeStatement) {
    attributes["db.statement"] = event.compactStatement;
    attributes["db.query.text"] = event.compactStatement;
  }

  return attributes;
};

const buildQuerySummary = (operation: string, tableName: string | undefined): string => {
  return tableName ? `${operation} ${tableName}` : operation;
};

const filterCustomTracingAttributes = (attributes: Record<string, string | number | boolean> | undefined) => {
  if (!attributes) {
    return {};
  }

  const filtered: Record<string, string | number | boolean> = {};
  for (const [key, value] of Object.entries(attributes)) {
    if (key.startsWith(RESERVED_TRACING_ATTRIBUTE_PREFIX)) {
      continue;
    }
    filtered[key] = value;
  }

  return filtered;
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

const buildClickHouseSpanName = (operation: string): string => {
  if (operation === "SELECT" || operation === "QUERY") {
    return "clickhouse QUERY";
  }
  return `clickhouse ${operation}`;
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
