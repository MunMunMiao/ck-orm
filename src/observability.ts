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
  event: Omit<ClickHouseORMQueryEvent, "executionId" | "system" | "statementHash" | "querySummary">,
): ClickHouseORMQueryEvent => {
  return {
    executionId: createUuid(),
    system: "clickhouse",
    statementHash: hashStatement(event.statement),
    querySummary: buildQuerySummary(event.operation, event.tableName),
    ...event,
  };
};

export const createQuerySuccessEvent = (
  event: ClickHouseORMQueryEvent,
  durationMs: number,
  rowCount?: number,
  statistics?: ClickHouseORMQueryStatistics,
): ClickHouseORMQueryResultEvent => {
  return {
    ...event,
    durationMs,
    ...(rowCount === undefined ? {} : { rowCount }),
    ...(statistics?.serverElapsedMs === undefined ? {} : { serverElapsedMs: statistics.serverElapsedMs }),
    ...(statistics?.readRows === undefined ? {} : { readRows: statistics.readRows }),
    ...(statistics?.readBytes === undefined ? {} : { readBytes: statistics.readBytes }),
    ...(statistics?.resultRows === undefined ? {} : { resultRows: statistics.resultRows }),
    ...(statistics?.rowsBeforeLimitAtLeast === undefined
      ? {}
      : { rowsBeforeLimitAtLeast: statistics.rowsBeforeLimitAtLeast }),
  };
};

export const createQueryErrorEvent = (
  event: ClickHouseORMQueryEvent,
  error: unknown,
  durationMs: number,
  partialRowCount?: number,
): ClickHouseORMQueryErrorEvent => {
  return {
    ...event,
    durationMs,
    error,
    ...(partialRowCount === undefined ? {} : { rowCount: partialRowCount, partialRowCount }),
  };
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
  for (const instrumentation of [...instrumentations].reverse()) {
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
  for (const instrumentation of [...instrumentations].reverse()) {
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
      const span = spanByExecutionId.get(event.executionId);
      if (!span) {
        return;
      }
      spanByExecutionId.delete(event.executionId);
      if (event.format) {
        span.setAttribute("db.response.format", event.format);
      }
      if (shouldIncludeRowCount(options.includeRowCount, event.mode) && typeof event.rowCount === "number") {
        span.setAttribute("db.response.row_count", event.rowCount);
        span.setAttribute("db.response.returned_rows", event.rowCount);
      }
      setOptionalNumberAttribute(span, "ck_orm.server.elapsed_ms", event.serverElapsedMs);
      setOptionalNumberAttribute(span, "ck_orm.read.rows", event.readRows);
      setOptionalNumberAttribute(span, "ck_orm.read.bytes", event.readBytes);
      setOptionalNumberAttribute(span, "ck_orm.result.rows", event.resultRows);
      setOptionalNumberAttribute(span, "ck_orm.rows_before_limit_at_least", event.rowsBeforeLimitAtLeast);
      span.setAttribute("db.query.duration_ms", event.durationMs);
      span.setAttribute("ck_orm.duration_ms", event.durationMs);
      span.setStatus({ code: SpanStatusCode.OK });
      span.end();
    },
    onQueryError(event) {
      const span = spanByExecutionId.get(event.executionId);
      if (!span) {
        return;
      }
      spanByExecutionId.delete(event.executionId);
      if (event.format) {
        span.setAttribute("db.response.format", event.format);
      }
      if (shouldIncludeRowCount(options.includeRowCount, event.mode) && typeof event.rowCount === "number") {
        span.setAttribute("db.response.row_count", event.rowCount);
        span.setAttribute("db.response.returned_rows", event.rowCount);
      }
      setErrorAttributes(span, event.error);
      span.setAttribute("db.query.duration_ms", event.durationMs);
      span.setAttribute("ck_orm.duration_ms", event.durationMs);
      span.recordException(toSpanException(event.error));
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: event.error instanceof Error ? event.error.message : String(event.error),
      });
      span.end();
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
  const customAttributes = filterCustomTracingAttributes(options.attributes);
  const attributes: Record<string, string | number | boolean> = {
    ...customAttributes,
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
    ...(event.databaseName === undefined ? {} : { "db.name": event.databaseName, "db.namespace": event.databaseName }),
    ...(event.queryId === undefined ? {} : { "db.query.id": event.queryId }),
    ...(event.queryId === undefined ? {} : { "ck_orm.query_id": event.queryId }),
    ...(event.sessionId === undefined ? {} : { "db.session.id": event.sessionId }),
    ...(event.sessionId === undefined ? {} : { "ck_orm.session_id": event.sessionId }),
    ...(event.tableName === undefined ? {} : { "db.table": event.tableName, "db.collection.name": event.tableName }),
    ...(event.format === undefined ? {} : { "db.response.format": event.format }),
    ...(event.format === undefined ? {} : { "ck_orm.format": event.format }),
    ...(event.serverAddress === undefined ? {} : { "server.address": event.serverAddress }),
    ...(event.serverPort === undefined ? {} : { "server.port": event.serverPort }),
    ...(event.requestTimeoutMs === undefined ? {} : { "ck_orm.request_timeout_ms": event.requestTimeoutMs }),
  };

  if (options.includeStatement) {
    attributes["db.statement"] = compactStatement(event.statement);
    attributes["db.query.text"] = compactStatement(event.statement);
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

export const resolveSqlOperation = (statement: string): string => {
  const normalized = compactStatement(stripLeadingSqlComments(statement)).toUpperCase();
  if (!normalized) {
    return "QUERY";
  }

  const tokens = normalized.split(/\s+/);
  const firstToken = tokens[0] ?? normalized;

  if (firstToken === "WITH") {
    for (const token of tokens) {
      if (token === "SELECT" || token === "INSERT" || token === "UPDATE" || token === "DELETE") {
        return token;
      }
    }
    return "QUERY";
  }

  switch (firstToken) {
    case "SELECT":
    case "INSERT":
    case "UPDATE":
    case "DELETE":
    case "MERGE":
    case "CALL":
    case "CREATE":
    case "ALTER":
    case "DROP":
    case "TRUNCATE":
      return firstToken;
    default:
      return "QUERY";
  }
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
