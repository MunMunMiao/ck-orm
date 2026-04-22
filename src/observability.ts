import { type Span, SpanKind, SpanStatusCode, type Tracer, trace } from "@opentelemetry/api";
import { createUuid, hashString } from "./platform";

export type ClickHouseOrmLogLevel = "trace" | "debug" | "info" | "warn" | "error";
export type ClickHouseOrmQueryMode = "query" | "stream" | "command" | "insert";
export type ClickHouseOrmQueryKind = "typed" | "raw";

export interface ClickHouseOrmLogger {
  trace(message: string, fields?: Record<string, unknown>): void;
  debug(message: string, fields?: Record<string, unknown>): void;
  info(message: string, fields?: Record<string, unknown>): void;
  warn(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
}

export interface ClickHouseOrmTracingOptions {
  readonly tracer?: Tracer;
  readonly attributes?: Record<string, string | number | boolean>;
  readonly includeStatement?: boolean;
  readonly includeRowCount?: boolean;
  readonly dbName?: string;
}

export interface ClickHouseOrmQueryEvent {
  readonly executionId: string;
  readonly system: "clickhouse";
  readonly mode: ClickHouseOrmQueryMode;
  readonly queryKind: ClickHouseOrmQueryKind;
  readonly statement: string;
  readonly operation: string;
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

export interface ClickHouseOrmQueryResultEvent extends ClickHouseOrmQueryEvent {
  readonly durationMs: number;
  /** Number of rows produced by the query / yielded by the stream. Undefined for command and insert modes. */
  readonly rowCount?: number;
}

export interface ClickHouseOrmQueryErrorEvent extends ClickHouseOrmQueryEvent {
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

export interface ClickHouseOrmInstrumentation {
  onQueryStart?(event: ClickHouseOrmQueryEvent): void | Promise<void>;
  onQuerySuccess?(event: ClickHouseOrmQueryResultEvent): void | Promise<void>;
  onQueryError?(event: ClickHouseOrmQueryErrorEvent): void | Promise<void>;
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
  event: Omit<ClickHouseOrmQueryEvent, "executionId" | "system">,
): ClickHouseOrmQueryEvent => {
  return {
    executionId: createUuid(),
    system: "clickhouse",
    ...event,
  };
};

export const createQuerySuccessEvent = (
  event: ClickHouseOrmQueryEvent,
  durationMs: number,
  rowCount?: number,
): ClickHouseOrmQueryResultEvent => {
  return {
    ...event,
    durationMs,
    ...(rowCount === undefined ? {} : { rowCount }),
  };
};

export const createQueryErrorEvent = (
  event: ClickHouseOrmQueryEvent,
  error: unknown,
  durationMs: number,
  partialRowCount?: number,
): ClickHouseOrmQueryErrorEvent => {
  return {
    ...event,
    durationMs,
    error,
    ...(partialRowCount === undefined ? {} : { rowCount: partialRowCount, partialRowCount }),
  };
};

export const emitQueryStart = async (
  instrumentations: readonly ClickHouseOrmInstrumentation[],
  event: ClickHouseOrmQueryEvent,
): Promise<void> => {
  for (const instrumentation of instrumentations) {
    if (!instrumentation.onQueryStart) {
      continue;
    }
    await invokeInstrumentation(() => instrumentation.onQueryStart?.(event));
  }
};

export const emitQuerySuccess = async (
  instrumentations: readonly ClickHouseOrmInstrumentation[],
  event: ClickHouseOrmQueryResultEvent,
): Promise<void> => {
  for (const instrumentation of [...instrumentations].reverse()) {
    if (!instrumentation.onQuerySuccess) {
      continue;
    }
    await invokeInstrumentation(() => instrumentation.onQuerySuccess?.(event));
  }
};

export const emitQueryError = async (
  instrumentations: readonly ClickHouseOrmInstrumentation[],
  event: ClickHouseOrmQueryErrorEvent,
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

const logLevelPriority: Record<ClickHouseOrmLogLevel, number> = {
  trace: 10,
  debug: 20,
  info: 30,
  warn: 40,
  error: 50,
};

const shouldWriteLog = (minimumLevel: ClickHouseOrmLogLevel, eventLevel: ClickHouseOrmLogLevel) => {
  return logLevelPriority[eventLevel] >= logLevelPriority[minimumLevel];
};

export const createLoggerInstrumentation = (
  logger: ClickHouseOrmLogger,
  minimumLevel: ClickHouseOrmLogLevel = "warn",
): ClickHouseOrmInstrumentation => {
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
      });
    },
    onQueryError(event) {
      logger.error("[clickhouse] orm", {
        ...buildBaseLogFields(event),
        durationMs: event.durationMs,
        outcome: "error",
        rowCount: event.rowCount,
        error: event.error,
      });
    },
  };
};

export const createTracingInstrumentation = (
  options: ClickHouseOrmTracingOptions = {},
): ClickHouseOrmInstrumentation => {
  const tracer = options.tracer ?? trace.getTracer("ck-orm");
  const dbName = options.dbName;
  const includeStatement = options.includeStatement ?? true;
  const spanByExecutionId = new Map<string, Span>();

  return {
    onQueryStart(event) {
      const span = tracer.startSpan(buildClickHouseSpanName(event.operation), {
        kind: SpanKind.CLIENT,
        attributes: buildSpanAttributes(event, {
          attributes: options.attributes,
          dbName,
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
      }
      span.setAttribute("db.query.duration_ms", event.durationMs);
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
      }
      span.setAttribute("db.query.duration_ms", event.durationMs);
      span.recordException(toSpanException(event.error));
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: event.error instanceof Error ? event.error.message : String(event.error),
      });
      span.end();
    },
  };
};

const shouldIncludeRowCount = (includeRowCount: boolean | undefined, mode: ClickHouseOrmQueryMode) => {
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

const buildBaseLogFields = (event: ClickHouseOrmQueryEvent) => {
  return {
    executionId: event.executionId,
    provider: "clickhouse",
    system: event.system,
    mode: event.mode,
    queryKind: event.queryKind,
    operation: event.operation,
    statement: event.statement,
    statementHash: hashStatement(event.statement),
    queryId: event.queryId,
    sessionId: event.sessionId,
    format: event.format,
    settings: event.settings,
    tableName: event.tableName,
    startedAt: event.startedAt,
  };
};

const buildSpanAttributes = (
  event: ClickHouseOrmQueryEvent,
  options: {
    attributes?: Record<string, string | number | boolean>;
    dbName?: string;
    includeStatement: boolean;
  },
) => {
  const customAttributes = filterCustomTracingAttributes(options.attributes);
  const attributes: Record<string, string | number | boolean> = {
    "db.system": "clickhouse",
    "db.operation": event.operation,
    "db.query.kind": event.queryKind,
    "db.query.mode": event.mode,
    "db.statement.hash": hashStatement(event.statement),
    ...(options.dbName === undefined ? {} : { "db.name": options.dbName }),
    ...(event.queryId === undefined ? {} : { "db.query.id": event.queryId }),
    ...(event.sessionId === undefined ? {} : { "db.session.id": event.sessionId }),
    ...(event.tableName === undefined ? {} : { "db.table": event.tableName }),
    ...(event.format === undefined ? {} : { "db.response.format": event.format }),
    ...customAttributes,
  };

  if (options.includeStatement) {
    attributes["db.statement"] = compactStatement(event.statement);
  }

  return attributes;
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
