import {
  context,
  propagation,
  type Span,
  SpanKind,
  SpanStatusCode,
  TraceFlags,
  type Tracer,
  trace,
} from "@opentelemetry/api";
import { isClickHouseORMError } from "./errors";
import type {
  ClickHouseORMPlugin,
  ClickHouseORMQueryErrorEvent,
  ClickHouseORMQueryEvent,
  ClickHouseORMQueryMode,
  ClickHouseORMQueryResultEvent,
} from "./observability";

export interface ClickHouseORMTracingOptions {
  readonly tracer?: Tracer;
  readonly attributes?: Record<string, string | number | boolean>;
  readonly includeStatement?: boolean;
  readonly includeRowCount?: boolean;
  /**
   * Send the span's W3C trace context (`traceparent` / `tracestate`) with each
   * request so ClickHouse records its server-side spans under it. Only sampled
   * spans are propagated, and only those two headers are forwarded. Defaults
   * to `false`.
   */
  readonly propagateTraceContext?: boolean;
}

const RESERVED_TRACING_ATTRIBUTE_PREFIX = "db.";
const TRACE_CONTEXT_HEADERS = ["traceparent", "tracestate"] as const;

export const otelTracing = (options: ClickHouseORMTracingOptions = {}): ClickHouseORMPlugin => {
  const tracer = options.tracer ?? trace.getTracer("ck-orm");
  const includeStatement = options.includeStatement ?? true;
  const propagateTraceContext = options.propagateTraceContext ?? false;
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
    const rowCount = "error" in event ? event.partialRowCount : event.rowCount;
    if (shouldIncludeRowCount(options.includeRowCount, event.mode) && typeof rowCount === "number") {
      span.setAttribute("db.response.row_count", rowCount);
      span.setAttribute("db.response.returned_rows", rowCount);
    }
    span.setAttribute("db.query.duration_ms", event.durationMs);
    span.setAttribute("ck_orm.duration_ms", event.durationMs);

    finalize(span);
    span.end();
  };

  return {
    name: "otel",
    onQueryStart(event) {
      const span = tracer.startSpan(buildClickHouseSpanName(event.operation), {
        kind: SpanKind.CLIENT,
        attributes: buildSpanAttributes(event, {
          attributes: options.attributes,
          includeStatement,
        }),
      });
      spanByExecutionId.set(event.executionId, span);

      // ClickHouse records server spans for any trace context it receives,
      // sampled or not, so only propagate traces the SDK will export.
      if (!propagateTraceContext || (span.spanContext().traceFlags & TraceFlags.SAMPLED) === 0) {
        return;
      }
      const carrier: Record<string, string> = {};
      propagation.inject(trace.setSpan(context.active(), span), carrier);
      // Forward only W3C trace context: the global propagator may also emit
      // baggage (often user or tenant ids) or vendor headers ClickHouse ignores.
      const headers: Record<string, string> = {};
      for (const key of TRACE_CONTEXT_HEADERS) {
        const value = carrier[key];
        if (value !== undefined) {
          headers[key] = value;
        }
      }
      return { http_headers: headers };
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

const buildClickHouseSpanName = (operation: string): string => {
  if (operation === "SELECT" || operation === "QUERY") {
    return "clickhouse QUERY";
  }
  return `clickhouse ${operation}`;
};
