import { afterEach, describe, expect, it } from "bun:test";
import { propagation, type Span, type TextMapPropagator, TraceFlags, type Tracer, trace } from "@opentelemetry/api";
import { createRequestFailedError } from "./errors";
import { createQueryErrorEvent, createQueryEvent, createQuerySuccessEvent, hashStatement } from "./observability";
import * as otelApi from "./otel";
import { otelTracing } from "./otel";
import { clickhouseClient } from "./runtime";
import { sql } from "./sql";

const TRACE_ID = "0af7651916cd43dd8448eb211c80319c";

// Minimal W3C injector so the tests need no OTel SDK.
const testPropagator: TextMapPropagator = {
  inject(ctx, carrier, setter) {
    const spanContext = trace.getSpanContext(ctx);
    if (spanContext) {
      setter.set(carrier, "traceparent", `00-${spanContext.traceId}-${spanContext.spanId}-0${spanContext.traceFlags}`);
    }
  },
  extract: (ctx) => ctx,
  fields: () => ["traceparent"],
};

const createCapturedTracer = (traceFlags: number = TraceFlags.NONE) => {
  const spans: Array<{
    name: string;
    spanId: string;
    attributes: Record<string, unknown>;
    ended: boolean;
    status?: unknown;
    exceptions: unknown[];
  }> = [];

  const tracer = {
    startSpan(name: string, options?: { attributes?: Record<string, unknown> }) {
      const spanState = {
        name,
        spanId: (spans.length + 1).toString(16).padStart(16, "0"),
        attributes: { ...(options?.attributes ?? {}) } as Record<string, unknown>,
        ended: false,
        status: undefined as unknown,
        exceptions: [] as unknown[],
      };
      spans.push(spanState);

      const span: Partial<Span> = {
        spanContext: () => ({ traceId: TRACE_ID, spanId: spanState.spanId, traceFlags }),
        setAttribute(key, value) {
          spanState.attributes[key] = value;
          return span as Span;
        },
        setStatus(status) {
          spanState.status = status;
          return span as Span;
        },
        recordException(exception) {
          spanState.exceptions.push(exception);
        },
        end() {
          spanState.ended = true;
        },
      };
      return span as Span;
    },
  } as Tracer;

  return { spans, tracer };
};

const clientCapturingHeaders = (plugin: ReturnType<typeof otelTracing>, seen: Headers[]) =>
  clickhouseClient({
    host: "http://localhost:8123",
    plugins: [plugin],
    fetch(request) {
      seen.push(request.headers);
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    },
  });

describe("ck-orm/otel", function describeOtelPlugin() {
  afterEach(function resetPropagator() {
    propagation.disable();
  });

  it("exports only otelTracing at runtime", function testOtelExports() {
    expect(Object.keys(otelApi)).toEqual(["otelTracing"]);
    expect(otelTracing().name).toBe("otel");
  });

  it("sends the sampled span's traceparent with the request", async function testPropagatesSampledSpan() {
    propagation.setGlobalPropagator(testPropagator);
    const tracer = createCapturedTracer(TraceFlags.SAMPLED);
    const seen: Headers[] = [];

    await clientCapturingHeaders(otelTracing({ tracer: tracer.tracer, propagateTraceContext: true }), seen).execute(
      sql`select 1`,
    );

    expect(seen[0]?.get("traceparent")).toBe(`00-${TRACE_ID}-${tracer.spans[0]?.spanId}-01`);
  });

  it("does not propagate unsampled spans", async function testSkipsUnsampledSpan() {
    propagation.setGlobalPropagator(testPropagator);
    const seen: Headers[] = [];

    await clientCapturingHeaders(
      otelTracing({ tracer: createCapturedTracer(TraceFlags.NONE).tracer, propagateTraceContext: true }),
      seen,
    ).execute(sql`select 1`);

    expect(seen[0]?.get("traceparent")).toBeNull();
  });

  it("does not propagate by default", async function testPropagationOffByDefault() {
    propagation.setGlobalPropagator(testPropagator);
    const seen: Headers[] = [];

    await clientCapturingHeaders(
      otelTracing({ tracer: createCapturedTracer(TraceFlags.SAMPLED).tracer }),
      seen,
    ).execute(sql`select 1`);

    expect(seen[0]?.get("traceparent")).toBeNull();
  });

  it("forwards only W3C trace context headers", async function testForwardsOnlyTraceContext() {
    propagation.setGlobalPropagator({
      inject(_ctx, carrier, setter) {
        setter.set(carrier, "traceparent", `00-${TRACE_ID}-${"3".repeat(16)}-01`);
        setter.set(carrier, "tracestate", "vendor=1");
        setter.set(carrier, "baggage", "tenant.id=acme");
        setter.set(carrier, "b3", "1-2-1");
      },
      extract: (ctx) => ctx,
      fields: () => ["traceparent", "tracestate", "baggage", "b3"],
    });
    const seen: Headers[] = [];

    await clientCapturingHeaders(
      otelTracing({ tracer: createCapturedTracer(TraceFlags.SAMPLED).tracer, propagateTraceContext: true }),
      seen,
    ).execute(sql`select 1`);

    expect(seen[0]?.get("traceparent")).toBe(`00-${TRACE_ID}-${"3".repeat(16)}-01`);
    expect(seen[0]?.get("tracestate")).toBe("vendor=1");
    expect(seen[0]?.get("baggage")).toBeNull();
    expect(seen[0]?.get("b3")).toBeNull();
  });

  it("sends nothing when no propagator is registered", async function testNoPropagator() {
    const seen: Headers[] = [];

    await clientCapturingHeaders(
      otelTracing({ tracer: createCapturedTracer(TraceFlags.SAMPLED).tracer, propagateTraceContext: true }),
      seen,
    ).execute(sql`select 1`);

    expect(seen[0]?.get("traceparent")).toBeNull();
  });

  it("lets a per-request traceparent override the plugin", async function testRequestHeaderWins() {
    propagation.setGlobalPropagator(testPropagator);
    const seen: Headers[] = [];
    const explicit = `00-${"1".repeat(32)}-${"2".repeat(16)}-01`;

    await clientCapturingHeaders(
      otelTracing({ tracer: createCapturedTracer(TraceFlags.SAMPLED).tracer, propagateTraceContext: true }),
      seen,
    ).execute(sql`select 1`, { http_headers: { traceparent: explicit } });

    expect(seen[0]?.get("traceparent")).toBe(explicit);
  });

  it("names ping and replicas_status spans", async function testEndpointSpans() {
    const tracer = createCapturedTracer();
    const db = clickhouseClient({
      host: "http://localhost:8123",
      plugins: [otelTracing({ tracer: tracer.tracer })],
      fetch: () => new Response("Ok.\n", { status: 200 }),
    });

    await db.ping();
    await db.replicasStatus();

    expect(tracer.spans.map((span) => span.name)).toEqual(["clickhouse PING", "clickhouse REPLICAS_STATUS"]);
  });

  it("creates OTel spans and keeps custom attributes additive only", async function testTracingInstrumentation() {
    const tracer = createCapturedTracer();
    const instrumentation = otelTracing({
      tracer: tracer.tracer,
      includeStatement: false,
      attributes: {
        scope: "custom",
        "db.system": "evil",
        "db.statement": "select secret",
        "db.operation": "DELETE",
      },
    });

    const event = createQueryEvent({
      mode: "query",
      queryKind: "typed",
      statement: "select * from users",
      operation: "SELECT",
      databaseName: "demo_store",
      serverAddress: "localhost",
      serverPort: 8123,
      requestTimeoutMs: 30_000,
      queryId: "query_1",
      tableName: "users",
      startedAt: 100,
    });

    await instrumentation.onQueryStart?.(event);
    await instrumentation.onQuerySuccess?.(
      createQuerySuccessEvent(event, 8, 1, {
        serverElapsedMs: 2,
        readRows: 10,
        readBytes: 128,
        resultRows: 1,
        rowsBeforeLimitAtLeast: 20,
      }),
    );

    expect(tracer.spans).toHaveLength(1);
    expect(tracer.spans[0]).toMatchObject({
      name: "clickhouse QUERY",
      ended: true,
      attributes: expect.objectContaining({
        "db.system": "clickhouse",
        "db.system.name": "clickhouse",
        "db.operation": "SELECT",
        "db.operation.name": "SELECT",
        "db.namespace": "demo_store",
        "db.name": "demo_store",
        "db.collection.name": "users",
        "db.query.summary": "SELECT users",
        "db.response.returned_rows": 1,
        "db.response.row_count": 1,
        "server.address": "localhost",
        "server.port": 8123,
        "ck_orm.query_id": "query_1",
        "ck_orm.statement.hash": hashStatement("select * from users"),
        "ck_orm.server.elapsed_ms": 2,
        "ck_orm.read.rows": 10,
        "ck_orm.read.bytes": 128,
        "ck_orm.rows_before_limit_at_least": 20,
        scope: "custom",
      }),
    });
    expect(tracer.spans[0]?.attributes["db.statement"]).toBeUndefined();
    expect(tracer.spans[0]?.attributes["db.query.text"]).toBeUndefined();

    const streamEvent = createQueryEvent({
      mode: "stream",
      queryKind: "raw",
      statement: "select * from users",
      operation: "SELECT",
      startedAt: 200,
      format: "JSONEachRow",
    });
    await instrumentation.onQueryStart?.(streamEvent);
    await instrumentation.onQueryError?.(createQueryErrorEvent(streamEvent, "bad", 20, 5));
    expect(tracer.spans[1]?.attributes["db.response.row_count"]).toBeUndefined();
    expect(tracer.spans[1]?.exceptions).toEqual(["bad"]);

    const clickhouseErrorEvent = createQueryEvent({
      mode: "query",
      queryKind: "raw",
      statement: "select coalesce(toFloat64(1), toInt64(0))",
      operation: "SELECT",
      startedAt: 300,
    });
    await instrumentation.onQueryStart?.(clickhouseErrorEvent);
    await instrumentation.onQueryError?.(
      createQueryErrorEvent(
        clickhouseErrorEvent,
        createRequestFailedError({
          httpStatus: 500,
          clickhouseCode: 386,
          clickhouseName: "NO_COMMON_TYPE",
          responseText: "Code: 386. DB::Exception: no common type (NO_COMMON_TYPE)",
        }),
        30,
      ),
    );
    expect(tracer.spans[2]?.attributes).toMatchObject({
      "error.type": "NO_COMMON_TYPE",
      "ck_orm.error.kind": "request_failed",
      "ck_orm.execution_state": "rejected",
      "db.response.status_code": "386",
    });

    const httpErrorEvent = createQueryEvent({
      mode: "query",
      queryKind: "raw",
      statement: "select 1",
      operation: "SELECT",
      startedAt: 400,
    });
    await instrumentation.onQueryStart?.(httpErrorEvent);
    await instrumentation.onQueryError?.(
      createQueryErrorEvent(
        httpErrorEvent,
        createRequestFailedError({
          httpStatus: 503,
          responseText: "temporarily unavailable",
        }),
        40,
      ),
    );
    expect(tracer.spans[3]?.attributes).toMatchObject({
      "error.type": "request_failed",
      "db.response.status_code": "503",
    });
  });

  it("covers tracing early returns", async function testTracingEdgeCases() {
    const noStartTracer = createCapturedTracer();
    const noStartInstrumentation = otelTracing({
      tracer: noStartTracer.tracer,
    });
    const baseEvent = createQueryEvent({
      mode: "query",
      queryKind: "typed",
      statement: "select * from users",
      operation: "SELECT",
      startedAt: 300,
    });

    await noStartInstrumentation.onQuerySuccess?.(createQuerySuccessEvent(baseEvent, 3, 1));
    await noStartInstrumentation.onQueryError?.(createQueryErrorEvent(baseEvent, new Error("boom"), 4, 2));
    expect(noStartTracer.spans).toHaveLength(0);

    const traced = createCapturedTracer();
    const tracedInstrumentation = otelTracing({
      tracer: traced.tracer,
      includeRowCount: true,
    });
    const streamEvent = createQueryEvent({
      mode: "stream",
      queryKind: "raw",
      statement: "select * from users",
      operation: "SELECT",
      startedAt: 400,
      format: "JSONEachRow",
    });
    await tracedInstrumentation.onQueryStart?.(streamEvent);
    await tracedInstrumentation.onQuerySuccess?.(createQuerySuccessEvent(streamEvent, 5, 2));

    const errorEvent = createQueryEvent({
      mode: "stream",
      queryKind: "raw",
      statement: "insert into users values (1)",
      operation: "INSERT",
      startedAt: 500,
      format: "JSONEachRow",
    });
    const boom = new Error("boom");
    await tracedInstrumentation.onQueryStart?.(errorEvent);
    await tracedInstrumentation.onQueryError?.(createQueryErrorEvent(errorEvent, boom, 6, 3));

    expect(traced.spans[0]?.attributes["db.response.format"]).toBe("JSONEachRow");
    expect(traced.spans[0]?.attributes["db.response.row_count"]).toBe(2);
    expect(traced.spans[0]?.attributes["db.response.returned_rows"]).toBe(2);
    expect(traced.spans[1]?.exceptions).toEqual([boom]);
    expect(traced.spans[1]?.attributes["error.type"]).toBe("Error");
    expect(traced.spans[1]?.attributes["db.response.row_count"]).toBe(3);
    expect(traced.spans[1]?.attributes["db.response.returned_rows"]).toBe(3);

    const noCountTracer = createCapturedTracer();
    const noCountInstrumentation = otelTracing({
      tracer: noCountTracer.tracer,
      includeRowCount: false,
    });
    await noCountInstrumentation.onQueryStart?.(baseEvent);
    await noCountInstrumentation.onQuerySuccess?.(createQuerySuccessEvent(baseEvent, 7, 4));
    expect(noCountTracer.spans[0]?.attributes["db.response.row_count"]).toBeUndefined();
    expect(noCountTracer.spans[0]?.attributes["db.response.returned_rows"]).toBeUndefined();
  });

  it("attaches db.session.id and ck_orm.session_id to tracing spans when the event carries sessionId", async function testTracingSessionAttributes() {
    const tracer = createCapturedTracer();
    const instrumentation = otelTracing({
      tracer: tracer.tracer,
    });

    const event = createQueryEvent({
      mode: "query",
      queryKind: "typed",
      statement: "select 1",
      operation: "SELECT",
      startedAt: 0,
      sessionId: "session_abc",
    });

    await instrumentation.onQueryStart?.(event);

    const attributes = tracer.spans[0]?.attributes;
    expect(attributes?.["db.session.id"]).toBe("session_abc");
    expect(attributes?.["ck_orm.session_id"]).toBe("session_abc");

    // Cross-check: events without sessionId must not surface either attribute
    // — guarding the `event.sessionId !== undefined` gate from regressing into
    // an always-on `?? null` shape.
    const sessionlessTracer = createCapturedTracer();
    const sessionlessInstrumentation = otelTracing({
      tracer: sessionlessTracer.tracer,
    });
    const sessionlessEvent = createQueryEvent({
      mode: "query",
      queryKind: "typed",
      statement: "select 1",
      operation: "SELECT",
      startedAt: 0,
    });
    await sessionlessInstrumentation.onQueryStart?.(sessionlessEvent);
    expect(sessionlessTracer.spans[0]?.attributes["db.session.id"]).toBeUndefined();
    expect(sessionlessTracer.spans[0]?.attributes["ck_orm.session_id"]).toBeUndefined();
  });
});
