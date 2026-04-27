import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import type { Span, Tracer } from "@opentelemetry/api";
import { int32, string } from "./columns";
import {
  type ClickHouseORMInstrumentation,
  compactStatement,
  createLoggerInstrumentation,
  createQueryErrorEvent,
  createQueryEvent,
  createQuerySuccessEvent,
  createTracingInstrumentation,
  emitQueryError,
  emitQueryStart,
  emitQuerySuccess,
  hashStatement,
  resolveSafeClickHouseDestination,
  resolveSqlOperation,
} from "./observability";
import { clickhouseClient } from "./runtime";
import { ckTable } from "./schema";
import { sql } from "./sql";

const originalFetch = globalThis.fetch;

const users = ckTable(
  "users",
  {
    id: int32(),
    name: string(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

const createCapturedLogger = () => {
  const entries: Array<{
    level: string;
    message: string;
    fields?: Record<string, unknown>;
  }> = [];

  return {
    entries,
    logger: {
      trace(message: string, fields?: Record<string, unknown>) {
        entries.push({ level: "trace", message, fields });
      },
      debug(message: string, fields?: Record<string, unknown>) {
        entries.push({ level: "debug", message, fields });
      },
      info(message: string, fields?: Record<string, unknown>) {
        entries.push({ level: "info", message, fields });
      },
      warn(message: string, fields?: Record<string, unknown>) {
        entries.push({ level: "warn", message, fields });
      },
      error(message: string, fields?: Record<string, unknown>) {
        entries.push({ level: "error", message, fields });
      },
    },
  };
};

const createCapturedTracer = () => {
  const spans: Array<{
    name: string;
    attributes: Record<string, unknown>;
    ended: boolean;
    status?: unknown;
    exceptions: unknown[];
  }> = [];

  const tracer: Tracer = {
    startSpan(name: string, options?: { attributes?: Record<string, unknown> }) {
      const spanState = {
        name,
        attributes: { ...(options?.attributes ?? {}) } as Record<string, unknown>,
        ended: false,
        status: undefined as unknown,
        exceptions: [] as unknown[],
      };
      spans.push(spanState);

      const span: Partial<Span> = {
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

describe("ck-orm observability", function describeClickHouseORMObservability() {
  beforeEach(function setupMocks() {
    mock.restore();
  });

  afterEach(function teardownMocks() {
    globalThis.fetch = originalFetch;
    mock.restore();
  });

  it("logs ORM events with level filtering and resolves safe destinations", async function testLoggerInstrumentation() {
    const captured = createCapturedLogger();
    const warnOnly = createLoggerInstrumentation(captured.logger, "warn");
    const debugEnabled = createLoggerInstrumentation(captured.logger, "debug");
    const event = createQueryEvent({
      mode: "query",
      queryKind: "typed",
      statement: "select * from users",
      operation: "SELECT",
      startedAt: 100,
    });

    await warnOnly.onQueryStart?.(event);
    await warnOnly.onQuerySuccess?.(createQuerySuccessEvent(event, 10, 1));
    await warnOnly.onQueryError?.(createQueryErrorEvent(event, new Error("boom"), 20, 0));

    await debugEnabled.onQueryStart?.(event);
    await debugEnabled.onQuerySuccess?.(createQuerySuccessEvent(event, 10, 1));

    expect(captured.entries.map((entry) => entry.level)).toEqual(["error", "debug", "debug"]);
    expect(captured.entries[0]?.fields).toMatchObject({
      provider: "clickhouse",
      statement: "select * from users",
      statementHash: hashStatement("select * from users"),
      outcome: "error",
    });

    expect(resolveSafeClickHouseDestination("http://user:secret@localhost:8123/demo_store?foo=bar#hash")).toBe(
      "http://localhost:8123/demo_store",
    );
    expect(resolveSafeClickHouseDestination("https://localhost:8443")).toBe("https://localhost:8443");
    expect(resolveSafeClickHouseDestination("tcp://localhost:9000")).toBeUndefined();
    expect(resolveSafeClickHouseDestination(undefined)).toBeUndefined();
    expect(resolveSafeClickHouseDestination("not a url")).toBeUndefined();
  });

  it("creates OTel spans and keeps custom attributes additive only", async function testTracingInstrumentation() {
    const tracer = createCapturedTracer();
    const instrumentation = createTracingInstrumentation({
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
      startedAt: 100,
    });

    await instrumentation.onQueryStart?.(event);
    await instrumentation.onQuerySuccess?.(createQuerySuccessEvent(event, 8, 1));

    expect(tracer.spans).toHaveLength(1);
    expect(tracer.spans[0]).toMatchObject({
      name: "clickhouse QUERY",
      ended: true,
      attributes: expect.objectContaining({
        "db.system": "clickhouse",
        "db.operation": "SELECT",
        "db.response.row_count": 1,
        scope: "custom",
      }),
    });
    expect(tracer.spans[0]?.attributes["db.statement"]).toBeUndefined();

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
  });

  it("emits instrumentation hooks in forward/reverse order around runtime execution", async function testRuntimeInstrumentationOrder() {
    const order: string[] = [];
    const instrumentationA: ClickHouseORMInstrumentation = {
      onQueryStart() {
        order.push("start:a");
      },
      onQuerySuccess() {
        order.push("success:a");
      },
      onQueryError() {
        order.push("error:a");
      },
    };
    const instrumentationB: ClickHouseORMInstrumentation = {
      onQueryStart() {
        order.push("start:b");
      },
      onQuerySuccess() {
        order.push("success:b");
      },
      onQueryError() {
        order.push("error:b");
      },
    };

    globalThis.fetch = mock(async (_input: string | URL | Request, init?: RequestInit) => {
      const body =
        typeof init?.body === "string" ? init.body : String((init?.body as FormData | undefined)?.get("query") ?? "");
      if (body.includes("broken")) {
        return new Response("broken", { status: 500 });
      }
      return new Response(JSON.stringify({ data: [{ one: 1 }] }), {
        status: 200,
      });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      host: "http://localhost:8123",
      schema: { users },
      instrumentation: [instrumentationA, instrumentationB],
    });

    await db.execute(sql`select 1 as one`);
    await expect(db.execute(sql`select broken`)).rejects.toMatchObject({
      kind: "request_failed",
      executionState: "rejected",
      httpStatus: 500,
      responseText: "broken",
    });

    expect(order).toEqual(["start:a", "start:b", "success:b", "success:a", "start:a", "start:b", "error:b", "error:a"]);
  });

  it("records system endpoint helpers through logger, tracing and instrumentation", async function testSystemEndpointObservability() {
    const tracer = createCapturedTracer();
    const capturedLogger = createCapturedLogger();
    const order: string[] = [];

    globalThis.fetch = mock(async (input: string | URL | Request) => {
      const url = new URL(typeof input === "string" || input instanceof URL ? String(input) : input.url);
      if (url.pathname.endsWith("/ping")) {
        return new Response("Ok.\n", { status: 200 });
      }
      if (url.pathname.endsWith("/replicas_status")) {
        return new Response("Ok.\n", { status: 200 });
      }
      return new Response("missing", { status: 404 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      host: "http://localhost:8123",
      schema: { users },
      logger: capturedLogger.logger,
      logLevel: "debug",
      tracing: {
        tracer: tracer.tracer,
      },
      instrumentation: [
        {
          onQueryStart(event) {
            order.push(`start:${event.operation}`);
          },
          onQuerySuccess(event) {
            order.push(`success:${event.operation}`);
          },
        },
      ],
    });

    await db.ping();
    await db.replicasStatus();

    expect(order).toEqual(["start:PING", "success:PING", "start:REPLICAS_STATUS", "success:REPLICAS_STATUS"]);
    expect(tracer.spans.map((span) => span.name)).toEqual(["clickhouse PING", "clickhouse REPLICAS_STATUS"]);
    expect(
      capturedLogger.entries.filter((entry) => entry.level === "debug").map((entry) => entry.fields?.operation),
    ).toEqual(["PING", "PING", "REPLICAS_STATUS", "REPLICAS_STATUS"]);
  });

  it("exposes stable sql normalization helpers", function testSqlHelpers() {
    expect(compactStatement(" select\n  1  ")).toBe("select 1");
    expect(resolveSqlOperation("/* comment */\nselect 1")).toBe("SELECT");
    expect(resolveSqlOperation("-- comment\ncreate table x")).toBe("CREATE");
    expect(resolveSqlOperation("")).toBe("QUERY");
  });

  it("covers tracing early returns and SQL operation fallbacks", async function testTracingEdgeCases() {
    const noStartTracer = createCapturedTracer();
    const noStartInstrumentation = createTracingInstrumentation({
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
    const tracedInstrumentation = createTracingInstrumentation({
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
    expect(traced.spans[1]?.exceptions).toEqual([boom]);
    expect(traced.spans[1]?.attributes["db.response.row_count"]).toBe(3);

    const noCountTracer = createCapturedTracer();
    const noCountInstrumentation = createTracingInstrumentation({
      tracer: noCountTracer.tracer,
      includeRowCount: false,
    });
    await noCountInstrumentation.onQueryStart?.(baseEvent);
    await noCountInstrumentation.onQuerySuccess?.(createQuerySuccessEvent(baseEvent, 7, 4));
    expect(noCountTracer.spans[0]?.attributes["db.response.row_count"]).toBeUndefined();

    expect(resolveSqlOperation("/* lead */ with scoped as (select 1) insert into sink select 1")).toBe("INSERT");
    expect(resolveSqlOperation("with scoped as (select 1)")).toBe("QUERY");
    expect(resolveSqlOperation("-- comment only")).toBe("QUERY");
    expect(resolveSqlOperation("/* unterminated")).toBe("QUERY");
  });

  it("keeps manual instrumentation dispatch resilient to hook failures", async function testManualDispatch() {
    const calls: string[] = [];
    const flaky: ClickHouseORMInstrumentation = {
      onQueryStart() {
        calls.push("start");
        throw new Error("ignore start");
      },
      onQuerySuccess() {
        calls.push("success");
        throw new Error("ignore success");
      },
      onQueryError() {
        calls.push("error");
        throw new Error("ignore error");
      },
    };
    const event = createQueryEvent({
      mode: "command",
      queryKind: "raw",
      statement: "truncate table logs",
      operation: "TRUNCATE",
      startedAt: 1,
    });

    await emitQueryStart([flaky], event);
    await emitQuerySuccess([flaky], createQuerySuccessEvent(event, 5));
    await emitQueryError([flaky], createQueryErrorEvent(event, new Error("boom"), 7));
    expect(calls).toEqual(["start", "success", "error"]);
  });
});
