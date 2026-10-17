import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { type Attributes, type SpanStatus, SpanStatusCode, type Tracer } from "@opentelemetry/api";
import { createRequestFailedError, isClickHouseORMError } from "./errors";
import { CLICKHOUSE_ERROR_NAMES, HTTP_STATUS_CODES } from "./internal/clickhouse-error-codes";
import { createLoggerPlugin, createQueryErrorEvent, createQueryEvent, emitQueryError } from "./observability";
import { otelTracing } from "./otel";
import { clickhouseClient } from "./runtime";
import { sql } from "./sql";

const requireExample = createRequire(resolve(import.meta.dir, "../examples/otel-tracing/package.json"));
type FinishedSpan = {
  name: string;
  status: SpanStatus;
  attributes: Attributes;
  events: ReadonlyArray<{ name: string; attributes?: Attributes }>;
  links: readonly unknown[];
};
const {
  InMemorySpanExporter,
  NodeTracerProvider,
  SimpleSpanProcessor,
}: {
  InMemorySpanExporter: new () => { getFinishedSpans(): FinishedSpan[] };
  SimpleSpanProcessor: new (exporter: unknown) => unknown;
  NodeTracerProvider: new (options: {
    spanProcessors: unknown[];
  }) => {
    getTracer(name: string): Tracer;
    forceFlush(): Promise<void>;
    shutdown(): Promise<void>;
  };
} = requireExample("@opentelemetry/sdk-trace-node");

const MARKER = "AUDIT_SEC005_SYNTHETIC_SENSITIVE_VALUE_7f4c";
const NAME = "AUDIT_REV2_NAME";
const BODY = "AUDIT_REV2_REVIEW_BODY_9c2e";
const CODE = 8675309123;
const FORBIDDEN = new Set([
  "message",
  "stack",
  "name",
  "toString",
  "valueOf",
  "toJSON",
  "clickhouseName",
  "responseText",
  "cause",
]);

type Sdk = {
  exporter: { getFinishedSpans(): FinishedSpan[] };
  provider: {
    getTracer(name: string): Tracer;
    forceFlush(): Promise<void>;
    shutdown(): Promise<void>;
  };
  tracer: Tracer;
};

const tracing = (): Sdk => {
  const exporter = new InMemorySpanExporter();
  const provider = new NodeTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  return { exporter, provider, tracer: provider.getTracer("ck-orm-sec-005") };
};

const dump = (value: unknown) => {
  if (value instanceof Error) {
    return `${value.name}: ${value.message}`;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return Object.prototype.toString.call(value);
  }
};

const exported = (span: FinishedSpan | undefined) => {
  if (!span) {
    return "";
  }
  return JSON.stringify({
    name: span.name,
    status: span.status,
    attributes: span.attributes,
    events: span.events,
    links: span.links,
  });
};

const exceptionType = (span: FinishedSpan | undefined) =>
  span?.events.find((item) => item.name === "exception")?.attributes?.["exception.type"];

const requireSpan = (span: FinishedSpan | undefined): FinishedSpan => {
  expect(span).toBeDefined();
  if (span === undefined) {
    throw new Error("missing finished span");
  }
  return span;
};

const exposedChannels = (span: FinishedSpan) => {
  const channels = {
    name: span.name,
    status: span.status,
    attributes: span.attributes,
    events: span.events,
    links: span.links,
  };
  return Object.entries(channels)
    .filter(([, value]) => JSON.stringify(value).includes(MARKER))
    .map(([key]) => key);
};

const orm = (fields: Record<string, unknown> = {}) =>
  createRequestFailedError({ httpStatus: 500, responseText: "plain", ...fields });

const withPrototype = async (key: string, descriptor: PropertyDescriptor, run: () => Promise<void>) => {
  const prior = Object.getOwnPropertyDescriptor(Object.prototype, key);
  Object.defineProperty(Object.prototype, key, descriptor);
  try {
    await run();
  } finally {
    if (prior) {
      Object.defineProperty(Object.prototype, key, prior);
    } else {
      delete (Object.prototype as Record<string, unknown>)[key];
    }
  }
};

const finish = async (plugin: ReturnType<typeof otelTracing>, error: unknown, sdk: Sdk) => {
  const stderr: string[] = [];
  const orig = console.error;
  console.error = (...args: unknown[]) => {
    stderr.push(args.map(dump).join(" "));
  };
  const event = createQueryEvent({
    mode: "query",
    queryKind: "raw",
    operation: "SELECT",
    statement: "SELECT 1",
    startedAt: Date.now(),
  });
  try {
    await plugin.onQueryStart?.(event);
    await emitQueryError([plugin], createQueryErrorEvent(event, error, 1));
    await sdk.provider.forceFlush();
    const spans = sdk.exporter.getFinishedSpans();
    return { spans, span: spans[0], stderr: stderr.join("\n"), text: exported(spans[0]) };
  } finally {
    console.error = orig;
    await sdk.provider.shutdown();
  }
};

const capture = async (error: unknown, options: Record<string, unknown> = {}, direct = false) => {
  const sdk = tracing();
  const pluginOptions = direct ? options : { includeStatement: false, ...options };
  pluginOptions.tracer = sdk.tracer;
  if (!direct) {
    pluginOptions.includeStatement = false;
  }
  return await finish(otelTracing(pluginOptions), error, sdk);
};

describe("SEC-005 explicit-v2 error projection", () => {
  it("SEC-005 RED: unit request-error telemetry omits sensitive error content while retaining failure classification", async () => {
    const error = createRequestFailedError({
      httpStatus: 500,
      clickhouseCode: 53,
      clickhouseName: "TYPE_MISMATCH",
      responseText: MARKER,
    });
    const result = await capture(error);
    expect(error.message).toContain(MARKER);
    expect(result.spans).toHaveLength(1);
    expect(result.span?.status.code).toBe(SpanStatusCode.ERROR);
    expect(result.span?.status.message).toBeUndefined();
    expect(result.span?.attributes).toMatchObject({
      "ck_orm.error.kind": "request_failed",
      "ck_orm.execution_state": "rejected",
      "db.response.status_code": "53",
      "error.type": "TYPE_MISMATCH",
    });
    expect(exposedChannels(requireSpan(result.span))).toEqual([]);
  });

  it("SEC-005 RED: unit non-error-string telemetry omits sensitive error content while retaining failure classification", async () => {
    const result = await capture(MARKER);
    expect(result.spans).toHaveLength(1);
    expect(result.span?.status.code).toBe(SpanStatusCode.ERROR);
    expect(result.span?.status.message).toBeUndefined();
    expect(result.span?.attributes["error.type"]).toBe("string");
    expect(exceptionType(result.span)).toBe("string");
    expect(result.text).not.toContain(MARKER);
  });

  it("SEC-005 CONTROL: unit includeStatement false omits SQL but preserves successful span metadata", async () => {
    const sdk = tracing();
    try {
      const plugin = otelTracing({ tracer: sdk.tracer, includeStatement: false });
      const event = createQueryEvent({
        mode: "query",
        queryKind: "raw",
        operation: "SELECT",
        statement: `SELECT '${MARKER}'`,
        startedAt: Date.now(),
      });
      await plugin.onQueryStart?.(event);
      await plugin.onQuerySuccess?.({
        ...event,
        durationMs: 1,
        rowCount: 1,
      });
      await sdk.provider.forceFlush();
      const spans = sdk.exporter.getFinishedSpans();
      expect(spans).toHaveLength(1);
      const successSpan = requireSpan(spans[0]);
      expect(successSpan.status.code).toBe(SpanStatusCode.OK);
      expect(successSpan.attributes["db.response.row_count"]).toBe(1);
      expect(successSpan.attributes["db.statement"]).toBeUndefined();
      expect(exposedChannels(successSpan)).toEqual([]);
    } finally {
      await sdk.provider.shutdown();
    }
  });

  for (const responseKind of ["http-error", "invalid-json", "invalid-stream-row"] as const) {
    it(`SEC-005 RED: e2e ${responseKind} public API through HTTP and real SDK exports no sensitive error content`, async () => {
      const sdk = tracing();
      let requests = 0;
      const body = responseKind === "http-error" ? MARKER : `{"value":"${MARKER}${"x".repeat(4_096)}"`;
      const server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        async fetch(request) {
          requests += 1;
          await request.text();
          return new Response(body, { status: responseKind === "http-error" ? 500 : 200 });
        },
      });
      try {
        const db = clickhouseClient({
          host: server.url.href,
          username: "audit_fixture",
          password: "",
          request_timeout: 2_000,
          plugins: [otelTracing({ tracer: sdk.tracer, includeStatement: false })],
        });
        let caught: unknown;
        try {
          if (responseKind === "invalid-stream-row") {
            for await (const _row of db.stream(sql`SELECT 1`)) {
              /* consume */
            }
          } else {
            await db.execute(sql`SELECT 1`);
          }
        } catch (error) {
          caught = error;
        }
        expect(requests).toBe(1);
        expect(isClickHouseORMError(caught)).toBe(true);
        if (!isClickHouseORMError(caught)) {
          throw new Error("missing ORM error");
        }
        expect(caught.kind).toBe("request_failed");
        expect(caught.message).toContain(MARKER);
        expect(caught.responseText).toBe(body);
        await sdk.provider.forceFlush();
        const spans = sdk.exporter.getFinishedSpans();
        expect(spans).toHaveLength(1);
        const failedSpan = requireSpan(spans[0]);
        expect(failedSpan.attributes["ck_orm.error.kind"]).toBe("request_failed");
        expect(failedSpan.attributes["ck_orm.execution_state"]).toBe("rejected");
        expect(failedSpan.status.message).toBeUndefined();
        expect(exposedChannels(failedSpan)).toEqual([]);
      } finally {
        await server.stop(true);
        await sdk.provider.shutdown();
      }
    }, 10_000);
  }

  it("SEC-005 CONTROL: e2e successful sensitive data reaches caller without entering exported spans", async () => {
    const sdk = tracing();
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: () => Response.json({ data: [{ value: MARKER }] }),
    });
    try {
      const db = clickhouseClient({
        host: server.url.href,
        username: "audit_fixture",
        password: "",
        plugins: [otelTracing({ tracer: sdk.tracer, includeStatement: false })],
      });
      const rows = await db.execute(sql`SELECT 1`);
      expect(rows).toEqual([{ value: MARKER }]);
      await sdk.provider.forceFlush();
      const spans = sdk.exporter.getFinishedSpans();
      expect(spans).toHaveLength(1);
      expect(requireSpan(spans[0]).status.code).toBe(SpanStatusCode.OK);
      expect(exported(spans[0])).not.toContain(MARKER);
    } finally {
      await server.stop(true);
      await sdk.provider.shutdown();
    }
  });

  it("SEC-005 CONTROL: e2e no tracing plugin exports nothing for an HTTP failure", async () => {
    const sdk = tracing();
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: () => new Response(MARKER, { status: 500 }),
    });
    try {
      const db = clickhouseClient({
        host: server.url.href,
        username: "audit_fixture",
        password: "",
        request_timeout: 2_000,
      });
      await expect(db.execute(sql`SELECT 1`)).rejects.toMatchObject({
        kind: "request_failed",
        responseText: MARKER,
      });
      await sdk.provider.forceFlush();
      expect(sdk.exporter.getFinishedSpans()).toEqual([]);
    } finally {
      await server.stop(true);
      await sdk.provider.shutdown();
    }
  }, 10_000);

  it("SEC-005 RED: code 53 uses the canonical name and omits the message marker", async () => {
    const error = orm({ clickhouseCode: 53, clickhouseName: "TYPE_MISMATCH", responseText: MARKER });
    const result = await capture(error);
    expect(error.message).toContain(MARKER);
    expect(result.span?.status.message).toBeUndefined();
    expect(result.span?.attributes).toMatchObject({
      "error.type": "TYPE_MISMATCH",
      "ck_orm.error.kind": "request_failed",
      "ck_orm.execution_state": "rejected",
      "db.response.status_code": "53",
    });
    expect(exceptionType(result.span)).toBe("TYPE_MISMATCH");
    expect(result.text).not.toContain(MARKER);
  });

  it("SEC-005 RED: forged clickhouseName is not a type when the code is absent", async () => {
    const result = await capture(orm({ clickhouseName: NAME }));
    expect(result.span?.attributes["error.type"]).toBe("request_failed");
    expect(exceptionType(result.span)).toBe("request_failed");
    expect(result.text).not.toContain(NAME);
  });

  it("SEC-005 RED: canonical name does not read a clickhouseName getter", async () => {
    let reads = 0;
    const error = orm({ clickhouseCode: 53, responseText: "plain" });
    Object.defineProperty(error, "clickhouseName", {
      get() {
        reads += 1;
        return MARKER;
      },
      configurable: true,
    });
    const result = await capture(error);
    expect(reads).toBe(0);
    expect(result.span?.attributes["error.type"]).toBe("TYPE_MISMATCH");
    expect(result.text).not.toContain(MARKER);
  });

  it("SEC-005 RED: gap code and unbounded integers are not status codes", async () => {
    const gap = await capture(orm({ clickhouseCode: 5, clickhouseName: "TYPE_MISMATCH" }));
    const hugeCode = await capture(orm({ clickhouseCode: CODE, clickhouseName: NAME }));
    const hugeHttp = await capture(orm({ httpStatus: CODE }));
    const fractional = await capture(orm({ clickhouseCode: -8675309.25 }));
    expect(gap.span?.attributes["db.response.status_code"]).toBeUndefined();
    expect(gap.span?.attributes["error.type"]).toBe("request_failed");
    expect(gap.text).not.toContain("TYPE_MISMATCH");
    expect(hugeCode.span?.attributes["db.response.status_code"]).toBeUndefined();
    expect(hugeCode.text).not.toContain(String(CODE));
    expect(hugeCode.text).not.toContain(NAME);
    expect(hugeHttp.span?.attributes["db.response.status_code"]).toBeUndefined();
    expect(fractional.text).not.toContain("-8675309.25");
  });

  it("SEC-005 RED: assigned HTTP status is kept only when no integer ClickHouse code is present", async () => {
    const httpOnly = await capture(orm({ httpStatus: 503 }));
    const masked = await capture(orm({ clickhouseCode: 53, httpStatus: 503 }));
    expect(httpOnly.span?.attributes["db.response.status_code"]).toBe("503");
    expect(httpOnly.span?.attributes["error.type"]).toBe("request_failed");
    expect(masked.span?.attributes["db.response.status_code"]).toBe("53");
    expect(masked.span?.attributes["error.type"]).toBe("TYPE_MISMATCH");
  });

  it("SEC-005 RED: classification getters are not invoked", async () => {
    let reads = 0;
    const error = new Error("safe");
    for (const key of [
      "kind",
      "executionState",
      "clickhouseName",
      "clickhouseCode",
      "httpStatus",
      "message",
      "stack",
      "responseText",
      "cause",
      "name",
    ]) {
      Object.defineProperty(error, key, {
        get() {
          reads += 1;
          return MARKER;
        },
        configurable: true,
      });
    }
    const result = await capture(error);
    expect(reads).toBe(0);
    expect(result.spans).toHaveLength(1);
    expect(result.text).not.toContain(MARKER);
    expect(result.stderr).not.toContain(MARKER);
  });

  it("SEC-005 RED: throwing kind getter still ends one ERROR span", async () => {
    const error = new Error("safe");
    Object.defineProperty(error, "kind", {
      get() {
        throw new Error(MARKER);
      },
      configurable: true,
    });
    const result = await capture(error);
    expect(result.spans).toHaveLength(1);
    expect(result.span?.status.code).toBe(SpanStatusCode.ERROR);
    expect(result.stderr).not.toContain(MARKER);
    expect(result.text).not.toContain(MARKER);
  });

  it("SEC-005 RED: getPrototypeOf trap still ends an ERROR span", async () => {
    const error = new Proxy(Object.create(null), {
      getPrototypeOf() {
        throw new Error(MARKER);
      },
    });
    const result = await capture(error);
    expect(result.spans).toHaveLength(1);
    expect(result.span?.status.code).toBe(SpanStatusCode.ERROR);
    expect(result.stderr).not.toContain(MARKER);
    expect(result.text).not.toContain(MARKER);
  });

  it("SEC-005 COVERAGE: getPrototypeOf trap with own kind and executionState still classifies as object", async () => {
    let traps = 0;
    const target = Object.create(null) as { kind: string; executionState: string };
    target.kind = "request_failed";
    target.executionState = "rejected";
    const error = new Proxy(target, {
      getPrototypeOf() {
        traps += 1;
        throw new Error(MARKER);
      },
    });
    const result = await capture(error);
    expect(traps).toBeGreaterThan(0);
    expect(result.spans).toHaveLength(1);
    expect(result.span?.status.code).toBe(SpanStatusCode.ERROR);
    expect(result.span?.attributes["error.type"]).toBe("object");
    expect(result.span?.attributes["ck_orm.error.kind"]).toBeUndefined();
    expect(result.span?.attributes["ck_orm.execution_state"]).toBeUndefined();
    expect(result.text).not.toContain(MARKER);
    expect(result.stderr).not.toContain(MARKER);
  });

  it("SEC-005 RED: accessor ORM fields are not a trusted shape", async () => {
    let reads = 0;
    const error = orm({ clickhouseCode: 53, responseText: "plain" });
    for (const key of ["kind", "executionState"]) {
      Object.defineProperty(error, key, {
        get() {
          reads += 1;
          return MARKER;
        },
        configurable: true,
      });
    }
    const result = await capture(error);
    expect(reads).toBe(0);
    expect(result.span?.attributes["error.type"]).toBe("Error");
    expect(result.span?.attributes["db.response.status_code"]).toBeUndefined();
    expect(result.text).not.toContain(MARKER);
  });

  it("SEC-005 RED: Object.prototype.code does not replace exception.type", async () => {
    await withPrototype(
      "code",
      {
        configurable: true,
        enumerable: false,
        value: {
          toString() {
            return MARKER;
          },
        },
      },
      async () => {
        const result = await capture(orm({ clickhouseCode: 53 }));
        expect(exceptionType(result.span)).toBe("TYPE_MISMATCH");
        expect(result.text).not.toContain(MARKER);
      },
    );
  });

  it("SEC-005 RED: inherited, polluted, string-false, and accessor flags stay closed", async () => {
    const inherited = Object.create({ includeErrorDiagnostics: true }) as Record<string, unknown>;
    inherited.includeStatement = false;
    const inheritedResult = await capture(new Error(MARKER), inherited, true);
    let polluted: Awaited<ReturnType<typeof capture>> | undefined;
    await withPrototype("includeErrorDiagnostics", { configurable: true, enumerable: false, value: true }, async () => {
      polluted = await capture(new Error(MARKER), {});
    });
    const stringFalse = await capture(new Error(MARKER), { includeErrorDiagnostics: "false" });
    let accessorReads = 0;
    const accessorOptions: Record<string, unknown> = { includeStatement: false };
    Object.defineProperty(accessorOptions, "includeErrorDiagnostics", {
      get() {
        accessorReads += 1;
        return true;
      },
      enumerable: true,
      configurable: true,
    });
    const accessorResult = await capture(new Error(MARKER), accessorOptions, true);
    expect(inheritedResult.text).not.toContain(MARKER);
    expect(polluted?.text).not.toContain(MARKER);
    expect(stringFalse.text).not.toContain(MARKER);
    expect(accessorReads).toBe(0);
    expect(accessorResult.text).not.toContain(MARKER);
  });

  it("SEC-005 RED: throwing options descriptor does not escape or opt in", async () => {
    const sdk = tracing();
    let traps = 0;
    const options = new Proxy(
      { includeStatement: false, tracer: sdk.tracer },
      {
        getOwnPropertyDescriptor() {
          traps += 1;
          throw new Error(MARKER);
        },
      },
    );
    const result = await finish(otelTracing(options), new Error(MARKER), sdk);
    expect(result.spans).toHaveLength(1);
    expect(result.text).not.toContain(MARKER);
    expect(result.stderr).not.toContain(MARKER);
    expect(traps).toBeGreaterThan(0);
  });

  it("SEC-005 RED: opt-in accessor throw falls through without reaching console.error", async () => {
    const error = new Error("safe");
    Object.defineProperty(error, "kind", {
      get() {
        throw new Error(MARKER);
      },
      configurable: true,
    });
    const result = await capture(error, { includeErrorDiagnostics: true });
    expect(result.spans).toHaveLength(1);
    expect(result.span?.status.code).toBe(SpanStatusCode.ERROR);
    expect(result.stderr).not.toContain(MARKER);
    expect(result.text).not.toContain(MARKER);
  });

  it("SEC-005 RED: own boolean true is explicit-only and does not mutate the caller", async () => {
    const error = orm({ responseText: MARKER });
    const before = error.message;
    const result = await capture(error, { includeErrorDiagnostics: true });
    expect(error.message).toBe(before);
    expect(result.text).toContain(MARKER);
  });

  it("SEC-005 RED: untrusted values use a fixed type and are not converted", async () => {
    let calls = 0;
    const leak = {
      toString() {
        calls += 1;
        return MARKER;
      },
      valueOf() {
        calls += 1;
        return MARKER;
      },
      toJSON() {
        calls += 1;
        return MARKER;
      },
      [Symbol.toPrimitive]() {
        calls += 1;
        return MARKER;
      },
    };
    const objectResult = await capture(leak);
    const symbolResult = await capture(Symbol(MARKER));
    const nullResult = await capture(null);
    const stringResult = await capture(MARKER);
    expect(calls).toBe(0);
    expect(objectResult.span?.attributes["error.type"]).toBe("object");
    expect(objectResult.text).not.toContain(MARKER);
    expect(symbolResult.span?.attributes["error.type"]).toBe("symbol");
    expect(symbolResult.text).not.toContain(MARKER);
    expect(nullResult.span?.attributes["error.type"]).toBe("null");
    expect(stringResult.span?.attributes["error.type"]).toBe("string");
    expect(stringResult.text).not.toContain(MARKER);
  });

  it("SEC-005 RED: loopback body token and unbounded code are absent", async () => {
    const sdk = tracing();
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: () => new Response(`Code: ${CODE}. DB::Exception: ${MARKER} (${NAME})\n`, { status: 500 }),
    });
    try {
      const db = clickhouseClient({
        host: server.url.href,
        username: "audit_fixture",
        password: "",
        request_timeout: 2_000,
        plugins: [otelTracing({ tracer: sdk.tracer, includeStatement: false })],
      });
      let caught: unknown;
      try {
        await db.execute(sql`SELECT 1`);
      } catch (error) {
        caught = error;
      }
      await sdk.provider.forceFlush();
      const span = sdk.exporter.getFinishedSpans()[0];
      const text = exported(span);
      expect(caught instanceof Error && caught.message.includes(MARKER)).toBe(true);
      expect(text).not.toContain(MARKER);
      expect(text).not.toContain(NAME);
      expect(text).not.toContain(String(CODE));
      expect(span?.attributes["db.response.status_code"]).toBeUndefined();
      expect(span?.status.code).toBe(SpanStatusCode.ERROR);
    } finally {
      await server.stop(true);
      await sdk.provider.shutdown();
    }
  });

  it("SEC-005 CONTROL: logger still receives the raw error", async () => {
    const seen: unknown[] = [];
    const plugin = createLoggerPlugin({
      trace() {},
      debug() {},
      info() {},
      warn() {},
      error(_message, fields) {
        seen.push(fields);
      },
    });
    const event = createQueryEvent({
      mode: "query",
      queryKind: "raw",
      operation: "SELECT",
      statement: "SELECT 1",
      startedAt: Date.now(),
    });
    await emitQueryError([plugin], createQueryErrorEvent(event, new Error(MARKER), 1));
    const fields = seen[0] as { error?: unknown } | undefined;
    expect(fields?.error instanceof Error && fields.error.message.includes(MARKER)).toBe(true);
  });

  it("SEC-005 RED: default path does not get message, stack, name, or toString", async () => {
    const gets: PropertyKey[] = [];
    const described: string[] = [];
    const error = new Proxy(orm({ clickhouseCode: 53, responseText: MARKER }), {
      get(target, key, receiver) {
        gets.push(key);
        return Reflect.get(target, key, receiver);
      },
      getOwnPropertyDescriptor(target, key) {
        described.push(String(key));
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });
    const result = await capture(error);
    expect(gets.filter((key) => key === Symbol.toPrimitive || (typeof key === "string" && FORBIDDEN.has(key)))).toEqual(
      [],
    );
    for (const key of ["message", "stack", "name", "toString", "clickhouseName", "responseText", "cause"]) {
      expect(described).not.toContain(key);
    }
    expect(described).toContain("clickhouseCode");
    expect(result.span?.status.message).toBeUndefined();
    expect(result.span?.attributes["error.type"]).toBe("TYPE_MISMATCH");
    expect(result.text).not.toContain(MARKER);
  });

  it("SEC-005 RED: throwing get and descriptor traps still end one span", async () => {
    const error = new Proxy(new Error("safe"), {
      get() {
        throw new Error(MARKER);
      },
      getOwnPropertyDescriptor() {
        throw new Error(MARKER);
      },
      getPrototypeOf() {
        throw new Error(MARKER);
      },
    });
    const result = await capture(error);
    expect(result.spans).toHaveLength(1);
    expect(result.span?.status.code).toBe(SpanStatusCode.ERROR);
    expect(result.span?.attributes["error.type"]).toBe("object");
    expect(result.text).not.toContain(MARKER);
    expect(result.stderr).not.toContain(MARKER);
  });

  it("SEC-005 RED: revoked proxy degrades without copying its message", async () => {
    const { proxy, revoke } = Proxy.revocable(new Error(MARKER), {});
    revoke();
    const result = await capture(proxy);
    expect(result.text).not.toContain(MARKER);
    expect(result.stderr).not.toContain(MARKER);
    expect(result.spans).toHaveLength(1);
    expect(result.span?.status.code).toBe(SpanStatusCode.ERROR);
    expect(result.span?.attributes["error.type"]).toBe("object");
  });

  it("SEC-005 RED: descriptor value is snapshotted once", async () => {
    let reads = 0;
    const descriptor = new Proxy(
      { value: 53, writable: true, enumerable: true, configurable: true },
      {
        get(target, key, receiver) {
          if (key === "value") {
            reads += 1;
            return reads === 1 ? 53 : MARKER;
          }
          return Reflect.get(target, key, receiver);
        },
      },
    );
    const error = new Proxy(orm({ responseText: BODY }), {
      getOwnPropertyDescriptor(target, key) {
        if (key === "clickhouseCode") {
          return descriptor;
        }
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });
    const result = await capture(error);
    expect(result.text).not.toContain(MARKER);
    expect(reads).toBe(1);
    expect(result.text).not.toContain(BODY);
    expect(result.span?.attributes["error.type"]).toBe("TYPE_MISMATCH");
    expect(result.span?.attributes["db.response.status_code"]).toBe("53");
  });

  it("SEC-005 RED: flag get-trap, string, and late mutation stay closed", async () => {
    const sdk = tracing();
    let gets = 0;
    const getTrap = new Proxy(
      { includeStatement: false, tracer: sdk.tracer },
      {
        get(target, key, receiver) {
          if (key === "includeErrorDiagnostics") {
            gets += 1;
            return true;
          }
          return Reflect.get(target, key, receiver);
        },
      },
    );
    const trapped = await finish(otelTracing(getTrap), new Error(MARKER), sdk);
    const stringTrue = await capture(new Error(MARKER), { includeErrorDiagnostics: "true" });
    const lateSdk = tracing();
    const lateOptions: Record<string, unknown> = { includeStatement: false, tracer: lateSdk.tracer };
    const latePlugin = otelTracing(lateOptions);
    lateOptions.includeErrorDiagnostics = true;
    const late = await finish(latePlugin, new Error(MARKER), lateSdk);
    const snapSdk = tracing();
    const snapOptions: Record<string, unknown> = {
      includeStatement: false,
      tracer: snapSdk.tracer,
      includeErrorDiagnostics: true,
    };
    const snapPlugin = otelTracing(snapOptions);
    delete snapOptions.includeErrorDiagnostics;
    const snapped = await finish(snapPlugin, new Error(MARKER), snapSdk);
    let flagTraps = 0;
    const throwSdk = tracing();
    const throwing = new Proxy(
      { includeStatement: false, tracer: throwSdk.tracer },
      {
        getOwnPropertyDescriptor(target, key) {
          if (key === "includeErrorDiagnostics") {
            flagTraps += 1;
            throw new Error(MARKER);
          }
          return Reflect.getOwnPropertyDescriptor(target, key);
        },
      },
    );
    const thrown = await finish(otelTracing(throwing), new Error(BODY), throwSdk);
    expect(gets).toBe(0);
    expect(trapped.text).not.toContain(MARKER);
    expect(stringTrue.text).not.toContain(MARKER);
    expect(late.text).not.toContain(MARKER);
    expect(snapped.text).toContain(MARKER);
    expect(thrown.text).not.toContain(MARKER);
    expect(thrown.text).not.toContain(BODY);
    expect(flagTraps).toBe(1);
  });

  it("SEC-005 RED: own true is explicit-only; opt-in still inherits prototype code", async () => {
    const error = orm({ responseText: MARKER, clickhouseCode: 53 });
    const before = error.message;
    const own = await capture(error, { includeErrorDiagnostics: true });
    expect(error.message).toBe(before);
    expect(own.text).toContain(MARKER);
    await withPrototype(
      "code",
      {
        configurable: true,
        enumerable: false,
        value: {
          toString() {
            return MARKER;
          },
        },
      },
      async () => {
        const closed = await capture(orm({ clickhouseCode: 53 }));
        const opted = await capture(orm({ clickhouseCode: 53 }), { includeErrorDiagnostics: true });
        expect(exceptionType(closed.span)).toBe("TYPE_MISMATCH");
        expect(closed.text).not.toContain(MARKER);
        expect(exceptionType(opted.span)).toBe(MARKER);
      },
    );
  });

  it("SEC-005 RED: fabricated {value:true} is an explicit ceiling, string value is not", async () => {
    const trueSdk = tracing();
    const fabricatedTrue = new Proxy(
      { includeStatement: false, tracer: trueSdk.tracer },
      {
        getOwnPropertyDescriptor(target, key) {
          if (key === "includeErrorDiagnostics") {
            return { value: true, writable: true, enumerable: true, configurable: true };
          }
          return Reflect.getOwnPropertyDescriptor(target, key);
        },
      },
    );
    const opened = await finish(otelTracing(fabricatedTrue), new Error(MARKER), trueSdk);
    const stringSdk = tracing();
    const fabricatedString = new Proxy(
      { includeStatement: false, tracer: stringSdk.tracer },
      {
        getOwnPropertyDescriptor(target, key) {
          if (key === "includeErrorDiagnostics") {
            return { value: "true", writable: true, enumerable: true, configurable: true };
          }
          return Reflect.getOwnPropertyDescriptor(target, key);
        },
      },
    );
    const closed = await finish(otelTracing(fabricatedString), new Error(MARKER), stringSdk);
    expect(opened.text).toContain(MARKER);
    expect(closed.text).not.toContain(MARKER);
  });

  it("SEC-005 static classification tables keep the frozen closed-set sizes", () => {
    expect(CLICKHOUSE_ERROR_NAMES.size).toBe(647);
    expect(HTTP_STATUS_CODES.size).toBe(64);
    expect(CLICKHOUSE_ERROR_NAMES.get(53)).toBe("TYPE_MISMATCH");
    expect(CLICKHOUSE_ERROR_NAMES.get(386)).toBe("NO_COMMON_TYPE");
    expect(CLICKHOUSE_ERROR_NAMES.get(1004)).toBe("STARTUP_SCRIPTS_ERROR");
    expect(CLICKHOUSE_ERROR_NAMES.has(5)).toBe(false);
    expect(CLICKHOUSE_ERROR_NAMES.has(14)).toBe(false);
    expect(HTTP_STATUS_CODES.has(104)).toBe(true);
    expect(HTTP_STATUS_CODES.has(418)).toBe(true);
    expect(HTTP_STATUS_CODES.has(503)).toBe(true);
    expect(HTTP_STATUS_CODES.has(427)).toBe(false);
    expect(HTTP_STATUS_CODES.has(509)).toBe(false);
  });

  it("SEC-005 static classification tables match the reviewed canonical digest", () => {
    // Insertion-order identity of the in-tree maps/sets. This digest does not
    // re-fetch ErrorCodes.cpp or the IANA registry; provenance stays in the
    // table module comments and historical source comparison.
    const digest = createHash("sha256")
      .update(JSON.stringify({ clickhouse: [...CLICKHOUSE_ERROR_NAMES], http: [...HTTP_STATUS_CODES] }))
      .digest("hex");
    expect(digest).toBe("2c7a3e4dd80f08f3ac552c32cf6698942a8fe3bfb8a7d979066ffa4c8a496925");
  });

  it("SEC-005 LIMIT: inherited format is still exported", async () => {
    await withPrototype("format", { configurable: true, enumerable: false, value: MARKER }, async () => {
      const result = await capture(new Error("safe"));
      expect(result.spans).toHaveLength(1);
      expect(result.span?.attributes["db.response.format"]).toBe(MARKER);
      expect(result.span?.attributes["ck_orm.format"]).toBe(MARKER);
    });
  });

  it("SEC-005 LIMIT: inherited includeRowCount getter still aborts closeSpan", async () => {
    await withPrototype(
      "includeRowCount",
      {
        configurable: true,
        enumerable: false,
        get() {
          throw new Error(MARKER);
        },
      },
      async () => {
        const result = await capture(new Error(BODY));
        expect(result.spans).toHaveLength(0);
        expect(result.stderr).toContain(MARKER);
        expect(result.stderr).not.toContain(BODY);
        expect(result.text).not.toContain(BODY);
      },
    );
  });
});
