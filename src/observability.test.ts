import { afterEach, beforeEach, describe, expect, it, mock, spyOn } from "bun:test";
import { int32, string } from "./columns";
import {
  type ClickHouseORMPlugin,
  compactStatement,
  createLoggerPlugin,
  createQueryErrorEvent,
  createQueryEvent,
  createQuerySuccessEvent,
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
const originalRequest = globalThis.Request;

// ck-orm hands fetch a single Request. Recording the init each Request was built from lets fetch mocks keep
// asserting on the original body (FormData, string or stream) and duplex mode.
const requestInits = new WeakMap<Request, RequestInit & { duplex?: "half" }>();
class RecordingRequest extends originalRequest {
  constructor(input: RequestInfo | URL, init?: RequestInit) {
    super(input, init);
    requestInits.set(this, init ?? {});
  }
}
const initOf = (input: string | URL | Request, init?: RequestInit) =>
  (input instanceof originalRequest ? requestInits.get(input) : undefined) ?? init;

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

describe("ck-orm observability", function describeClickHouseORMObservability() {
  beforeEach(function setupMocks() {
    globalThis.Request = RecordingRequest as typeof Request;
    mock.restore();
  });

  afterEach(function teardownMocks() {
    globalThis.fetch = originalFetch;
    globalThis.Request = originalRequest;
    mock.restore();
  });

  it("logs ORM events with level filtering and resolves safe destinations", async function testLoggerInstrumentation() {
    const captured = createCapturedLogger();
    const warnOnly = createLoggerPlugin(captured.logger, "warn");
    const debugEnabled = createLoggerPlugin(captured.logger, "debug");
    const event = createQueryEvent({
      mode: "query",
      queryKind: "typed",
      statement: "select * from users",
      operation: "SELECT",
      startedAt: 100,
    });

    const errorEvent = createQueryErrorEvent(event, new Error("boom"), 20, 0);
    expect(errorEvent.partialRowCount).toBe(0);
    expect(errorEvent.rowCount).toBe(0);

    await warnOnly.onQueryStart?.(event);
    await warnOnly.onQuerySuccess?.(createQuerySuccessEvent(event, 10, 1));
    await warnOnly.onQueryError?.(errorEvent);

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

  it("emits instrumentation hooks in forward/reverse order around runtime execution", async function testRuntimeInstrumentationOrder() {
    const order: string[] = [];
    const instrumentationA: ClickHouseORMPlugin = {
      name: "a",
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
    const instrumentationB: ClickHouseORMPlugin = {
      name: "b",
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

    globalThis.fetch = mock(async (input: string | URL | Request, requestInit?: RequestInit) => {
      const init = initOf(input, requestInit);
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
      plugins: [instrumentationA, instrumentationB],
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

  it("derives database and server fields from both structured config and databaseUrl", async function testRuntimeEventContext() {
    const events: Array<{
      databaseName?: string;
      serverAddress?: string;
      serverPort?: number;
      requestTimeoutMs?: number;
      querySummary?: string;
      statementHash?: string;
      readRows?: number;
      readBytes?: number;
      resultRows?: number;
      tableName?: string;
    }> = [];

    globalThis.fetch = mock(async () => {
      return new Response(
        JSON.stringify({
          data: [{ id: 1, name: "alice" }],
          rows: 1,
          rows_before_limit_at_least: 4,
          statistics: {
            elapsed: 0.002,
            rows_read: 4,
            bytes_read: 128,
          },
        }),
        { status: 200 },
      );
    }) as unknown as typeof fetch;

    const instrumentation: ClickHouseORMPlugin = {
      name: "success",
      onQuerySuccess(event) {
        events.push({
          databaseName: event.databaseName,
          serverAddress: event.serverAddress,
          serverPort: event.serverPort,
          requestTimeoutMs: event.requestTimeoutMs,
          querySummary: event.querySummary,
          statementHash: event.statementHash,
          readRows: event.readRows,
          readBytes: event.readBytes,
          resultRows: event.resultRows,
          tableName: event.tableName,
        });
      },
    };

    const structuredDb = clickhouseClient({
      host: "http://clickhouse.local:8123",
      database: "structured_db",
      request_timeout: 12_000,
      plugins: [instrumentation],
    });
    const urlDb = clickhouseClient({
      databaseUrl: "https://default:secret@clickhouse.example/url_db",
      plugins: [instrumentation],
    });

    await structuredDb.select().from(users).execute();
    await urlDb.execute(sql`select 1 as id`);

    expect(events[0]).toMatchObject({
      databaseName: "structured_db",
      serverAddress: "clickhouse.local",
      serverPort: 8123,
      requestTimeoutMs: 12_000,
      querySummary: "SELECT users",
      readRows: 4,
      readBytes: 128,
      resultRows: 1,
      tableName: "users",
    });
    expect(events[0]?.statementHash).toBeDefined();
    expect(events[1]).toMatchObject({
      databaseName: "url_db",
      serverAddress: "clickhouse.example",
      serverPort: 443,
      requestTimeoutMs: undefined,
      querySummary: "SELECT",
      tableName: undefined,
    });
  });

  it("records system endpoint helpers through logger and plugins", async function testSystemEndpointObservability() {
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
      logger: capturedLogger.logger,
      logLevel: "debug",
      plugins: [
        {
          name: "order",
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
    expect(
      capturedLogger.entries.filter((entry) => entry.level === "debug").map((entry) => entry.fields?.operation),
    ).toEqual(["PING", "PING", "REPLICAS_STATUS", "REPLICAS_STATUS"]);
  });

  it("exposes stable sql normalization helpers", function testSqlHelpers() {
    expect(compactStatement(" select\n  1  ")).toBe("select 1");
    expect(resolveSqlOperation("/* comment */\nselect 1")).toBe("SELECT");
    expect(resolveSqlOperation("-- comment\ncreate table x")).toBe("CREATE");
    expect(resolveSqlOperation("")).toBe("QUERY");
    expect(resolveSqlOperation("/* lead */ with scoped as (select 1) insert into sink select 1")).toBe("INSERT");
    expect(resolveSqlOperation("with scoped as (select 1)")).toBe("QUERY");
    expect(resolveSqlOperation("-- comment only")).toBe("QUERY");
    expect(resolveSqlOperation("/* unterminated")).toBe("QUERY");
  });

  it("keeps manual instrumentation dispatch resilient to hook failures", async function testManualDispatch() {
    const calls: string[] = [];
    const flaky: ClickHouseORMPlugin = {
      name: "flaky",
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

    const consoleError = spyOn(console, "error").mockImplementation(() => undefined);
    try {
      await emitQueryStart([flaky], event);
      await emitQuerySuccess([flaky], createQuerySuccessEvent(event, 5));
      await emitQueryError([flaky], createQueryErrorEvent(event, new Error("boom"), 7));
      expect(calls).toEqual(["start", "success", "error"]);
      const messages = consoleError.mock.calls.map((call) => call[0]);
      expect(messages).toEqual([
        '[ck-orm] plugin "flaky" onQueryStart threw:',
        '[ck-orm] plugin "flaky" onQuerySuccess threw:',
        '[ck-orm] plugin "flaky" onQueryError threw:',
      ]);
    } finally {
      consoleError.mockRestore();
    }
  });

  it("skips instrumentations that do not declare a matching lifecycle hook", async function testPartialHookSkip() {
    const calls: string[] = [];
    const tagged = (tag: string): ClickHouseORMPlugin => ({
      name: tag,
      onQueryStart() {
        calls.push(`${tag}:start`);
      },
      onQuerySuccess() {
        calls.push(`${tag}:success`);
      },
      onQueryError() {
        calls.push(`${tag}:error`);
      },
    });
    // Three instrumentations exercising every skip branch:
    //   - `full` declares all three hooks
    //   - `empty` declares none (every emit must skip it)
    //   - `noError` declares start + success but not onQueryError (the previously
    //     uncovered observability.ts:201-202 continue branch)
    const full = tagged("full");
    const empty: ClickHouseORMPlugin = { name: "empty" };
    const noError: ClickHouseORMPlugin = {
      name: "noError",
      onQueryStart() {
        calls.push("noError:start");
      },
      onQuerySuccess() {
        calls.push("noError:success");
      },
    };
    const instrumentations = [full, empty, noError];

    const event = createQueryEvent({
      mode: "query",
      queryKind: "typed",
      statement: "select 1",
      operation: "SELECT",
      startedAt: 0,
    });

    await emitQueryStart(instrumentations, event);
    await emitQuerySuccess(instrumentations, createQuerySuccessEvent(event, 1));
    await emitQueryError(instrumentations, createQueryErrorEvent(event, new Error("boom"), 2));

    // `emitQueryStart` walks instrumentations forwards; `emitQuerySuccess` and
    // `emitQueryError` walk in reverse so wrap-around tracers close their span
    // after inner hooks complete. Encode that contract into the assertion so a
    // future ordering regression fails this test instead of silently flipping
    // span boundaries.
    expect(calls).toEqual(["full:start", "noError:start", "noError:success", "full:success", "full:error"]);
  });
});

describe("ck-orm plugins", function describePlugins() {
  it("adds onQueryStart headers to every request path", async function testPluginHeadersOnEveryPath() {
    const seen: Array<string | null> = [];
    const plugin: ClickHouseORMPlugin = {
      name: "tag",
      onQueryStart() {
        return { http_headers: { "x-plugin": "on" } };
      },
    };
    const clientFor = (body: string) =>
      clickhouseClient({
        host: "http://localhost:8123",
        plugins: [plugin],
        fetch(request) {
          seen.push(request.headers.get("x-plugin"));
          return new Response(body, { status: 200 });
        },
      });
    const json = clientFor(JSON.stringify({ data: [] }));
    const empty = clientFor("");
    const ok = clientFor("Ok.\n");

    await json.execute(sql`select 1`);
    await json.select().from(users).execute();
    await Array.fromAsync(empty.stream(sql`select 1`));
    await Array.fromAsync(empty.select().from(users).iterator());
    await empty.command(sql`select 1`);
    await empty.insertJsonEachRow(users, [{ id: 1, name: "alice" }]);
    await ok.ping();
    await ok.replicasStatus();

    expect(seen).toEqual(Array(8).fill("on"));
  });

  it("keeps client-level and session headers above plugin headers inside runInSession", async function testPluginHeaderPrecedenceInSession() {
    let headers = new Headers();
    const db = clickhouseClient({
      host: "http://localhost:8123",
      http_headers: { "x-client": "client" },
      plugins: [
        {
          name: "override-attempt",
          onQueryStart: () => ({
            http_headers: { "x-client": "plugin", "x-session": "plugin", "X-ClickHouse-Replica-Tag": "plugin" },
          }),
        },
      ],
      fetch(request) {
        headers = request.headers;
        return new Response("", { status: 200 });
      },
    });

    await db.runInSession((session) => session.command(sql`select 1`), {
      session_id: "session_under_test",
      http_headers: { "x-session": "session" },
    });

    expect(headers.get("x-client")).toBe("client");
    expect(headers.get("x-session")).toBe("session");
    expect(headers.get("x-clickhouse-replica-tag")).toBe("session_under_test");
  });

  it("keeps explicit and runtime-owned headers above plugin headers", async function testPluginHeaderPrecedence() {
    let headers = new Headers();
    const db = clickhouseClient({
      host: "http://localhost:8123",
      username: "alice",
      password: "secret",
      http_headers: { "x-client": "client" },
      plugins: [
        {
          name: "first",
          onQueryStart: () => ({
            http_headers: {
              "x-plugin": "first",
              "x-client": "plugin",
              "x-request": "plugin",
              Authorization: "Bearer plugin",
            },
          }),
        },
        { name: "second", onQueryStart: () => ({ http_headers: { "X-Plugin": "second" } }) },
      ],
      fetch(request) {
        headers = request.headers;
        return new Response(JSON.stringify({ data: [] }), { status: 200 });
      },
    });

    await db.execute(sql`select 1`, { http_headers: { "x-request": "request" } });

    expect(headers.get("x-plugin")).toBe("second");
    expect(headers.get("x-client")).toBe("client");
    expect(headers.get("x-request")).toBe("request");
    expect(headers.get("authorization")).toBe(`Basic ${btoa("alice:secret")}`);
  });

  it("logs failing hooks with the plugin name and drops their headers", async function testPluginHookFailures() {
    const consoleError = spyOn(console, "error").mockImplementation(() => undefined);
    let headers = new Headers();
    const db = clickhouseClient({
      host: "http://localhost:8123",
      plugins: [
        {
          name: "throws",
          onQueryStart() {
            throw new Error("start failed");
          },
        },
        { name: "bad-header", onQueryStart: () => ({ http_headers: { "bad header": "x", "x-bad": "dropped" } }) },
        { name: "good", onQueryStart: () => ({ http_headers: { "x-good": "yes" } }) },
      ],
      fetch(request) {
        headers = request.headers;
        return new Response(JSON.stringify({ data: [] }), { status: 200 });
      },
    });

    try {
      await db.execute(sql`select 1`);
      expect(consoleError.mock.calls.map((call) => call[0])).toEqual([
        '[ck-orm] plugin "throws" onQueryStart threw:',
        '[ck-orm] plugin "bad-header" onQueryStart threw:',
      ]);
      expect(headers.get("x-bad")).toBeNull();
      expect(headers.get("x-good")).toBe("yes");
    } finally {
      consoleError.mockRestore();
    }
  });

  it("reports a stream stopped early as a success with the rows read so far", async function testEarlyStreamExit() {
    const events: string[] = [];
    const db = clickhouseClient({
      host: "http://localhost:8123",
      plugins: [
        {
          name: "probe",
          onQueryStart() {
            events.push("start");
          },
          onQuerySuccess(event) {
            events.push(`success:${event.rowCount}`);
          },
          onQueryError() {
            events.push("error");
          },
        },
      ],
      fetch: () => new Response('{"n":1}\n{"n":2}\n{"n":3}\n', { status: 200 }),
    });

    for await (const _row of db.stream(sql`select n`)) {
      break;
    }

    expect(events).toEqual(["start", "success:1"]);
  });
});
