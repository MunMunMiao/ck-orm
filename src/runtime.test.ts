import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { nullable, string } from "./columns";
import { isClickHouseORMError } from "./errors";
import { fn } from "./functions";
import { compileCreateMaterializedView, compileQuerySymbol, eq } from "./query";
import { type ClickHouseClientConfig, type ClickHouseQueryOptions, clickhouseClient } from "./runtime";
import { ckTable } from "./schema";
import { buildCreateTableStatement, buildDropTableStatement } from "./schema-ddl";
import { sql } from "./sql";
import { orderRewardLog } from "./test-schema/commerce";

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

type CapturedCall = {
  url: URL;
  init: RequestInit & { duplex?: "half" };
};

const setFetchMock = (handler: (url: URL, init: RequestInit & { duplex?: "half" }) => Promise<Response> | Response) => {
  const calls: CapturedCall[] = [];
  const fetchMock = mock(async (input: string | URL | Request, requestInit?: RequestInit) => {
    const init = initOf(input, requestInit);
    const url = new URL(typeof input === "string" || input instanceof URL ? String(input) : input.url);
    const normalizedInit = (init ?? {}) as RequestInit & { duplex?: "half" };
    calls.push({ url, init: normalizedInit });
    return await handler(url, normalizedInit);
  });
  globalThis.fetch = fetchMock as unknown as typeof fetch;
  return {
    calls,
    fetchMock,
  };
};

const readBodyText = async (body: unknown) => {
  if (typeof body === "string") {
    return body;
  }
  if (body instanceof URLSearchParams) {
    return body.toString();
  }
  if (body instanceof Blob) {
    return await body.text();
  }
  if (body instanceof ReadableStream) {
    return await new Response(body).text();
  }
  return undefined;
};

const flushAsyncWork = async () => {
  await Promise.resolve();
  await Promise.resolve();
};

const expectRejectsWithClickhouseError = async (promise: Promise<unknown>, expected: Record<string, unknown>) => {
  try {
    await promise;
    throw new Error("Expected promise to reject with ClickHouseORMError");
  } catch (error) {
    expect(isClickHouseORMError(error)).toBe(true);
    for (const [key, value] of Object.entries(expected)) {
      expect((error as Record<string, unknown>)[key]).toEqual(value);
    }
  }
};

describe("ck-orm runtime", function describeClickHouseORMRuntime() {
  beforeEach(function setupMocks() {
    globalThis.Request = RecordingRequest as typeof Request;
    mock.restore();
  });

  afterEach(function teardownMocks() {
    globalThis.fetch = originalFetch;
    globalThis.Request = originalRequest;
    mock.restore();
  });

  it("keeps request compression out of the public config surface", function testCompressionRequestTypeBoundary() {
    const validConfig: ClickHouseClientConfig = {
      databaseUrl: "http://localhost:8123",
      compression: {
        response: true,
      },
    };

    expect(validConfig.compression?.response).toBe(true);

    const invalidConfig: ClickHouseClientConfig = {
      databaseUrl: "http://localhost:8123",
      compression: {
        response: true,
        // @ts-expect-error request compression is intentionally unsupported in fetch runtime
        request: true,
      },
    };

    expect((invalidConfig.compression as Record<string, unknown>).request).toBe(true);
  });

  it("keeps multipart parameter transport out of the public config surface", function testMultipartTypeBoundary() {
    const validOptions: ClickHouseQueryOptions = {
      query_id: "query_1",
      query_params: {
        user_id: "u_1",
      },
    };

    expect(validOptions.query_params?.user_id).toBe("u_1");

    const invalidOptions: ClickHouseQueryOptions = {
      query_id: "query_2",
      // @ts-expect-error multipart transport is internal and chosen automatically
      use_multipart_params: false,
    };

    expect((invalidOptions as Record<string, unknown>).use_multipart_params).toBe(false);
  });

  it("keeps connection modes mutually exclusive in the public config surface", function testConnectionModeTypeBoundary() {
    const validDatabaseUrlConfig: ClickHouseClientConfig = {
      databaseUrl: "http://localhost:8123/demo_store",
    };

    expect(validDatabaseUrlConfig.databaseUrl).toBe("http://localhost:8123/demo_store");

    const validStructuredConfig: ClickHouseClientConfig = {
      host: "http://localhost:8123",
      database: "demo_store",
      username: "default",
      password: "",
    };

    expect(validStructuredConfig.host).toBe("http://localhost:8123");

    // @ts-expect-error databaseUrl is mutually exclusive with structured fields
    const invalidMixedConfig: ClickHouseClientConfig = {
      databaseUrl: "http://localhost:8123/demo_store",
      username: "default",
    };

    expect((invalidMixedConfig as Record<string, unknown>).username).toBe("default");
  });

  it("keeps internal transport hooks and session lifetime defaults out of the client config surface", function testInternalConfigTypeBoundary() {
    const validConfig: ClickHouseClientConfig = {
      databaseUrl: "http://localhost:8123/demo_store",
      session_id: "default_session",
    };

    expect(validConfig.session_id).toBe("default_session");

    const invalidJsonConfig: ClickHouseClientConfig = {
      databaseUrl: "http://localhost:8123/demo_store",
      // @ts-expect-error json hooks are intentionally internal to the fetch transport
      json: {
        parse: JSON.parse,
        stringify: JSON.stringify,
      },
    };

    const invalidSessionTimeoutConfig: ClickHouseClientConfig = {
      databaseUrl: "http://localhost:8123/demo_store",
      // @ts-expect-error session_timeout belongs on requests and runInSession(), not client defaults
      session_timeout: 30,
    };

    const invalidSessionCheckConfig: ClickHouseClientConfig = {
      databaseUrl: "http://localhost:8123/demo_store",
      // @ts-expect-error session_check belongs on requests and runInSession(), not client defaults
      session_check: 1,
    };

    expect((invalidJsonConfig as Record<string, unknown>).json).toBeTruthy();
    expect((invalidSessionTimeoutConfig as Record<string, unknown>).session_timeout).toBe(30);
    expect((invalidSessionCheckConfig as Record<string, unknown>).session_check).toBe(1);
  });

  it("passes compiled query and clickhouse options through fetch transport", async function testExecutePassThrough() {
    const { calls } = setFetchMock((_url, _init) => {
      return new Response(
        JSON.stringify({
          data: [
            {
              total_reward_points: "12.50000",
              activeUsers: "3",
            },
          ],
        }),
        { status: 200 },
      );
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
      clickhouse_settings: {
        max_execution_time: 10,
      },
      session_id: "default_session",
      role: "analyst",
    });

    const rows = await db
      .select({
        totalRewardPoints: fn.sum(orderRewardLog.reward_points).as("total_reward_points"),
        activeUsers: fn.uniqExact(orderRewardLog.user_id),
      })
      .from(orderRewardLog)
      .where(eq(orderRewardLog.user_id, "u_1"))
      .execute({
        query_id: "query_1",
        clickhouse_settings: {
          max_memory_usage: 1024,
        },
        session_id: "runtime_session",
      });

    expect(rows).toEqual([
      {
        totalRewardPoints: "12.50000",
        activeUsers: 3,
      },
    ]);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.init.method).toBe("POST");
    expect(calls[0]?.url.searchParams.get("query_id")).toBe("query_1");
    expect(calls[0]?.url.searchParams.get("session_id")).toBe("runtime_session");
    expect(calls[0]?.url.searchParams.get("max_execution_time")).toBe("10");
    expect(calls[0]?.url.searchParams.get("max_memory_usage")).toBe("1024");
    expect(calls[0]?.url.searchParams.get("wait_end_of_query")).toBe("1");
    expect(calls[0]?.url.searchParams.get("http_write_exception_in_output_format")).toBe("0");
    expect(calls[0]?.url.searchParams.get("output_format_json_quote_64bit_integers")).toBe("1");
    expect(calls[0]?.url.searchParams.get("output_format_json_quote_decimals")).toBe("1");
    expect(calls[0]?.url.searchParams.get("date_time_output_format")).toBe("iso");
    expect(calls[0]?.url.searchParams.getAll("role")).toEqual(["analyst"]);

    const form = calls[0]?.init.body;
    expect(form).toBeInstanceOf(FormData);
    expect((form as FormData).get("query")).toBeTruthy();
    expect(String((form as FormData).get("query"))).toContain("FORMAT JSON");
    expect((form as FormData).get("param_orm_param1")).toBe("u_1");
  });

  it("passes the client fetch a Request for queries, streamed inserts and endpoints", async function testCustomFetch() {
    globalThis.fetch = mock(async () => {
      throw new Error("global fetch must not be called");
    }) as unknown as typeof fetch;
    const seen: { method: string; path: string; authorization: string | null; payload: string }[] = [];
    const db = clickhouseClient({
      databaseUrl: "http://default:secret@localhost:8123/demo_store",
      fetch: async (request) => {
        const url = new URL(request.url);
        const isForm = request.headers.get("content-type")?.startsWith("multipart/form-data");
        const payload = isForm ? String((await request.formData()).get("param_id")) : await request.text();
        seen.push({
          method: request.method,
          path: url.pathname,
          authorization: request.headers.get("authorization"),
          payload,
        });
        return new Response(url.pathname.endsWith("/ping") ? "Ok.\n" : '{"data":[]}');
      },
    });
    const rows = async function* () {
      yield { id: 1 };
      yield { id: 2 };
    };

    await db.execute(sql`select {id:Int32}`, { query_params: { id: 7 } });
    await db.insertJsonEachRow("events", rows());
    await db.ping();

    const authorization = `Basic ${btoa("default:secret")}`;
    expect(seen).toEqual([
      { method: "POST", path: "/", authorization, payload: "7" },
      { method: "POST", path: "/", authorization, payload: '{"id":1}\n{"id":2}\n' },
      { method: "GET", path: "/ping", authorization, payload: "" },
    ]);
  });

  it("uses one generated query_id for instrumentation and transport", async function testGeneratedQueryIdConsistency() {
    const { calls } = setFetchMock(() => {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    });
    const startedQueryIds: Array<string | null | undefined> = [];

    const db = clickhouseClient({
      host: "http://localhost:8123",
      plugins: [
        {
          name: "probe",
          onQueryStart(event) {
            startedQueryIds.push(event.queryId);
          },
        },
      ],
    });

    await db.execute(sql`select 1`);

    const transportQueryId = calls[0]?.url.searchParams.get("query_id");
    expect(transportQueryId).toMatch(/^[a-f0-9-]{36}$/);
    expect(startedQueryIds).toEqual([transportQueryId]);
  });

  it("reports default HTTP and HTTPS ports to instrumentation", async function testDefaultPortInstrumentation() {
    setFetchMock((_url, _init) => {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    });
    const serverPorts: Array<number | undefined> = [];

    const httpDb = clickhouseClient({
      host: "http://localhost",
      plugins: [
        {
          name: "probe",
          onQueryStart(event) {
            serverPorts.push(event.serverPort);
          },
        },
      ],
    });
    await httpDb.execute(sql`select 1`);

    const httpsDb = clickhouseClient({
      host: "https://localhost",
      plugins: [
        {
          name: "probe",
          onQueryStart(event) {
            serverPorts.push(event.serverPort);
          },
        },
      ],
    });
    await httpsDb.execute(sql`select 1`);

    expect(serverPorts).toEqual([80, 443]);
  });

  it("routes one session id and cleans temp tables after runInSession failure", async function testRunInSessionCleanup() {
    const bodies: string[] = [];
    const tmpScope = ckTable("tmp_scope", {
      user_id: string(),
    });
    const { calls } = setFetchMock(async (_url, init) => {
      const text = await readBodyText(init.body);
      if (typeof text === "string") {
        bodies.push(text);
      }
      return new Response("", { status: 200 });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
    });

    await expect(
      db.runInSession(
        async (session) => {
          await session.createTemporaryTable(tmpScope);
          session.registerTempTable("tmp_manual");
          await session.command(sql`select 1`, {
            http_headers: {
              "X-ClickHouse-Replica-Tag": "caller_override",
            },
          });
          throw new Error("boom");
        },
        { session_id: "session_under_test" },
      ),
    ).rejects.toThrow("boom");

    expect(bodies).toEqual([
      "CREATE TEMPORARY TABLE `tmp_scope`\n(\n  `user_id` String\n)\nENGINE = Memory",
      "select 1",
      "DROP TABLE IF EXISTS `tmp_manual`",
      "DROP TABLE IF EXISTS `tmp_scope`",
    ]);
    for (const call of calls) {
      expect(call.url.searchParams.get("session_id")).toBe("session_under_test");
      expect(new Headers(call.init.headers).get("X-ClickHouse-Replica-Tag")).toBe("session_under_test");
      expect(call.init.method).toBe("POST");
    }
  });

  it("merges runInSession settings, headers and query params over the parent defaults", async function testRunInSessionMergesDefaults() {
    const { calls } = setFetchMock(async () => new Response("", { status: 200 }));
    const db = clickhouseClient({
      host: "http://localhost:8123",
      http_headers: { "X-Parent": "parent" },
    });

    await db.withSettings({ join_use_nulls: 0, max_threads: 1 }).runInSession(
      async (session) => {
        await session.runInSession(
          async (child) => {
            await child.command(sql`select 1`);
          },
          { query_params: { inner: "b" } },
        );
      },
      {
        clickhouse_settings: { max_threads: 2 },
        http_headers: { "X-Session": "session" },
        query_params: { outer: "a" },
      },
    );

    expect(calls).toHaveLength(1);
    const [call] = calls;
    expect(call?.url.searchParams.get("join_use_nulls")).toBe("0");
    expect(call?.url.searchParams.get("max_threads")).toBe("2");
    const headers = new Headers(call?.init.headers);
    expect(headers.get("X-Parent")).toBe("parent");
    expect(headers.get("X-Session")).toBe("session");
    const form = call?.init.body as FormData;
    expect(form.get("param_outer")).toBe("a");
    expect(form.get("param_inner")).toBe("b");
  });

  it("applies join_use_nulls from runInSession settings to session left joins", async function testRunInSessionJoinUseNulls() {
    const events = ckTable("session_events", { id: string() });
    const dim = ckTable("session_dim", { id: string(), label: string() });
    const { calls } = setFetchMock(async () => new Response(JSON.stringify({ data: [] }), { status: 200 }));
    const db = clickhouseClient({ host: "http://localhost:8123" });

    await db.runInSession(
      async (session) => {
        await session.select({ id: events.id, label: dim.label }).from(events).leftJoin(dim, eq(events.id, dim.id));
      },
      { clickhouse_settings: { join_use_nulls: 0 } },
    );
    await db.withSettings({ join_use_nulls: 0 }).runInSession(
      async (session) => {
        await session.select({ id: events.id, label: dim.label }).from(events).leftJoin(dim, eq(events.id, dim.id));
      },
      { clickhouse_settings: { join_use_nulls: 1 } },
    );

    expect(calls.map((call) => call.url.searchParams.get("join_use_nulls"))).toEqual(["0", "1"]);
  });

  const sessionLockedResponse = () =>
    new Response("Code: 373. DB::Exception: Session is locked. (SESSION_IS_LOCKED)", { status: 500 });

  it("reports SESSION_IS_LOCKED from temporary-table cleanup to onCleanupError", async function testCleanupReportsSessionLocked() {
    const tmpScope = ckTable("tmp_scope", {
      user_id: string(),
    });
    setFetchMock(async (_url, init) => {
      const text = await readBodyText(init.body);
      if (typeof text === "string" && text.startsWith("DROP TABLE")) {
        return sessionLockedResponse();
      }
      return new Response("", { status: 200 });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
    });
    const cleanupErrors: unknown[] = [];
    await db.runInSession(
      async (session) => {
        await session.createTemporaryTable(tmpScope);
      },
      {
        session_id: "cleanup_locked",
        onCleanupError(errors) {
          cleanupErrors.push(...errors);
        },
      },
    );

    expect(cleanupErrors).toHaveLength(1);
    expect(isClickHouseORMError(cleanupErrors[0])).toBe(true);
    if (isClickHouseORMError(cleanupErrors[0])) {
      expect(cleanupErrors[0].clickhouseName).toBe("SESSION_IS_LOCKED");
    }
  });

  it("throws SessionError when temporary-table cleanup hits SESSION_IS_LOCKED", async function testCleanupSessionLockedThrows() {
    const tmpScope = ckTable("tmp_scope", {
      user_id: string(),
    });
    setFetchMock(async (_url, init) => {
      const text = await readBodyText(init.body);
      if (typeof text === "string" && text.startsWith("DROP TABLE")) {
        return sessionLockedResponse();
      }
      return new Response("", { status: 200 });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
    });
    await expect(
      db.runInSession(
        async (session) => {
          await session.createTemporaryTable(tmpScope);
        },
        { session_id: "cleanup_locked_throw" },
      ),
    ).rejects.toThrow("Failed to drop 1 temporary table");
  });

  it("serializes concurrent requests that share the same session id by default", async function testDefaultSessionConcurrency() {
    const pendingResponses: Array<() => void> = [];
    let inFlight = 0;
    let maxInFlight = 0;

    const { calls } = setFetchMock((_url, _init) => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);

      return new Promise<Response>((resolve) => {
        pendingResponses.push(() => {
          inFlight -= 1;
          resolve(
            new Response(JSON.stringify({ data: [] }), {
              status: 200,
            }),
          );
        });
      });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
    });

    const first = db.execute(sql`select 1`, {
      query_id: "shared_session_q1",
      session_id: "shared_session",
    });
    const second = db.execute(sql`select 2`, {
      query_id: "shared_session_q2",
      session_id: "shared_session",
    });

    await flushAsyncWork();
    expect(calls).toHaveLength(1);
    expect(maxInFlight).toBe(1);

    pendingResponses.shift()?.();
    await first;

    await flushAsyncWork();
    expect(calls).toHaveLength(2);
    expect(maxInFlight).toBe(1);
    expect(calls.map((call) => call.url.searchParams.get("query_id"))).toEqual([
      "shared_session_q1",
      "shared_session_q2",
    ]);

    pendingResponses.shift()?.();
    await second;
  });

  it("serializes requests that inherit the client default session id", async function testClientDefaultSessionIdConcurrency() {
    const pendingResponses: Array<() => void> = [];
    let inFlight = 0;
    let maxInFlight = 0;

    const { calls } = setFetchMock((_url, _init) => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);

      return new Promise<Response>((resolve) => {
        pendingResponses.push(() => {
          inFlight -= 1;
          resolve(
            new Response(JSON.stringify({ data: [] }), {
              status: 200,
            }),
          );
        });
      });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
      session_id: "default_session",
    });

    const first = db.execute(sql`select 1`, {
      query_id: "default_session_q1",
    });
    const second = db.execute(sql`select 2`, {
      query_id: "default_session_q2",
    });

    await flushAsyncWork();
    expect(calls).toHaveLength(1);
    expect(maxInFlight).toBe(1);
    expect(calls[0]?.url.searchParams.get("session_id")).toBe("default_session");

    pendingResponses.shift()?.();
    await first;

    await flushAsyncWork();
    expect(calls).toHaveLength(2);
    expect(maxInFlight).toBe(1);
    expect(calls[1]?.url.searchParams.get("session_id")).toBe("default_session");
    expect(calls.map((call) => call.url.searchParams.get("query_id"))).toEqual([
      "default_session_q1",
      "default_session_q2",
    ]);

    pendingResponses.shift()?.();
    await second;
  });

  it("shares the same session concurrency controller across root and runInSession clients", async function testSharedSessionLimiterAcrossChildren() {
    const pendingResponses: Array<() => void> = [];

    const { calls } = setFetchMock((_url, _init) => {
      return new Promise<Response>((resolve) => {
        pendingResponses.push(() => {
          resolve(
            new Response(JSON.stringify({ data: [] }), {
              status: 200,
            }),
          );
        });
      });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
    });

    const outside = db.execute(sql`select 1`, {
      query_id: "outside_shared_session",
      session_id: "shared_session",
    });
    const inside = db.runInSession(
      async (session) => {
        await session.execute(sql`select 2`, {
          query_id: "inside_shared_session",
        });
      },
      {
        session_id: "shared_session",
      },
    );

    await flushAsyncWork();
    expect(calls).toHaveLength(1);
    expect(calls[0]?.url.searchParams.get("query_id")).toBe("outside_shared_session");

    pendingResponses.shift()?.();
    await outside;

    await flushAsyncWork();
    expect(calls).toHaveLength(2);
    expect(calls[1]?.url.searchParams.get("query_id")).toBe("inside_shared_session");
    expect(calls[1]?.url.searchParams.get("session_id")).toBe("shared_session");

    pendingResponses.shift()?.();
    await inside;
  });

  it("waits for the server session lock after breaking out of a session stream", async function testSessionStreamBreakWaitsForServerLock() {
    // Models ClickHouse: a streamed query holds its session until the server
    // notices the closed connection, a little after the client cancels.
    let lockedBy: string | undefined;
    const sessionLocked = () =>
      new Response("Code: 373. DB::Exception: Session is locked by a concurrent client. (SESSION_IS_LOCKED)", {
        status: 500,
      });
    const db = clickhouseClient({
      host: "http://localhost:8123",
      fetch: async (request) => {
        const query =
          (
            await request
              .clone()
              .formData()
              .catch(() => undefined)
          )?.get("query") ?? (await request.text());
        if (lockedBy) {
          return sessionLocked();
        }
        if (!String(query).startsWith("select number")) {
          return Response.json({ data: [{ one: 1 }] });
        }
        lockedBy = new URL(request.url).searchParams.get("session_id") ?? undefined;
        let number = 0;
        return new Response(
          new ReadableStream({
            pull(controller) {
              controller.enqueue(new TextEncoder().encode(`{"number":${number++}}\n`));
            },
            cancel() {
              setTimeout(() => {
                lockedBy = undefined;
              }, 20);
            },
          }),
        );
      },
    });

    const rows = await db.runInSession(async (session) => {
      for await (const row of session.stream(sql`select number from numbers(1000000000)`)) {
        if (row.number === 2) break;
      }
      return session.execute(sql`select 1 as one`);
    });

    expect(rows).toEqual([{ one: 1 }]);
  });

  it("stops waiting for a session lock that outlives request_timeout", async function testSessionLockWaitDeadline() {
    let streamed = false;
    const db = clickhouseClient({
      host: "http://localhost:8123",
      request_timeout: 100,
      fetch: () => {
        if (streamed) {
          return new Response("Code: 373. DB::Exception: Session is locked. (SESSION_IS_LOCKED)", { status: 500 });
        }
        streamed = true;
        return new Response('{"number":0}\n{"number":1}\n');
      },
    });

    const next = db.runInSession(async (session) => {
      for await (const _row of session.stream(sql`select number from numbers(10)`)) {
        break;
      }
      return session.execute(sql`select 1 as one`);
    });

    await expect(next).rejects.toThrow("SESSION_IS_LOCKED");
  });

  it("gives up a hanging session poll at the deadline", async function testHangingSessionPollDeadline() {
    const sessionLocked = () =>
      new Response("Code: 373. DB::Exception: Session is locked. (SESSION_IS_LOCKED)", { status: 500 });
    let polls = 0;
    const db = clickhouseClient({
      host: "http://localhost:8123",
      request_timeout: 200,
      fetch: async (request) => {
        const query = await request.clone().text();
        if (query === "SELECT 1") {
          polls += 1;
          // The first poll returns locked after most of the budget, so a
          // later poll that only watches the request signal must stop at
          // the deadline instead of starting a fresh request_timeout.
          if (polls === 1) {
            await Bun.sleep(100);
            return sessionLocked();
          }
          return await new Promise<Response>((resolve) => {
            const finish = () => resolve(new Response("poll aborted", { status: 500 }));
            if (request.signal.aborted) {
              finish();
              return;
            }
            request.signal.addEventListener("abort", finish, { once: true });
          });
        }
        if (query.startsWith("select number")) {
          return new Response('{"number":0}\n');
        }
        return Response.json({ data: [{ one: 1 }] });
      },
    });

    const started = Date.now();
    const next = db.runInSession(async (session) => {
      for await (const _row of session.stream(sql`select number from numbers(10)`)) {
        break;
      }
      return session.execute(sql`select 1 as one`);
    });
    await Promise.race([
      next.then(
        () => undefined,
        () => undefined,
      ),
      Bun.sleep(1_000).then(() => {
        throw new Error("next same-session request still waiting");
      }),
    ]);
    expect(Date.now() - started).toBeLessThan(250);
  }, 1_500);

  it("keeps holding the slot when the stream used ignore_error_response", async function testIgnoreErrorResponseKeepsSessionSlot() {
    const sessionLocked = () =>
      new Response("Code: 373. DB::Exception: Session is locked by a concurrent client. (SESSION_IS_LOCKED)", {
        status: 500,
      });
    let polls = 0;
    let serverReleased = false;
    const order: string[] = [];
    const db = clickhouseClient({
      host: "http://localhost:8123",
      fetch: async (request) => {
        const query = await request.clone().text();
        if (query.startsWith("select number")) {
          order.push("stream");
          return new Response('{"number":0}\n');
        }
        if (query.includes("KILL QUERY")) {
          return new Response("", { status: 200 });
        }
        if (query === "SELECT 1") {
          polls += 1;
          order.push(`poll-${polls}`);
          if (polls === 1) {
            return sessionLocked();
          }
          serverReleased = true;
          return new Response("", { status: 200 });
        }
        order.push("next");
        if (!serverReleased) {
          return sessionLocked();
        }
        return Response.json({ data: [{ one: 1 }] });
      },
    });

    const rows = await db.runInSession(async (session) => {
      for await (const _row of session.stream(sql`select number from numbers(10)`, {
        ignore_error_response: true,
      })) {
        break;
      }
      return session.execute(sql`select 1 as one`);
    });

    expect(rows).toEqual([{ one: 1 }]);
    expect(order).toEqual(["stream", "poll-1", "poll-2", "next"]);
  });

  it("cancels an abandoned session stream with KILL QUERY outside the session", async function testKillAbandonedSessionStream() {
    const requests: Array<{ url: URL; headers: Headers; query: string; paramId: string | null }> = [];
    const readQuery = async (request: Request) => {
      const form = await request
        .clone()
        .formData()
        .catch(() => undefined);
      const query = String(form?.get("query") ?? (await request.clone().text()));
      return { query, paramId: form?.get("param_id") == null ? null : String(form.get("param_id")) };
    };
    const db = clickhouseClient({
      host: "http://localhost:8123",
      username: "alice",
      password: "secret",
      fetch: async (request) => {
        const url = new URL(request.url);
        const { query, paramId } = await readQuery(request);
        requests.push({ url, headers: new Headers(request.headers), query, paramId });
        if (query.includes("FORMAT JSONEachRow") || query.startsWith("select number")) {
          return new Response('{"number":0}\n');
        }
        if (query.includes("KILL QUERY")) {
          return new Response("", { status: 200 });
        }
        if (query === "SELECT 1") {
          return new Response("", { status: 200 });
        }
        return Response.json({ data: [{ one: 1 }] });
      },
    });

    const rows = await db.runInSession(async (session) => {
      for await (const _row of session.stream(sql`select number from numbers(10)`, {
        query_id: "stream_q",
        session_timeout: 60,
        session_check: 1,
        role: "analyst",
        clickhouse_settings: { max_threads: 2 },
      })) {
        break;
      }
      return session.execute(sql`select 1 as one`);
    });

    expect(rows).toEqual([{ one: 1 }]);
    const kill = requests[1];
    const stream = requests[0];
    expect(kill?.query).toContain("KILL QUERY WHERE query_id = {id:String} SYNC");
    expect(kill?.url.searchParams.get("session_id")).toBeNull();
    expect(kill?.url.searchParams.get("session_timeout")).toBeNull();
    expect(kill?.url.searchParams.get("session_check")).toBeNull();
    expect(kill?.paramId).toBe("stream_q");
    expect(kill?.url.searchParams.get("query_id")).not.toBe("stream_q");
    expect(kill?.url.searchParams.get("role")).toBe("analyst");
    expect(kill?.url.searchParams.get("max_threads")).toBe("2");
    expect(kill?.headers.get("authorization")).toBe(stream?.headers.get("authorization"));
    expect(kill?.headers.get("x-clickhouse-replica-tag")).toBe(stream?.url.searchParams.get("session_id"));
    expect(requests.some((request) => request.query === "SELECT 1")).toBe(true);
    expect(requests.at(-1)?.query).toContain("select 1 as one");
  });

  it("polls the session when KILL QUERY fails", async function testKillAbandonedSessionStreamFailure() {
    let polls = 0;
    let kills = 0;
    const db = clickhouseClient({
      host: "http://localhost:8123",
      fetch: async (request) => {
        const form = await request
          .clone()
          .formData()
          .catch(() => undefined);
        const query = String(form?.get("query") ?? (await request.clone().text()));
        if (query.startsWith("select number")) {
          return new Response('{"number":0}\n');
        }
        if (query.includes("KILL QUERY")) {
          kills += 1;
          return new Response("Code: 497. DB::Exception: Not enough privileges. (ACCESS_DENIED)", { status: 500 });
        }
        if (query === "SELECT 1") {
          polls += 1;
          if (polls === 1) {
            return new Response("Code: 373. DB::Exception: Session is locked. (SESSION_IS_LOCKED)", { status: 500 });
          }
          return new Response("", { status: 200 });
        }
        return Response.json({ data: [{ one: 1 }] });
      },
    });

    const rows = await db.runInSession(async (session) => {
      for await (const _row of session.stream(sql`select number from numbers(10)`, {
        query_id: "stream_q",
        ignore_error_response: true,
      })) {
        break;
      }
      return session.execute(sql`select 1 as one`);
    });

    expect(rows).toEqual([{ one: 1 }]);
    expect(kills).toBe(1);
    expect(polls).toBe(2);
  });

  it("removes the local same-session queue when session_max_concurrent_requests is raised", async function testConfigurableSessionConcurrency() {
    const pendingResponses: Array<() => void> = [];
    let inFlight = 0;
    let maxInFlight = 0;

    const { calls } = setFetchMock((_url, _init) => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);

      return new Promise<Response>((resolve) => {
        pendingResponses.push(() => {
          inFlight -= 1;
          resolve(
            new Response(JSON.stringify({ data: [] }), {
              status: 200,
            }),
          );
        });
      });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
      session_max_concurrent_requests: 2,
    });

    const first = db.execute(sql`select 1`, {
      query_id: "parallel_session_q1",
      session_id: "parallel_session",
    });
    const second = db.execute(sql`select 2`, {
      query_id: "parallel_session_q2",
      session_id: "parallel_session",
    });

    await flushAsyncWork();
    expect(calls).toHaveLength(2);
    expect(maxInFlight).toBe(2);

    pendingResponses.shift()?.();
    pendingResponses.shift()?.();

    await Promise.all([first, second]);
  });

  it("encodes auth with pure UTF-8 base64 and skips User-Agent in restricted runtimes", async function testAuthAndUserAgentBoundaries() {
    // Browsers drop a User-Agent set by scripts; a real Request with that behavior.
    class BrowserLikeRequest extends RecordingRequest {
      constructor(input: RequestInfo | URL, init?: RequestInit) {
        super(input, init);
        this.headers.delete("User-Agent");
      }
    }

    globalThis.Request = BrowserLikeRequest as unknown as typeof Request;

    const { calls } = setFetchMock((_url, _init) => {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
      application: "demo-store-test",
      username: "demo-user",
      password: "demo-pass",
    });

    await db.execute(sql`select 1`);

    const headers = new Headers(calls[0]?.init.headers);
    expect(headers.get("Authorization")).toBe("Basic ZGVtby11c2VyOmRlbW8tcGFzcw==");
    expect(headers.get("User-Agent")).toBeNull();
  });

  it("uses databaseUrl credentials and strips them from outgoing request URLs", async function testDatabaseUrlCredentialFlow() {
    const { calls } = setFetchMock((_url, _init) => {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    });

    const db = clickhouseClient({
      databaseUrl: "http://demo-user:demo-pass@localhost:8123/demo_store",
    });

    await db.execute(sql`select 1`);

    const headers = new Headers(calls[0]?.init.headers);
    expect(headers.get("Authorization")).toBe("Basic ZGVtby11c2VyOmRlbW8tcGFzcw==");
    expect(calls[0]?.url.username).toBe("");
    expect(calls[0]?.url.password).toBe("");
    expect(calls[0]?.url.pathname).toBe("/");
    expect(calls[0]?.url.searchParams.get("database")).toBe("demo_store");

    await expect(
      db.execute(sql`select 2`, {
        auth: {
          username: "request-user",
          password: "request-pass",
        },
      }),
    ).rejects.toThrow("Per-request auth cannot override credentials embedded in databaseUrl");
    await expect(
      db.ping({
        auth: {
          username: "request-user",
          password: "request-pass",
        },
      }),
    ).rejects.toThrow("Per-request auth cannot override credentials embedded in databaseUrl");
  });

  it("uses structured credentials and rejects legacy or mixed connection fields", async function testStructuredAuthAndLegacyGuards() {
    const { calls } = setFetchMock((_url, _init) => {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
      database: "default",
      username: "demo-user",
      password: "demo-pass",
    });

    await db.execute(sql`select 1`);

    const headers = new Headers(calls[0]?.init.headers);
    expect(headers.get("Authorization")).toBe("Basic ZGVtby11c2VyOmRlbW8tcGFzcw==");

    expect(() =>
      clickhouseClient({
        databaseUrl: "http://localhost:8123/default",
        username: "demo-user",
      } as unknown as ClickHouseClientConfig),
    ).toThrow();

    try {
      clickhouseClient({
        databaseUrl: "http://embedded-user:embedded-pass@localhost:8123/default",
        username: "structured-user",
        password: "structured-pass",
      } as unknown as ClickHouseClientConfig);
      throw new Error("expected URL-credentials + username/password to be rejected");
    } catch (error) {
      expect(isClickHouseORMError(error)).toBe(true);
      expect((error as { kind?: unknown }).kind).toBe("client_validation");
      expect((error as { executionState?: unknown }).executionState).toBe("not_sent");
      expect((error as Error).message).toContain("databaseUrl cannot be combined with");
      expect((error as Error).message).toContain("username");
      expect((error as Error).message).toContain("password");
    }

    expect(() =>
      clickhouseClient({
        url: "http://localhost:8123",
      } as unknown as ClickHouseClientConfig),
    ).toThrow();

    expect(() =>
      clickhouseClient({
        access_token: "token",
      } as unknown as ClickHouseClientConfig),
    ).toThrow();

    expect(() =>
      clickhouseClient({
        additional_headers: {
          "x-demo": "1",
        },
      } as unknown as ClickHouseClientConfig),
    ).toThrow();
  });

  it("rejects internal-only json hooks and session lifetime defaults at client construction", function testUnsupportedClientConfigFields() {
    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        json: {
          parse: JSON.parse,
          stringify: JSON.stringify,
        },
      } as unknown as ClickHouseClientConfig),
    ).toThrow("clickhouseClient() no longer accepts json hooks");

    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        session_timeout: 30,
      } as unknown as ClickHouseClientConfig),
    ).toThrow("clickhouseClient() no longer accepts session_timeout");

    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        session_check: 1,
      } as unknown as ClickHouseClientConfig),
    ).toThrow("clickhouseClient() no longer accepts session_check");

    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        session_max_concurrent_requests: 0,
      }),
    ).toThrow("clickhouseClient() session_max_concurrent_requests must be a positive integer");

    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        session_max_concurrent_requests: 1.5,
      }),
    ).toThrow("clickhouseClient() session_max_concurrent_requests must be a positive integer");

    for (const request_timeout of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() =>
        clickhouseClient({
          host: "http://localhost:8123",
          request_timeout,
        }),
      ).toThrow("clickhouseClient() request_timeout must be a positive number");
    }

    for (const request_timeout of [undefined, null]) {
      expect(() =>
        clickhouseClient({
          host: "http://localhost:8123",
          request_timeout: request_timeout as number | undefined,
        }),
      ).not.toThrow();
    }

    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
      }),
    ).not.toThrow();

    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        instrumentation: [],
      } as unknown as ClickHouseClientConfig),
    ).toThrow("clickhouseClient() no longer accepts instrumentation; use plugins instead");

    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        tracing: {},
      } as unknown as ClickHouseClientConfig),
    ).toThrow(
      'clickhouseClient() no longer accepts tracing; remove it, or use plugins: [otelTracing()] from "ck-orm/otel" instead',
    );
    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        tracing: false,
      } as unknown as ClickHouseClientConfig),
    ).toThrow(
      'clickhouseClient() no longer accepts tracing; remove it, or use plugins: [otelTracing()] from "ck-orm/otel" instead',
    );
  });

  it("supports ping and replicasStatus through GET endpoint helpers", async function testSystemEndpointHelpers() {
    const { calls } = setFetchMock((url, _init) => {
      if (url.pathname === "/proxy/ping") {
        return new Response("Ok.\n", { status: 200 });
      }
      if (url.pathname === "/proxy/replicas_status") {
        return new Response("replica lag detected", { status: 503 });
      }
      return new Response("missing", { status: 404 });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123/base",
      pathname: "/proxy",
      http_headers: {
        "x-client-header": "default",
      },
      username: "demo-user",
      password: "demo-pass",
    });

    await db.ping({
      http_headers: {
        "x-extra-header": "override",
      },
    });

    await expectRejectsWithClickhouseError(db.replicasStatus(), {
      kind: "request_failed",
      executionState: "rejected",
      httpStatus: 503,
      responseText: "replica lag detected",
    });

    expect(calls).toHaveLength(2);
    expect(calls[0]?.url.pathname).toBe("/proxy/ping");
    expect(calls[1]?.url.pathname).toBe("/proxy/replicas_status");
    expect(calls[0]?.init.method).toBe("GET");
    expect(calls[1]?.init.method).toBe("GET");
    expect(calls[0]?.init.body).toBeUndefined();
    expect(calls[1]?.init.body).toBeUndefined();

    const headers = new Headers(calls[0]?.init.headers);
    expect(headers.get("Authorization")).toBe("Basic ZGVtby11c2VyOmRlbW8tcGFzcw==");
    expect(headers.get("x-client-header")).toBe("default");
    expect(headers.get("x-extra-header")).toBe("override");
  });

  it("applies timeout and abort handling to system endpoint helpers", async function testSystemEndpointTimeoutAndAbort() {
    const { calls } = setFetchMock(async (_url, init) => {
      return await new Promise<Response>((_resolve, reject) => {
        const rejectWithAbort = () => {
          reject(init.signal?.reason ?? new Error("aborted"));
        };

        if (init.signal?.aborted) {
          rejectWithAbort();
          return;
        }

        init.signal?.addEventListener("abort", rejectWithAbort, { once: true });
      });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
      request_timeout: 5,
    });

    await expectRejectsWithClickhouseError(db.ping(), {
      kind: "timeout",
      executionState: "unknown",
      requestTimeoutMs: 5,
    });

    const abortDb = clickhouseClient({
      host: "http://localhost:8123",
      request_timeout: 1_000,
    });

    const controller = new AbortController();
    const replicasPromise = abortDb.replicasStatus({
      abort_signal: controller.signal,
    });
    controller.abort(new Error("manual abort"));
    await expectRejectsWithClickhouseError(replicasPromise, {
      kind: "aborted",
      executionState: "unknown",
    });

    expect(calls).toHaveLength(2);
    expect(calls.every((call) => call.init.method === "GET")).toBe(true);
  });

  it("does not allow user-provided http_headers to override Authorization", async function testAuthorizationCannotBeOverridden() {
    const { calls } = setFetchMock((_url, _init) => {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
      username: "demo-user",
      password: "demo-pass",
      http_headers: {
        Authorization: "Bearer attacker-token",
      },
    });

    await db.execute(sql`select 1`, {
      http_headers: {
        Authorization: "Bearer per-call-attacker-token",
      },
    });

    const headers = new Headers(calls[0]?.init.headers);
    expect(headers.get("Authorization")).toBe("Basic ZGVtby11c2VyOmRlbW8tcGFzcw==");
  });

  it("decodes unmatched asofLeftJoin rows from a fake fetch", async function testAsofLeftJoinFakeFetchDecode() {
    const trades = ckTable("trades", { id: string(), symbol: string() });
    const quotes = ckTable("quotes", { symbol: string(), note: string() });
    const nullableQuotes = ckTable("nullable_quotes", {
      symbol: nullable(string()),
      note: nullable(string()),
    });
    const json = (data: readonly Record<string, unknown>[]) => new Response(JSON.stringify({ data }), { status: 200 });
    const unmatchedRow = (query: {
      [compileQuerySymbol](): { selection: readonly { sqlAlias: string; path: readonly string[] }[] };
    }) => {
      const row: Record<string, unknown> = {};
      for (const item of query[compileQuerySymbol]().selection) {
        const [group, field] = item.path;
        row[item.sqlAlias] = group === "trades" ? (field === "id" ? "1" : "BTCUSD") : null;
      }
      return row;
    };

    const explicitDb = clickhouseClient({ host: "http://localhost:8123" });
    const explicit = explicitDb
      .select({ id: trades.id, note: quotes.note })
      .from(trades)
      .asofLeftJoin(quotes, eq(trades.symbol, quotes.symbol));
    const explicitFetch = setFetchMock(() => json([{ id: "1", note: null }]));
    expect(await explicit).toEqual([{ id: "1", note: null }]);
    expect(explicitFetch.calls[0]?.url.searchParams.get("join_use_nulls")).toBe("1");

    const implicit = explicitDb
      .select()
      .from(trades)
      .asofLeftJoin(nullableQuotes, eq(trades.symbol, nullableQuotes.symbol));
    setFetchMock(() => json([unmatchedRow(implicit)]));
    expect(await implicit).toEqual([{ trades: { id: "1", symbol: "BTCUSD" }, nullable_quotes: null }]);

    const zero = clickhouseClient({
      host: "http://localhost:8123",
      clickhouse_settings: { join_use_nulls: 0 },
    });
    const zeroQuery = zero.select().from(trades).asofLeftJoin(nullableQuotes, eq(trades.symbol, nullableQuotes.symbol));
    const zeroFetch = setFetchMock(() => json([unmatchedRow(zeroQuery)]));
    expect(await zeroQuery).toEqual([
      {
        trades: { id: "1", symbol: "BTCUSD" },
        nullable_quotes: { symbol: null, note: null },
      },
    ]);
    expect(zeroFetch.calls[0]?.url.searchParams.get("join_use_nulls")).toBe("0");
  });

  it("sends createTable, createMaterializedView and dropTable statements", async function testPersistentDdlStatements() {
    const bodies: string[] = [];
    const events = ckTable("ddl_events", { id: string() });
    const dim = ckTable("ddl_dim", { id: string(), label: string() });
    const target = ckTable("ddl_out", { id: string(), label: nullable(string()) });
    setFetchMock(async (_url, init) => {
      const text = await readBodyText(init.body);
      if (typeof text === "string") {
        bodies.push(text);
      }
      return new Response("", { status: 200 });
    });

    const db = clickhouseClient({ host: "http://localhost:8123" });
    const selectId = db.select({ id: events.id }).from(events);
    const joined = db.select({ id: events.id, label: dim.label }).from(events).leftJoin(dim, eq(events.id, dim.id));

    await db.createTable(events);
    await db.createTable(events, { mode: "or_replace" });
    await db.createTable(events, { mode: "if_not_exists" });
    await db.dropTable(events);
    await db.dropTable("ddl_events", { ifExists: true });
    await db.dropTable(events, { sync: true });
    await db.dropTable("ddl_events", { ifExists: true, sync: true });
    await db.createMaterializedView("ddl_mv", { to: events, as: selectId });
    await db.createMaterializedView("ddl_mv", { to: target, as: joined, mode: "if_not_exists" });

    expect(bodies).toEqual([
      buildCreateTableStatement(events),
      buildCreateTableStatement(events, "or_replace"),
      buildCreateTableStatement(events, "if_not_exists"),
      buildDropTableStatement(events),
      buildDropTableStatement("ddl_events", { ifExists: true }),
      buildDropTableStatement(events, { sync: true }),
      buildDropTableStatement("ddl_events", { ifExists: true, sync: true }),
      compileCreateMaterializedView("ddl_mv", events, selectId).statement,
      compileCreateMaterializedView("ddl_mv", target, joined, "if_not_exists").statement,
    ]);
    expect(bodies[8]).toContain("SETTINGS join_use_nulls = 1");
  });

  it("keeps session ids on persistent DDL and does not drop createTable targets", async function testPersistentDdlSession() {
    const bodies: string[] = [];
    const events = ckTable("ddl_events", { id: string() });
    const { calls } = setFetchMock(async (_url, init) => {
      const text = await readBodyText(init.body);
      if (typeof text === "string") {
        bodies.push(text);
      }
      return new Response("", { status: 200 });
    });

    const db = clickhouseClient({ host: "http://localhost:8123" });
    await db.runInSession(
      async (session) => {
        await session.createTable(events);
        await session.createMaterializedView("ddl_mv", {
          to: events,
          as: session.select({ id: events.id }).from(events),
        });
        await session.dropTable("ddl_mv", { sync: true });
      },
      { session_id: "ddl_session" },
    );

    expect(bodies).toEqual([
      buildCreateTableStatement(events),
      compileCreateMaterializedView("ddl_mv", events, db.select({ id: events.id }).from(events)).statement,
      "DROP TABLE `ddl_mv` SYNC",
    ]);
    expect(calls).toHaveLength(3);
    for (const call of calls) {
      expect(call.url.searchParams.get("session_id")).toBe("ddl_session");
    }
  });
});
