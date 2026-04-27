import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { string } from "./columns";
import { isClickHouseORMError } from "./errors";
import { fn } from "./functions";
import { eq } from "./query";
import { type ClickHouseClientConfig, type ClickHouseQueryOptions, clickhouseClient } from "./runtime";
import { ckTable } from "./schema";
import { sql } from "./sql";
import { orderRewardLog } from "./test-schema/commerce";

const originalFetch = globalThis.fetch;
const originalRequest = globalThis.Request;

type CapturedCall = {
  url: URL;
  init: RequestInit & { duplex?: "half" };
};

const setFetchMock = (handler: (url: URL, init: RequestInit & { duplex?: "half" }) => Promise<Response> | Response) => {
  const calls: CapturedCall[] = [];
  const fetchMock = mock(async (input: string | URL | Request, init?: RequestInit) => {
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
    mock.restore();
  });

  afterEach(function teardownMocks() {
    globalThis.fetch = originalFetch;
    globalThis.Request = originalRequest;
    mock.restore();
  });

  it("keeps request compression out of the public config surface", function testCompressionRequestTypeBoundary() {
    const validConfig: ClickHouseClientConfig<{
      orderRewardLog: typeof orderRewardLog;
    }> = {
      databaseUrl: "http://localhost:8123",
      schema: { orderRewardLog },
      compression: {
        response: true,
      },
    };

    expect(validConfig.compression?.response).toBe(true);

    const invalidConfig: ClickHouseClientConfig<{
      orderRewardLog: typeof orderRewardLog;
    }> = {
      databaseUrl: "http://localhost:8123",
      schema: { orderRewardLog },
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
    const validDatabaseUrlConfig: ClickHouseClientConfig<{
      orderRewardLog: typeof orderRewardLog;
    }> = {
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { orderRewardLog },
    };

    expect(validDatabaseUrlConfig.databaseUrl).toBe("http://localhost:8123/demo_store");

    const validStructuredConfig: ClickHouseClientConfig<{
      orderRewardLog: typeof orderRewardLog;
    }> = {
      host: "http://localhost:8123",
      database: "demo_store",
      username: "default",
      password: "",
      schema: { orderRewardLog },
    };

    expect(validStructuredConfig.host).toBe("http://localhost:8123");

    const invalidMixedConfig: ClickHouseClientConfig<{
      orderRewardLog: typeof orderRewardLog;
    }> = {
      databaseUrl: "http://localhost:8123/demo_store",
      // @ts-expect-error databaseUrl is mutually exclusive with structured fields
      username: "default",
      schema: { orderRewardLog },
    };

    expect((invalidMixedConfig as Record<string, unknown>).username).toBe("default");
  });

  it("keeps internal transport hooks and session lifetime defaults out of the client config surface", function testInternalConfigTypeBoundary() {
    const validConfig: ClickHouseClientConfig<{
      orderRewardLog: typeof orderRewardLog;
    }> = {
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { orderRewardLog },
      session_id: "default_session",
    };

    expect(validConfig.session_id).toBe("default_session");

    const invalidJsonConfig: ClickHouseClientConfig<{
      orderRewardLog: typeof orderRewardLog;
    }> = {
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { orderRewardLog },
      // @ts-expect-error json hooks are intentionally internal to the fetch transport
      json: {
        parse: JSON.parse,
        stringify: JSON.stringify,
      },
    };

    const invalidSessionTimeoutConfig: ClickHouseClientConfig<{
      orderRewardLog: typeof orderRewardLog;
    }> = {
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { orderRewardLog },
      // @ts-expect-error session_timeout belongs on requests and runInSession(), not client defaults
      session_timeout: 30,
    };

    const invalidSessionCheckConfig: ClickHouseClientConfig<{
      orderRewardLog: typeof orderRewardLog;
    }> = {
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { orderRewardLog },
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
      schema: { orderRewardLog },
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
        activeUsers: "3",
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
    expect(calls[0]?.url.searchParams.getAll("role")).toEqual(["analyst"]);

    const form = calls[0]?.init.body;
    expect(form).toBeInstanceOf(FormData);
    expect((form as FormData).get("query")).toBeTruthy();
    expect(String((form as FormData).get("query"))).toContain("FORMAT JSON");
    expect((form as FormData).get("param_orm_param1")).toBe("u_1");
  });

  it("reuses one session id and cleans temp tables after runInSession failure", async function testRunInSessionCleanup() {
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
      schema: { orderRewardLog },
    });

    await expect(
      db.runInSession(
        async (session) => {
          await session.createTemporaryTable(tmpScope);
          session.registerTempTable("tmp_manual");
          await session.command(sql`select 1`);
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
      expect(call.init.method).toBe("POST");
    }
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
      schema: { orderRewardLog },
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
      schema: { orderRewardLog },
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
      schema: { orderRewardLog },
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
      schema: { orderRewardLog },
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
    class BrowserLikeRequest {
      readonly headers: Headers;

      constructor(_input: string | URL | Request, init?: RequestInit) {
        const headers = new Headers(init?.headers);
        headers.delete("User-Agent");
        this.headers = headers;
      }
    }

    globalThis.Request = BrowserLikeRequest as unknown as typeof Request;

    const { calls } = setFetchMock((_url, _init) => {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
      schema: { orderRewardLog },
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
      schema: { orderRewardLog },
    });

    await db.execute(sql`select 1`);

    const headers = new Headers(calls[0]?.init.headers);
    expect(headers.get("Authorization")).toBe("Basic ZGVtby11c2VyOmRlbW8tcGFzcw==");
    expect(calls[0]?.url.username).toBe("");
    expect(calls[0]?.url.password).toBe("");
    expect(calls[0]?.url.pathname).toBe("/");
    expect(calls[0]?.url.searchParams.get("database")).toBe("demo_store");
  });

  it("uses structured credentials and rejects legacy or mixed connection fields", async function testStructuredAuthAndLegacyGuards() {
    const { calls } = setFetchMock((_url, _init) => {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    });

    const db = clickhouseClient({
      host: "http://localhost:8123",
      database: "default",
      schema: { orderRewardLog },
      username: "demo-user",
      password: "demo-pass",
    });

    await db.execute(sql`select 1`);

    const headers = new Headers(calls[0]?.init.headers);
    expect(headers.get("Authorization")).toBe("Basic ZGVtby11c2VyOmRlbW8tcGFzcw==");

    expect(() =>
      clickhouseClient({
        databaseUrl: "http://localhost:8123/default",
        schema: { orderRewardLog },
        username: "demo-user",
      } as unknown as ClickHouseClientConfig<{
        orderRewardLog: typeof orderRewardLog;
      }>),
    ).toThrow();

    try {
      clickhouseClient({
        databaseUrl: "http://embedded-user:embedded-pass@localhost:8123/default",
        schema: { orderRewardLog },
        username: "structured-user",
        password: "structured-pass",
      } as unknown as ClickHouseClientConfig<{
        orderRewardLog: typeof orderRewardLog;
      }>);
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
        schema: { orderRewardLog },
      } as unknown as ClickHouseClientConfig<{
        orderRewardLog: typeof orderRewardLog;
      }>),
    ).toThrow();

    expect(() =>
      clickhouseClient({
        access_token: "token",
        schema: { orderRewardLog },
      } as unknown as ClickHouseClientConfig<{
        orderRewardLog: typeof orderRewardLog;
      }>),
    ).toThrow();

    expect(() =>
      clickhouseClient({
        additional_headers: {
          "x-demo": "1",
        },
        schema: { orderRewardLog },
      } as unknown as ClickHouseClientConfig<{
        orderRewardLog: typeof orderRewardLog;
      }>),
    ).toThrow();
  });

  it("rejects internal-only json hooks and session lifetime defaults at client construction", function testUnsupportedClientConfigFields() {
    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        schema: { orderRewardLog },
        json: {
          parse: JSON.parse,
          stringify: JSON.stringify,
        },
      } as unknown as ClickHouseClientConfig<{
        orderRewardLog: typeof orderRewardLog;
      }>),
    ).toThrow("clickhouseClient() no longer accepts json hooks");

    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        schema: { orderRewardLog },
        session_timeout: 30,
      } as unknown as ClickHouseClientConfig<{
        orderRewardLog: typeof orderRewardLog;
      }>),
    ).toThrow("clickhouseClient() no longer accepts session_timeout");

    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        schema: { orderRewardLog },
        session_check: 1,
      } as unknown as ClickHouseClientConfig<{
        orderRewardLog: typeof orderRewardLog;
      }>),
    ).toThrow("clickhouseClient() no longer accepts session_check");

    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        schema: { orderRewardLog },
        session_max_concurrent_requests: 0,
      }),
    ).toThrow("clickhouseClient() session_max_concurrent_requests must be a positive integer");

    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        schema: { orderRewardLog },
        session_max_concurrent_requests: 1.5,
      }),
    ).toThrow("clickhouseClient() session_max_concurrent_requests must be a positive integer");
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
      schema: { orderRewardLog },
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
      schema: { orderRewardLog },
      request_timeout: 5,
    });

    await expectRejectsWithClickhouseError(db.ping(), {
      kind: "timeout",
      executionState: "unknown",
      requestTimeoutMs: 5,
    });

    const abortDb = clickhouseClient({
      host: "http://localhost:8123",
      schema: { orderRewardLog },
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
      schema: { orderRewardLog },
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
});
