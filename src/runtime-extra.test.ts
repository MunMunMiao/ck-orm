import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { int32, string } from "./columns";
import { ClickHouseOrmError } from "./errors";
import { expr } from "./query";
import { clickhouseClient } from "./runtime";
import type { AnyTable } from "./schema";
import { chTable } from "./schema";
import { sql } from "./sql";
import { orderRewardLog } from "./test-schema/commerce";

const originalFetch = globalThis.fetch;
const originalRequest = globalThis.Request;

const users = chTable(
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

const expectRejectsWithClickhouseError = async (promise: Promise<unknown>, expected: Record<string, unknown>) => {
  try {
    await promise;
    throw new Error("Expected promise to reject with ClickHouseOrmError");
  } catch (error) {
    expect(error).toBeInstanceOf(ClickHouseOrmError);
    for (const [key, value] of Object.entries(expected)) {
      expect((error as Record<string, unknown>)[key]).toEqual(value);
    }
  }
};

const takeAsync = async <TValue>(iterable: AsyncIterable<TValue>, limit: number) => {
  const rows: TValue[] = [];
  for await (const row of iterable) {
    rows.push(row);
    if (rows.length >= limit) {
      break;
    }
  }
  return rows;
};

describe("ck-orm runtime extras", function describeClickHouseOrmRuntimeExtras() {
  beforeEach(function setupMocks() {
    mock.restore();
  });

  afterEach(function teardownMocks() {
    globalThis.fetch = originalFetch;
    globalThis.Request = originalRequest;
    mock.restore();
  });

  it("covers raw query normalization, command, stream and insert branches", async function testRuntimeBranches() {
    const capturedBodies: string[] = [];
    const capturedUrls: URL[] = [];

    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(typeof input === "string" || input instanceof URL ? String(input) : input.url);
      capturedUrls.push(url);

      if (init?.body instanceof FormData) {
        const query = String(init.body.get("query") ?? "");
        capturedBodies.push(query);
        if (query.includes("as `value`") || query.includes(" as value")) {
          return new Response(JSON.stringify({ data: [{ value: "2.5" }] }), {
            status: 200,
          });
        }
        return new Response(JSON.stringify({ data: [{ raw: "row" }] }), {
          status: 200,
        });
      }

      const bodyText = await readBodyText(init?.body);
      if (typeof bodyText === "string") {
        capturedBodies.push(bodyText);
      }

      if (bodyText?.includes("FORMAT JSONEachRow")) {
        if (bodyText.includes("sum(")) {
          return new Response('{"total":"2.5"}\n', { status: 200 });
        }
        return new Response('{"raw":"first"}\n{"raw":"second"}\n', {
          status: 200,
        });
      }

      if (bodyText?.includes("as `value`") || bodyText?.includes(" as value")) {
        return new Response(JSON.stringify({ data: [{ value: "2.5" }] }), {
          status: 200,
        });
      }

      return new Response(JSON.stringify({ data: [{ raw: "row" }] }), {
        status: 200,
      });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      host: "http://localhost:8123",
      schema: { orderRewardLog, users },
    });

    const typedRows = await db
      .select({
        value: expr(sql.raw("'2.5'"))
          .mapWith((value) => ({ wrapped: String(value) }))
          .as("value"),
      })
      .from(orderRewardLog)
      .limit(1);

    expect(await db.execute("select 1")).toEqual([{ raw: "row" }]);
    expect(await db.execute(sql`select ${users.id} from ${users} where ${users.id} = ${1}`)).toEqual([{ raw: "row" }]);
    expect(await db.execute(sql`select '2.5' as value`)).toEqual([{ value: "2.5" }]);
    expect(await db.execute("select ';' as raw; -- trailing comment")).toEqual([{ raw: "row" }]);
    expect(typedRows).toEqual([{ value: { wrapped: "2.5" } }]);
    expect(capturedBodies.some((body) => body.includes("select ';' as raw -- trailing comment"))).toBe(true);

    await db.command("truncate table logs", { query_id: "cmd_1" });
    expect(capturedBodies).toContain("truncate table logs");

    await db.insert(users).values({ id: 7, name: "zoe" });
    expect(capturedBodies.some((body) => body.includes("insert into `users`"))).toBe(true);

    const streamRows: Record<string, unknown>[] = [];
    for await (const row of db.stream("select 1")) {
      streamRows.push(row);
    }
    expect(streamRows).toEqual([{ raw: "first" }, { raw: "second" }]);

    await db.insertJsonEachRow("manual_table", [{ id: 1 }], {
      query_id: "insert_1",
    });
    await db.insertJsonEachRow(orderRewardLog as AnyTable, [{ id: 2 }], {
      query_id: "insert_2",
    });
    expect(
      capturedUrls.some((url) => url.searchParams.get("query") === "INSERT INTO `manual_table` FORMAT JSONEachRow"),
    ).toBe(true);
    expect(
      capturedUrls.some((url) => url.searchParams.get("query") === "INSERT INTO `order_reward_log` FORMAT JSONEachRow"),
    ).toBe(true);

    expect(() => db.registerTempTable("tmp_scope")).toThrow(ClickHouseOrmError);
    await expectRejectsWithClickhouseError(db.createTemporaryTable(chTable("tmp_scope", { id: int32() })), {
      kind: "session",
      executionState: "not_sent",
      message: "[ck-orm] createTemporaryTable() requires runInSession()",
    });
    await expectRejectsWithClickhouseError(db.execute("select 1", { format: "CSV" }), {
      kind: "client_validation",
      executionState: "not_sent",
      message: "[ck-orm] Unsupported eager query format: CSV",
    });
    await expectRejectsWithClickhouseError(
      (async () => {
        for await (const _row of db.stream("select 1", { format: "CSV" })) {
          // noop
        }
      })(),
      {
        kind: "client_validation",
        executionState: "not_sent",
        message: "[ck-orm] Unsupported streaming query format: CSV",
      },
    );
    await expectRejectsWithClickhouseError(db.command("select 1; select 2"), {
      kind: "client_validation",
      executionState: "not_sent",
      message: "[ck-orm] Query contains multiple statements; only a single statement is allowed per request",
    });
  });

  it("covers session iterator cleanup, automatic multipart params and timeout/abort handling", async function testSessionAndAbortBranches() {
    let cancelCount = 0;
    let insertReturnCount = 0;
    const bodies: string[] = [];
    const urls: URL[] = [];
    const forms = new Map<string, Record<string, string>>();

    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(typeof input === "string" || input instanceof URL ? String(input) : input.url);
      urls.push(url);

      if (init?.body instanceof FormData) {
        const queryId = url.searchParams.get("query_id");
        const entries: Record<string, string> = {};
        for (const [key, value] of init.body.entries()) {
          entries[key] = String(value);
        }
        if (queryId) {
          forms.set(queryId, entries);
        }
        if (entries.query) {
          bodies.push(entries.query);
        }
      }

      if (url.searchParams.get("query_id") === "timeout_case") {
        return await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            "abort",
            () => {
              reject(init.signal?.reason ?? new Error("aborted"));
            },
            { once: true },
          );
        });
      }

      if (url.searchParams.get("query_id") === "body_timeout_case") {
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            init?.signal?.addEventListener(
              "abort",
              () => {
                controller.error(init.signal?.reason ?? new Error("aborted"));
              },
              { once: true },
            );
          },
        });
        return new Response(stream, { status: 200 });
      }

      if (url.searchParams.get("query_id") === "stream_timeout_case") {
        const encoder = new TextEncoder();
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(encoder.encode('{"raw":"first"}\n'));
            init?.signal?.addEventListener(
              "abort",
              () => {
                controller.error(init.signal?.reason ?? new Error("aborted"));
              },
              { once: true },
            );
          },
        });
        return new Response(stream, { status: 200 });
      }

      if (url.searchParams.get("query_id") === "insert_abort") {
        const body = init?.body;
        if (body instanceof ReadableStream) {
          const reader = body.getReader();
          await reader.read();
          await reader.cancel(new Error("stop reading")).catch(() => undefined);
          return new Response("", { status: 200 });
        }
      }

      if (
        init?.body instanceof ReadableStream &&
        url.searchParams.get("query")?.includes("INSERT INTO users FORMAT JSONEachRow")
      ) {
        bodies.push((await readBodyText(init.body)) ?? "");
        return new Response("", { status: 200 });
      }

      const text = await readBodyText(init?.body);
      if (typeof text === "string") {
        bodies.push(text);
      }

      if (text?.includes("FORMAT JSONEachRow")) {
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new TextEncoder().encode('{"id":1,"name":"alice"}\n'));
          },
          cancel() {
            cancelCount += 1;
          },
        });
        return new Response(stream, { status: 200 });
      }

      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
      request_timeout: 5,
    });

    await db.runInSession(
      async (session) => {
        for await (const _row of session.select().from(users).iterator()) {
          break;
        }
        await session.command("select 1");
        return undefined;
      },
      { session_id: "session_iter" },
    );

    expect(cancelCount).toBeGreaterThanOrEqual(1);
    expect(urls.some((url) => url.searchParams.get("session_id") === "session_iter")).toBe(true);
    expect(bodies).toContain("select 1");

    await db.execute(sql`select ${users.id} from ${users} where ${users.id} = ${1}`, {
      query_id: "multipart_params",
    });
    const multipartForm = forms.get("multipart_params");
    expect(multipartForm?.param_orm_param1).toBe("1");
    expect(
      bodies.some((body) => body.includes("select `users`.`id` from `users` where `users`.`id` = {orm_param1:Int64}")),
    ).toBe(true);

    await db.insertJsonEachRow(
      users,
      (async function* rows() {
        yield { id: 1, name: "alice" };
        yield { id: 2, name: "bob" };
      })(),
      { query_id: "async_insert" },
    );
    expect(bodies.some((body) => body.includes('{"id":1,"name":"alice"}\n{"id":2,"name":"bob"}\n'))).toBe(true);

    await expectRejectsWithClickhouseError(db.execute("select 1", { query_id: "timeout_case" }), {
      kind: "timeout",
      executionState: "unknown",
      requestTimeoutMs: 5,
      queryId: "timeout_case",
    });
    await expectRejectsWithClickhouseError(db.execute("select 1", { query_id: "body_timeout_case" }), {
      kind: "timeout",
      executionState: "unknown",
      requestTimeoutMs: 5,
      queryId: "body_timeout_case",
    });

    const streamedRows: Record<string, unknown>[] = [];
    await expectRejectsWithClickhouseError(
      (async () => {
        for await (const row of db.stream("select 1", {
          query_id: "stream_timeout_case",
        })) {
          streamedRows.push(row);
        }
      })(),
      {
        kind: "timeout",
        executionState: "unknown",
        requestTimeoutMs: 5,
        queryId: "stream_timeout_case",
      },
    );
    expect(streamedRows).toEqual([{ raw: "first" }]);

    await db.insertJsonEachRow(
      users,
      (async function* rows() {
        try {
          yield { id: 1, name: "alice" };
          yield { id: 2, name: "bob" };
        } finally {
          insertReturnCount += 1;
        }
      })(),
      { query_id: "insert_abort" },
    );
    expect(insertReturnCount).toBe(1);
  });

  it("cleans up async iterable inserts and buffers bodies when stream uploads are unsupported", async function testInsertCleanupAndBufferedFallback() {
    let returnCount = 0;
    const bodies: string[] = [];

    class NoStreamUploadRequest {
      readonly headers: Headers;

      constructor(_input: string | URL | Request, init?: RequestInit) {
        if (init?.body instanceof ReadableStream) {
          throw new TypeError("stream uploads are unsupported in this runtime");
        }
        this.headers = new Headers(init?.headers);
      }
    }

    globalThis.Request = NoStreamUploadRequest as unknown as typeof Request;

    globalThis.fetch = mock(async (_input: string | URL | Request, init?: RequestInit) => {
      const bodyText = await readBodyText(init?.body);
      if (typeof bodyText === "string") {
        bodies.push(bodyText);
      }
      return new Response("", { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      host: "http://localhost:8123",
      schema: { users },
    });

    await db.insertJsonEachRow(
      users,
      (async function* rows() {
        try {
          yield { id: 1, name: "alice" };
          yield { id: 2, name: "bob" };
        } finally {
          returnCount += 1;
        }
      })(),
      { query_id: "buffered_insert" },
    );

    expect(bodies).toContain('{"id":1,"name":"alice"}\n{"id":2,"name":"bob"}\n');
    expect(returnCount).toBe(1);
  });

  it("keeps runtime sources free of node-only imports", async function testRuntimePortabilitySources() {
    const runtimeSource = await Bun.file(new URL("./runtime.ts", import.meta.url)).text();
    const querySource = await Bun.file(new URL("./query.ts", import.meta.url)).text();
    const observabilitySource = await Bun.file(new URL("./observability.ts", import.meta.url)).text();

    for (const source of [runtimeSource, querySource, observabilitySource]) {
      expect(source.includes("node:")).toBe(false);
      expect(source.includes("Buffer.from")).toBe(false);
    }
  });

  it("rejects request compression at runtime when config is forced through as any", function testRequestCompressionRuntimeGuard() {
    expect(() =>
      clickhouseClient({
        host: "http://localhost:8123",
        schema: { users },
        compression: {
          response: true,
          request: true,
        },
      } as unknown as Parameters<typeof clickhouseClient<{ users: typeof users }>>[0]),
    ).toThrow(ClickHouseOrmError);
  });

  it("detects ClickHouse exception blocks even when HTTP status is 200", async function testEmbeddedExceptionBlocks() {
    globalThis.fetch = mock(async (_input: string | URL | Request, init?: RequestInit) => {
      const bodyText = await readBodyText(init?.body);
      if (typeof bodyText === "string" && bodyText.includes("FORMAT JSONEachRow")) {
        return new Response('{"raw":"first"}\nCode: 60. DB::Exception: table missing (UNKNOWN_TABLE)\n', {
          status: 200,
        });
      }
      return new Response("Code: 62. DB::Exception: syntax error (SYNTAX_ERROR)", { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      host: "http://localhost:8123",
      schema: { users },
    });

    await expectRejectsWithClickhouseError(db.execute("select broken", { query_id: "embedded_json_error" }), {
      kind: "request_failed",
      executionState: "rejected",
      httpStatus: 200,
      clickhouseCode: 62,
      clickhouseName: "SYNTAX_ERROR",
      queryId: "embedded_json_error",
    });

    const streamedRows: Record<string, unknown>[] = [];
    await expectRejectsWithClickhouseError(
      (async () => {
        for await (const row of db.stream("select broken", {
          query_id: "embedded_stream_error",
        })) {
          streamedRows.push(row);
        }
      })(),
      {
        kind: "request_failed",
        executionState: "rejected",
        httpStatus: 200,
        clickhouseCode: 60,
        clickhouseName: "UNKNOWN_TABLE",
        queryId: "embedded_stream_error",
      },
    );
    expect(streamedRows).toEqual([{ raw: "first" }]);
  });

  it("guards exception output parsing and unsafe async insert settings before sending requests", async function testStrictErrorSettings() {
    const fetchSpy = mock(async () => new Response(JSON.stringify({ data: [] }), { status: 200 }));
    globalThis.fetch = fetchSpy as unknown as typeof fetch;

    const db = clickhouseClient({
      host: "http://localhost:8123",
      schema: { users },
    });

    await expectRejectsWithClickhouseError(
      db.execute("select 1", {
        clickhouse_settings: {
          http_write_exception_in_output_format: 1,
        },
      }),
      {
        kind: "client_validation",
        executionState: "not_sent",
        message: "[ck-orm] ck-orm requires http_write_exception_in_output_format=0 for stable HTTP exception parsing",
      },
    );

    await expectRejectsWithClickhouseError(
      db.insertJsonEachRow(users, [{ id: 1, name: "alice" }], {
        clickhouse_settings: {
          async_insert: 1,
          wait_for_async_insert: 0,
        },
      }),
      {
        kind: "client_validation",
        executionState: "not_sent",
        message: "[ck-orm] ck-orm requires wait_for_async_insert=1 when async_insert=1",
      },
    );

    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("covers complex query params, child query builders and session raw streams", async function testChildClientsAndQueryParams() {
    const urls: URL[] = [];
    const bodies: string[] = [];
    const forms = new Map<string, Record<string, string>>();

    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(typeof input === "string" || input instanceof URL ? String(input) : input.url);
      urls.push(url);

      const bodyText = await readBodyText(init?.body);
      const formQuery = init?.body instanceof FormData ? String(init.body.get("query") ?? "") : "";
      if (init?.body instanceof FormData) {
        const queryId = url.searchParams.get("query_id");
        const entries: Record<string, string> = {};
        for (const [key, value] of init.body.entries()) {
          entries[key] = String(value);
        }
        if (queryId) {
          forms.set(queryId, entries);
        }
      }

      if (typeof bodyText === "string") {
        bodies.push(bodyText);
      }
      if (formQuery) {
        bodies.push(formQuery);
      }

      const statement = formQuery || bodyText || url.searchParams.get("query") || "";

      if (statement.includes("FORMAT JSONEachRow")) {
        return new Response('{"id":1,"name":"alice"}\n{"id":2,"name":"bob"}\n', {
          status: 200,
        });
      }

      if (statement.includes("insert into `users`")) {
        return new Response("", { status: 200 });
      }

      return new Response(
        JSON.stringify({
          data: [{ id: 1, name: "alice" }],
        }),
        { status: 200 },
      );
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    expect(
      await takeAsync(
        db
          .select({
            id: users.id,
            name: users.name,
          })
          .from(users)
          .limit(1)
          .iterator(),
        1,
      ),
    ).toEqual([{ id: 1, name: "alice" }]);

    const childDb = db.withSettings({
      join_use_nulls: 0 as const,
      max_threads: 1,
    });

    expect(
      await childDb
        .select({
          id: users.id,
          name: users.name,
        })
        .from(users)
        .limit(1),
    ).toEqual([{ id: 1, name: "alice" }]);

    expect(
      await takeAsync(
        childDb
          .select({
            id: users.id,
            name: users.name,
          })
          .from(users)
          .limit(1)
          .iterator(),
        1,
      ),
    ).toEqual([{ id: 1, name: "alice" }]);

    await childDb.insert(users).values({
      id: 9,
      name: "zoe",
    });

    await db.execute(
      "select {filters:Array(String)} as filters, {score_map:Map(String, Int64)} as score_map, {payload:Map(String, String)} as payload",
      {
        query_id: "complex_query_params",
        query_params: {
          filters: ["vip", null, "trial"],
          score_map: new Map([
            ["gold", 1],
            ["silver", 2],
          ]),
          payload: {
            channel: "web",
            region: "sg",
          },
        },
      },
    );

    await db.execute("select 1", {
      query_id: "role_array_query",
      role: ["analyst", "auditor"],
    });

    const complexParamsForm = forms.get("complex_query_params");
    expect(complexParamsForm?.param_filters).toBe("['vip',NULL,'trial']");
    expect(complexParamsForm?.param_score_map).toBe("{'gold':1,'silver':2}");
    expect(complexParamsForm?.param_payload).toBe("{'channel':'web','region':'sg'}");

    const roleArrayUrl = urls.find((url) => url.searchParams.get("query_id") === "role_array_query");
    expect(roleArrayUrl?.searchParams.getAll("role")).toEqual(["analyst", "auditor"]);

    const sessionRows = await db.runInSession(
      async (session) => {
        return await takeAsync(session.stream("select 1 as id, 'alice' as name"), 1);
      },
      { session_id: "raw_stream_session" },
    );

    expect(sessionRows).toEqual([{ id: 1, name: "alice" }]);
    expect(urls.some((url) => url.searchParams.get("session_id") === "raw_stream_session")).toBe(true);
    expect(urls.some((url) => url.searchParams.get("max_threads") === "1")).toBe(true);
    expect(bodies.some((body) => body.includes("insert into `users`"))).toBe(true);
  });

  it("returns undecoded rows from executeCompiled and iteratorCompiled when compiled selection is empty", async function testCompiledRawSelectionBypass() {
    globalThis.fetch = mock(async (_input: string | URL | Request, init?: RequestInit) => {
      const bodyText = await readBodyText(init?.body);
      if (typeof bodyText === "string" && bodyText.includes("FORMAT JSONEachRow")) {
        return new Response('{"raw":"first"}\n{"raw":"second"}\n', {
          status: 200,
        });
      }

      return new Response(
        JSON.stringify({
          data: [{ raw: "row" }],
        }),
        { status: 200 },
      );
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    const compiled = {
      kind: "compiled-query" as const,
      mode: "query" as const,
      statement: "select 1 as raw",
      params: {},
      selection: [],
    };

    expect(await db.executeCompiled(compiled)).toEqual([{ raw: "row" }]);
    expect(await takeAsync(db.iteratorCompiled(compiled), 2)).toEqual([{ raw: "first" }, { raw: "second" }]);
  });

  it("rejects user query_params that try to use internal orm_param prefixes", async function testReservedQueryParamPrefix() {
    const fetchSpy = mock(async () => new Response(JSON.stringify({ data: [] }), { status: 200 }));
    globalThis.fetch = fetchSpy as unknown as typeof fetch;

    const db = clickhouseClient({
      host: "http://localhost:8123",
      schema: { users },
    });

    await expectRejectsWithClickhouseError(
      db.execute("select {user_id:String} as user_id", {
        query_params: {
          orm_param1: "unsafe",
        },
      }),
      {
        kind: "client_validation",
        executionState: "not_sent",
        message:
          '[ck-orm] query_params key "orm_param1" uses reserved internal prefix "orm_param". This prefix is reserved for sql`...` generated parameters.',
      },
    );

    await expectRejectsWithClickhouseError(
      db.command(sql`select ${1}`, {
        query_params: {
          orm_param1: 2,
        },
      }),
      {
        kind: "client_validation",
        executionState: "not_sent",
        message:
          '[ck-orm] query_params key "orm_param1" uses reserved internal prefix "orm_param". This prefix is reserved for sql`...` generated parameters.',
      },
    );

    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("covers nested session reuse, duplicate temp tables and child raw helpers", async function testNestedSessionReuseAndChildHelpers() {
    const urls: URL[] = [];
    const bodies: string[] = [];

    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(typeof input === "string" || input instanceof URL ? String(input) : input.url);
      urls.push(url);

      const bodyText = await readBodyText(init?.body);
      if (typeof bodyText === "string") {
        bodies.push(bodyText);
      }

      if (url.pathname.endsWith("/ping") || url.pathname.endsWith("/replicas_status")) {
        return new Response("", { status: 200 });
      }

      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    let nestedSessionId = "";

    await db.runInSession(
      async (session) => {
        session.registerTempTable("tmp_users");
        session.registerTempTable("tmp_users");

        await session.runInSession(async (nestedSession) => {
          nestedSessionId = nestedSession.sessionId;
          expect(nestedSession.sessionId).not.toBe(session.sessionId);
          await nestedSession.command("select 42");
          return undefined;
        });

        const childDb = session.withSettings({
          max_threads: 2,
        });

        await childDb.command("select 1");
        await childDb.ping();
        await childDb.replicasStatus();
      },
      {
        session_id: "nested_same_session",
      },
    );

    await expectRejectsWithClickhouseError(
      db.runInSession(
        async (session) => {
          await session.runInSession(async () => undefined, {
            session_id: session.sessionId,
          });
        },
        { session_id: "outer_session" },
      ),
      {
        kind: "session",
        executionState: "not_sent",
        message: "[ck-orm] Nested runInSession() cannot reuse an existing session_id",
      },
    );

    expect(urls.some((url) => url.searchParams.get("session_id") === "nested_same_session")).toBe(true);
    expect(nestedSessionId).not.toBe("");
    expect(urls.some((url) => url.searchParams.get("session_id") === nestedSessionId)).toBe(true);
    expect(urls.some((url) => url.searchParams.get("max_threads") === "2")).toBe(true);
    expect(bodies).toContain("select 42");
    expect(bodies).toContain("select 1");
    expect(bodies).toContain("DROP TABLE IF EXISTS `tmp_users`");
  });

  it("rejects runInSession session_check without an explicit session_id", async function testRunInSessionSessionCheckBoundary() {
    const fetchSpy = mock(async () => {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    });
    globalThis.fetch = fetchSpy as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    await expectRejectsWithClickhouseError(
      db.runInSession(async () => undefined, {
        session_check: 1,
      }),
      {
        kind: "client_validation",
        executionState: "not_sent",
        message:
          "[ck-orm] runInSession() requires an explicit session_id when session_check=1 because ClickHouse only validates existing sessions",
      },
    );

    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("rejects nested runInSession session_check=1 before sending requests", async function testNestedRunInSessionSessionCheckBoundary() {
    const fetchSpy = mock(async () => {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    });
    globalThis.fetch = fetchSpy as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    await expectRejectsWithClickhouseError(
      db.runInSession(
        async (session) => {
          await session.runInSession(async () => undefined, {
            session_id: "child_session",
            session_check: 1,
          });
        },
        { session_id: "outer_session" },
      ),
      {
        kind: "client_validation",
        executionState: "not_sent",
        message:
          "[ck-orm] Nested runInSession() cannot use session_check=1 because child sessions are created by ck-orm",
      },
    );

    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("rejects createTemporaryTableRaw outside runInSession", async function testCreateTemporaryTableRawOutsideSession() {
    globalThis.fetch = mock(async () => new Response("", { status: 200 })) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    await expectRejectsWithClickhouseError(db.createTemporaryTableRaw("tmp_outside", "(id Int32)"), {
      kind: "session",
      executionState: "not_sent",
      message: "[ck-orm] createTemporaryTableRaw() requires runInSession()",
    });
  });

  it("creates distinct ids across outer child grandchild sessions", async function testNestedSessionIdsAcrossThreeLevels() {
    const urls: URL[] = [];
    const bodies: string[] = [];

    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(typeof input === "string" || input instanceof URL ? String(input) : input.url);
      urls.push(url);

      const bodyText = await readBodyText(init?.body);
      if (typeof bodyText === "string") {
        bodies.push(bodyText);
      }

      return new Response("", { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    const sessionIds: string[] = [];

    await db.runInSession(async (session) => {
      sessionIds.push(session.sessionId);
      await session.command("select 1");

      await session.runInSession(async (nestedSession) => {
        sessionIds.push(nestedSession.sessionId);
        await nestedSession.command("select 2");

        await nestedSession.runInSession(async (grandchildSession) => {
          sessionIds.push(grandchildSession.sessionId);
          await grandchildSession.command("select 3");
        });
      });
    });

    expect(new Set(sessionIds).size).toBe(3);
    expect(sessionIds.every((value) => value !== "")).toBe(true);
    expect(sessionIds.every((value) => urls.some((url) => url.searchParams.get("session_id") === value))).toBe(true);
    expect(bodies).toContain("select 1");
    expect(bodies).toContain("select 2");
    expect(bodies).toContain("select 3");
  });

  it("rejects grandchild reuse of any ancestor session_id before sending requests", async function testGrandchildAncestorReuseBoundary() {
    const fetchSpy = mock(async () => {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    });
    globalThis.fetch = fetchSpy as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    await expectRejectsWithClickhouseError(
      db.runInSession(
        async (outer) => {
          await outer.runInSession(async (child) => {
            await child.runInSession(async () => undefined, {
              session_id: outer.sessionId,
            });
          });
        },
        { session_id: "outer_grandchild_reuse" },
      ),
      {
        kind: "session",
        executionState: "not_sent",
        message: "[ck-orm] Nested runInSession() cannot reuse an existing session_id",
      },
    );

    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("isolates sibling child sessions and cleans child temp tables before outer continues", async function testSiblingSessionIsolation() {
    const tempTablesBySession = new Map<string, Set<string>>();

    const getTempTables = (sessionId: string) => {
      let tables = tempTablesBySession.get(sessionId);
      if (!tables) {
        tables = new Set<string>();
        tempTablesBySession.set(sessionId, tables);
      }
      return tables;
    };

    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(typeof input === "string" || input instanceof URL ? String(input) : input.url);
      const sessionId = url.searchParams.get("session_id") ?? "";
      const bodyText = await readBodyText(init?.body);

      if (typeof bodyText !== "string") {
        return new Response(JSON.stringify({ data: [] }), { status: 200 });
      }

      const createMatch = bodyText.match(/^CREATE TEMPORARY TABLE `([^`]+)`/i);
      if (createMatch) {
        getTempTables(sessionId).add(createMatch[1]);
        return new Response("", { status: 200 });
      }

      const dropMatch = bodyText.match(/^DROP TABLE IF EXISTS `([^`]+)`/i);
      if (dropMatch) {
        getTempTables(sessionId).delete(dropMatch[1]);
        return new Response("", { status: 200 });
      }

      const tableMatch = bodyText.match(/from `([^`]+)`/i);
      if (tableMatch) {
        const tableName = tableMatch[1];
        if (!getTempTables(sessionId).has(tableName)) {
          return new Response(`Code: 60. DB::Exception: Table ${tableName} missing (UNKNOWN_TABLE)`, {
            status: 404,
          });
        }

        return new Response(JSON.stringify({ data: [{ id: 1 }] }), {
          status: 200,
        });
      }

      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    const outerScope = chTable("tmp_outer_scope", { id: int32() });
    const childOneScope = chTable("tmp_child_one_scope", { id: int32() });
    const childTwoScope = chTable("tmp_child_two_scope", { id: int32() });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(outerScope);
      expect(await session.execute(sql`select * from ${sql.identifier(outerScope.originalName)}`)).toEqual([{ id: 1 }]);

      await session.runInSession(async (childOne) => {
        await expectRejectsWithClickhouseError(
          childOne.execute(sql`select * from ${sql.identifier(outerScope.originalName)}`),
          {
            kind: "request_failed",
            executionState: "rejected",
          },
        );

        await childOne.createTemporaryTable(childOneScope);
        expect(await childOne.execute(sql`select * from ${sql.identifier(childOneScope.originalName)}`)).toEqual([
          { id: 1 },
        ]);
      });

      await expectRejectsWithClickhouseError(
        session.execute(sql`select * from ${sql.identifier(childOneScope.originalName)}`),
        {
          kind: "request_failed",
          executionState: "rejected",
        },
      );
      expect(await session.execute(sql`select * from ${sql.identifier(outerScope.originalName)}`)).toEqual([{ id: 1 }]);

      await session.runInSession(async (childTwo) => {
        await expectRejectsWithClickhouseError(
          childTwo.execute(sql`select * from ${sql.identifier(outerScope.originalName)}`),
          {
            kind: "request_failed",
            executionState: "rejected",
          },
        );
        await expectRejectsWithClickhouseError(
          childTwo.execute(sql`select * from ${sql.identifier(childOneScope.originalName)}`),
          {
            kind: "request_failed",
            executionState: "rejected",
          },
        );

        await childTwo.createTemporaryTable(childTwoScope);
        expect(await childTwo.execute(sql`select * from ${sql.identifier(childTwoScope.originalName)}`)).toEqual([
          { id: 1 },
        ]);
      });

      expect(await session.execute(sql`select * from ${sql.identifier(outerScope.originalName)}`)).toEqual([{ id: 1 }]);
    });
  });

  it("inherits settings into nested child sessions without mutating the parent session identity", async function testNestedWithSettingsInheritance() {
    const requests: Array<{ body: string; url: URL }> = [];

    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(typeof input === "string" || input instanceof URL ? String(input) : input.url);
      const bodyText = await readBodyText(init?.body);

      if (typeof bodyText === "string") {
        requests.push({ body: bodyText, url });
      }

      return new Response("", { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    let outerSessionId = "";
    let childSessionId = "";

    await db.runInSession(async (session) => {
      outerSessionId = session.sessionId;

      await session.withSettings({ max_threads: 2 }).runInSession(async (childSession) => {
        childSessionId = childSession.sessionId;
        await childSession.command("select 1");
      });

      await session.command("select 2");
    });

    expect(childSessionId).not.toBe("");
    expect(childSessionId).not.toBe(outerSessionId);

    const childRequest = requests.find(({ body }) => body === "select 1");
    expect(childRequest?.url.searchParams.get("session_id")).toBe(childSessionId);
    expect(childRequest?.url.searchParams.get("max_threads")).toBe("2");

    const outerRequest = requests.find(({ body }) => body === "select 2");
    expect(outerRequest?.url.searchParams.get("session_id")).toBe(outerSessionId);
    expect(outerRequest?.url.searchParams.get("max_threads")).toBeNull();
  });

  it("keeps raw streams quiet when ignore_error_response is enabled on an error response", async function testStreamIgnoreErrorResponse() {
    globalThis.fetch = mock(
      async () => new Response("Code: 60. DB::Exception: missing table (UNKNOWN_TABLE)", { status: 404 }),
    ) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    expect(
      await takeAsync(
        db.stream("select * from missing_table", {
          ignore_error_response: true,
        }),
        1,
      ),
    ).toEqual([]);
  });

  it("sets duplex=half when streaming async inserts in runtimes that support stream uploads", async function testAsyncInsertDuplexMode() {
    const duplexValues: Array<RequestInit["duplex"] | undefined> = [];

    class StreamUploadRequest {
      readonly headers: Headers;

      constructor(_input: string | URL | Request, init?: RequestInit & { duplex?: "half" }) {
        if (init?.body instanceof ReadableStream && init.duplex !== "half") {
          throw new TypeError("stream uploads require duplex=half in this runtime");
        }
        this.headers = new Headers(init?.headers);
        duplexValues.push(init?.duplex);
      }
    }

    globalThis.Request = StreamUploadRequest as unknown as typeof Request;
    globalThis.fetch = mock(async (_input: string | URL | Request, _init?: RequestInit) => {
      return new Response("", { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    await db.insertJsonEachRow(
      users,
      (async function* rows() {
        yield { id: 1, name: "alice" };
      })(),
      { query_id: "duplex_insert" },
    );

    expect(duplexValues).toContain("half");
  });

  it("allows createTemporaryTableRaw definitions with semicolons inside string literals", async function testCreateTemporaryTableLiteralSemicolon() {
    const bodies: string[] = [];

    globalThis.fetch = mock(async (_input: string | URL | Request, init?: RequestInit) => {
      const bodyText = await readBodyText(init?.body);
      if (typeof bodyText === "string") {
        bodies.push(bodyText);
      }
      return new Response("", { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTableRaw("tmp_scope", "(note String DEFAULT ';')");
    });

    expect(bodies).toContain("CREATE TEMPORARY TABLE `tmp_scope` (note String DEFAULT ';')");
    expect(bodies).toContain("DROP TABLE IF EXISTS `tmp_scope`");
  });

  it("rejects createTemporaryTableRaw definitions that contain inline semicolons", async function testCreateTemporaryTableRejectsSemicolons() {
    globalThis.fetch = (async () => new Response("", { status: 200 })) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    await expectRejectsWithClickhouseError(
      db.runInSession(async (session) => {
        await session.createTemporaryTableRaw("tmp_evil", "(id Int32); DROP TABLE users");
      }),
      {
        kind: "client_validation",
        executionState: "not_sent",
        message:
          "[ck-orm] createTemporaryTableRaw() definition must not contain multiple statements; use developer-controlled SQL only",
      },
    );
  });

  it("does not register invalid temp-table names for cleanup", async function testInvalidTempTableNameDoesNotPolluteCleanup() {
    const bodies: string[] = [];

    globalThis.fetch = mock(async (_input: string | URL | Request, init?: RequestInit) => {
      const bodyText = await readBodyText(init?.body);
      if (typeof bodyText === "string") {
        bodies.push(bodyText);
      }
      return new Response("", { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });

    await expectRejectsWithClickhouseError(
      db.runInSession(async (session) => {
        await session.createTemporaryTableRaw("evil`; DROP", "(id Int32)");
      }),
      {
        kind: "client_validation",
        executionState: "not_sent",
        message: "[ck-orm] Invalid SQL identifier: evil`; DROP",
      },
    );

    expect(bodies).toEqual([]);
  });

  it("aggregates temp-table cleanup errors into a session error when callback succeeded", async function testRunInSessionCleanupAggregation() {
    const bodies: string[] = [];

    globalThis.fetch = mock(async (_input: string | URL | Request, init?: RequestInit) => {
      const bodyText = await readBodyText(init?.body);
      if (typeof bodyText === "string") {
        bodies.push(bodyText);
        if (bodyText.startsWith("DROP TABLE IF EXISTS")) {
          throw new TypeError("network blew up");
        }
      }
      return new Response("", { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });
    const tmpA = chTable("tmp_a", { id: int32() });
    const tmpB = chTable("tmp_b", { id: int32() });

    await expectRejectsWithClickhouseError(
      db.runInSession(async (session) => {
        await session.createTemporaryTable(tmpA);
        await session.createTemporaryTable(tmpB);
      }),
      {
        kind: "session",
        executionState: "not_sent",
      },
    );

    expect(bodies).toContain("DROP TABLE IF EXISTS `tmp_a`");
    expect(bodies).toContain("DROP TABLE IF EXISTS `tmp_b`");
  });

  it("preserves the user error and routes cleanup errors to onCleanupError hook", async function testRunInSessionCleanupHookPreservesUserError() {
    globalThis.fetch = mock(async (_input: string | URL | Request, init?: RequestInit) => {
      const bodyText = await readBodyText(init?.body);
      if (typeof bodyText === "string" && bodyText.startsWith("DROP TABLE IF EXISTS")) {
        throw new TypeError("server gone");
      }
      return new Response("", { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
    });
    const tmpHook = chTable("tmp_hook", { id: int32() });

    const cleanupReports: Array<{ count: number; sessionId: string }> = [];
    const userError = new Error("user callback failed");

    await expect(
      db.runInSession(
        async (session) => {
          await session.createTemporaryTable(tmpHook);
          throw userError;
        },
        {
          onCleanupError: (errors, ctx) => {
            cleanupReports.push({ count: errors.length, sessionId: ctx.sessionId });
          },
        },
      ),
    ).rejects.toBe(userError);

    expect(cleanupReports).toHaveLength(1);
    expect(cleanupReports[0]?.count).toBe(1);
    expect(cleanupReports[0]?.sessionId).toMatch(/.+/);
  });

  it("does not leak abort listeners on the user-owned signal across success and timeout paths", async function testAbortListenerCleanup() {
    let addCount = 0;
    let removeCount = 0;
    const externalController = new AbortController();
    const originalAdd = externalController.signal.addEventListener.bind(externalController.signal);
    const originalRemove = externalController.signal.removeEventListener.bind(externalController.signal);
    externalController.signal.addEventListener = ((
      type: string,
      listener: EventListenerOrEventListenerObject,
      options?: boolean | AddEventListenerOptions,
    ) => {
      if (type === "abort") {
        addCount += 1;
      }
      return originalAdd(type, listener, options);
    }) as typeof externalController.signal.addEventListener;
    externalController.signal.removeEventListener = ((
      type: string,
      listener: EventListenerOrEventListenerObject,
      options?: boolean | EventListenerOptions,
    ) => {
      if (type === "abort") {
        removeCount += 1;
      }
      return originalRemove(type, listener, options);
    }) as typeof externalController.signal.removeEventListener;

    globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(_input as string);
      if (url.searchParams.get("query_id") === "leak_timeout") {
        return await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(init.signal?.reason ?? new Error("aborted")), {
            once: true,
          });
        });
      }
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    }) as unknown as typeof fetch;

    const db = clickhouseClient({
      databaseUrl: "http://localhost:8123/demo_store",
      schema: { users },
      request_timeout: 5,
    });

    // Success path: user-owned signal passed in, request resolves OK.
    await db.execute("select 1", { query_id: "leak_ok", abort_signal: externalController.signal });
    expect(addCount).toBe(1);
    expect(removeCount).toBe(1);

    // Timeout path: even if the caller's finalize() chain were skipped, the
    // listener must come off the external signal once the inner controller aborts.
    await expectRejectsWithClickhouseError(
      db.execute("select 1", { query_id: "leak_timeout", abort_signal: externalController.signal }),
      {
        kind: "timeout",
        executionState: "unknown",
        queryId: "leak_timeout",
      },
    );
    expect(addCount).toBe(2);
    expect(removeCount).toBe(2);
  });
});
