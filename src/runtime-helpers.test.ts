import { describe, expect, it } from "bun:test";
import { type ClickHouseORMError, isClickHouseORMError } from "./errors";
import { createAbortController } from "./runtime/abort";
import {
  assertValidQueryId,
  assertValidQueryParamKey,
  assertValidSessionId,
  buildSearchParams,
  createHeaders,
  formatQueryParamValue,
  mergeQueryParams,
  normalizeClientConfig,
  normalizeTransportSettings,
} from "./runtime/config";
import {
  createJsonEachRowBody,
  createLineStream,
  parseJsonEachRowLine,
  parseValidatedResponseJson,
  readValidatedResponseText,
} from "./runtime/json-stream";
import { normalizeQuery, normalizeSingleStatementSql } from "./runtime/sql-scan";

const json = {
  parse: (text: string) => JSON.parse(text) as unknown,
  stringify: (value: unknown) => JSON.stringify(value),
};

describe("ck-orm runtime/sql-scan", function describeSqlScan() {
  it("strips trailing semicolons across line, hash, and block comments", function testCommentForms() {
    expect(normalizeSingleStatementSql("select 1 -- trailing\n;", "x")).toBe("select 1 -- trailing");
    expect(normalizeSingleStatementSql("select 1 # hash trailing\n;", "x")).toBe("select 1 # hash trailing");
    expect(normalizeSingleStatementSql("select /* inline */ 1 ;", "x")).toBe("select /* inline */ 1");
    expect(normalizeSingleStatementSql("select /* multi\nline */ 1 ;", "x")).toBe("select /* multi\nline */ 1");
  });

  it("ignores semicolons embedded in single, double, and backtick quotes", function testQuotedSemicolons() {
    expect(normalizeSingleStatementSql("select ';' as raw", "x")).toBe("select ';' as raw");
    expect(normalizeSingleStatementSql('select ";" as raw', "x")).toBe('select ";" as raw');
    expect(normalizeSingleStatementSql("select `;` as raw", "x")).toBe("select `;` as raw");
  });

  it("handles escapes and double-quote/backtick doubling inside string literals", function testQuoteEscapes() {
    expect(normalizeSingleStatementSql("select 'a\\'b'", "x")).toBe("select 'a\\'b'");
    expect(normalizeSingleStatementSql("select 'a''b'", "x")).toBe("select 'a''b'");
    expect(normalizeSingleStatementSql('select "a\\"b"', "x")).toBe('select "a\\"b"');
    expect(normalizeSingleStatementSql('select "a""b"', "x")).toBe('select "a""b"');
    expect(normalizeSingleStatementSql("select `a``b`", "x")).toBe("select `a``b`");
  });

  it("rejects multi-statement input and reports the configured message", function testInlineSemicolonRejection() {
    expect(() => normalizeSingleStatementSql("select 1; select 2", "boom")).toThrow("boom");
    expect(() => normalizeSingleStatementSql("select 'a'; select 2", "boom")).toThrow("boom");
    expect(() => normalizeSingleStatementSql('select "a"; select 2', "boom")).toThrow("boom");
    expect(() => normalizeSingleStatementSql("select `a`; select 2", "boom")).toThrow("boom");
    // Quote characters appearing immediately after a top-level semicolon must also be flagged.
    expect(() => normalizeSingleStatementSql("select 1;'tail'", "boom")).toThrow("boom");
    expect(() => normalizeSingleStatementSql('select 1;"tail"', "boom")).toThrow("boom");
    expect(() => normalizeSingleStatementSql("select 1;`tail`", "boom")).toThrow("boom");
  });

  it("appends FORMAT suffix when provided", function testNormalizeQueryFormat() {
    expect(normalizeQuery("select 1;")).toBe("select 1");
    expect(normalizeQuery("select 1;", "JSON")).toBe("select 1\nFORMAT JSON");
  });

  it("ignores semicolons inside line comments that end at newline", function testLineCommentBoundary() {
    // Trailing semicolons after line comments are safely stripped
    expect(normalizeSingleStatementSql("select 1; -- foo\n;", "x")).toBe("select 1 -- foo");
    expect(normalizeSingleStatementSql("select 1 -- ; comment\n", "x")).toBe("select 1 -- ; comment");
  });

  it("ignores semicolons inside block comments", function testBlockCommentBoundary() {
    expect(normalizeSingleStatementSql("select /* ; */ 1;", "x")).toBe("select /* ; */ 1");
    expect(normalizeSingleStatementSql("select /* a;b */ 1;", "x")).toBe("select /* a;b */ 1");
  });

  it("ignores semicolons inside single-quote strings", function testStringLiteralBoundary() {
    expect(normalizeSingleStatementSql("select 'a;b' ;", "x")).toBe("select 'a;b'");
    expect(normalizeSingleStatementSql("select 'a--b' ;", "x")).toBe("select 'a--b'");
    expect(normalizeSingleStatementSql("select 'a/*b*/c' ;", "x")).toBe("select 'a/*b*/c'");
  });

  it("ignores semicolons inside double-quote identifiers", function testDoubleQuoteBoundary() {
    expect(normalizeSingleStatementSql('select "a;b" ;', "x")).toBe('select "a;b"');
  });

  it("ignores semicolons inside backtick identifiers", function testBacktickBoundary() {
    expect(normalizeSingleStatementSql("select `a;b` ;", "x")).toBe("select `a;b`");
  });

  it("rejects multiple statements with trailing comment", function testMultiStatementWithComment() {
    expect(() => normalizeSingleStatementSql("select 1; -- ok\nselect 2", "boom")).toThrow("boom");
  });

  it("handles empty and whitespace-only input", function testEmptyInput() {
    expect(normalizeSingleStatementSql("", "x")).toBe("");
    expect(normalizeSingleStatementSql("   ", "x")).toBe("");
  });

  it("handles escaped quotes inside strings", function testEscapedQuotes() {
    expect(normalizeSingleStatementSql("select 'a\\'b;c' ;", "x")).toBe("select 'a\\'b;c'");
    expect(normalizeSingleStatementSql('select "a\\"b;c" ;', "x")).toBe('select "a\\"b;c"');
  });

  it("treats ClickHouse heredoc bodies as string literal boundaries", function testHeredocBoundary() {
    expect(normalizeSingleStatementSql("select $ as dollar;", "x")).toBe("select $ as dollar");
    expect(normalizeSingleStatementSql("select $sql$select 1; select 2$sql$ as body;", "x")).toBe(
      "select $sql$select 1; select 2$sql$ as body",
    );
    expect(normalizeSingleStatementSql("select $tag_1$-- ;\n/* ; */$tag_1$;", "x")).toBe(
      "select $tag_1$-- ;\n/* ; */$tag_1$",
    );
    expect(() => normalizeSingleStatementSql("select $sql$;$sql$; select 2", "boom")).toThrow("boom");
    expect(() => normalizeSingleStatementSql("select 1; $sql$select 2$sql$", "boom")).toThrow("boom");
  });
});

describe("ck-orm runtime/abort", function describeAbort() {
  it("fires synchronously when the external signal is already aborted with an Error reason", function testPreAbortedError() {
    const ext = new AbortController();
    const reason = new Error("user cancelled");
    ext.abort(reason);
    const { signal, cleanup } = createAbortController(60_000, ext.signal);
    expect(signal.aborted).toBe(true);
    expect(isClickHouseORMError(signal.reason)).toBe(true);
    expect((signal.reason as ClickHouseORMError).cause).toBe(reason);
    cleanup();
  });

  it("wraps non-Error abort reasons via String(reason)", function testNonErrorReason() {
    const ext = new AbortController();
    ext.abort("string-reason");
    const { signal, cleanup } = createAbortController(60_000, ext.signal);
    expect(signal.aborted).toBe(true);
    expect(String((signal.reason as Error).message)).toContain("string-reason");
    cleanup();
  });

  it("falls back to a default abort error when reason is undefined", function testUndefinedReason() {
    const ext = new AbortController();
    // AbortController.abort() with no argument leaves reason as a default DOMException;
    // simulate truly-undefined reason by overriding the property.
    Object.defineProperty(ext.signal, "reason", { value: undefined, configurable: true });
    Object.defineProperty(ext.signal, "aborted", { value: true, configurable: true });
    const { signal, cleanup } = createAbortController(60_000, ext.signal);
    expect(signal.aborted).toBe(true);
    expect(isClickHouseORMError(signal.reason)).toBe(true);
    cleanup();
  });

  it("cleanup() is idempotent", function testIdempotentCleanup() {
    const { cleanup } = createAbortController(60_000);
    cleanup();
    cleanup();
  });
});

const makeResponse = (body: string, init?: ResponseInit) => new Response(body, init);

const expectRejectsWithClickHouseORMError = async (promise: Promise<unknown>) => {
  try {
    await promise;
    throw new Error("Expected promise to reject with ClickHouseORMError");
  } catch (error) {
    expect(isClickHouseORMError(error)).toBe(true);
  }
};

describe("ck-orm runtime/json-stream", function describeJsonStream() {
  it("returns text immediately when ignoreErrorResponse is true", async function testIgnoreError() {
    const text = await readValidatedResponseText({
      response: makeResponse("ignored", { status: 500 }),
      queryId: "q",
      ignoreErrorResponse: true,
    });
    expect(text).toBe("ignored");
  });

  it("normalises transport errors when reading the response body fails", async function testTransportError() {
    const broken = new Response("ok");
    Object.defineProperty(broken, "text", {
      value: () => Promise.reject(new TypeError("network down")),
    });
    await expectRejectsWithClickHouseORMError(
      readValidatedResponseText({
        response: broken,
        queryId: "q",
        ignoreErrorResponse: false,
      }),
    );
  });

  it("wraps malformed JSON in parseValidatedResponseJson as a request_failed error", async function testJsonParseFailure() {
    await expectRejectsWithClickHouseORMError(
      parseValidatedResponseJson({
        response: makeResponse("not-json", { status: 200 }),
        queryId: "q",
        json,
        ignoreErrorResponse: false,
      }),
    );
  });

  it("rejects ClickHouse exception lines and malformed lines in parseJsonEachRowLine", function testParseLineFailures() {
    const response = makeResponse("", { status: 200 });
    const exceptionLine = "Code: 60. DB::Exception: missing table";
    expect(() =>
      parseJsonEachRowLine({
        line: exceptionLine,
        response,
        queryId: "q",
        json,
      }),
    ).toThrow();
    expect(() =>
      parseJsonEachRowLine({
        line: "not-json",
        response,
        queryId: "q",
        json,
      }),
    ).toThrow();
    expect(
      parseJsonEachRowLine({
        line: '{"id":1}',
        response,
        queryId: "q",
        json,
      }),
    ).toEqual({ id: 1 });
  });

  it("createLineStream returns immediately when body is null and yields buffered tail line", async function testLineStream() {
    const empty = makeResponse("");
    Object.defineProperty(empty, "body", { value: null, configurable: true });
    const lines: string[] = [];
    for await (const line of createLineStream(empty)) {
      lines.push(line);
    }
    expect(lines).toEqual([]);

    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("first\nsecond"));
        controller.close();
      },
    });
    const response = new Response(stream);
    const collected: string[] = [];
    for await (const line of createLineStream(response)) {
      collected.push(line);
    }
    expect(collected).toEqual(["first", "second"]);
  });

  it("creates stable JSONEachRow request bodies for empty arrays and empty async iterables", async function testEmptyJsonEachRowBodies() {
    expect(createJsonEachRowBody([], json)).toEqual({ body: "" });

    const result = await createJsonEachRowBody(
      (async function* rows() {
        // empty by design
      })(),
      json,
    );
    if (typeof result.body === "string") {
      expect(result.body).toBe("");
    } else {
      expect(await new Response(result.body).text()).toBe("");
    }
  });

  it("buffers async row iterables when stream uploads are unsupported", async function testBufferedAsyncBody() {
    async function* rows() {
      yield { id: 1 };
      yield { id: 2 };
    }
    const result = await createJsonEachRowBody(rows(), json);
    if (typeof result.body === "string") {
      // platform does not support stream uploads – buffered path
      expect(result.body).toContain('{"id":1}');
      expect(result.body).toContain('{"id":2}');
    } else {
      // streaming path – consume to verify both rows are emitted
      const reader = result.body.getReader();
      const decoder = new TextDecoder();
      let collected = "";
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        collected += decoder.decode(chunk.value);
      }
      expect(collected).toContain('{"id":1}');
      expect(collected).toContain('{"id":2}');
    }
  });

  it("streams async iterables and propagates iterator errors through controller.error", async function testStreamingAsyncBody() {
    const original = globalThis.Request;
    let abortReached = false;
    async function* rows() {
      yield { id: 1 };
      throw new Error("iter blew up");
    }
    // Force "duplex-half" path by stubbing the platform body-mode probe via Request capability check.
    // resolveStreamRequestBodyMode returns "duplex-half" when Request supports it; in Bun it does.
    const result = createJsonEachRowBody(rows(), json) as { body: ReadableStream<Uint8Array>; duplex?: "half" };
    expect(result.body).toBeInstanceOf(ReadableStream);

    const reader = result.body.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    await expect(reader.read()).rejects.toBeInstanceOf(Error);
    abortReached = true;
    expect(abortReached).toBe(true);
    globalThis.Request = original;
  });

  it("cancels the stream body and closes the iterator without throwing", async function testStreamCancel() {
    let returnCalled = 0;
    const iter: AsyncIterableIterator<{ id: number }> = {
      [Symbol.asyncIterator]() {
        return this;
      },
      async next() {
        return { value: { id: 1 }, done: false };
      },
      async return() {
        returnCalled += 1;
        return { value: undefined, done: true };
      },
    };
    const result = createJsonEachRowBody(iter, json) as { body: ReadableStream<Uint8Array> };
    if (!(result.body instanceof ReadableStream)) {
      // Buffered fallback path – already covered above.
      return;
    }
    await result.body.cancel();
    // Cancelling again must be a no-op (closeIterator early-return branch).
    await result.body.cancel().catch(() => undefined);
    expect(returnCalled).toBe(1);
  });

  it("invokes pull twice in succession through the reader contract", async function testSequentialPulls() {
    let nextCalls = 0;
    const iter: AsyncIterableIterator<{ id: number }> = {
      [Symbol.asyncIterator]() {
        return this;
      },
      async next() {
        nextCalls += 1;
        if (nextCalls > 2) {
          return { value: undefined, done: true };
        }
        return { value: { id: nextCalls }, done: false };
      },
      async return() {
        return { value: undefined, done: true };
      },
    };
    const result = createJsonEachRowBody(iter, json) as { body: ReadableStream<Uint8Array> };
    if (!(result.body instanceof ReadableStream)) {
      return;
    }
    const reader = result.body.getReader();
    const decoder = new TextDecoder();
    let collected = "";
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      collected += decoder.decode(chunk.value);
    }
    expect(collected).toContain('{"id":1}');
    expect(collected).toContain('{"id":2}');
  });

  it("cancels after iterator error to exercise the closed-already branch in closeIterator", async function testCancelAfterError() {
    const iter: AsyncIterableIterator<{ id: number }> = {
      [Symbol.asyncIterator]() {
        return this;
      },
      async next() {
        throw new Error("iter blew up");
      },
      async return() {
        return { value: undefined, done: true };
      },
    };
    const result = createJsonEachRowBody(iter, json) as { body: ReadableStream<Uint8Array> };
    if (!(result.body instanceof ReadableStream)) {
      return;
    }
    const reader = result.body.getReader();
    await expect(reader.read()).rejects.toBeInstanceOf(Error);
    // After the error path closes the iterator, an explicit cancel must be a safe no-op.
    await reader.cancel().catch(() => undefined);
  });
});

describe("ck-orm runtime/config validation", function describeConfigValidation() {
  it("accepts valid query_id formats", function testValidQueryId() {
    expect(() => assertValidQueryId("abc123")).not.toThrow();
    expect(() => assertValidQueryId("query-id_123")).not.toThrow();
    expect(() => assertValidQueryId("a")).not.toThrow();
    expect(() => assertValidQueryId("x".repeat(100))).not.toThrow();
  });

  it("rejects invalid query_id formats", function testInvalidQueryId() {
    expect(() => assertValidQueryId("")).toThrow();
    expect(() => assertValidQueryId("x".repeat(101))).toThrow();
    expect(() => assertValidQueryId("query id")).toThrow();
    expect(() => assertValidQueryId("query@id")).toThrow();
    expect(() => assertValidQueryId("query:id")).toThrow();
    expect(() => assertValidQueryId("query/id")).toThrow();
  });

  it("accepts valid session_id formats", function testValidSessionId() {
    expect(() => assertValidSessionId("session_1")).not.toThrow();
    expect(() => assertValidSessionId("s-123")).not.toThrow();
    expect(() => assertValidSessionId("a".repeat(100))).not.toThrow();
  });

  it("rejects invalid session_id formats", function testInvalidSessionId() {
    expect(() => assertValidSessionId("")).toThrow();
    expect(() => assertValidSessionId("a".repeat(101))).toThrow();
    expect(() => assertValidSessionId("session id")).toThrow();
    expect(() => assertValidSessionId("session@id")).toThrow();
  });

  it("rejects overly long query parameter keys", function testLongQueryParamKey() {
    const key = "x".repeat(101);
    expect(() => assertValidQueryParamKey(key)).toThrow();
  });

  it("formats special query params and search params for transport helpers", function testQueryParamFormatting() {
    const wholeSecond = new Date("2026-04-21T00:00:00.000Z");
    const fractionalSecond = new Date("2026-04-21T00:00:00.123Z");
    const expectedWholeSecond = Math.floor(wholeSecond.getTime() / 1000)
      .toString()
      .padStart(10, "0");

    expect(mergeQueryParams(undefined, undefined)).toEqual({});
    expect(formatQueryParamValue(Number.NaN)).toBe("nan");
    expect(formatQueryParamValue(Number.POSITIVE_INFINITY)).toBe("+inf");
    expect(formatQueryParamValue(Number.NEGATIVE_INFINITY)).toBe("-inf");
    expect(formatQueryParamValue(true)).toBe("1");
    expect(formatQueryParamValue(false, { nested: true })).toBe("FALSE");
    expect(formatQueryParamValue(wholeSecond)).toBe(expectedWholeSecond);
    expect(formatQueryParamValue(fractionalSecond)).toBe(`${expectedWholeSecond}.123`);
    expect(formatQueryParamValue(["vip", null, false])).toBe("['vip',NULL,FALSE]");
    expect(formatQueryParamValue({ enabled: true, disabled: false })).toBe("{'enabled':TRUE,'disabled':FALSE}");
    expect(formatQueryParamValue([], { nested: true })).toBe("[]");
    expect(formatQueryParamValue(new Map())).toBe("{}");
    expect(
      formatQueryParamValue({
        nested: {
          quoted: "it's \\ complicated",
          unicode: "line\u2028paragraph\u2029end",
        },
        flags: [true, false, null],
      }),
    ).toBe(
      "{'nested':{'quoted':'it\\'s \\\\ complicated','unicode':'line\u2028paragraph\u2029end'},'flags':[TRUE,FALSE,NULL]}",
    );
    expect(formatQueryParamValue(new Map([["scores", new Map([["gold", 1]])]]))).toBe("{'scores':{'gold':1}}");

    const searchParams = buildSearchParams({
      query_id: "query_1",
      database: "demo_store",
      query: "select 1",
      session_id: "session_1",
      session_timeout: 30,
      session_check: 1,
      clickhouse_settings: {
        allow_experimental_correlated_subqueries: 1,
        future_clickhouse_setting: true,
        join_use_nulls: true,
        max_execution_time: 10,
      },
      role: ["analyst", "auditor"],
    });

    expect(searchParams.get("allow_experimental_correlated_subqueries")).toBe("1");
    expect(searchParams.get("future_clickhouse_setting")).toBe("1");
    expect(searchParams.get("join_use_nulls")).toBe("1");
    expect(searchParams.get("max_execution_time")).toBe("10");
    expect(searchParams.get("query_id")).toBe("query_1");
    expect(searchParams.get("database")).toBe("demo_store");
    expect(searchParams.get("query")).toBe("select 1");
    expect(searchParams.get("session_id")).toBe("session_1");
    expect(searchParams.get("session_timeout")).toBe("30");
    expect(searchParams.get("session_check")).toBe("1");
    expect(searchParams.getAll("role")).toEqual(["analyst", "auditor"]);

    for (const key of ["query", "database", "session_id", "role", "param_orm_param1"]) {
      expect(() =>
        buildSearchParams({
          query_id: "query_1",
          clickhouse_settings: {
            [key]: 1,
          },
        }),
      ).toThrow("conflicts with a reserved ClickHouse HTTP parameter");
    }
  });

  it("covers config guards, authless header creation and async insert normalization", function testRuntimeConfigBranches() {
    const originalRequest = globalThis.Request;

    class PlainRequest {
      readonly headers: Headers;

      constructor(_input: string | URL | Request, init?: RequestInit) {
        this.headers = new Headers(init?.headers);
      }
    }

    Object.defineProperty(globalThis, "Request", {
      configurable: true,
      writable: true,
      value: PlainRequest as unknown as typeof Request,
    });

    try {
      expect(() =>
        normalizeClientConfig({
          host: "http://demo-user:demo-pass@localhost:8123",
        }),
      ).toThrow(
        "Structured connection config does not accept credentials in host; use username/password or databaseUrl instead",
      );

      expect(() =>
        normalizeClientConfig({
          host: "http://localhost:8123",
          log: {
            level: "debug",
          },
        } as Record<string, unknown> as Parameters<typeof normalizeClientConfig>[0]),
      ).toThrow("clickhouseClient() does not accept native createClient({ log }) config");

      expect(() =>
        normalizeClientConfig({
          host: "http://localhost:8123",
          json: {
            parse: JSON.parse,
            stringify: JSON.stringify,
          },
        } as Record<string, unknown> as Parameters<typeof normalizeClientConfig>[0]),
      ).toThrow("clickhouseClient() no longer accepts json hooks");

      expect(() =>
        normalizeClientConfig({
          host: "http://localhost:8123",
          session_timeout: 30,
        } as Record<string, unknown> as Parameters<typeof normalizeClientConfig>[0]),
      ).toThrow("clickhouseClient() no longer accepts session_timeout");

      expect(() =>
        normalizeClientConfig({
          host: "http://localhost:8123",
          session_check: 1,
        } as Record<string, unknown> as Parameters<typeof normalizeClientConfig>[0]),
      ).toThrow("clickhouseClient() no longer accepts session_check");

      expect(() =>
        normalizeClientConfig({
          host: "http://localhost:8123",
          compression: {
            response: true,
          },
          clickhouse_settings: {
            enable_http_compression: 0,
          },
        }),
      ).toThrow("compression.response and clickhouse_settings.enable_http_compression must agree");

      expect(() =>
        normalizeTransportSettings({
          settings: {
            output_format_json_quote_64bit_integers: 0,
          },
          parseMode: "json",
        }),
      ).toThrow("ck-orm requires output_format_json_quote_64bit_integers=1 for lossless 64-bit integer decoding");

      const headers = createHeaders({
        config: normalizeClientConfig({
          host: "http://localhost:8123",
          application: "demo-app",
          compression: {
            response: true,
          },
          http_headers: {
            "x-default": "1",
          },
        }),
        options: {
          http_headers: {
            "x-extra": "2",
          },
        },
        auth: undefined,
      });

      expect(headers.get("Authorization")).toBeNull();
      expect(headers.get("User-Agent")).toBe("demo-app ck-orm");
      expect(headers.get("Accept-Encoding")).toBe("gzip");
      expect(headers.get("x-default")).toBe("1");
      expect(headers.get("x-extra")).toBe("2");

      expect(
        normalizeTransportSettings({
          settings: {
            async_insert: 1,
          },
          parseMode: "json",
        }),
      ).toEqual({
        async_insert: 1,
        http_write_exception_in_output_format: 0,
        output_format_json_quote_64bit_integers: 1,
        wait_end_of_query: 1,
        wait_for_async_insert: 1,
      });
    } finally {
      if (originalRequest === undefined) {
        Reflect.deleteProperty(globalThis, "Request");
      } else {
        Object.defineProperty(globalThis, "Request", {
          configurable: true,
          writable: true,
          value: originalRequest,
        });
      }
    }
  });
});
