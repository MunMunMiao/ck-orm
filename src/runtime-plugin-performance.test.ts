import { describe, expect, it, spyOn } from "bun:test";
import { join } from "node:path";
import type {
  ClickHouseORMPlugin,
  ClickHouseORMQueryErrorEvent,
  ClickHouseORMQueryEvent,
  ClickHouseORMQueryResultEvent,
} from "./observability";
import { ckSql as sql } from "./public_api";
import {
  capturedLogger,
  controlledSql,
  memoryClient,
  observeCost,
  queryOptions,
  statementOfSize,
  streamResponse,
  table,
  zeroCost,
} from "./runtime-plugin-performance-support.test";

const fixtureRoot = join(import.meta.dir, "runtime-plugin-performance-fixture.test.ts");
const restrictedEnv = { PATH: process.env.PATH ?? "", NO_COLOR: "1" };

const runPublicScenario = async (observed: boolean) => {
  const child = Bun.spawn(
    [
      process.execPath,
      "--no-env-file",
      "--no-install",
      fixtureRoot,
      "--ck-orm-fixture",
      observed ? "--observed" : "--unobserved",
    ],
    {
      stdout: "pipe",
      stderr: "pipe",
      stdin: "ignore",
      env: restrictedEnv,
    },
  );
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    child.kill("SIGKILL");
  }, 10_000);
  try {
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);
    expect(timedOut).toBe(false);
    expect({ exitCode, stderr }).toEqual({ exitCode: 0, stderr: "" });
    const result = JSON.parse(stdout) as {
      id: string;
      layer: string;
      completedOperations: number;
      peakActive: number;
      equivalent: boolean;
      observed: boolean;
    };
    expect(result).toMatchObject({
      id: "PERF-006",
      layer: "full-sdk-subprocess",
      completedOperations: 5,
      peakActive: 1,
      equivalent: true,
      observed,
    });
  } finally {
    clearTimeout(timer);
    child.kill();
  }
};

describe("PERF-006 empty-plugin observation cost", () => {
  for (const bytes of [64, 4096, 65_536]) {
    it(`PERF-006 RED: no consumers allocate or hash unconsumed ${bytes}-byte SQL`, async () => {
      const db = memoryClient({ plugins: [], logger: false });
      const query = controlledSql(statementOfSize(bytes));
      const { value, cost } = await observeCost(() => db.execute(query, queryOptions));
      expect(value).toEqual([{ n: 1 }, { n: 2 }]);
      expect(cost).toEqual(zeroCost);
    });
  }

  const paths = {
    command: async () => {
      await memoryClient().command(sql`SELECT 1 /* audit_perf006 */`, queryOptions);
    },
    stream: async () => {
      expect(await Array.fromAsync(memoryClient().stream(sql`SELECT n /* audit_perf006 */`, queryOptions))).toEqual([
        { n: 1 },
        { n: 2 },
      ]);
    },
    typed: async () => {
      expect(await memoryClient().select().from(table).execute(queryOptions)).toEqual([{ n: 1 }, { n: 2 }]);
    },
    iterator: async () => {
      expect(await Array.fromAsync(memoryClient().select().from(table).iterator(queryOptions))).toEqual([
        { n: 1 },
        { n: 2 },
      ]);
    },
    insert: async () => {
      await memoryClient().insertJsonEachRow(table, [{ n: 1 }], queryOptions);
    },
    ping: async () => {
      await memoryClient().ping(queryOptions);
    },
    replicasStatus: async () => {
      await memoryClient().replicasStatus(queryOptions);
    },
  };
  for (const [name, operation] of Object.entries(paths)) {
    it(`PERF-006 RED: ${name} does not build unconsumed observation metadata`, async () => {
      const { cost } = await observeCost(operation);
      expect(cost).toEqual(zeroCost);
    });
  }

  it("PERF-006 RED: failed requests preserve errors without allocating unconsumed events", async () => {
    const db = memoryClient();
    const { value, cost } = await observeCost(async () => {
      try {
        await db.execute(sql`SELECT audit_perf006_fail`, queryOptions);
      } catch (error) {
        return error;
      }
      throw new Error("Expected synthetic transport failure");
    });
    expect(value).toMatchObject({ kind: "request_failed", httpStatus: 500, queryId: queryOptions.query_id });
    expect(cost).toEqual(zeroCost);
  });

  it("PERF-006 RED: full SDK session does not allocate or hash unconsumed observation events", async () => {
    await runPublicScenario(false);
  }, 15_000);

  it("PERF-006 CONTROL: full SDK observed session retains events, identity, row counts and serialization", async () => {
    await runPublicScenario(true);
  }, 15_000);

  it("PERF-006 CONTROL: start order, reverse completion, headers, identity and statistics survive", async () => {
    const order: string[] = [];
    const starts: ClickHouseORMQueryEvent[] = [];
    const ends: ClickHouseORMQueryResultEvent[] = [];
    let sentHeaders: Headers | undefined;
    const plugin = (name: string): ClickHouseORMPlugin => ({
      name,
      async onQueryStart(event) {
        order.push(`start:${name}`);
        starts.push(event);
        return { http_headers: { "x-audit-hook": name } };
      },
      async onQuerySuccess(event) {
        order.push(`success:${name}`);
        ends.push(event);
      },
    });
    const db = memoryClient({
      plugins: [plugin("a"), plugin("b")],
      fetch: async (request) => {
        sentHeaders = request.headers;
        order.push("request");
        expect(await request.text()).toContain("`audit_perf006_rows`");
        return new Response(
          '{"data":[{"n":1}],"rows":1,"rows_before_limit_at_least":4,"statistics":{"elapsed":0.002,"rows_read":4,"bytes_read":32}}',
        );
      },
    });
    const before = Date.now();
    const { value, cost } = await observeCost(() => db.select().from(table).execute(queryOptions));
    expect(value).toEqual([{ n: 1 }]);
    expect(order).toEqual(["start:a", "start:b", "request", "success:b", "success:a"]);
    expect(sentHeaders?.get("x-audit-hook")).toBe("b");
    expect(starts[0]).toBe(starts[1]);
    expect(ends[0]).toBe(ends[1]);
    expect(ends[0]?.executionId).toBe(starts[0]?.executionId);
    expect(ends[0]?.statementHash).toBe(starts[0]?.statementHash);
    expect(starts[0]?.statementHash).toMatch(/^[0-9a-f]{8}$/);
    expect(cost.sqlEncodeCalls).toBe(1);
    expect(cost.randomUuids).toBe(1);
    expect(ends[0]).toMatchObject({
      mode: "query",
      queryKind: "typed",
      operation: "SELECT",
      queryId: queryOptions.query_id,
      tableName: "audit_perf006_rows",
      querySummary: "SELECT audit_perf006_rows",
      rowCount: 1,
      serverElapsedMs: 2,
      readRows: 4,
      readBytes: 32,
      resultRows: 1,
      rowsBeforeLimitAtLeast: 4,
    });
    expect(ends[0]?.startedAt).toBeGreaterThanOrEqual(before);
    expect(ends[0]?.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("PERF-006 CONTROL: sync and async hook failures do not replace operation outcomes", async () => {
    const calls: string[] = [];
    const errors: ClickHouseORMQueryErrorEvent[] = [];
    const consoleError = spyOn(console, "error").mockImplementation(() => undefined);
    const db = memoryClient({
      plugins: [
        {
          name: "sync",
          onQueryStart() {
            calls.push("start:sync");
            throw new Error("synthetic sync hook");
          },
          onQuerySuccess() {
            calls.push("success:sync");
            throw new Error("synthetic sync hook");
          },
          onQueryError() {
            calls.push("error:sync");
            throw new Error("synthetic sync hook");
          },
        },
        {
          name: "async",
          async onQueryStart() {
            calls.push("start:async");
            throw new Error("synthetic async hook");
          },
          async onQuerySuccess() {
            calls.push("success:async");
            throw new Error("synthetic async hook");
          },
          async onQueryError(event) {
            calls.push("error:async");
            errors.push(event);
            throw new Error("synthetic async hook");
          },
        },
      ],
    });
    try {
      expect(await db.execute(sql`SELECT 1 /* audit_perf006 */`, queryOptions)).toEqual([{ n: 1 }, { n: 2 }]);
      let rejected: unknown;
      try {
        await db.execute(sql`SELECT audit_perf006_fail`, queryOptions);
      } catch (error) {
        rejected = error;
      }
      expect(rejected).toMatchObject({ kind: "request_failed", httpStatus: 500 });
      expect(errors[0]?.error).toBe(rejected);
      expect(errors[0]?.partialRowCount).toBeUndefined();
      expect(calls).toEqual([
        "start:sync",
        "start:async",
        "success:async",
        "success:sync",
        "start:sync",
        "start:async",
        "error:async",
        "error:sync",
      ]);
      expect(consoleError.mock.calls).toHaveLength(8);
    } finally {
      consoleError.mockRestore();
    }
  });

  for (const mode of ["none", "start", "success", "error", "empty"] as const) {
    it(`PERF-006 CONTROL: partial hook ${mode} preserves success and error behavior`, async () => {
      const starts: ClickHouseORMQueryEvent[] = [];
      const ends: ClickHouseORMQueryResultEvent[] = [];
      const errors: ClickHouseORMQueryErrorEvent[] = [];
      const plugin: ClickHouseORMPlugin = { name: mode };
      if (mode === "start") {
        plugin.onQueryStart = (event) => {
          starts.push(event);
        };
      }
      if (mode === "success") {
        plugin.onQuerySuccess = (event) => {
          ends.push(event);
        };
      }
      if (mode === "error") {
        plugin.onQueryError = (event) => {
          errors.push(event);
        };
      }
      const db = memoryClient({ plugins: mode === "none" ? [] : [plugin] });
      const before = Date.now();
      expect(await db.execute(sql`SELECT 1 /* audit_perf006 */`, queryOptions)).toEqual([{ n: 1 }, { n: 2 }]);
      await expect(db.execute(sql`SELECT audit_perf006_fail`, queryOptions)).rejects.toMatchObject({
        kind: "request_failed",
        httpStatus: 500,
      });
      expect(starts).toHaveLength(mode === "start" ? 2 : 0);
      expect(ends).toHaveLength(mode === "success" ? 1 : 0);
      expect(errors).toHaveLength(mode === "error" ? 1 : 0);
      for (const event of [...starts, ...ends, ...errors]) {
        expect(event.executionId).toMatch(/^[0-9a-f-]{36}$/);
        expect(event.startedAt).toBeGreaterThanOrEqual(before);
        expect(event.statementHash).toMatch(/^[0-9a-f]{8}$/);
      }
    });
  }

  for (const pluginsEnabled of [false, true]) {
    it(`PERF-006 CONTROL: early stream return closes its reader and releases session, plugins=${pluginsEnabled}`, async () => {
      let cancelled = 0;
      let requestCount = 0;
      const successes: ClickHouseORMQueryResultEvent[] = [];
      const requests: URL[] = [];
      const bytes = new TextEncoder().encode(streamResponse);
      const db = memoryClient({
        plugins: pluginsEnabled
          ? [
              {
                name: "capture",
                onQuerySuccess: (event) => {
                  successes.push(event);
                },
              },
            ]
          : [],
        fetch: async (request) => {
          const url = new URL(request.url);
          requests.push(url);
          const text = await request.text();
          if (text.includes("FORMAT JSONEachRow")) {
            requestCount += 1;
            return new Response(
              new ReadableStream<Uint8Array>({
                start(controller) {
                  controller.enqueue(bytes);
                },
                cancel() {
                  cancelled += 1;
                },
              }),
            );
          }
          return new Response("");
        },
      });
      await db.runInSession(
        async (session) => {
          for await (const row of session.stream(sql`SELECT n /* audit_perf006 */`, queryOptions)) {
            expect(row).toEqual({ n: 1 });
            break;
          }
          await session.command(sql`SELECT 1 /* audit_perf006 */`, { query_id: "audit_perf006_after_stream" });
        },
        { session_id: "audit_perf006_session" },
      );
      expect(cancelled).toBe(1);
      expect(requestCount).toBe(1);
      expect(requests.at(-1)?.searchParams.get("query_id")).toBe("audit_perf006_after_stream");
      expect(requests.at(-1)?.searchParams.get("session_id")).toBe("audit_perf006_session");
      if (pluginsEnabled) {
        expect(successes[0]).toMatchObject({ mode: "stream", rowCount: 1, sessionId: "audit_perf006_session" });
        expect(successes).toHaveLength(2);
      } else {
        expect(successes).toHaveLength(0);
      }
    });

    it(`PERF-006 CONTROL: stream failure preserves partial count and original error, plugins=${pluginsEnabled}`, async () => {
      const events: ClickHouseORMQueryErrorEvent[] = [];
      const db = memoryClient({
        plugins: pluginsEnabled
          ? [
              {
                name: "capture",
                onQueryError: (event) => {
                  events.push(event);
                },
              },
            ]
          : [],
        fetch: () => new Response('{"n":1}\nnot-json\n'),
      });
      const seen: unknown[] = [];
      let rejection: unknown;
      try {
        for await (const row of db.stream(sql`SELECT n /* audit_perf006 */`, queryOptions)) {
          seen.push(row);
        }
      } catch (error) {
        rejection = error;
      }
      expect(seen).toEqual([{ n: 1 }]);
      expect(rejection).toBeDefined();
      if (pluginsEnabled) {
        expect(events).toHaveLength(1);
        expect(events[0]).toMatchObject({
          mode: "stream",
          partialRowCount: 1,
          rowCount: 1,
          queryId: queryOptions.query_id,
        });
        expect(events[0]?.error).toBe(rejection);
      }
    });

    it(`PERF-006 CONTROL: abort preserves rejection and session remains usable, plugins=${pluginsEnabled}`, async () => {
      const controller = new AbortController();
      const events: ClickHouseORMQueryErrorEvent[] = [];
      const db = memoryClient({
        plugins: pluginsEnabled
          ? [
              {
                name: "capture",
                onQueryError: (event) => {
                  events.push(event);
                },
              },
            ]
          : [],
        fetch: async (request) => {
          if (new URL(request.url).searchParams.get("query_id") === queryOptions.query_id) {
            controller.abort(new DOMException("synthetic cancellation", "AbortError"));
            request.signal.throwIfAborted();
          }
          return new Response("");
        },
      });
      await db.runInSession(
        async (session) => {
          await expect(
            session.execute(sql`SELECT 1 /* audit_perf006 */`, { ...queryOptions, abort_signal: controller.signal }),
          ).rejects.toMatchObject({ kind: "aborted" });
          await session.command(sql`SELECT 1 /* audit_perf006 */`, { query_id: "audit_perf006_after_abort" });
        },
        { session_id: "audit_perf006_abort_session" },
      );
      expect(events).toHaveLength(pluginsEnabled ? 1 : 0);
    });

    it(`PERF-006 CONTROL: consumer-injected stream error preserves identity, plugins=${pluginsEnabled}`, async () => {
      const errors: ClickHouseORMQueryErrorEvent[] = [];
      let cancelled = 0;
      const bytes = new TextEncoder().encode(streamResponse);
      const db = memoryClient({
        plugins: pluginsEnabled
          ? [
              {
                name: "capture",
                onQueryError(event) {
                  errors.push(event);
                },
              },
            ]
          : [],
        fetch: () =>
          new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(bytes);
              },
              cancel() {
                cancelled += 1;
              },
            }),
          ),
      });
      const iterator = db.stream(sql`SELECT n /* audit_perf006 */`, queryOptions);
      expect(await iterator.next()).toEqual({ value: { n: 1 }, done: false });
      const sentinel = new Error("synthetic consumer-injected stream error");
      await expect(iterator.throw(sentinel)).rejects.toBe(sentinel);
      expect(cancelled).toBe(1);
      if (pluginsEnabled) {
        expect(errors[0]?.error).toBe(sentinel);
      }
    });

    it(`PERF-006 CONTROL: partial stream abort releases the session, plugins=${pluginsEnabled}`, async () => {
      const controller = new AbortController();
      const events: ClickHouseORMQueryErrorEvent[] = [];
      const bytes = new TextEncoder().encode('{"n":1}\n');
      const db = memoryClient({
        plugins: pluginsEnabled
          ? [
              {
                name: "capture",
                onQueryError(event) {
                  events.push(event);
                },
              },
            ]
          : [],
        fetch: async (request) => {
          const text = await request.text();
          if (!text.endsWith("FORMAT JSONEachRow")) {
            return new Response("");
          }
          return new Response(
            new ReadableStream<Uint8Array>({
              start(stream) {
                stream.enqueue(bytes);
                request.signal.addEventListener("abort", () => stream.error(request.signal.reason), { once: true });
              },
            }),
          );
        },
      });
      await db.runInSession(
        async (session) => {
          const iterator = session.stream(sql`SELECT n /* audit_perf006 */`, {
            ...queryOptions,
            abort_signal: controller.signal,
          });
          expect(await iterator.next()).toEqual({ value: { n: 1 }, done: false });
          controller.abort(new DOMException("synthetic stream abort", "AbortError"));
          await expect(iterator.next()).rejects.toMatchObject({ kind: "aborted" });
          await session.command(sql`SELECT 1 /* audit_perf006 */`, { query_id: "audit_perf006_after_partial_abort" });
        },
        { session_id: "audit_perf006_partial_abort" },
      );
      expect(events).toHaveLength(pluginsEnabled ? 1 : 0);
      if (pluginsEnabled) {
        expect(events[0]).toMatchObject({ partialRowCount: 1, rowCount: 1 });
      }
    });
  }

  for (const logLevel of ["debug", "warn"] as const) {
    it(`PERF-006 CONTROL: logger-only ${logLevel} remains active and preserves filtering`, async () => {
      const captured = capturedLogger();
      const db = memoryClient({ logger: captured.logger, logLevel, plugins: [] });
      await db.execute(sql`SELECT 1 /* audit_perf006 */`, queryOptions);
      await expect(db.execute(sql`SELECT audit_perf006_fail`, queryOptions)).rejects.toMatchObject({ httpStatus: 500 });
      expect(captured.entries.map((entry) => entry.level)).toEqual(
        logLevel === "debug" ? ["debug", "debug", "debug", "error"] : ["error"],
      );
      expect(captured.entries.at(-1)?.fields).toMatchObject({ outcome: "error", queryId: queryOptions.query_id });
    });
  }

  it("PERF-006 CONTROL: terminal-only hooks retain the original start time and duration", async () => {
    let clock = 1000;
    const now = spyOn(Date, "now").mockImplementation(() => clock);
    try {
      for (const outcome of ["success", "error", "stream"] as const) {
        clock = 1000;
        const events: Array<ClickHouseORMQueryResultEvent | ClickHouseORMQueryErrorEvent> = [];
        const db = memoryClient({
          plugins: [
            {
              name: "terminal",
              onQuerySuccess(event) {
                events.push(event);
              },
              onQueryError(event) {
                events.push(event);
              },
            },
          ],
          fetch: () => {
            clock = 1040;
            return outcome === "error"
              ? new Response("synthetic failure", { status: 500 })
              : new Response(outcome === "stream" ? streamResponse : '{"data":[{"n":1}]}');
          },
        });
        if (outcome === "stream") {
          expect(await Array.fromAsync(db.stream(sql`SELECT n /* audit_perf006 */`, queryOptions))).toEqual([
            { n: 1 },
            { n: 2 },
          ]);
        } else if (outcome === "error") {
          await expect(db.execute(sql`SELECT 1 /* audit_perf006 */`, queryOptions)).rejects.toMatchObject({
            httpStatus: 500,
          });
        } else {
          expect(await db.execute(sql`SELECT 1 /* audit_perf006 */`, queryOptions)).toEqual([{ n: 1 }]);
        }
        expect(events).toHaveLength(1);
        expect(events[0]).toMatchObject({ startedAt: 1000, durationMs: 40 });
      }
    } finally {
      now.mockRestore();
    }
  });

  it("PERF-006 CONTROL: logger and user plugins retain wraparound order", async () => {
    const order: string[] = [];
    const noop = () => undefined;
    const db = memoryClient({
      logger: {
        trace: noop,
        debug: (_message, fields) => {
          order.push(`logger:${fields?.outcome}`);
        },
        info: noop,
        warn: noop,
        error: noop,
      },
      logLevel: "debug",
      plugins: [
        {
          name: "user",
          onQueryStart() {
            order.push("user:start");
          },
          onQuerySuccess() {
            order.push("user:success");
          },
        },
      ],
    });
    await db.execute(sql`SELECT 1 /* audit_perf006 */`, queryOptions);
    expect(order).toEqual(["logger:start", "user:start", "user:success", "logger:success"]);
  });

  for (const observed of [false, true]) {
    it(`PERF-006 CONTROL: fresh wire results and identities survive repeated/unicode SQL, observed=${observed}`, async () => {
      const wire: Array<{ body: string; queryId: string | null }> = [];
      const events: ClickHouseORMQueryResultEvent[] = [];
      const db = memoryClient({
        plugins: observed
          ? [
              {
                name: "terminal",
                onQuerySuccess(event) {
                  events.push(event);
                },
              },
            ]
          : [],
        fetch: async (request) => {
          const body = await request.text();
          wire.push({ body, queryId: new URL(request.url).searchParams.get("query_id") });
          const n = Number(/^SELECT (\d+) AS n/.exec(body)?.[1]);
          expect(Number.isFinite(n)).toBe(true);
          return new Response(JSON.stringify({ data: [{ n, sequence: wire.length }] }));
        },
      });
      const numbers = [1, 1, 2, 3];
      const statements = numbers.map((n) => `SELECT ${n} AS n /* audit_perf006 中 café */`);
      for (const [index, statement] of statements.entries()) {
        expect(await db.execute(controlledSql(statement))).toEqual([{ n: numbers[index], sequence: index + 1 }]);
      }
      expect(wire.map((entry) => entry.body)).toEqual(statements.map((statement) => `${statement}\nFORMAT JSON`));
      expect(wire.every((entry) => /^[0-9a-f-]{36}$/.test(entry.queryId ?? ""))).toBe(true);
      expect(new Set(wire.map((entry) => entry.queryId)).size).toBe(4);
      if (observed) {
        expect(events.map((event) => event.queryId ?? null)).toEqual(wire.map((entry) => entry.queryId));
        expect(events.map((event) => event.statement)).toEqual(statements);
        expect(events.map((event) => event.compactStatement)).toEqual(statements);
        expect(new Set(events.map((event) => event.executionId)).size).toBe(4);
        expect(events[0]?.statementHash).toBe(events[1]?.statementHash);
        expect(new Set(events.map((event) => event.statementHash)).size).toBe(3);
      } else {
        expect(events).toEqual([]);
      }
    });
  }

  for (const typed of [false, true]) {
    it(`PERF-006 CONTROL: lazy stream and consumer throw preserve session reuse, typed=${typed}`, async () => {
      let fetches = 0;
      let cancellations = 0;
      const bytes = new TextEncoder().encode('{"n":1}\n{"n":2}\n');
      const db = memoryClient({
        fetch: async (request) => {
          fetches += 1;
          const body = await request.text();
          if (!body.endsWith("FORMAT JSONEachRow")) {
            return new Response("");
          }
          return new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(bytes);
              },
              cancel() {
                cancellations += 1;
              },
            }),
          );
        },
      });
      await db.runInSession(
        async (session) => {
          const stream = () =>
            typed
              ? session.select().from(table).iterator(queryOptions)
              : session.stream(sql`SELECT n /* audit_perf006 */`, queryOptions);
          const untouched = stream();
          await untouched.return();
          expect(fetches).toBe(0);
          const iterator = stream();
          expect(await iterator.next()).toEqual({ done: false, value: { n: 1 } });
          const sentinel = new Error("audit_perf006 independent consumer throw");
          await expect(iterator.throw(sentinel)).rejects.toBe(sentinel);
          expect(cancellations).toBe(1);
          await session.command(sql`SELECT 1 /* audit_perf006 */`, { query_id: "audit_perf006_after_throw" });
        },
        { session_id: "audit_perf006_adversarial" },
      );
      expect(fetches).toBeGreaterThanOrEqual(2);
    });
  }

  for (const stream of [false, true]) {
    for (const failure of [false, true]) {
      it(`PERF-006 CONTROL: terminal accessor respects reverse unwinding, stream=${stream}, failure=${failure}`, async () => {
        let innerFinished = false;
        let fetches = 0;
        const order: string[] = [];
        const sentinel = new Error("audit_perf006 outer hook resolved before inner completion");
        const outerHook = () => {
          if (!innerFinished) {
            throw sentinel;
          }
          return () => {
            order.push("outer");
          };
        };
        const outer: ClickHouseORMPlugin = failure
          ? {
              name: "outer",
              get onQueryError() {
                return outerHook();
              },
            }
          : {
              name: "outer",
              get onQuerySuccess() {
                return outerHook();
              },
            };
        const innerHook = async () => {
          await Promise.resolve();
          innerFinished = true;
          order.push("inner");
        };
        const inner: ClickHouseORMPlugin = failure
          ? { name: "inner", onQueryError: innerHook }
          : { name: "inner", onQuerySuccess: innerHook };
        const db = memoryClient({
          plugins: [outer, inner],
          fetch: () => {
            fetches += 1;
            return failure
              ? new Response("synthetic transport failure", { status: 500 })
              : new Response(stream ? '{"n":7}\n' : '{"data":[{"n":7}]}');
          },
        });
        const query = sql`SELECT 7 AS n /* audit_perf006 */`;
        const result = stream ? Array.fromAsync(db.stream(query, queryOptions)) : db.execute(query, queryOptions);
        const outcome = await result.then(
          (value) => ({ value, error: undefined }),
          (error: unknown) => ({ value: undefined, error }),
        );
        expect(fetches).toBe(1);
        if (failure) {
          expect(outcome.error).toMatchObject({ kind: "request_failed", httpStatus: 500 });
        } else {
          expect(outcome.error).toBeUndefined();
          expect(outcome.value).toEqual([{ n: 7 }]);
        }
        expect(order).toEqual(["inner", "outer"]);
      });
    }
  }
});
