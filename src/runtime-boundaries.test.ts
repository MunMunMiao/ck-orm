import { describe, expect, it } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { string, uint32 } from "./columns";
import { extractClickHouseException, isClickHouseORMError } from "./errors";
import type { ClickHouseBaseQueryOptions, Session } from "./runtime";
import { clickhouseClient } from "./runtime";
import { createJsonEachRowBody, parseJsonEachRowLine, readValidatedResponseText } from "./runtime/json-stream";
import {
  dsnMarker,
  dsnUsername,
  expectedAuthorization,
  headerCases,
  headerClientConfig,
  headerSessionId,
  invalidDsns,
  leakingChannels,
  serializeError,
  validDsn,
} from "./runtime-boundaries-support.test";
import { ckTable } from "./schema";
import { sql } from "./sql";

const host = "http://127.0.0.1:1";
const fixtureRoot = join(import.meta.dir, "runtime-boundaries-dsn-fixture.test.ts");
const json = {
  parse: (text: string) => JSON.parse(text) as unknown,
  stringify: (value: unknown) => JSON.stringify(value),
};
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
const markerTable = ckTable("audit_sec004_unit", {
  id: uint32(),
  message: string(),
  note: string(),
});
const mappedUsers = ckTable("audit_upload_unit", {
  userId: uint32("user_id"),
  name: string(),
});
const mappedHangTable = ckTable("audit_final_upload_hang", {
  logicalId: uint32("id"),
});
const mappedCloseTable = ckTable("audit_final_mapped_close", {
  logicalId: uint32("id"),
});
const markerRows = [
  { id: 1, message: "before", note: "synthetic-before" },
  { id: 2, message: "ordinary __exception__ business text", note: "synthetic-marker" },
  { id: 3, message: "after", note: "synthetic-after" },
];
const controlRows = [
  { id: 1, message: "ordinary business text", note: "synthetic-control" },
  { id: 2, message: "Code: 60. DB::Exception: quoted business text (UNKNOWN_TABLE)", note: "synthetic-control" },
];
const modes = ["raw-eager", "typed-eager", "raw-stream", "typed-stream"] as const;
type Mode = (typeof modes)[number];

const responseBody = (mode: Mode, rows: readonly Record<string, unknown>[]) =>
  mode.endsWith("stream")
    ? `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`
    : JSON.stringify({
        meta: [
          { name: "id", type: "UInt32" },
          { name: "message", type: "String" },
          { name: "note", type: "String" },
        ],
        data: rows,
        rows: rows.length,
      });

const readRows = async (db: ReturnType<typeof clickhouseClient>, mode: Mode) => {
  const query = sql`SELECT ${markerTable.id}, ${markerTable.message}, ${markerTable.note} FROM ${markerTable} ORDER BY ${markerTable.id}`;
  if (mode === "raw-eager") {
    return db.execute(query);
  }
  if (mode === "typed-eager") {
    return db.select().from(markerTable).orderBy(markerTable.id).execute();
  }
  const stream =
    mode === "raw-stream" ? db.stream(query) : db.select().from(markerTable).orderBy(markerTable.id).iterator();
  const rows: Record<string, unknown>[] = [];
  for await (const row of stream) {
    rows.push(row);
  }
  return rows;
};

const queuedRequests = async (secondOptions: ClickHouseBaseQueryOptions) => {
  const firstStarted = Promise.withResolvers<void>();
  const releaseFirst = Promise.withResolvers<void>();
  const wireSessions: Array<string | null> = [];
  let secondDispatchedWhileHeld = false;
  let held = true;
  const db = clickhouseClient({
    host,
    session_id: "audit_root",
    session_max_concurrent_requests: 1,
    request_timeout: 5_000,
    fetch: async (request) => {
      const url = new URL(request.url);
      wireSessions.push(url.searchParams.get("session_id"));
      if (url.searchParams.get("query_id") === "audit_hold") {
        firstStarted.resolve();
        await releaseFirst.promise;
      } else {
        secondDispatchedWhileHeld = held;
      }
      return Response.json({ data: [{ value: 1 }] });
    },
  });
  await db.runInSession(
    async (session) => {
      const first = session.execute(sql`SELECT 1 AS value`, { query_id: "audit_hold" });
      let second: ReturnType<Session["execute"]> | undefined;
      try {
        await firstStarted.promise;
        second = session.execute(sql`SELECT 1 AS value`, { query_id: "audit_wait", ...secondOptions });
        await tick();
        await tick();
      } finally {
        held = false;
        releaseFirst.resolve();
        await Promise.all([first, second]);
      }
    },
    { session_id: "audit_child" },
  );
  return { secondDispatchedWhileHeld, wireSessions };
};

describe("runtime protocol boundaries", () => {
  describe("SEC-003 temporary-table cleanup", () => {
    for (const mode of ["failed-create", "already-dropped"] as const) {
      it(`SEC-003 RED: ${mode} cleanup is restricted to temporary tables`, async () => {
        const statements: string[] = [];
        const name = "audit_cleanup_unit";
        const db = clickhouseClient({
          host,
          fetch: async (request) => {
            const statement = await request.text();
            statements.push(statement);
            return mode === "failed-create" && statement.startsWith("CREATE TEMPORARY")
              ? new Response("Code: 50. DB::Exception: Unknown data type AuditAbsentType. (UNKNOWN_TYPE)", {
                  status: 400,
                })
              : new Response("");
          },
        });
        const operation = db.runInSession(
          async (session) => {
            if (mode === "failed-create") {
              await session.createTemporaryTableRaw(name, "(id AuditAbsentType)");
            } else {
              await session.createTemporaryTable(ckTable(name, { id: uint32() }));
              await session.dropTable(name);
            }
          },
          { session_id: "audit_cleanup_session" },
        );
        if (mode === "failed-create") {
          await expect(operation).rejects.toMatchObject({ kind: "request_failed" });
        } else {
          await operation;
        }
        expect(statements.some((statement) => /^DROP TABLE IF EXISTS/i.test(statement))).toBe(false);
        expect(statements.at(-1)).toMatch(/^DROP\s+TEMPORARY\s+TABLE\s+IF\s+EXISTS\s+`audit_cleanup_unit`\s*$/i);
      });
    }

    it("SEC-003 CONTROL: normal cleanup stays in its session and preserves the callback error", async () => {
      const requests: Array<{ statement: string; session: string | null }> = [];
      const original = new Error("audit callback failure");
      const db = clickhouseClient({
        host,
        fetch: async (request) => {
          requests.push({
            statement: await request.text(),
            session: new URL(request.url).searchParams.get("session_id"),
          });
          return new Response("");
        },
      });
      await expect(
        db.runInSession(
          async (session) => {
            await session.createTemporaryTableRaw("audit_control_unit", "(id UInt32)");
            throw original;
          },
          { session_id: "audit_control_session" },
        ),
      ).rejects.toBe(original);
      expect(requests).toHaveLength(2);
      expect(requests.every((request) => request.session === "audit_control_session")).toBe(true);
      expect(requests[1]?.statement).toMatch(/^DROP TEMPORARY TABLE IF EXISTS `audit_control_unit`$/);
    });
  });

  describe("REL-001 session identity", () => {
    for (const scope of ["child", "nested-child", "child-without-root-session"] as const) {
      it(`REL-001 RED: undefined session_id inherits ${scope} for transport and events`, async () => {
        const wireSessions: Array<string | null> = [];
        const eventSessions: Array<string | undefined> = [];
        const db = clickhouseClient({
          host,
          ...(scope === "child-without-root-session" ? {} : { session_id: "audit_root" }),
          plugins: [
            {
              name: "audit-session-observer",
              onQueryStart(event) {
                eventSessions.push(event.sessionId);
              },
            },
          ],
          fetch: (request) => {
            wireSessions.push(new URL(request.url).searchParams.get("session_id"));
            return Response.json({ data: [{ value: 1 }] });
          },
        });
        const run = (session: Session) => session.execute(sql`SELECT 1 AS value`, { session_id: undefined });
        const rows = await db.runInSession(
          async (child) =>
            scope === "nested-child" ? child.runInSession(run, { session_id: "audit_nested" }) : run(child),
          { session_id: "audit_child" },
        );
        expect(rows).toEqual([{ value: 1 }]);
        const expected = scope === "nested-child" ? "audit_nested" : "audit_child";
        expect(wireSessions).toEqual([expected]);
        expect(eventSessions).toEqual([expected]);
      });
    }

    it("REL-001 RED: undefined session_id cannot bypass the current child queue", async () => {
      const observed = await queuedRequests({ session_id: undefined });
      expect(observed.secondDispatchedWhileHeld).toBe(false);
      expect(observed.wireSessions).toEqual(["audit_child", "audit_child"]);
    });

    it("REL-001 CONTROL: omitted and explicit child sessions both use the queue", async () => {
      for (const options of [{}, { session_id: "audit_child" }]) {
        expect(await queuedRequests(options)).toEqual({
          secondDispatchedWhileHeld: false,
          wireSessions: ["audit_child", "audit_child"],
        });
      }
    });

    it("REL-001 CONTROL: an explicit empty session_id still disables the wire session", async () => {
      const wireSessions: Array<string | null> = [];
      const db = clickhouseClient({
        host,
        session_id: "audit_root",
        fetch: (request) => {
          wireSessions.push(new URL(request.url).searchParams.get("session_id"));
          return Response.json({ data: [] });
        },
      });
      await db.runInSession((session) => session.execute(sql`SELECT 1`, { session_id: "" }), {
        session_id: "audit_child",
      });
      expect(wireSessions).toEqual([null]);
    });
  });

  describe("REL-002 upload source close", () => {
    for (const mode of ["invalid-query-id", "fetch-reject", "early-http-500", "mapped-fetch-reject"] as const) {
      it(`REL-002 RED: ${mode} closes every started upload generator`, async () => {
        const state = { opened: 0, closed: 0, fetchCalls: 0 };
        const source = (async function* () {
          state.opened += 1;
          try {
            yield { id: 1 };
            yield { id: 2 };
          } finally {
            state.closed += 1;
          }
        })();
        const db = clickhouseClient({
          host,
          fetch: () => {
            state.fetchCalls += 1;
            if (mode.endsWith("fetch-reject")) {
              throw new TypeError("audit upload transport failure");
            }
            return new Response("audit upload rejected", { status: 500 });
          },
        });
        try {
          await expect(
            db.insertJsonEachRow(mode === "mapped-fetch-reject" ? mappedUsers : "audit_upload_unit", source, {
              query_id: mode === "invalid-query-id" ? "invalid query id" : "audit_upload",
            }),
          ).rejects.toMatchObject({ kind: mode === "invalid-query-id" ? "client_validation" : "request_failed" });
          await tick();
          expect(state.fetchCalls).toBe(mode === "invalid-query-id" ? 0 : 1);
          expect(state.closed).toBe(state.opened);
        } finally {
          await source.return(undefined);
          expect(state.closed).toBe(state.opened);
        }
      });
    }

    it("REL-002 RED: sync iterator.return throw does not replace the original send failure", async () => {
      const state = { opened: 0, closed: 0 };
      const source: AsyncIterable<Record<string, unknown>> = {
        [Symbol.asyncIterator]: () => {
          state.opened += 1;
          let done = false;
          return {
            next: async () => {
              if (done) {
                return { done: true as const, value: undefined };
              }
              done = true;
              return { value: { id: 1 }, done: false as const };
            },
            return: () => {
              state.closed += 1;
              throw new Error("iterator return failed");
            },
          };
        },
      };
      const db = clickhouseClient({
        host,
        fetch: () => {
          throw new TypeError("audit upload transport failure");
        },
      });
      await expect(db.insertJsonEachRow("audit_upload_unit", source)).rejects.toMatchObject({
        kind: "request_failed",
        cause: expect.objectContaining({ message: "audit upload transport failure" }),
      });
      expect(state.closed).toBe(state.opened);
    });

    it("REL-002 RED: mapping errors close the underlying JSONEachRow source", async () => {
      const state = { opened: 0, closed: 0 };
      const source = (async function* () {
        state.opened += 1;
        try {
          yield { userId: 1, user_id: 1, name: "alice" };
        } finally {
          state.closed += 1;
        }
      })();
      const db = clickhouseClient({
        host,
        fetch: async (request) => {
          await request.text();
          return new Response("", { status: 200 });
        },
      });
      try {
        await expect(db.insertJsonEachRow(mappedUsers, source)).rejects.toMatchObject({
          kind: "client_validation",
        });
        expect(state.closed).toBe(state.opened);
      } finally {
        await source.return(undefined);
        expect(state.closed).toBe(state.opened);
      }
    });

    it("REL-002 RED: early HTTP 200 exception still closes the upload source", async () => {
      const state = { opened: 0, closed: 0 };
      const source = (async function* () {
        state.opened += 1;
        try {
          yield { id: 1 };
        } finally {
          state.closed += 1;
        }
      })();
      const db = clickhouseClient({
        host,
        fetch: async () => new Response("Code: 60. DB::Exception: missing table (UNKNOWN_TABLE)\n", { status: 200 }),
      });
      try {
        await expect(db.insertJsonEachRow("audit_upload_unit", source)).rejects.toMatchObject({
          kind: "request_failed",
        });
        expect(state.closed).toBe(state.opened);
      } finally {
        await source.return(undefined);
      }
    });

    it("REL-002 RED: locked stream close is idempotent when iterator.return throws synchronously", async () => {
      let returned = 0;
      const sentinel = new Error("cleanup sentinel");
      const source: AsyncIterable<Record<string, unknown>> = {
        [Symbol.asyncIterator]: () => ({
          next: async () => ({ done: false as const, value: { n: 1 } }),
          return: () => {
            returned += 1;
            throw sentinel;
          },
        }),
      };
      const value = await createJsonEachRowBody(source, json);
      expect(value.body).toBeInstanceOf(ReadableStream);
      const reader = (value.body as ReadableStream<Uint8Array>).getReader();
      await reader.read();
      const close = (value as { close: () => Promise<void> }).close;
      await expect(close()).rejects.toBe(sentinel);
      await expect(close()).rejects.toBe(sentinel);
      reader.releaseLock();
      expect(returned).toBe(1);
    });

    it("REL-002 RED: overlapping close and cancel do not retry a throwing iterator.return", async () => {
      let returned = 0;
      const sentinel = new Error("cleanup sentinel");
      const source: AsyncIterable<Record<string, unknown>> = {
        [Symbol.asyncIterator]: () => ({
          next: async () => ({ done: false as const, value: { n: 1 } }),
          return: () => {
            returned += 1;
            throw sentinel;
          },
        }),
      };
      const value = await createJsonEachRowBody(source, json);
      if (!(value.body instanceof ReadableStream)) {
        throw new Error("expected stream");
      }
      const reader = value.body.getReader();
      await reader.read();
      const close = (value as { close: () => Promise<void> }).close;
      const [first, , second] = await Promise.allSettled([close(), value.body.cancel(), close()]);
      reader.releaseLock();
      expect(returned).toBe(1);
      expect(first).toEqual({ status: "rejected", reason: sentinel });
      expect(second).toEqual({ status: "rejected", reason: sentinel });
    });

    it("REL-002 RED: natural completion still surfaces a synchronous iterator.return throw", async () => {
      const sentinel = new Error("natural completion cleanup failed");
      let returned = 0;
      const rows: AsyncIterable<Record<string, unknown>> = {
        [Symbol.asyncIterator]: () => ({
          next: async () => ({ done: true as const, value: undefined }),
          return: () => {
            returned += 1;
            throw sentinel;
          },
        }),
      };
      const value = await createJsonEachRowBody(rows, json);
      if (!(value.body instanceof ReadableStream)) {
        throw new Error("expected stream");
      }
      let caught: unknown;
      try {
        await new Response(value.body).text();
      } catch (error) {
        caught = error;
      }
      expect(caught).toBe(sentinel);
      expect(returned).toBe(1);
    });

    it("REL-002 RED: natural completion still surfaces a rejecting iterator.return promise", async () => {
      const sentinel = new Error("natural completion cleanup failed");
      let returned = 0;
      const rows: AsyncIterable<Record<string, unknown>> = {
        [Symbol.asyncIterator]: () => ({
          next: async () => ({ done: true as const, value: undefined }),
          return: () => {
            returned += 1;
            return Promise.reject(sentinel);
          },
        }),
      };
      const value = await createJsonEachRowBody(rows, json);
      if (!(value.body instanceof ReadableStream)) {
        throw new Error("expected stream");
      }
      let caught: unknown;
      try {
        await new Response(value.body).text();
      } catch (error) {
        caught = error;
      }
      expect(caught).toBe(sentinel);
      expect(returned).toBe(1);
    });

    it("REL-002 RED: insertJsonEachRow surfaces iterator.return failure after HTTP 200", async () => {
      const sentinel = new Error("natural completion cleanup failed");
      let returned = 0;
      const source: AsyncIterable<Record<string, unknown>> = {
        [Symbol.asyncIterator]: () => ({
          next: async () => ({ done: true as const, value: undefined }),
          return: () => {
            returned += 1;
            throw sentinel;
          },
        }),
      };
      const db = clickhouseClient({
        host,
        fetch: () => new Response(""),
      });
      await expect(db.insertJsonEachRow("valid_table", source)).rejects.toBe(sentinel);
      expect(returned).toBe(1);
    });

    it("REL-002 RED: createJsonEachRowBody exposes idempotent close for live sources", async () => {
      const state = { opened: 0, closed: 0 };
      const source = (async function* () {
        state.opened += 1;
        try {
          yield { id: 1 };
        } finally {
          state.closed += 1;
        }
      })();
      const body = await createJsonEachRowBody(source, json);
      expect(typeof (body as { close?: unknown }).close).toBe("function");
      await (body as { close: () => Promise<void> }).close();
      await (body as { close: () => Promise<void> }).close();
      expect(state.closed).toBe(state.opened);
    });

    it("REL-002 RED: early HTTP 200 with an unconsumed source still closes it", async () => {
      const state = { opened: 0, closed: 0 };
      const source = (async function* () {
        state.opened += 1;
        try {
          yield { n: 1 };
          yield { n: 2 };
        } finally {
          state.closed += 1;
        }
      })();
      const db = clickhouseClient({
        host,
        fetch: () => new Response(""),
      });
      try {
        await db.insertJsonEachRow("valid_table", source);
        await tick();
        expect(state.closed).toBe(state.opened);
        expect(state.opened).toBeGreaterThan(0);
      } finally {
        await source.return(undefined);
        expect(state.closed).toBe(state.opened);
      }
    });

    it("REL-002 RED: early HTTP 200 after a partial body read still closes the source", async () => {
      const state = { opened: 0, closed: 0 };
      const source = (async function* () {
        state.opened += 1;
        try {
          yield { n: 1 };
          yield { n: 2 };
          yield { n: 3 };
        } finally {
          state.closed += 1;
        }
      })();
      const db = clickhouseClient({
        host,
        fetch: async (request) => {
          const reader = request.body?.getReader();
          if (reader) {
            await reader.read();
            reader.releaseLock();
          }
          return new Response("");
        },
      });
      try {
        await db.insertJsonEachRow("valid_table", source);
        await tick();
        expect(state.closed).toBe(state.opened);
        expect(state.opened).toBeGreaterThan(0);
      } finally {
        await source.return(undefined);
        expect(state.closed).toBe(state.opened);
      }
    });

    it("REL-002 RED: empty table name on transport insertJsonEachRow does not start the source", async () => {
      const state = { opened: 0, closed: 0, fetches: 0 };
      const source = (async function* () {
        state.opened += 1;
        try {
          yield { n: 1 };
          yield { n: 2 };
        } finally {
          state.closed += 1;
        }
      })();
      const db = clickhouseClient({
        host,
        fetch: () => {
          state.fetches += 1;
          return new Response("");
        },
      });
      try {
        await expect(db.$client.insertJsonEachRow("", source)).rejects.toMatchObject({ kind: "client_validation" });
        await tick();
        expect(state).toEqual({ opened: 0, closed: 0, fetches: 0 });
      } finally {
        await source.return(undefined);
        expect(state.closed).toBe(state.opened);
      }
    });

    it("REL-002 RED: empty table name on public insertJsonEachRow does not start the source", async () => {
      const state = { opened: 0, closed: 0, fetches: 0 };
      const source = (async function* () {
        state.opened += 1;
        try {
          yield { n: 1 };
        } finally {
          state.closed += 1;
        }
      })();
      const db = clickhouseClient({
        host,
        fetch: () => {
          state.fetches += 1;
          return new Response("");
        },
      });
      try {
        await expect(db.insertJsonEachRow("", source)).rejects.toMatchObject({ kind: "client_validation" });
        await tick();
        expect(state).toEqual({ opened: 0, closed: 0, fetches: 0 });
      } finally {
        await source.return(undefined);
        expect(state.closed).toBe(state.opened);
      }
    });

    it("REL-002 RED: invalid session_id does not start the upload source", async () => {
      const state = { opened: 0, closed: 0, fetches: 0 };
      const source = (async function* () {
        state.opened += 1;
        try {
          yield { n: 1 };
        } finally {
          state.closed += 1;
        }
      })();
      const db = clickhouseClient({
        host,
        fetch: () => {
          state.fetches += 1;
          return new Response("");
        },
      });
      try {
        await expect(db.insertJsonEachRow("valid_table", source, { session_id: "bad session" })).rejects.toMatchObject({
          kind: "client_validation",
        });
        await tick();
        expect(state).toEqual({ opened: 0, closed: 0, fetches: 0 });
      } finally {
        await source.return(undefined);
        expect(state.closed).toBe(state.opened);
      }
    });

    it("REL-002 RED: invalid http header does not start the upload source", async () => {
      const state = { opened: 0, closed: 0, fetches: 0 };
      const source = (async function* () {
        state.opened += 1;
        try {
          yield { n: 1 };
        } finally {
          state.closed += 1;
        }
      })();
      const db = clickhouseClient({
        host,
        fetch: () => {
          state.fetches += 1;
          return new Response("");
        },
      });
      try {
        await expect(
          db.insertJsonEachRow("valid_table", source, { http_headers: { "Bad\nHeader": "x" } }),
        ).rejects.toBeInstanceOf(Error);
        await tick();
        expect(state.fetches).toBe(0);
        expect(state.opened).toBe(0);
        expect(state.closed).toBe(state.opened);
      } finally {
        await source.return(undefined);
        expect(state.closed).toBe(state.opened);
      }
    });

    it("REL-002 CONTROL: full upload consumption closes the generator exactly once", async () => {
      const state = { opened: 0, closed: 0 };
      let body: string | undefined;
      const source = (async function* () {
        state.opened += 1;
        try {
          yield { id: 1 };
          yield { id: 2 };
        } finally {
          state.closed += 1;
        }
      })();
      const db = clickhouseClient({
        host,
        fetch: async (request) => {
          body = await request.text();
          return new Response("");
        },
      });
      try {
        await db.insertJsonEachRow("audit_upload_control", source);
        expect(body).toBe('{"id":1}\n{"id":2}\n');
        expect(state).toEqual({ opened: 1, closed: 1 });
      } finally {
        await source.return(undefined);
      }
    });

    for (const mode of ["raw", "mapped"] as const) {
      it(`F1: ${mode} fetch failure reports cause and releases session while source next is blocked`, async () => {
        const original = new TypeError("synthetic failed upload fetch");
        const upstreamEntered = Promise.withResolvers<void>();
        const releaseUpstream = Promise.withResolvers<void>();
        const releaseFetchFailure = Promise.withResolvers<void>();
        const fetchFailed = Promise.withResolvers<void>();
        let sourceClosed = 0;
        let returned = 0;
        let siblingDispatched = false;
        let insertState = "pending";
        let upstreamReleased = false;
        let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
        let pendingRead: Promise<unknown> | undefined;
        const generator = (async function* () {
          try {
            yield { logicalId: 1 };
            upstreamEntered.resolve();
            await releaseUpstream.promise;
            yield { logicalId: 2 };
          } finally {
            sourceClosed += 1;
          }
        })();
        const source: AsyncIterable<Record<string, unknown>> = {
          [Symbol.asyncIterator]: () => ({
            next: () => generator.next(),
            return: () => {
              returned += 1;
              return generator.return(undefined);
            },
          }),
        };
        const db = clickhouseClient({
          host,
          session_id: "audit_final_upload_hang_session",
          session_max_concurrent_requests: 1,
          request_timeout: 5_000,
          fetch: async (request) => {
            if (new URL(request.url).searchParams.get("query_id") === "audit_final_upload_first") {
              const body = request.body;
              if (!body) {
                throw new Error("missing upload body");
              }
              reader = body.getReader();
              await reader.read();
              pendingRead = reader.read().then(
                () => undefined,
                () => undefined,
              );
              await upstreamEntered.promise;
              await releaseFetchFailure.promise;
              fetchFailed.resolve();
              throw original;
            }
            siblingDispatched = true;
            return new Response("");
          },
        });
        const insert = db
          .insertJsonEachRow(mode === "raw" ? "audit_final_upload_hang" : mappedHangTable, source, {
            query_id: "audit_final_upload_first",
          })
          .then(
            () => {
              insertState = "fulfilled";
              return { status: "fulfilled" as const };
            },
            (reason: unknown) => {
              insertState = "rejected";
              return { status: "rejected" as const, reason };
            },
          );
        let sibling: Promise<unknown> | undefined;
        let deadline: ReturnType<typeof setTimeout> | undefined;
        try {
          await upstreamEntered.promise;
          sibling = db.command(sql`SELECT 1`, { query_id: "audit_final_upload_sibling" }).then(
            () => ({ status: "fulfilled" as const }),
            (reason: unknown) => ({ status: "rejected" as const, reason }),
          );
          expect(siblingDispatched).toBe(false);
          releaseFetchFailure.resolve();
          await fetchFailed.promise;
          const completed = await Promise.race([
            Promise.all([insert, sibling]).then((results) => ({ kind: "settled" as const, results })),
            new Promise<{ kind: "deadline" }>((resolve) => {
              deadline = setTimeout(() => resolve({ kind: "deadline" }), 250);
            }),
          ]);
          expect(upstreamReleased).toBe(false);
          expect(completed.kind).toBe("settled");
          expect(insertState).toBe("rejected");
          expect(siblingDispatched).toBe(true);
          if (completed.kind === "settled") {
            expect(completed.results[0]).toEqual({
              status: "rejected",
              reason: expect.objectContaining({ kind: "request_failed", cause: original }),
            });
            expect(completed.results[1]).toEqual({ status: "fulfilled" });
          }
        } finally {
          if (deadline !== undefined) clearTimeout(deadline);
          releaseFetchFailure.resolve();
          upstreamReleased = true;
          releaseUpstream.resolve();
          await pendingRead;
          await Promise.all([insert, sibling]);
          await generator.return(undefined);
          reader?.releaseLock();
          expect(sourceClosed).toBe(1);
          expect(returned).toBe(1);
        }
      }, 5_000);
    }

    it("F1: HTTP 500 after a blocked next reports the primary error and releases the session", async () => {
      const upstreamEntered = Promise.withResolvers<void>();
      const releaseUpstream = Promise.withResolvers<void>();
      let sourceClosed = 0;
      let siblingDispatched = false;
      let insertState = "pending";
      let upstreamReleased = false;
      let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
      let pendingRead: Promise<unknown> | undefined;
      const generator = (async function* () {
        try {
          yield { logicalId: 1 };
          upstreamEntered.resolve();
          await releaseUpstream.promise;
          yield { logicalId: 2 };
        } finally {
          sourceClosed += 1;
        }
      })();
      const source: AsyncIterable<Record<string, unknown>> = {
        [Symbol.asyncIterator]: () => ({
          next: () => generator.next(),
          return: () => generator.return(undefined),
        }),
      };
      const db = clickhouseClient({
        host,
        session_id: "audit_final_upload_http500_session",
        session_max_concurrent_requests: 1,
        request_timeout: 5_000,
        fetch: async (request) => {
          if (new URL(request.url).searchParams.get("query_id") === "audit_final_upload_http500") {
            const body = request.body;
            if (!body) {
              throw new Error("missing upload body");
            }
            reader = body.getReader();
            await reader.read();
            pendingRead = reader.read().then(
              () => undefined,
              () => undefined,
            );
            await upstreamEntered.promise;
            return new Response("Code: 60. DB::Exception: missing table (UNKNOWN_TABLE)\n", { status: 500 });
          }
          siblingDispatched = true;
          return new Response("");
        },
      });
      const insert = db
        .insertJsonEachRow("audit_final_upload_http500", source, { query_id: "audit_final_upload_http500" })
        .then(
          () => {
            insertState = "fulfilled";
            return { status: "fulfilled" as const };
          },
          (reason: unknown) => {
            insertState = "rejected";
            return { status: "rejected" as const, reason };
          },
        );
      let sibling: Promise<unknown> | undefined;
      let deadline: ReturnType<typeof setTimeout> | undefined;
      try {
        await upstreamEntered.promise;
        sibling = db.command(sql`SELECT 1`, { query_id: "audit_final_upload_http500_sibling" }).then(
          () => ({ status: "fulfilled" as const }),
          (reason: unknown) => ({ status: "rejected" as const, reason }),
        );
        const completed = await Promise.race([
          Promise.all([insert, sibling]).then((results) => ({ kind: "settled" as const, results })),
          new Promise<{ kind: "deadline" }>((resolve) => {
            deadline = setTimeout(() => resolve({ kind: "deadline" }), 250);
          }),
        ]);
        expect(upstreamReleased).toBe(false);
        expect(completed.kind).toBe("settled");
        expect(insertState).toBe("rejected");
        expect(siblingDispatched).toBe(true);
        if (completed.kind === "settled") {
          expect(completed.results[0]).toEqual({
            status: "rejected",
            reason: expect.objectContaining({ kind: "request_failed" }),
          });
          expect(completed.results[1]).toEqual({ status: "fulfilled" });
        }
      } finally {
        if (deadline !== undefined) clearTimeout(deadline);
        upstreamReleased = true;
        releaseUpstream.resolve();
        await pendingRead;
        await Promise.all([insert, sibling]);
        await generator.return(undefined);
        reader?.releaseLock();
        expect(sourceClosed).toBe(1);
      }
    }, 5_000);

    it("F1: late cleanup rejection after a primary fetch failure is not unhandled", async () => {
      const original = new TypeError("synthetic failed upload fetch");
      const sentinel = new Error("synthetic late cleanup failure");
      const upstreamEntered = Promise.withResolvers<void>();
      const releaseUpstream = Promise.withResolvers<void>();
      const releaseFetchFailure = Promise.withResolvers<void>();
      const unhandled: unknown[] = [];
      const onUnhandled = (reason: unknown) => {
        unhandled.push(reason);
      };
      process.on("unhandledRejection", onUnhandled);
      let sourceClosed = 0;
      let returned = 0;
      let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
      let pendingRead: Promise<unknown> | undefined;
      const generator = (async function* () {
        try {
          yield { logicalId: 1 };
          upstreamEntered.resolve();
          await releaseUpstream.promise;
          yield { logicalId: 2 };
        } finally {
          sourceClosed += 1;
        }
      })();
      const source: AsyncIterable<Record<string, unknown>> = {
        [Symbol.asyncIterator]: () => ({
          next: () => generator.next(),
          return: () => {
            returned += 1;
            return generator.return(undefined).then(() => {
              throw sentinel;
            });
          },
        }),
      };
      const db = clickhouseClient({
        host,
        session_id: "audit_final_upload_late_cleanup_session",
        session_max_concurrent_requests: 1,
        request_timeout: 5_000,
        fetch: async (request) => {
          if (new URL(request.url).searchParams.get("query_id") === "audit_final_upload_late") {
            const body = request.body;
            if (!body) {
              throw new Error("missing upload body");
            }
            reader = body.getReader();
            await reader.read();
            pendingRead = reader.read().then(
              () => undefined,
              () => undefined,
            );
            await upstreamEntered.promise;
            await releaseFetchFailure.promise;
            throw original;
          }
          return new Response("");
        },
      });
      const insert = db.insertJsonEachRow("audit_final_upload_late", source, { query_id: "audit_final_upload_late" });
      let deadline: ReturnType<typeof setTimeout> | undefined;
      try {
        await upstreamEntered.promise;
        releaseFetchFailure.resolve();
        const completed = await Promise.race([
          insert.then(
            () => ({ kind: "fulfilled" as const }),
            (reason: unknown) => ({ kind: "rejected" as const, reason }),
          ),
          new Promise<{ kind: "deadline" }>((resolve) => {
            deadline = setTimeout(() => resolve({ kind: "deadline" }), 250);
          }),
        ]);
        expect(completed.kind).toBe("rejected");
        if (completed.kind === "rejected") {
          expect(completed.reason).toEqual(expect.objectContaining({ kind: "request_failed", cause: original }));
        }
        releaseUpstream.resolve();
        await pendingRead;
        await insert.then(
          () => undefined,
          () => undefined,
        );
        await tick();
        await tick();
        expect(returned).toBe(1);
        expect(sourceClosed).toBe(1);
        expect(unhandled).toEqual([]);
      } finally {
        if (deadline !== undefined) clearTimeout(deadline);
        process.removeListener("unhandledRejection", onUnhandled);
        releaseFetchFailure.resolve();
        releaseUpstream.resolve();
        await pendingRead;
        await insert.then(
          () => undefined,
          () => undefined,
        );
        await generator.return(undefined);
        reader?.releaseLock();
      }
    }, 5_000);

    for (const tableKind of ["string", "typed"] as const) {
      for (const failureKind of ["sync-throw", "async-reject"] as const) {
        it(`F2: ${tableKind} ${failureKind} preserves sole natural-close failure`, async () => {
          const sentinel = new Error("synthetic sole cleanup failure");
          let nextCalls = 0;
          let returnCalls = 0;
          const source: AsyncIterable<Record<string, unknown>> = {
            [Symbol.asyncIterator]: () => ({
              next: async () =>
                ++nextCalls === 1
                  ? { done: false as const, value: { logicalId: 1 } }
                  : { done: true as const, value: undefined },
              return: () => {
                returnCalls += 1;
                if (failureKind === "sync-throw") throw sentinel;
                return Promise.reject(sentinel);
              },
            }),
          };
          const db = clickhouseClient({
            host,
            fetch: async (request) => {
              await request.text();
              return new Response("");
            },
          });
          const result = await db
            .insertJsonEachRow(tableKind === "string" ? "audit_final_mapped_close" : mappedCloseTable, source)
            .then(
              () => ({ status: "fulfilled" as const }),
              (reason: unknown) => ({ status: "rejected" as const, reason }),
            );
          expect(result.status).toBe("rejected");
          if (result.status === "rejected") {
            expect(result.reason === sentinel || (result.reason as { cause?: unknown }).cause === sentinel).toBe(true);
          }
          expect(returnCalls).toBe(1);
          expect(nextCalls).toBe(2);
        });
      }
    }

    it("F2 CONTROL: typed next() failure is preserved when cleanup also fails", async () => {
      const nextSentinel = new Error("synthetic next failure");
      const closeSentinel = new Error("synthetic cleanup failure");
      let returnCalls = 0;
      let nextCalls = 0;
      const source: AsyncIterable<Record<string, unknown>> = {
        [Symbol.asyncIterator]: () => ({
          next: async () => {
            nextCalls += 1;
            if (nextCalls === 1) {
              return { done: false as const, value: { logicalId: 1 } };
            }
            throw nextSentinel;
          },
          return: () => {
            returnCalls += 1;
            throw closeSentinel;
          },
        }),
      };
      const db = clickhouseClient({
        host,
        fetch: async (request) => {
          await request.text();
          return new Response("");
        },
      });
      const result = await db.insertJsonEachRow(mappedCloseTable, source).then(
        () => ({ status: "fulfilled" as const }),
        (reason: unknown) => ({ status: "rejected" as const, reason }),
      );
      expect(result.status).toBe("rejected");
      if (result.status === "rejected") {
        expect(result.reason === nextSentinel || (result.reason as { cause?: unknown }).cause === nextSentinel).toBe(
          true,
        );
        expect(result.reason).not.toBe(closeSentinel);
      }
      expect(returnCalls).toBe(1);
    });

    it("F2 CONTROL: typed mapping failure is preserved when cleanup also fails", async () => {
      const closeSentinel = new Error("synthetic cleanup failure");
      let returnCalls = 0;
      const source: AsyncIterable<Record<string, unknown>> = {
        [Symbol.asyncIterator]: () => ({
          next: async () => ({ done: false as const, value: { logicalId: 1, id: 1 } }),
          return: () => {
            returnCalls += 1;
            throw closeSentinel;
          },
        }),
      };
      const db = clickhouseClient({
        host,
        fetch: async (request) => {
          await request.text();
          return new Response("");
        },
      });
      await expect(db.insertJsonEachRow(mappedCloseTable, source)).rejects.toMatchObject({
        kind: "client_validation",
      });
      expect(returnCalls).toBe(1);
    });

    it("F2 CONTROL: typed transport failure is preserved when cleanup also fails", async () => {
      const original = new TypeError("synthetic failed upload fetch");
      const closeSentinel = new Error("synthetic cleanup failure");
      let returnCalls = 0;
      const source: AsyncIterable<Record<string, unknown>> = {
        [Symbol.asyncIterator]: () => ({
          next: async () => ({ done: false as const, value: { logicalId: 1 } }),
          return: () => {
            returnCalls += 1;
            throw closeSentinel;
          },
        }),
      };
      const db = clickhouseClient({
        host,
        fetch: () => {
          throw original;
        },
      });
      await expect(db.insertJsonEachRow(mappedCloseTable, source)).rejects.toMatchObject({
        kind: "request_failed",
        cause: original,
      });
      expect(returnCalls).toBe(1);
    });

    for (const thrown of [undefined, null] as const) {
      it(`F2 CONTROL: typed next() ${String(thrown)} throw is preserved when cleanup also fails`, async () => {
        const closeSentinel = new Error("synthetic cleanup failure");
        let returnCalls = 0;
        let nextCalls = 0;
        const source: AsyncIterable<Record<string, unknown>> = {
          [Symbol.asyncIterator]: () => ({
            next: async () => {
              nextCalls += 1;
              if (nextCalls === 1) {
                return { done: false as const, value: { logicalId: 1 } };
              }
              throw thrown;
            },
            return: () => {
              returnCalls += 1;
              throw closeSentinel;
            },
          }),
        };
        const db = clickhouseClient({
          host,
          fetch: async (request) => {
            await request.text();
            return new Response("");
          },
        });
        const result = await db.insertJsonEachRow(mappedCloseTable, source).then(
          () => ({ status: "fulfilled" as const }),
          (reason: unknown) => ({ status: "rejected" as const, reason }),
        );
        expect(result.status).toBe("rejected");
        if (result.status === "rejected") {
          expect(result.reason === thrown || (result.reason as { cause?: unknown }).cause === thrown).toBe(true);
          expect(result.reason).not.toBe(closeSentinel);
        }
        expect(returnCalls).toBe(1);
      });
    }

    for (const mode of ["invalid-query-id", "aborted-queued-request"] as const) {
      it(`F2: typed ${mode} does not acquire the source iterator`, async () => {
        let acquired = 0;
        let reads = 0;
        let uploadFetches = 0;
        const source: AsyncIterable<Record<string, unknown>> = {
          [Symbol.asyncIterator]() {
            acquired += 1;
            return {
              async next() {
                reads += 1;
                return { done: true as const, value: undefined };
              },
              async return() {
                return { done: true as const, value: undefined };
              },
            };
          },
        };
        const holderStarted = Promise.withResolvers<void>();
        const releaseHolder = Promise.withResolvers<void>();
        const db = clickhouseClient({
          host,
          session_id: "audit_mapper_acquisition_session",
          session_max_concurrent_requests: 1,
          fetch: async (request) => {
            if (new URL(request.url).searchParams.get("query_id") === "audit_mapper_hold") {
              holderStarted.resolve();
              await releaseHolder.promise;
            } else {
              uploadFetches += 1;
            }
            return new Response("");
          },
        });
        const holder =
          mode === "aborted-queued-request" ? db.command(sql`SELECT 1`, { query_id: "audit_mapper_hold" }) : undefined;
        try {
          if (holder) {
            await holderStarted.promise;
          }
          const abort = new AbortController();
          const insert = db
            .insertJsonEachRow(mappedCloseTable, source, {
              query_id: mode === "invalid-query-id" ? "invalid query id" : "audit_mapper_waiter",
              abort_signal: abort.signal,
            })
            .then(
              () => ({ status: "fulfilled" as const }),
              () => ({ status: "rejected" as const }),
            );
          if (mode === "aborted-queued-request") {
            abort.abort(new Error("synthetic canceled waiter"));
          }
          const outcome = await insert;
          expect(outcome.status).toBe("rejected");
          expect(acquired).toBe(0);
          expect(reads).toBe(0);
          expect(uploadFetches).toBe(0);
        } finally {
          releaseHolder.resolve();
          await holder;
        }
      });
    }
  });

  describe("SEC-004 ClickHouse exception framing", () => {
    it("SEC-004 RED: legal JSON values are not treated as ClickHouse exceptions", () => {
      expect(extractClickHouseException('{"note":"__exception__"}')).toBeUndefined();
      expect(extractClickHouseException('{"__exception__":false}')).toBeUndefined();
      expect(extractClickHouseException('{"nested":{"note":"see __exception__ later"}}')).toBeUndefined();
    });

    it("SEC-004 RED: protocol exception frames are still detected", () => {
      expect(extractClickHouseException("Code: 62. DB::Exception: syntax\n")).toEqual(
        expect.objectContaining({ responseText: expect.stringContaining("DB::Exception") }),
      );
      expect(extractClickHouseException('{"id":1}\nCode: 47. DB::Exception: no column\n')).toEqual(
        expect.objectContaining({ clickhouseCode: 47 }),
      );
    });

    for (const mode of modes) {
      it(`SEC-004 RED: ${mode} preserves ordinary marker text and following rows`, async () => {
        const db = clickhouseClient({
          host,
          fetch: () => new Response(responseBody(mode, markerRows), { status: 200 }),
        });
        await expect(readRows(db, mode)).resolves.toEqual(markerRows);
      });

      it(`SEC-004 CONTROL: ${mode} decodes normal strings and quoted exception text`, async () => {
        const db = clickhouseClient({
          host,
          fetch: () => new Response(responseBody(mode, controlRows), { status: 200 }),
        });
        await expect(readRows(db, mode)).resolves.toEqual(controlRows);
      });

      it(`SEC-004 CONTROL: ${mode} still rejects a real HTTP-200 exception frame`, async () => {
        const exception = "Code: 60. DB::Exception: synthetic missing table (UNKNOWN_TABLE)";
        const body = mode.endsWith("stream") ? `${JSON.stringify(controlRows[0])}\n${exception}\n` : exception;
        const db = clickhouseClient({
          host,
          fetch: () => new Response(body, { status: 200 }),
        });
        await expect(readRows(db, mode)).rejects.toMatchObject({
          kind: "request_failed",
          httpStatus: 200,
          clickhouseCode: 60,
          clickhouseName: "UNKNOWN_TABLE",
        });
      });
    }

    it("SEC-004 RED: standalone ClickHouse exception marker frames are rejected", async () => {
      const tag = "0123456789abcdef";
      const message = "request interrupted\n";
      const frame = `\r\n__exception__\r\n${tag}\r\n${message}${message.length} ${tag}\r\n__exception__\r\n`;
      expect(extractClickHouseException(frame)).toEqual(
        expect.objectContaining({ responseText: expect.stringContaining("__exception__") }),
      );
      let rejected = false;
      try {
        await readValidatedResponseText({
          response: new Response(frame),
          queryId: "marker-review",
          ignoreErrorResponse: false,
        });
      } catch (error) {
        rejected = isClickHouseORMError(error) && error.kind === "request_failed";
      }
      expect(rejected).toBe(true);
    });

    it("SEC-004 RED: eager command and stream reject standalone marker frames", async () => {
      const tag = "0123456789abcdef";
      const message = "request interrupted\n";
      const frame = `\r\n__exception__\r\n${tag}\r\n${message}${message.length} ${tag}\r\n__exception__\r\n`;
      const db = clickhouseClient({
        host,
        fetch: () => new Response(frame, { status: 200 }),
      });
      await expect(db.execute(sql`SELECT 1`)).rejects.toMatchObject({ kind: "request_failed" });
      await expect(db.command(sql`SELECT 1`)).rejects.toMatchObject({ kind: "request_failed" });
      await expect(
        (async () => {
          for await (const _row of db.stream(sql`SELECT 1`)) {
            return;
          }
        })(),
      ).rejects.toMatchObject({ kind: "request_failed" });
    });

    it("SEC-004 RED: custom JSON parse still rejects a standalone marker line", () => {
      const response = new Response("", { status: 200 });
      let parsed = false;
      try {
        parseJsonEachRowLine({
          line: "__exception__",
          response,
          queryId: "q",
          json: {
            parse: () => {
              parsed = true;
              throw new Error("custom parser should not see exception frames");
            },
            stringify: JSON.stringify,
          },
        });
        throw new Error("expected protocol exception");
      } catch (error) {
        expect(parsed).toBe(false);
        expect(isClickHouseORMError(error)).toBe(true);
        expect(error).toMatchObject({ kind: "request_failed", responseText: "__exception__" });
      }
    });

    it("SEC-004 RED: incomplete JSON still fails without the marker shortcut", async () => {
      const db = clickhouseClient({
        host,
        fetch: () => new Response('{"id":', { status: 200 }),
      });
      await expect(db.execute(sql`SELECT 1`)).rejects.toMatchObject({ kind: "request_failed" });
    });

    it("SEC-004 RED: custom JSON parse still rejects protocol exception frames", () => {
      const response = new Response("", { status: 200 });
      try {
        parseJsonEachRowLine({
          line: "Code: 62. DB::Exception: syntax",
          response,
          queryId: "q",
          json: {
            parse: () => {
              throw new Error("custom parser should not see exception frames");
            },
            stringify: JSON.stringify,
          },
        });
        throw new Error("expected protocol exception");
      } catch (error) {
        expect(isClickHouseORMError(error)).toBe(true);
        expect(error).toMatchObject({ kind: "request_failed", clickhouseCode: 62 });
      }
    });

    it("SEC-004 CONTROL: custom JSON parse still decodes ordinary marker text", async () => {
      const text = JSON.stringify({ data: [{ note: "__exception__" }] });
      await expect(
        readValidatedResponseText({
          response: new Response(text, { status: 200 }),
          queryId: "q",
          ignoreErrorResponse: false,
        }),
      ).resolves.toBe(text);
    });
  });

  describe("SEC-006 connection URL errors", () => {
    for (const [label, databaseUrl] of invalidDsns) {
      it(`SEC-006 RED: ${label} construction errors omit credential markers from every serialization channel`, () => {
        let error: unknown;
        let fetchCalls = 0;
        try {
          clickhouseClient({
            databaseUrl,
            fetch: () => {
              fetchCalls += 1;
              throw new Error("Unexpected fixture network request");
            },
          });
        } catch (caught) {
          error = caught;
        }
        expect(error).toBeInstanceOf(Error);
        expect(fetchCalls).toBe(0);
        expect(leakingChannels(serializeError(error))).toEqual([]);
        expect(error).toMatchObject({ kind: "client_validation", executionState: "not_sent" });
        expect(error).not.toHaveProperty("input");
        expect(error).not.toHaveProperty("cause");
      });
    }

    it("SEC-006 CONTROL: separate credentials do not enter a malformed-host error", () => {
      let error: unknown;
      try {
        clickhouseClient({ host: "http://127.0.0.1:INVALID", username: dsnUsername, password: dsnMarker });
      } catch (caught) {
        error = caught;
      }
      expect(error).toBeInstanceOf(Error);
      expect(leakingChannels(serializeError(error))).toEqual([]);
    });

    it("SEC-006 CONTROL: valid DSN removes userinfo while preserving authentication", async () => {
      let request: Request | undefined;
      const db = clickhouseClient({
        databaseUrl: validDsn,
        fetch: (incoming) => {
          request = incoming;
          return Response.json({ data: [{ ok: 1 }] });
        },
      });
      await expect(db.execute(sql`SELECT 1 AS ok`)).resolves.toEqual([{ ok: 1 }]);
      expect(request).toBeInstanceOf(Request);
      if (!request) {
        throw new Error("Expected a captured Request");
      }
      expect(request.url).not.toContain(dsnMarker);
      expect(new URL(request.url).username).toBe("");
      expect(new URL(request.url).password).toBe("");
      expect(request.headers.get("authorization")).toBe(
        `Basic ${Buffer.from(`${dsnUsername}:${dsnMarker}`).toString("base64")}`,
      );
    });

    it("SEC-006 RED: Bun and Node subprocess serialization stay safe", async () => {
      const dir = await mkdtemp(join(tmpdir(), "ck-orm-dsn-"));
      const outfile = join(dir, "dsn-fixture.mjs");
      try {
        const build = spawnSync(
          process.execPath,
          ["--no-env-file", "--no-install", "build", fixtureRoot, "--outfile", outfile, "--target=node"],
          {
            encoding: "utf8",
            timeout: 8_000,
            env: { PATH: process.env.PATH ?? "", NO_COLOR: "1" },
          },
        );
        expect(build.error, build.error?.message).toBeUndefined();
        expect(build.status, build.stderr).toBe(0);
        for (const [runtime, command] of [
          ["bun", [process.execPath, "--no-env-file", "--no-install", fixtureRoot, "--ck-orm-fixture", "invalid-port"]],
          ["node", ["node", outfile, "--ck-orm-fixture", "invalid-port"]],
          [
            "bun-host",
            [
              process.execPath,
              "--no-env-file",
              "--no-install",
              fixtureRoot,
              "--ck-orm-fixture",
              "separate-credentials",
            ],
          ],
          ["node-host", ["node", outfile, "--ck-orm-fixture", "separate-credentials"]],
        ] as const) {
          const result = spawnSync(command[0], command.slice(1), {
            encoding: "utf8",
            timeout: 8_000,
            env: { PATH: process.env.PATH ?? "", NO_COLOR: "1" },
          });
          expect(result.error, `${runtime} spawn`).toBeUndefined();
          expect(result.signal, `${runtime} ${result.stderr}`).toBeNull();
          expect(result.status, `${runtime} status ${result.stderr}`).toBe(0);
          const payload = JSON.parse(result.stdout) as {
            serialized: ReturnType<typeof serializeError>;
            kind: string;
            executionState: string;
          };
          expect(leakingChannels(payload.serialized), runtime).toEqual([]);
          expect(payload.kind).toBe("client_validation");
          expect(payload.executionState).toBe("not_sent");
        }
      } finally {
        await rm(dir, { recursive: true, force: true });
      }
    });
  });

  describe("REL-005 HTTP header merge", () => {
    for (const item of headerCases) {
      it(`REL-005 ${item.kind}: Request ${item.label}`, async () => {
        const requests: Request[] = [];
        const db = clickhouseClient(
          headerClientConfig(host, item, (request) => {
            requests.push(request);
            return new Response("");
          }),
        );
        await db.runInSession((session) => session.command(sql`SELECT 1`, { http_headers: item.request }), {
          session_id: headerSessionId,
          http_headers: item.scope,
        });
        expect(requests).toHaveLength(1);
        const request = requests[0];
        expect(request).toBeInstanceOf(Request);
        expect(new URL(request.url).searchParams.get("session_id")).toBe(headerSessionId);
        expect(request.headers.get("authorization")).toBe(expectedAuthorization);
        expect(request.headers.get("x-clickhouse-replica-tag")).toBe(headerSessionId);
        if (item.shared) {
          expect(request.headers.get("x-shared")).toBe(item.shared);
        }
      });
    }

    it("REL-005 RED: loopback Request construction preserves replica tag and request headers", async () => {
      const seen: Array<{ replica: string | null; authorization: string | null; shared: string | null }> = [];
      const server = createServer((req, res) => {
        seen.push({
          replica: req.headers["x-clickhouse-replica-tag"]?.toString() ?? null,
          authorization: req.headers.authorization ?? null,
          shared: req.headers["x-shared"]?.toString() ?? null,
        });
        res.writeHead(200, { "content-type": "application/json" });
        res.end("");
      });
      const loopback = await new Promise<string>((resolve) => {
        server.listen(0, "127.0.0.1", () => {
          const address = server.address();
          if (address && typeof address === "object") {
            resolve(`http://127.0.0.1:${address.port}`);
          }
        });
      });
      try {
        for (const item of headerCases) {
          const db = clickhouseClient(headerClientConfig(loopback, item));
          await db.runInSession((session) => session.command(sql`SELECT 1`, { http_headers: item.request }), {
            session_id: headerSessionId,
            http_headers: item.scope,
          });
          const last = seen.at(-1);
          expect(last?.replica, item.label).toBe(headerSessionId);
          expect(last?.authorization, item.label).toBe(expectedAuthorization);
          if (item.shared) {
            expect(last?.shared, item.label).toBe(item.shared);
          }
        }
      } finally {
        await new Promise<void>((resolve, reject) => {
          server.close((error) => (error ? reject(error) : resolve()));
        });
      }
    });
  });
});
