import { expect, it } from "bun:test";
import { csql } from "./ck-orm";
import { createE2EDb } from "./shared";
import { describeE2E, expectClickhouseError } from "./test-helpers";

const sleepQuery = csql`SELECT sleep(1) AS slept, 1 AS value`;

const createId = (prefix: string) => `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

const measureElapsedMs = async <TValue>(operation: () => Promise<TValue>) => {
  const startedAt = performance.now();
  const result = await operation();
  return {
    elapsedMs: performance.now() - startedAt,
    result,
  };
};

const expectSleepRows = (rowsList: ReadonlyArray<ReadonlyArray<Record<string, unknown>>>) => {
  expect(rowsList).toHaveLength(2);
  for (const rows of rowsList) {
    expect(rows).toHaveLength(1);
    expect(Number(rows[0]?.value)).toBe(1);
  }
};

const expectSerializedDuration = (elapsedMs: number, baselineMs: number) => {
  expect(elapsedMs).toBeGreaterThan(baselineMs + Math.max(baselineMs * 0.6, 600));
};

const expectParallelDuration = (elapsedMs: number, baselineMs: number) => {
  expect(elapsedMs).toBeLessThan(baselineMs + Math.max(baselineMs * 0.4, 500));
};

describeE2E("ck-orm e2e session concurrency", function describeSessionConcurrency() {
  it("serializes concurrent execute requests that share an explicit session_id", async function testExplicitSessionSerialization() {
    const db = createE2EDb();
    const sharedSessionId = createId("shared_session");
    const { elapsedMs: baselineMs } = await measureElapsedMs(() =>
      db.execute(sleepQuery, {
        query_id: createId("baseline"),
        session_id: sharedSessionId,
      }),
    );

    const { elapsedMs, result } = await measureElapsedMs(() =>
      Promise.all([
        db.execute(sleepQuery, {
          query_id: createId("same_session_q1"),
          session_id: sharedSessionId,
        }),
        db.execute(sleepQuery, {
          query_id: createId("same_session_q2"),
          session_id: sharedSessionId,
        }),
      ]),
    );

    expectSleepRows(result);
    expectSerializedDuration(elapsedMs, baselineMs);
  });

  it("does not block different explicit session_id values", async function testDifferentSessionsRemainParallel() {
    const db = createE2EDb();
    const { elapsedMs: baselineMs } = await measureElapsedMs(() =>
      db.execute(sleepQuery, {
        query_id: createId("baseline"),
        session_id: createId("baseline_session"),
      }),
    );

    const { elapsedMs, result } = await measureElapsedMs(() =>
      Promise.all([
        db.execute(sleepQuery, {
          query_id: createId("different_session_q1"),
          session_id: createId("parallel_session_a"),
        }),
        db.execute(sleepQuery, {
          query_id: createId("different_session_q2"),
          session_id: createId("parallel_session_b"),
        }),
      ]),
    );

    expectSleepRows(result);
    expectParallelDuration(elapsedMs, baselineMs);
  });

  it("serializes requests that inherit the client default session_id", async function testClientDefaultSessionSerialization() {
    const defaultSessionId = createId("client_default_session");
    const db = createE2EDb({
      session_id: defaultSessionId,
    });
    const { elapsedMs: baselineMs } = await measureElapsedMs(() =>
      db.execute(sleepQuery, {
        query_id: createId("baseline"),
      }),
    );

    const { elapsedMs, result } = await measureElapsedMs(() =>
      Promise.all([
        db.execute(sleepQuery, {
          query_id: createId("client_default_q1"),
        }),
        db.execute(sleepQuery, {
          query_id: createId("client_default_q2"),
        }),
      ]),
    );

    expectSleepRows(result);
    expectSerializedDuration(elapsedMs, baselineMs);
  });

  it("surfaces ClickHouse session locking when same-session local concurrency is raised above one", async function testClickHouseSessionLockBoundary() {
    const db = createE2EDb({
      session_max_concurrent_requests: 2,
    });
    const sharedSessionId = createId("parallel_same_session");
    const results = await Promise.allSettled([
      db.execute(sleepQuery, {
        query_id: createId("parallel_same_session_q1"),
        session_id: sharedSessionId,
      }),
      db.execute(sleepQuery, {
        query_id: createId("parallel_same_session_q2"),
        session_id: sharedSessionId,
      }),
    ]);

    const fulfilled = results.filter((result) => result.status === "fulfilled");
    const rejected = results.filter((result) => result.status === "rejected");

    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect((fulfilled[0] as PromiseFulfilledResult<Record<string, unknown>[]>).value).toHaveLength(1);

    const rejectedError = (rejected[0] as PromiseRejectedResult).reason;
    expectClickhouseError(rejectedError, {
      kind: "request_failed",
      executionState: "rejected",
      clickhouseCode: 373,
      clickhouseName: "SESSION_IS_LOCKED",
      sessionId: sharedSessionId,
    });
    expect((rejectedError as Error).message).toContain("SESSION_IS_LOCKED");
  });
});
