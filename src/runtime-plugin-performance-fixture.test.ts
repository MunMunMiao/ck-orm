import assert from "node:assert/strict";
import { ckSql, clickhouseClient } from "./public_api";
import {
  controlledSql,
  observeCost,
  queryOptions,
  statementOfSize,
  table,
  zeroCost,
} from "./runtime-plugin-performance-support.test";

const main = async () => {
  const observed = process.argv.includes("--observed");
  const unobserved = process.argv.includes("--unobserved");
  if (observed === unobserved) {
    throw new Error("ck-orm fixture: plugin mode must be exactly one of --observed or --unobserved");
  }
  const requests: Array<{ queryId: string | null; sessionId: string | null }> = [];
  const starts: string[] = [];
  const successes: Array<{ executionId: string; rowCount?: number }> = [];
  let active = 0;
  let peakActive = 0;
  const db = clickhouseClient({
    host: "http://127.0.0.1:1",
    plugins: observed
      ? [
          {
            name: "capture",
            onQueryStart(event) {
              starts.push(event.executionId);
              return { http_headers: { "x-audit-perf006": "observed" } };
            },
            onQuerySuccess(event) {
              successes.push({ executionId: event.executionId, rowCount: event.rowCount });
            },
          },
        ]
      : [],
    fetch: async (request) => {
      const url = new URL(request.url);
      assert.equal(url.searchParams.get("session_id"), "audit_perf006_e2e");
      assert.equal(request.headers.get("x-clickhouse-replica-tag"), "audit_perf006_e2e");
      assert.equal(request.headers.get("x-audit-perf006"), observed ? "observed" : null);
      requests.push({ queryId: url.searchParams.get("query_id"), sessionId: url.searchParams.get("session_id") });
      active += 1;
      peakActive = Math.max(active, peakActive);
      const body = await request.text();
      await Promise.resolve();
      active -= 1;
      if (body.endsWith("FORMAT JSONEachRow")) {
        return new Response('{"n":1}\n{"n":2}\n');
      }
      if (body.endsWith("FORMAT JSON")) {
        return new Response('{"data":[{"n":1},{"n":2}],"rows":2}');
      }
      return new Response("");
    },
  });
  const { value, cost } = await observeCost(() =>
    db.runInSession(
      async (session) => {
        const eager = await session.execute(controlledSql(statementOfSize(4096)), queryOptions);
        const typed = await session.select().from(table).execute(queryOptions);
        const streamed = await Array.fromAsync(session.stream(ckSql`SELECT n /* audit_perf006 */`, queryOptions));
        await Promise.all([
          session.command(ckSql`SELECT 1 /* audit_perf006 */`, queryOptions),
          session.command(ckSql`SELECT 2 /* audit_perf006 */`, queryOptions),
        ]);
        return { eager, typed, streamed };
      },
      { session_id: "audit_perf006_e2e" },
    ),
  );
  assert.deepEqual(value, { eager: [{ n: 1 }, { n: 2 }], typed: [{ n: 1 }, { n: 2 }], streamed: [{ n: 1 }, { n: 2 }] });
  assert.equal(requests.length, 5);
  assert.equal(peakActive, 1);
  for (const request of requests) {
    assert.equal(request.queryId, queryOptions.query_id);
  }
  if (observed) {
    assert.equal(new Set(starts).size, 5);
    assert.deepEqual(
      successes.map((event) => event.executionId),
      starts,
    );
    assert.deepEqual(
      successes.map((event) => event.rowCount),
      [2, 2, 2, undefined, undefined],
    );
    assert.equal(cost.randomUuids, 5);
    assert.equal(cost.sqlEncodeCalls, 5);
  }
  process.stdout.write(
    `${JSON.stringify({
      id: "PERF-006",
      layer: "full-sdk-subprocess",
      observed,
      completedOperations: requests.length,
      peakActive,
      equivalent: true,
      cost,
    })}\n`,
  );
  if (!observed) {
    assert.deepEqual(
      cost,
      zeroCost,
      "PERF-006 RED: full SDK session does not allocate or hash unconsumed observation events",
    );
  }
};

if (import.meta.main && process.argv[2] === "--ck-orm-fixture") {
  main().catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`);
    process.exitCode = 1;
  });
}
