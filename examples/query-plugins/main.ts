/**
 * Query plugins
 *
 * A plugin sees every request ck-orm sends to ClickHouse, including the CREATE and DROP of a session temporary table.
 * This one adds a header to each request and logs the statements in the order they ran.
 */
import type { ClickHouseORMClient, ClickHouseORMPlugin } from "ck-orm";
import { ckTable, ckType, clickhouseClient, fn } from "ck-orm";

const readings = ckTable("readings", {
  probeId: ckType.string("probe_id"),
  signal: ckType.float64(),
});

// The headers `onQueryStart` returns go out with that request.
export const requestLog = () => {
  const statements: string[] = [];
  const plugin: ClickHouseORMPlugin = {
    name: "request-log",
    onQueryStart: () => ({ http_headers: { "x-request-source": "billing-worker" } }),
    onQuerySuccess: (event) => {
      statements.push(event.querySummary);
    },
    onQueryError: (event) => {
      statements.push(`${event.querySummary} failed`);
    },
  };
  return { plugin, statements };
};

// A session temporary table: the example needs no tables on the server.
export const run = (db: ClickHouseORMClient) =>
  db.runInSession(async (session) => {
    await session.createTemporaryTable(readings);
    await session.insert(readings).values([
      { probeId: "alpha", signal: 1.5 },
      { probeId: "alpha", signal: 2.5 },
      { probeId: "beta", signal: 4 },
    ]);

    return session
      .select({
        probeId: readings.probeId,
        average: fn.avg(readings.signal).as("average"),
      })
      .from(readings)
      .groupBy(readings.probeId)
      .orderBy(readings.probeId);
  });

if (import.meta.main) {
  const { plugin, statements } = requestLog();
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
    plugins: [plugin],
  });
  console.log(await run(db));
  // [{ probeId: "alpha", average: 2 }, { probeId: "beta", average: 4 }]
  console.log(statements);
  // ["CREATE", "INSERT readings", "SELECT readings", "DROP"]
}
