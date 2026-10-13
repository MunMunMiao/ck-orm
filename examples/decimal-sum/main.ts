/**
 * Decimal sum
 *
 * `fn.sum` over a `Decimal(20, 5)` column is cast to `Decimal(38, 5)` and decoded as a string, so totals never pass through a float.
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ck, ckTable, ckType, clickhouseClient, fn } from "ck-orm";

const probes = ckTable("probes", {
  probeId: ckType.string("probe_id"),
  signal: ckType.decimal({ precision: 20, scale: 5 }),
  status: ckType.int16(),
});

// A session temporary table: the example needs no tables on the server.
export const run = (db: ClickHouseORMClient) =>
  db.runInSession(async (session) => {
    await session.createTemporaryTable(probes);
    await session.insert(probes).values([
      { probeId: "alpha", signal: "1.10001", status: 1 },
      { probeId: "alpha", signal: "2.20002", status: 1 },
      { probeId: "beta", signal: "9.99999", status: 0 },
    ]);

    return session
      .select({
        probeId: probes.probeId,
        total: fn.sum(probes.signal).as("total"),
      })
      .from(probes)
      .where(ck.eq(probes.status, 1))
      .groupBy(probes.probeId)
      .orderBy(ck.desc(fn.sum(probes.signal)))
      .limit(20);
  });

if (import.meta.main) {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  });
  const rows = await run(db);
  console.log(rows);
  // [{ probeId: "alpha", total: "3.30003" }]
}
