/**
 * Left join
 *
 * `leftJoin()` sends `join_use_nulls = 1`, so unmatched right-side columns are `NULL` in ClickHouse and `null` in the row type.
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ck, ckTable, ckType, clickhouseClient } from "ck-orm";

const probes = ckTable("probes", {
  probeId: ckType.string("probe_id"),
});

const missions = ckTable("missions", {
  probeId: ckType.string("probe_id"),
  name: ckType.string(),
});

// Session temporary tables: the example needs no tables on the server.
export const run = (db: ClickHouseORMClient) =>
  db.runInSession(async (session) => {
    await session.createTemporaryTable(probes);
    await session.createTemporaryTable(missions);
    await session.insert(probes).values([{ probeId: "alpha" }, { probeId: "beta" }]);
    await session.insert(missions).values([{ probeId: "alpha", name: "Voyager" }]);

    return session
      .select({
        probeId: probes.probeId,
        mission: missions.name,
      })
      .from(probes)
      .leftJoin(missions, ck.eq(probes.probeId, missions.probeId))
      .orderBy(probes.probeId)
      .limit(10);
  });

if (import.meta.main) {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  });
  const rows = await run(db);
  console.log(rows);
  // [
  //   { probeId: "alpha", mission: "Voyager" },
  //   { probeId: "beta", mission: null },
  // ]
}
