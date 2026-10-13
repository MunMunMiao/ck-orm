/**
 * Literal search
 *
 * Values are sent as typed query parameters, never spliced into SQL. `ck.contains` also escapes `%` and `_`, so `"alpha%"` matches literally.
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ck, ckTable, ckType, clickhouseClient } from "ck-orm";

const probes = ckTable("probes", {
  probeId: ckType.string("probe_id"),
  seenAt: ckType.dateTime64("seen_at", { precision: 3, timezone: "UTC" }),
});

const since = new Date("2026-01-01");

// A session temporary table: the example needs no tables on the server.
export const run = (db: ClickHouseORMClient) =>
  db.runInSession(async (session) => {
    await session.createTemporaryTable(probes);
    await session.insert(probes).values([
      { probeId: "alpha%1", seenAt: new Date("2026-02-01") },
      { probeId: "alphax", seenAt: new Date("2026-02-01") },
      { probeId: "alpha%2", seenAt: new Date("2025-12-01") },
    ]);

    return session
      .select({ probeId: probes.probeId })
      .from(probes)
      .where(ck.contains(probes.probeId, "alpha%"), ck.gte(probes.seenAt, since))
      .limit(5);
  });

if (import.meta.main) {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  });
  const rows = await run(db);
  console.log(rows);
  // [{ probeId: "alpha%1" }]
}
