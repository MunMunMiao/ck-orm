/**
 * Dynamic signals
 *
 * Adapted from Darwinium's official model-signal Map use case:
 * https://clickhouse.com/blog/fast-feature-rich-and-mutable-clickhouse-powers-darwiniums-security-and-fraud-analytics-use-cases
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ck, ckSql, ckTable, ckType, clickhouseClient, fn } from "ck-orm";

const interactions = ckTable("interactions", {
  interactionId: ckType.string("interaction_id"),
  signals: ckType.map(ckType.string(), ckType.float64()),
});

// Model names and thresholds change independently of the Map column's schema.
export const run = (
  db: ClickHouseORMClient,
  { signal = "new_model", threshold = 0.8 }: { signal?: string; threshold?: number } = {},
) =>
  db.runInSession(async (session) => {
    await session.createTemporaryTable(interactions);
    await session.insert(interactions).values([
      { interactionId: "above", signals: { new_model: 0.9, legacy_model: 0.2 } },
      { interactionId: "boundary", signals: { new_model: 0.8 } },
      { interactionId: "below", signals: { new_model: 0.79 } },
      { interactionId: "zero", signals: { new_model: 0 } },
      { interactionId: "missing", signals: { legacy_model: 0.99 } },
      { interactionId: "empty", signals: {} },
    ]);

    const score = ckSql<number>`${interactions.signals}[${signal}]`;
    // A missing key reads as 0, not NULL: existence and the score are separate checks.
    // At >= 0.8, only above (0.9) and boundary (0.8) qualify; zero is a real score.
    return session
      .select({ interactionId: interactions.interactionId, score })
      .from(interactions)
      .where(
        ck.eq(fn.call<number>("mapContains", interactions.signals, signal), 1),
        ck.gte(score, threshold),
      )
      .orderBy(ck.asc(interactions.interactionId));
  });

if (import.meta.main) {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  });
  const rows = await run(db);
  console.log(rows);
  // [
  //   { interactionId: "above", score: 0.9 },
  //   { interactionId: "boundary", score: 0.8 }
  // ]
}
