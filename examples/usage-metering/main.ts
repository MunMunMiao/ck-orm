/**
 * Usage metering with insert-triggered materialized aggregation.
 * Inspired by https://openmeter.io/blog/how-openmeter-uses-clickhouse-for-usage-metering
 * These fixtures and SQL are a minimal demonstration, not OpenMeter's implementation.
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ck, ckSql, ckTable, ckType, clickhouseClient, fn } from "ck-orm";

const events = ckTable("ck_orm_example_usage_events", {
  tenantId: ckType.string("tenant_id"),
  userId: ckType.string("user_id"),
  meter: ckType.string(),
  occurredAt: ckType.dateTime64("occurred_at", { precision: 3, timezone: "UTC" }),
  units: ckType.uint64(),
});

const minutes = ckTable("ck_orm_example_usage_minutes", {
  tenantId: ckType.string("tenant_id"),
  userId: ckType.string("user_id"),
  meter: ckType.string(),
  minute: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
  usageState: ckType.aggregateFunction("usage_state", {
    name: "sum",
    args: [ckType.uint64()],
  }),
  eventsState: ckType.aggregateFunction("events_state", { name: "count" }),
});
const viewName = "ck_orm_example_usage_mv";

// Persistent tables are required. Use an isolated database; do not run concurrently.
export const run = async (db: ClickHouseORMClient) => {
  const created: string[] = [];
  const errors: unknown[] = [];
  let rows: {
    tenantId: string;
    userId: string;
    minute: Date;
    usage: string;
    events: string;
  }[] = [];
  try {
    await db.command(ckSql`CREATE TABLE ${events}
(
  tenant_id String,
  user_id String,
  meter String,
  occurred_at DateTime64(3, 'UTC'),
  units UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, user_id, meter, occurred_at)`);
    created.push(events.originalName);

    await db.command(ckSql`CREATE TABLE ${minutes}
(
  tenant_id String,
  user_id String,
  meter String,
  minute DateTime64(3, 'UTC'),
  usage_state AggregateFunction(sum, UInt64),
  events_state AggregateFunction(count)
)
ENGINE = AggregatingMergeTree
ORDER BY (tenant_id, user_id, meter, minute)`);
    created.push(minutes.originalName);

    await db.command(ckSql`CREATE MATERIALIZED VIEW ${ckSql.identifier(viewName)}
TO ${minutes} AS
SELECT tenant_id, user_id, meter,
  toStartOfInterval(occurred_at, INTERVAL 1 MINUTE, 'UTC') AS minute,
  sumState(units) AS usage_state,
  countState() AS events_state
FROM ${events}
GROUP BY tenant_id, user_id, meter, minute`);
    created.push(viewName);

    await db.insert(events).values([
      {
        tenantId: "alpha",
        userId: "alex",
        meter: "api_tokens",
        occurredAt: new Date("2026-01-01T00:00:00Z"),
        units: "2",
      },
      {
        tenantId: "alpha",
        userId: "alex",
        meter: "api_tokens",
        occurredAt: new Date("2026-01-01T00:00:59.999Z"),
        units: "3",
      },
      {
        tenantId: "alpha",
        userId: "blair",
        meter: "api_tokens",
        occurredAt: new Date("2026-01-01T00:00:15Z"),
        units: "7",
      },
      {
        tenantId: "beta",
        userId: "alex",
        meter: "api_tokens",
        occurredAt: new Date("2026-01-01T00:00:20Z"),
        units: "11",
      },
      {
        tenantId: "alpha",
        userId: "alex",
        meter: "storage_bytes",
        occurredAt: new Date("2026-01-01T00:00:30Z"),
        units: "13",
      },
      {
        tenantId: "alpha",
        userId: "alex",
        meter: "api_tokens",
        occurredAt: new Date("2025-12-31T23:59:59.999Z"),
        units: "1000",
      },
    ]);
    // A second insert contributes another state to the same minute key.
    await db.insert(events).values([
      {
        tenantId: "alpha",
        userId: "alex",
        meter: "api_tokens",
        occurredAt: new Date("2026-01-01T00:00:30Z"),
        units: "5",
      },
      {
        tenantId: "alpha",
        userId: "blair",
        meter: "api_tokens",
        occurredAt: new Date("2026-01-01T00:01:00Z"),
        units: "17",
      },
      {
        tenantId: "beta",
        userId: "alex",
        meter: "api_tokens",
        occurredAt: new Date("2026-01-01T00:01:59.999Z"),
        units: "19",
      },
      {
        tenantId: "alpha",
        userId: "alex",
        meter: "api_tokens",
        occurredAt: new Date("2026-01-01T00:01:00Z"),
        units: "23",
      },
      {
        tenantId: "alpha",
        userId: "alex",
        meter: "api_tokens",
        occurredAt: new Date("2026-01-01T00:02:00Z"),
        units: "2000",
      },
    ]);

    // Merge states at read time, without FINAL or waiting for background merges.
    rows = await db
      .select({
        tenantId: minutes.tenantId,
        userId: minutes.userId,
        minute: minutes.minute,
        usage: fn.toString(fn.call("sumMerge", minutes.usageState)),
        events: fn.toString(fn.call("countMerge", minutes.eventsState)),
      })
      .from(minutes)
      .where(
        ck.and(
          ck.eq(minutes.meter, "api_tokens"),
          ck.gte(minutes.minute, new Date("2026-01-01T00:00:00Z")),
          ck.lt(minutes.minute, new Date("2026-01-01T00:02:00Z")),
        ),
      )
      .groupBy(minutes.tenantId, minutes.userId, minutes.minute)
      .orderBy(ck.asc(minutes.tenantId), ck.asc(minutes.userId), ck.asc(minutes.minute));
  } catch (error) {
    errors.push(error);
  } finally {
    // Only acknowledged CREATEs are owned; a colliding object is never dropped.
    for (const name of created.reverse()) {
      try {
        await db.command(ckSql`DROP TABLE ${ckSql.identifier(name)} SYNC`);
      } catch (error) {
        errors.push(error);
      }
    }
  }
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) {
    throw new AggregateError(errors, "Usage metering example failed during cleanup");
  }
  return rows;
};

if (import.meta.main) {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  });
  console.log(await run(db));
  // [
  //   {
  //     tenantId: "alpha", userId: "alex",
  //     minute: 2026-01-01T00:00:00.000Z, usage: "10", events: "3"
  //   },
  //   {
  //     tenantId: "alpha", userId: "alex",
  //     minute: 2026-01-01T00:01:00.000Z, usage: "23", events: "1"
  //   },
  //   {
  //     tenantId: "alpha", userId: "blair",
  //     minute: 2026-01-01T00:00:00.000Z, usage: "7", events: "1"
  //   },
  //   {
  //     tenantId: "alpha", userId: "blair",
  //     minute: 2026-01-01T00:01:00.000Z, usage: "17", events: "1"
  //   },
  //   {
  //     tenantId: "beta", userId: "alex",
  //     minute: 2026-01-01T00:00:00.000Z, usage: "11", events: "1"
  //   },
  //   {
  //     tenantId: "beta", userId: "alex",
  //     minute: 2026-01-01T00:01:00.000Z, usage: "19", events: "1"
  //   }
  // ]
}
