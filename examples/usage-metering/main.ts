/**
 * Usage metering
 *
 * Insert-triggered materialized aggregation.
 * Inspired by https://openmeter.io/blog/how-openmeter-uses-clickhouse-for-usage-metering
 * These fixtures and SQL are a minimal demonstration, not OpenMeter's implementation.
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ck, ckTable, ckType, clickhouseClient, fn } from "ck-orm";

const events = ckTable(
  "ck_orm_example_usage_events",
  {
    tenantId: ckType.string("tenant_id"),
    userId: ckType.string("user_id"),
    meter: ckType.string(),
    occurredAt: ckType.dateTime64("occurred_at", { precision: 3, timezone: "UTC" }),
    units: ckType.uint64(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.tenantId, table.userId, table.meter, table.occurredAt],
  }),
);

const minutes = ckTable(
  "ck_orm_example_usage_minutes",
  {
    tenantId: ckType.string("tenant_id"),
    userId: ckType.string("user_id"),
    meter: ckType.string(),
    minute: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    usageState: ckType.aggregateFunction("usage_state", {
      name: "sum",
      args: [ckType.uint64()],
    }),
    eventsState: ckType.aggregateFunction("events_state", { name: "count" }),
  },
  (table) => ({
    engine: "AggregatingMergeTree",
    orderBy: [table.tenantId, table.userId, table.meter, table.minute],
  }),
);
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
    await db.createTable(events);
    created.push(events.originalName);

    await db.createTable(minutes);
    created.push(minutes.originalName);

    const minute = fn.call<Date>(
      "toStartOfInterval",
      events.occurredAt,
      fn.toIntervalMinute(1),
      "UTC",
    );
    await db.createMaterializedView(viewName, {
      to: minutes,
      as: db
        .select({
          tenantId: events.tenantId,
          userId: events.userId,
          meter: events.meter,
          minute,
          usageState: fn.call<string>("sumState", events.units),
          eventsState: fn.call<string>("countState"),
        })
        .from(events)
        .groupBy(events.tenantId, events.userId, events.meter, minute),
    });
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
        await db.dropTable(name, { sync: true });
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
