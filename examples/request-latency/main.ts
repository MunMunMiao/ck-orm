/**
 * Request latency percentiles
 *
 * Insert-triggered materialized aggregation.
 * Inspired by Inigo's materialized-view analytics in the ClickHouse blog.
 * quantilesExact is deterministic for these small fixtures and retains all samples;
 * it is a demonstration choice, not a recommendation for production memory usage.
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ck, ckTable, ckType, clickhouseClient, fn } from "ck-orm";

const requests = ckTable(
  "ck_orm_example_latency_requests",
  {
    tenantId: ckType.string("tenant_id"),
    operation: ckType.string(),
    occurredAt: ckType.dateTime64("occurred_at", { precision: 3, timezone: "UTC" }),
    status: ckType.uint16(),
    latencyMs: ckType.float64("latency_ms"),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.tenantId, table.operation, table.occurredAt],
  }),
);

const minutes = ckTable(
  "ck_orm_example_latency_minutes",
  {
    tenantId: ckType.string("tenant_id"),
    operation: ckType.string(),
    minute: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    requestsState: ckType.aggregateFunction("requests_state", { name: "count" }),
    averageState: ckType.aggregateFunction("average_state", {
      name: "avg",
      args: [ckType.float64()],
    }),
    latencyState: ckType.aggregateFunction("latency_state", {
      name: "quantilesExact(0.95, 0.99)",
      args: [ckType.float64()],
    }),
  },
  (table) => ({
    engine: "AggregatingMergeTree",
    orderBy: [table.tenantId, table.operation, table.minute],
  }),
);
const viewName = "ck_orm_example_latency_mv";

// Persistent tables are required. Use an isolated database; do not run concurrently.
export const run = async (db: ClickHouseORMClient) => {
  const created: string[] = [];
  const errors: unknown[] = [];
  let rows: {
    tenantId: string;
    operation: string;
    minute: Date;
    requests: string;
    averageMs: number;
    p95Ms: number;
    p99Ms: number;
  }[] = [];
  try {
    await db.createTable(requests);
    created.push(requests.originalName);

    await db.createTable(minutes);
    created.push(minutes.originalName);

    const minute = fn.call<Date>(
      "toStartOfInterval",
      requests.occurredAt,
      fn.toIntervalMinute(1),
      "UTC",
    );
    await db.createMaterializedView(viewName, {
      to: minutes,
      as: db
        .select({
          tenantId: requests.tenantId,
          operation: requests.operation,
          minute,
          requestsState: fn.call<string>("countState"),
          averageState: fn.call<string>("avgState", requests.latencyMs),
          latencyState: fn.withParams<string>(
            "quantilesExactState",
            [0.95, 0.99],
            requests.latencyMs,
          ),
        })
        .from(requests)
        .where(ck.gte(requests.status, 200), ck.lt(requests.status, 300))
        .groupBy(requests.tenantId, requests.operation, minute),
    });
    created.push(viewName);

    const samples = Array.from({ length: 21 }, (_, index) => ({
      tenantId: "alpha",
      operation: "Search",
      occurredAt: new Date(
        index === 20 ? "2026-01-01T00:00:59.999Z" : "2026-01-01T00:00:00Z",
      ),
      status: index === 20 ? 299 : 200,
      latencyMs: index + 1,
    }));
    await db.insert(requests).values([
      ...samples.slice(0, 10),
      {
        tenantId: "alpha",
        operation: "GetUser",
        status: 200,
        occurredAt: new Date("2026-01-01T00:00:15Z"),
        latencyMs: 100,
      },
      {
        tenantId: "beta",
        operation: "Search",
        status: 200,
        occurredAt: new Date("2026-01-01T00:00:20Z"),
        latencyMs: 500,
      },
      {
        tenantId: "alpha",
        operation: "Search",
        status: 200,
        occurredAt: new Date("2026-01-01T00:01:00Z"),
        latencyMs: 5,
      },
      {
        tenantId: "alpha",
        operation: "Search",
        status: 200,
        occurredAt: new Date("2025-12-31T23:59:59.999Z"),
        latencyMs: 9999,
      },
      {
        tenantId: "alpha",
        operation: "Search",
        status: 199,
        occurredAt: new Date("2026-01-01T00:00:30Z"),
        latencyMs: 9999,
      },
    ]);
    // Unequal block sizes also prove that avgMerge weights the samples correctly.
    await db.insert(requests).values([
      ...samples.slice(10),
      {
        tenantId: "alpha",
        operation: "GetUser",
        status: 200,
        occurredAt: new Date("2026-01-01T00:00:45Z"),
        latencyMs: 200,
      },
      {
        tenantId: "beta",
        operation: "Search",
        status: 200,
        occurredAt: new Date("2026-01-01T00:00:50Z"),
        latencyMs: 700,
      },
      {
        tenantId: "alpha",
        operation: "Search",
        status: 200,
        occurredAt: new Date("2026-01-01T00:01:59.999Z"),
        latencyMs: 15,
      },
      {
        tenantId: "beta",
        operation: "Search",
        status: 200,
        occurredAt: new Date("2026-01-01T00:01:00Z"),
        latencyMs: 40,
      },
      {
        tenantId: "alpha",
        operation: "Search",
        status: 200,
        occurredAt: new Date("2026-01-01T00:02:00Z"),
        latencyMs: 9999,
      },
      {
        tenantId: "alpha",
        operation: "Search",
        status: 300,
        occurredAt: new Date("2026-01-01T00:00:30Z"),
        latencyMs: 9999,
      },
      {
        tenantId: "alpha",
        operation: "Search",
        status: 500,
        occurredAt: new Date("2026-01-01T00:00:30Z"),
        latencyMs: 9999,
      },
    ]);

    // Keep the plural aggregate family and the same levels as the stored state.
    const quantiles = fn.withParams<number[]>(
      "quantilesExactMerge",
      [0.95, 0.99],
      minutes.latencyState,
    );
    rows = await db
      .select({
        tenantId: minutes.tenantId,
        operation: minutes.operation,
        minute: minutes.minute,
        requests: fn.toString(fn.call("countMerge", minutes.requestsState)),
        averageMs: fn.call<number>("avgMerge", minutes.averageState),
        p95Ms: fn.arrayElement<number>(quantiles, 1),
        p99Ms: fn.arrayElement<number>(quantiles, 2),
      })
      .from(minutes)
      .where(
        ck.and(
          ck.gte(minutes.minute, new Date("2026-01-01T00:00:00Z")),
          ck.lt(minutes.minute, new Date("2026-01-01T00:02:00Z")),
        ),
      )
      .groupBy(minutes.tenantId, minutes.operation, minutes.minute)
      .orderBy(
        ck.asc(minutes.tenantId),
        ck.asc(minutes.operation),
        ck.asc(minutes.minute),
      );
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
    throw new AggregateError(errors, "Request latency example failed during cleanup");
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
  //     tenantId: "alpha", operation: "GetUser", minute: 2026-01-01T00:00:00.000Z,
  //     requests: "2", averageMs: 150, p95Ms: 200, p99Ms: 200
  //   },
  //   {
  //     tenantId: "alpha", operation: "Search", minute: 2026-01-01T00:00:00.000Z,
  //     requests: "21", averageMs: 11, p95Ms: 20, p99Ms: 21
  //   },
  //   {
  //     tenantId: "alpha", operation: "Search", minute: 2026-01-01T00:01:00.000Z,
  //     requests: "2", averageMs: 10, p95Ms: 15, p99Ms: 15
  //   },
  //   {
  //     tenantId: "beta", operation: "Search", minute: 2026-01-01T00:00:00.000Z,
  //     requests: "2", averageMs: 600, p95Ms: 700, p99Ms: 700
  //   },
  //   {
  //     tenantId: "beta", operation: "Search", minute: 2026-01-01T00:01:00.000Z,
  //     requests: "1", averageMs: 40, p95Ms: 40, p99Ms: 40
  //   }
  // ]
}
