/**
 * Trace list with filters and a total
 *
 * Modeled on Langfuse's traces table, which filters and orders tracing data stored
 * in ClickHouse:
 * https://langfuse.com/self-hosting/upgrade/upgrade-guides/upgrade-v2-to-v3
 * The data below is synthetic.
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ck, ckTable, ckType, clickhouseClient } from "ck-orm";

const traces = ckTable("traces", {
  projectId: ckType.string("project_id"),
  traceId: ckType.string("trace_id"),
  name: ckType.string(),
  userId: ckType.string("user_id"),
  tags: ckType.array(ckType.string()),
  latencyMs: ckType.float64("latency_ms"),
  totalCost: ckType.decimal("total_cost", { precision: 18, scale: 6 }),
  timestamp: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
});

export type TraceFilters = {
  name?: string;
  userId?: string;
  tag?: string;
  minLatencyMs?: number;
  from?: Date;
  to?: Date;
  page?: number;
  pageSize?: number;
};

// A session temporary table: no persistent tables or schema changes.
export const run = (db: ClickHouseORMClient, filters: TraceFilters = {}) => {
  const { page = 1, pageSize = 2 } = filters;
  return db.runInSession(async (session) => {
    await session.createTemporaryTable(traces);
    await session.insert(traces).values([
      {
        projectId: "acme",
        traceId: "t1",
        name: "chat",
        userId: "alice",
        tags: ["prod"],
        latencyMs: 820,
        totalCost: "0.001200",
        timestamp: new Date("2026-03-01T10:00:00.000Z"),
      },
      {
        projectId: "acme",
        traceId: "t2",
        name: "chat",
        userId: "bob",
        tags: ["prod", "beta"],
        latencyMs: 2400,
        totalCost: "0.004500",
        timestamp: new Date("2026-03-01T10:05:00.000Z"),
      },
      {
        projectId: "acme",
        traceId: "t3",
        name: "summarize",
        userId: "alice",
        tags: ["prod"],
        latencyMs: 1500,
        totalCost: "0.002000",
        timestamp: new Date("2026-03-01T10:10:00.000Z"),
      },
      {
        projectId: "acme",
        traceId: "t4",
        name: "chat",
        userId: "alice",
        tags: ["staging"],
        latencyMs: 3100,
        totalCost: "0.006100",
        timestamp: new Date("2026-03-01T10:15:00.000Z"),
      },
      {
        projectId: "acme",
        traceId: "t5",
        name: "chat",
        userId: "carol",
        tags: [],
        latencyMs: 450,
        totalCost: "0.000700",
        timestamp: new Date("2026-03-01T10:15:00.000Z"),
      },
      {
        projectId: "other",
        traceId: "x1",
        name: "chat",
        userId: "alice",
        tags: ["prod"],
        latencyMs: 9000,
        totalCost: "0.100000",
        timestamp: new Date("2026-03-01T10:20:00.000Z"),
      },
    ]);

    // ck-orm skips undefined predicates. Testing with !== undefined keeps 0 and "" as
    // real filter values.
    const where = [
      ck.eq(traces.projectId, "acme"),
      filters.name !== undefined ? ck.eq(traces.name, filters.name) : undefined,
      filters.userId !== undefined ? ck.eq(traces.userId, filters.userId) : undefined,
      filters.tag !== undefined ? ck.has(traces.tags, filters.tag) : undefined,
      filters.minLatencyMs !== undefined
        ? ck.gte(traces.latencyMs, filters.minLatencyMs)
        : undefined,
      filters.from !== undefined ? ck.gte(traces.timestamp, filters.from) : undefined,
      filters.to !== undefined ? ck.lt(traces.timestamp, filters.to) : undefined,
    ];

    // traceId breaks ties between rows that share a timestamp, so pages are stable.
    const rows = await session
      .select({
        traceId: traces.traceId,
        name: traces.name,
        userId: traces.userId,
        latencyMs: traces.latencyMs,
        totalCost: traces.totalCost,
        timestamp: traces.timestamp,
      })
      .from(traces)
      .where(...where)
      .orderBy(ck.desc(traces.timestamp), ck.asc(traces.traceId))
      .limit(pageSize)
      .offset((page - 1) * pageSize);

    // The count uses the same filters and ignores LIMIT/OFFSET.
    const totalRows = await session.select({ total: session.count(traces, ...where) });
    const total = totalRows[0]?.total ?? 0;

    return { total, rows };
  });
};

if (import.meta.main) {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  });
  console.log(await run(db));
  // {
  //   total: 5,
  //   rows: [
  //     { traceId: "t4", name: "chat", userId: "alice", latencyMs: 3100,
  //       totalCost: "0.0061", timestamp: 2026-03-01T10:15:00.000Z },
  //     { traceId: "t5", name: "chat", userId: "carol", latencyMs: 450,
  //       totalCost: "0.0007", timestamp: 2026-03-01T10:15:00.000Z }
  //   ]
  // }
}
