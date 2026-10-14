import { ckTable, ckType, clickhouseClient, fn } from "../index";

const events = ckTable("usage_events", {
  tenantId: ckType.string("tenant_id"),
  occurredAt: ckType.dateTime64("occurred_at", { precision: 3, timezone: "UTC" }),
  units: ckType.uint64(),
});

const minutes = ckTable(
  "usage_minutes",
  {
    tenantId: ckType.string("tenant_id"),
    minute: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    unitsState: ckType.aggregateFunction("units_state", {
      name: "sum",
      args: [ckType.uint64()],
    }),
  },
  (table) => ({
    engine: "AggregatingMergeTree",
    orderBy: [table.tenantId, table.minute],
  }),
);

const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123",
});

const minute = fn.call<Date>("toStartOfMinute", events.occurredAt);

await db.createTable(events);
await db.createTable(events, { mode: "if_not_exists" });
await db.createTable(events, { mode: "or_replace" });
await db.dropTable(events);
await db.dropTable(events, { ifExists: true, sync: true });
await db.dropTable("usage_minutes_mv", { sync: true });

await db.createMaterializedView("usage_minutes_mv", {
  to: minutes,
  as: db
    .select({
      tenantId: events.tenantId,
      minute,
      unitsState: fn.call<string>("sumState", events.units),
    })
    .from(events)
    .groupBy(events.tenantId, minute),
});

await db.createMaterializedView("usage_minutes_mv_untyped", {
  to: minutes,
  mode: "if_not_exists",
  as: db
    .select({
      tenantId: events.tenantId,
      minute,
      unitsState: fn.call("sumState", events.units),
    })
    .from(events)
    .groupBy(events.tenantId, minute),
});

await db.runInSession(async (session) => {
  await session.createTable(events, { mode: "if_not_exists" });
  await session.createMaterializedView("usage_minutes_mv", {
    to: minutes,
    as: session
      .select({
        tenantId: events.tenantId,
        minute,
        unitsState: fn.call<string>("sumState", events.units),
      })
      .from(events)
      .groupBy(events.tenantId, minute),
  });
  await session.dropTable("usage_minutes_mv", { ifExists: true, sync: true });
});

await db.createMaterializedView("usage_minutes_missing", {
  to: minutes,
  // @ts-expect-error createMaterializedView requires every required target column
  as: db
    .select({
      tenantId: events.tenantId,
      minute,
    })
    .from(events),
});

await db.createMaterializedView("usage_minutes_unknown", {
  to: minutes,
  // @ts-expect-error createMaterializedView rejects a select key the target table does not have
  as: db
    .select({
      tenantId: events.tenantId,
      minute,
      unitsState: fn.call<string>("sumState", events.units),
      extra: events.units,
    })
    .from(events)
    .groupBy(events.tenantId, minute),
});

await db.createMaterializedView("usage_minutes_replace", {
  to: minutes,
  as: db
    .select({
      tenantId: events.tenantId,
      minute,
      unitsState: fn.call<string>("sumState", events.units),
    })
    .from(events)
    .groupBy(events.tenantId, minute),
  // @ts-expect-error materialized views have no or_replace mode
  mode: "or_replace",
});
