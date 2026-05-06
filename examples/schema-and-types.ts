import { ckSql, ckTable, ckType, type InferInsertModel, type InferSelectModel, type InferSelectSchema } from "./ck-orm";
import type { probeSchema, probeTelemetry } from "./schema/probe";

export const auditEvent = ckTable(
  "audit_events",
  {
    id: ckType.uuid().comment("Application event id"),
    actorId: ckType.lowCardinality("actor_id", ckType.string()),
    action: ckType.enum8<"created" | "updated" | "deleted">({
      created: 1,
      updated: 2,
      deleted: 3,
    }),
    payload: ckType.json<Record<string, unknown>>(),
    labels: ckType.array(ckType.string()).default(ckSql`[]`),
    amountDelta: ckType.nullable("amount_delta", ckType.decimal({ precision: 18, scale: 5 })),
    createdAt: ckType.dateTime64("created_at", { precision: 3, timezone: "UTC" }),
    createdDay: ckType.date("created_day").materialized(ckSql`toDate(created_at)`),
    searchText: ckType.string("search_text").aliasExpr(ckSql`lowerUTF8(JSONExtractString(payload, 'message'))`),
    ingestVersion: ckType.uint64("_ingest_version"),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    partitionBy: ckSql`toYYYYMM(${table.createdAt})`,
    orderBy: [table.actorId, table.createdAt, table.id],
    versionColumn: table.ingestVersion,
    settings: {
      index_granularity: 8192,
    },
    comment: "Example audit stream table",
  }),
);

export const logicalSignalEvent = ckTable("logical_signal_events", {
  probeId: ckType.string("probe_id"),
  signalStrength: ckType.decimal("signal_strength", { precision: 20, scale: 5 }),
  createdAt: ckType.dateTime64("created_at", { precision: 3, timezone: "UTC" }),
});

export const columnNameShowcase = ckTable("column_name_showcase", {
  id: ckType.int32(),
  probeId: ckType.string("probe_id"),
  signalStrength: ckType.decimal("signal_strength", { precision: 20, scale: 5 }),
  fixedCode: ckType.fixedString("fixed_code", { length: 8 }),
  tags: ckType.array("tags", ckType.string()),
  attrs: ckType.map("attrs", ckType.string(), ckType.string()),
  embedding: ckType.qbit("embedding", ckType.float32(), { dimensions: 8 }),
  // The outer Nested column is "components"; nested field names come from shape keys.
  components: ckType.nested("components", {
    componentId: ckType.string(),
    reading: ckType.float64(),
  }),
  signalSumState: ckType.aggregateFunction("signal_sum_state", {
    name: "sum",
    args: [ckType.decimal({ precision: 20, scale: 5 })],
  }),
  signalSum: ckType.simpleAggregateFunction("signal_sum", {
    name: "sum",
    value: ckType.decimal({ precision: 20, scale: 5 }),
  }),
});

export const aggregateFunctionNameExample = ckTable("aggregate_function_name_example", {
  // Here "sum" and "quantile(0.5)" are ClickHouse aggregate function names. Column names come from object keys.
  signalSumState: ckType.aggregateFunction("sum", ckType.uint64()),
  medianSignalState: ckType.aggregateFunction("quantile(0.5)", ckType.float64()),
  signalSum: ckType.simpleAggregateFunction("sum", ckType.uint64()),
});

export type AuditEventRow = InferSelectModel<typeof auditEvent>;
export type AuditEventInsert = InferInsertModel<typeof auditEvent>;
export type LogicalSignalEventRow = InferSelectModel<typeof logicalSignalEvent>;
export type LogicalSignalEventInsert = InferInsertModel<typeof logicalSignalEvent>;
export type ColumnNameShowcaseRow = InferSelectModel<typeof columnNameShowcase>;
export type ColumnNameShowcaseInsert = InferInsertModel<typeof columnNameShowcase>;
export type ProbeRows = InferSelectSchema<typeof probeSchema>;
export type ProbeTelemetryRow = typeof probeTelemetry.$inferSelect;
export type ProbeTelemetryInsert = typeof probeTelemetry.$inferInsert;

export const exampleAuditInsert = {
  id: "018fc4c4-7d57-7112-812a-8c8c36d0f8c1",
  actorId: "probe_alpha",
  action: "updated",
  payload: {
    message: "Probe status changed",
    before: 0,
    after: 1,
  },
  labels: ["probe", "status"],
  amountDelta: "42.50000",
  createdAt: new Date("2026-04-24T00:00:00.000Z"),
  ingestVersion: "1",
} satisfies Partial<AuditEventInsert>;

export const exampleLogicalSignalInsert = {
  probeId: "probe_alpha",
  signalStrength: "42.50000",
  createdAt: new Date("2026-04-24T00:00:00.000Z"),
} satisfies LogicalSignalEventInsert;

export const useColumnNameShowcaseRow = (row: ColumnNameShowcaseRow): string => {
  return row.probeId;
};

export const useInferredRows = (row: ProbeRows["probeTelemetry"]): ProbeTelemetryRow => {
  return row;
};
