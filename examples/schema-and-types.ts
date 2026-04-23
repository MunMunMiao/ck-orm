import { chTable, chType, csql, type InferInsertModel, type InferSelectModel, type InferSelectSchema } from "./ck-orm";
import type { commerceSchema, orderRewardLog } from "./schema/commerce";

export const auditEvent = chTable(
  "audit_events",
  {
    id: chType.uuid().comment("Application event id"),
    actor_id: chType.lowCardinality(chType.string()),
    action: chType.enum8<"created" | "updated" | "deleted">({
      created: 1,
      updated: 2,
      deleted: 3,
    }),
    payload: chType.json<Record<string, unknown>>(),
    labels: chType.array(chType.string()).default(csql`[]`),
    amount_delta: chType.nullable(chType.decimal({ precision: 18, scale: 5 })),
    created_at: chType.dateTime64({ precision: 3, timezone: "UTC" }),
    created_day: chType.date().materialized(csql`toDate(created_at)`),
    search_text: chType.string().aliasExpr(csql`lowerUTF8(JSONExtractString(payload, 'message'))`),
    _peerdb_version: chType.uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    partitionBy: csql`toYYYYMM(${table.created_at})`,
    orderBy: [table.actor_id, table.created_at, table.id],
    versionColumn: table._peerdb_version,
    settings: {
      index_granularity: 8192,
    },
    comment: "Example audit stream table",
  }),
);

export const logicalRewardEvent = chTable("logical_reward_events", {
  userId: chType.string("user_id"),
  rewardPoints: chType.decimal("reward_points", { precision: 20, scale: 5 }),
  createdAt: chType.dateTime64("created_at", { precision: 3, timezone: "UTC" }),
});

export const columnNameShowcase = chTable("column_name_showcase", {
  id: chType.int32(),
  userId: chType.string("user_id"),
  rewardPoints: chType.decimal("reward_points", { precision: 20, scale: 5 }),
  fixedCode: chType.fixedString("fixed_code", { length: 8 }),
  tags: chType.array("tags", chType.string()),
  attrs: chType.map("attrs", chType.string(), chType.string()),
  embedding: chType.qbit("embedding", chType.float32(), { dimensions: 8 }),
  // The outer Nested column is "line_items"; nested field names come from shape keys.
  lineItems: chType.nested("line_items", {
    productSku: chType.string(),
    quantity: chType.float64(),
  }),
  rewardSumState: chType.aggregateFunction("reward_sum_state", {
    name: "sum",
    args: [chType.decimal({ precision: 20, scale: 5 })],
  }),
  rewardSum: chType.simpleAggregateFunction("reward_sum", {
    name: "sum",
    value: chType.decimal({ precision: 20, scale: 5 }),
  }),
});

export const aggregateFunctionNameExample = chTable("aggregate_function_name_example", {
  // Here "sum" is the ClickHouse aggregate function name. The column names come from the object keys.
  rewardSumState: chType.aggregateFunction("sum", chType.uint64()),
  rewardSum: chType.simpleAggregateFunction("sum", chType.uint64()),
});

export type AuditEventRow = InferSelectModel<typeof auditEvent>;
export type AuditEventInsert = InferInsertModel<typeof auditEvent>;
export type LogicalRewardEventRow = InferSelectModel<typeof logicalRewardEvent>;
export type LogicalRewardEventInsert = InferInsertModel<typeof logicalRewardEvent>;
export type ColumnNameShowcaseRow = InferSelectModel<typeof columnNameShowcase>;
export type ColumnNameShowcaseInsert = InferInsertModel<typeof columnNameShowcase>;
export type CommerceRows = InferSelectSchema<typeof commerceSchema>;
export type RewardLogRow = typeof orderRewardLog.$inferSelect;
export type RewardLogInsert = typeof orderRewardLog.$inferInsert;

export const exampleAuditInsert = {
  id: "018fc4c4-7d57-7112-812a-8c8c36d0f8c1",
  actor_id: "user_100",
  action: "updated",
  payload: {
    message: "Reward status changed",
    before: 0,
    after: 1,
  },
  labels: ["reward", "status"],
  amount_delta: "42.50000",
  created_at: new Date("2026-04-24T00:00:00.000Z"),
  _peerdb_version: "1",
} satisfies Partial<AuditEventInsert>;

export const exampleLogicalRewardInsert = {
  userId: "user_100",
  rewardPoints: "42.50000",
  createdAt: new Date("2026-04-24T00:00:00.000Z"),
} satisfies LogicalRewardEventInsert;

export const useColumnNameShowcaseRow = (row: ColumnNameShowcaseRow): string => {
  return row.userId;
};

export const useInferredRows = (row: CommerceRows["orderRewardLog"]): RewardLogRow => {
  return row;
};
