import { ckSql, ckTable, ckType, type InferInsertModel, type InferSelectModel, type InferSelectSchema } from "./ck-orm";
import type { commerceSchema, orderRewardLog } from "./schema/commerce";

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
    peerdbVersion: ckType.uint64("_peerdb_version"),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    partitionBy: ckSql`toYYYYMM(${table.createdAt})`,
    orderBy: [table.actorId, table.createdAt, table.id],
    versionColumn: table.peerdbVersion,
    settings: {
      index_granularity: 8192,
    },
    comment: "Example audit stream table",
  }),
);

export const logicalRewardEvent = ckTable("logical_reward_events", {
  userId: ckType.string("user_id"),
  rewardPoints: ckType.decimal("reward_points", { precision: 20, scale: 5 }),
  createdAt: ckType.dateTime64("created_at", { precision: 3, timezone: "UTC" }),
});

export const columnNameShowcase = ckTable("column_name_showcase", {
  id: ckType.int32(),
  userId: ckType.string("user_id"),
  rewardPoints: ckType.decimal("reward_points", { precision: 20, scale: 5 }),
  fixedCode: ckType.fixedString("fixed_code", { length: 8 }),
  tags: ckType.array("tags", ckType.string()),
  attrs: ckType.map("attrs", ckType.string(), ckType.string()),
  embedding: ckType.qbit("embedding", ckType.float32(), { dimensions: 8 }),
  // The outer Nested column is "line_items"; nested field names come from shape keys.
  lineItems: ckType.nested("line_items", {
    productSku: ckType.string(),
    quantity: ckType.float64(),
  }),
  rewardSumState: ckType.aggregateFunction("reward_sum_state", {
    name: "sum",
    args: [ckType.decimal({ precision: 20, scale: 5 })],
  }),
  rewardSum: ckType.simpleAggregateFunction("reward_sum", {
    name: "sum",
    value: ckType.decimal({ precision: 20, scale: 5 }),
  }),
});

export const aggregateFunctionNameExample = ckTable("aggregate_function_name_example", {
  // Here "sum" is the ClickHouse aggregate function name. The column names come from the object keys.
  rewardSumState: ckType.aggregateFunction("sum", ckType.uint64()),
  rewardSum: ckType.simpleAggregateFunction("sum", ckType.uint64()),
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
  actorId: "user_100",
  action: "updated",
  payload: {
    message: "Reward status changed",
    before: 0,
    after: 1,
  },
  labels: ["reward", "status"],
  amountDelta: "42.50000",
  createdAt: new Date("2026-04-24T00:00:00.000Z"),
  peerdbVersion: "1",
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
