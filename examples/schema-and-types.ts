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
    amount_delta: chType.nullable(chType.decimal(18, 5)),
    created_at: chType.dateTime64(3, "UTC"),
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

export type AuditEventRow = InferSelectModel<typeof auditEvent>;
export type AuditEventInsert = InferInsertModel<typeof auditEvent>;
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

export const useInferredRows = (row: CommerceRows["orderRewardLog"]): RewardLogRow => {
  return row;
};
