import { ckSql, ckTable, ckType } from "../index";

export const logicalMetrics = ckTable(
  "logical_metrics",
  {
    userId: ckType.string("user_id"),
    metricValue: ckType.decimal("metric_value", { precision: 20, scale: 5 }),
    createdAt: ckType.dateTime64("created_at", { precision: 9 }),
    tags: ckType.array("tag_names", ckType.string()),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.userId, table.createdAt],
  }),
);

export const activityMetricLog = ckTable(
  "activity_metric_log",
  {
    id: ckType.int32(),
    userId: ckType.string("user_id"),
    groupId: ckType.int32("group_id"),
    itemId: ckType.uint64("item_id"),
    metricValue: ckType.decimal("metric_value", { precision: 20, scale: 5 }),
    channel: ckType.int8(),
    status: ckType.int8(),
    eventType: ckType.string("event_type"),
    createdAt: ckType.dateTime64("created_at", { precision: 6 }),
    eventDate: ckType.date("event_date"),
    payload: ckType.json<{ labels: string[] }>(),
    _peerdb_is_deleted: ckType.int8(),
    _peerdb_version: ckType.uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.userId, table.createdAt, table.id],
    versionColumn: table._peerdb_version,
  }),
);

export const activityLedger = ckTable("activity_ledger", {
  system_id: ckType.string(),
  source_kind: ckType.enum8<"alpha" | "beta">({ alpha: 1, beta: 2 }),
  event_id: ckType.uint64(),
  batch_id: ckType.int32(),
  entity_id: ckType.int32(),
  actor_id: ckType.int32(),
  event_time: ckType.dateTime64({ precision: 6 }),
  event_phase: ckType.int8(),
  action_kind: ckType.int8(),
  observed_value: ckType.decimal({ precision: 18, scale: 5 }),
  delta_value: ckType.decimal({ precision: 18, scale: 5 }),
  _peerdb_is_deleted: ckType.int8(),
  _peerdb_version: ckType.uint64(),
});

export const workflowEntity = ckTable(
  "workflow_entity",
  {
    workflowId: ckType.string("workflow_id"),
    itemId: ckType.uint64("item_id"),
    userId: ckType.string("user_id"),
    itemCode: ckType.string("item_code"),
    quantity: ckType.decimal({ precision: 18, scale: 5 }),
    createdAt: ckType.dateTime64("created_at", { precision: 6 }),
    note: ckType.nullable(ckType.string()),
    status: ckType.int8(),
    _peerdb_is_deleted: ckType.int8(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.workflowId, table.createdAt],
  }),
);

export const workflowEvent = ckTable(
  "workflow_event",
  {
    workflowId: ckType.string("workflow_id"),
    itemId: ckType.uint64("item_id"),
    userId: ckType.string("user_id"),
    completedAt: ckType.nullable(ckType.dateTime64("completed_at", { precision: 6 })),
    correctionScore: ckType.float64("correction_score"),
    quantity: ckType.decimal({ precision: 18, scale: 5 }),
    note: ckType.nullable(ckType.string()),
    status: ckType.int8(),
    processedAt: ckType.dateTime64("processed_at", { precision: 6 }),
    _peerdb_is_deleted: ckType.int8(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.workflowId, table.processedAt],
  }),
);

export const petOwners = ckTable("pet_owners", {
  id: ckType.int32(),
  name: ckType.string(),
});

export const pets = ckTable("pets", {
  id: ckType.int32(),
  ownerId: ckType.int32("owner_id"),
  petName: ckType.string("pet_name"),
});

export const tempMetricScope = ckTable("tmp_metric_scope", {
  userId: ckType.string("user_id").default(ckSql`'anonymous'`),
  groupId: ckType.int32("group_id"),
});

export const typeScenarioSchema = {
  activityMetricLog,
  workflowEvent,
  workflowEntity,
  logicalMetrics,
  petOwners,
  pets,
  activityLedger,
  tempMetricScope,
};
