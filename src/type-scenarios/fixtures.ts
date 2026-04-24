import { chTable, chType, csql } from "../index";

export const logicalMetrics = chTable(
  "logical_metrics",
  {
    userId: chType.string("user_id"),
    metricValue: chType.decimal("metric_value", { precision: 20, scale: 5 }),
    createdAt: chType.dateTime64("created_at", { precision: 9 }),
    tags: chType.array("tag_names", chType.string()),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.userId, table.createdAt],
  }),
);

export const activityMetricLog = chTable(
  "activity_metric_log",
  {
    id: chType.int32(),
    userId: chType.string("user_id"),
    groupId: chType.int32("group_id"),
    itemId: chType.uint64("item_id"),
    metricValue: chType.decimal("metric_value", { precision: 20, scale: 5 }),
    channel: chType.int8(),
    status: chType.int8(),
    eventType: chType.string("event_type"),
    createdAt: chType.dateTime64("created_at", { precision: 6 }),
    eventDate: chType.date("event_date"),
    payload: chType.json<{ labels: string[] }>(),
    _peerdb_is_deleted: chType.int8(),
    _peerdb_version: chType.uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.userId, table.createdAt, table.id],
    versionColumn: table._peerdb_version,
  }),
);

export const activityLedger = chTable("activity_ledger", {
  system_id: chType.string(),
  source_kind: chType.enum8<"alpha" | "beta">({ alpha: 1, beta: 2 }),
  event_id: chType.uint64(),
  batch_id: chType.int32(),
  entity_id: chType.int32(),
  actor_id: chType.int32(),
  event_time: chType.dateTime64({ precision: 6 }),
  event_phase: chType.int8(),
  action_kind: chType.int8(),
  observed_value: chType.decimal({ precision: 18, scale: 5 }),
  delta_value: chType.decimal({ precision: 18, scale: 5 }),
  _peerdb_is_deleted: chType.int8(),
  _peerdb_version: chType.uint64(),
});

export const workflowEntity = chTable(
  "workflow_entity",
  {
    workflowId: chType.string("workflow_id"),
    itemId: chType.uint64("item_id"),
    userId: chType.string("user_id"),
    itemCode: chType.string("item_code"),
    quantity: chType.decimal({ precision: 18, scale: 5 }),
    createdAt: chType.dateTime64("created_at", { precision: 6 }),
    note: chType.nullable(chType.string()),
    status: chType.int8(),
    _peerdb_is_deleted: chType.int8(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.workflowId, table.createdAt],
  }),
);

export const workflowEvent = chTable(
  "workflow_event",
  {
    workflowId: chType.string("workflow_id"),
    itemId: chType.uint64("item_id"),
    userId: chType.string("user_id"),
    completedAt: chType.nullable(chType.dateTime64("completed_at", { precision: 6 })),
    correctionScore: chType.float64("correction_score"),
    quantity: chType.decimal({ precision: 18, scale: 5 }),
    note: chType.nullable(chType.string()),
    status: chType.int8(),
    processedAt: chType.dateTime64("processed_at", { precision: 6 }),
    _peerdb_is_deleted: chType.int8(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.workflowId, table.processedAt],
  }),
);

export const petOwners = chTable("pet_owners", {
  id: chType.int32(),
  name: chType.string(),
});

export const pets = chTable("pets", {
  id: chType.int32(),
  ownerId: chType.int32("owner_id"),
  petName: chType.string("pet_name"),
});

export const tempMetricScope = chTable("tmp_metric_scope", {
  userId: chType.string("user_id").default(csql`'anonymous'`),
  groupId: chType.int32("group_id"),
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
