import {
  ckSql,
  ckTable,
  ckType,
  type InferInsertModel,
  type InferSelectModel,
  type InferSelectSchema,
} from "../ck-orm";

export const probeTelemetry = ckTable(
  "probe_telemetry",
  {
    id: ckType.int32(),
    probeId: ckType.string("probe_id"),
    missionId: ckType.int32("mission_id"),
    sampleId: ckType.int64("sample_id"),
    signalStrength: ckType.decimal("signal_strength", { precision: 20, scale: 5 }),
    status: ckType.int16(),
    observedDay: ckType.date("observed_day"),
    missionDay: ckType.date32("mission_day"),
    createdAt: ckType.dateTime64("created_at", { precision: 3, timezone: "UTC" }),
    deletedAt: ckType.nullable("deleted_at", ckType.dateTime64({ precision: 3, timezone: "UTC" })),
    tags: ckType.array(ckType.string()),
    metadata: ckType.json<{
      alerts?: string[];
      risk?: {
        score?: number;
        level?: string;
      };
      samples?: Array<{
        id: string;
        channel: string;
      }>;
    }>(),
    position: ckType.tuple("position", ckType.float64(), ckType.float64()),
    components: ckType.nested("components", {
      componentId: ckType.string(),
      reading: ckType.decimal({ precision: 18, scale: 5 }),
      flagged: ckType.uint8(),
    }),
    ingestedAt: ckType.dateTime64("_ingested_at", { precision: 9, timezone: "UTC" }),
    ingestVersion: ckType.uint64("_ingest_version"),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    partitionBy: ckSql`toYYYYMM(${table.createdAt})`,
    orderBy: [table.probeId, table.createdAt, table.id],
    versionColumn: table.ingestVersion,
  }),
);

export const probeSignalRollup = ckTable(
  "probe_signal_rollups",
  {
    probeId: ckType.string("probe_id"),
    bucketStart: ckType.dateTime64("bucket_start", { precision: 3, timezone: "UTC" }),
    bucketDay: ckType.date("bucket_day"),
    totalSignalStrength: ckType.decimal("total_signal_strength", { precision: 38, scale: 5 }),
    sampleCount: ckType.uint64("sample_count"),
    updatedAt: ckType.dateTime64("updated_at", { precision: 3, timezone: "UTC" }),
    ingestVersion: ckType.uint64("_ingest_version"),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    partitionBy: ckSql`toYYYYMM(${table.bucketStart})`,
    orderBy: [table.probeId, table.bucketStart],
    versionColumn: table.ingestVersion,
  }),
);

export const probeSchema = {
  probeSignalRollup,
  probeTelemetry,
};

export type ProbeTelemetryRow = InferSelectModel<typeof probeTelemetry>;
export type ProbeTelemetryInsert = InferInsertModel<typeof probeTelemetry>;
export type ProbeSignalRollupRow = InferSelectModel<typeof probeSignalRollup>;
export type ProbeSignalRollupInsert = InferInsertModel<typeof probeSignalRollup>;
export type ProbeRows = InferSelectSchema<typeof probeSchema>;
