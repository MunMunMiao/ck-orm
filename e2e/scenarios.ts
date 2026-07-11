import { ckSql, ckTable, ckType } from "./ck-orm";

export const clickhouseLogPlatform = ckTable(
  "scenario_clickhouse_log_platform",
  {
    timestamp: ckType.dateTime64({ precision: 9, timezone: "UTC" }).codec(ckSql`Delta(8), ZSTD(1)`),
    event_date: ckType.date(),
    event_time: ckType.dateTime(),
    trace_id: ckType.string().codec(ckSql`ZSTD(1)`),
    span_id: ckType.string().codec(ckSql`ZSTD(1)`),
    trace_flags: ckType.uint32().codec(ckSql`ZSTD(1)`),
    severity_text: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    severity_number: ckType.int32().codec(ckSql`ZSTD(1)`),
    service_name: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    body: ckType.string().codec(ckSql`ZSTD(1)`),
    namespace: ckType.lowCardinality(ckType.string()),
    cell: ckType.lowCardinality(ckType.string()),
    cloud_provider: ckType.lowCardinality(ckType.string()),
    region: ckType.lowCardinality(ckType.string()),
    container_name: ckType.lowCardinality(ckType.string()),
    pod_name: ckType.lowCardinality(ckType.string()),
    logger_name: ckType.lowCardinality(ckType.string()),
    log_level: ckType.lowCardinality(ckType.string()),
    scope_attributes: ckType.map(ckType.string(), ckType.string()).codec(ckSql`ZSTD(1)`),
    resource_attributes: ckType.map(ckType.string(), ckType.string()).codec(ckSql`ZSTD(1)`),
    log_attributes: ckType.map(ckType.string(), ckType.string()).codec(ckSql`ZSTD(1)`),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: table.event_date,
    orderBy: [table.pod_name, table.timestamp],
    ttl: ckSql`${ckSql.identifier("event_time")} + toIntervalDay(180)`,
    settings: { index_granularity: 8192, ttl_only_drop_parts: 1 },
    comment: "Synthetic OpenTelemetry log table",
  }),
);

export const cdpUserEvents = ckTable(
  "scenario_cdp_user_events",
  {
    event_id: ckType.uuid(),
    event_time: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    event_date: ckType.date().materialized(ckSql`toDate(event_time)`),
    user_id: ckType.uint64(),
    session_id: ckType.string(),
    event_type: ckType.lowCardinality(ckType.string()),
    page_url: ckType.string(),
    referrer: ckType.lowCardinality(ckType.string()),
    product_id: ckType.nullable(ckType.uint32()),
    category_id: ckType.nullable(ckType.uint16()),
    search_query: ckType.string(),
    device_type: ckType.lowCardinality(ckType.string()),
    country: ckType.lowCardinality(ckType.string()),
    properties: ckType.map(ckType.string(), ckType.string()),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: table.event_date,
    orderBy: [table.user_id, table.event_date, table.event_type, table.event_time],
    ttl: ckSql`${ckSql.identifier("event_date")} + toIntervalDay(730)`,
  }),
);

export const cdpOrders = ckTable(
  "scenario_cdp_orders",
  {
    order_id: ckType.string(),
    updated_at: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    user_id: ckType.uint64(),
    order_date: ckType.date(),
    status: ckType.lowCardinality(ckType.string()),
    total_amount: ckType.decimal({ precision: 18, scale: 4 }),
    currency: ckType.lowCardinality(ckType.string()),
    item_count: ckType.uint16(),
    items: ckType.array(ckType.uint32()),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    versionColumn: table.updated_at,
    partitionBy: ckSql`toYYYYMM(${ckSql.identifier("order_date")})`,
    orderBy: [table.user_id, table.order_id],
  }),
);

export const newjsonEvents = ckTable(
  "scenario_newjson_events",
  {
    id: ckType.uint64(),
    received_at: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    payload: ckType.json<{
      readonly user_id: string;
      readonly action: string;
      readonly revenue?: number;
      readonly session: { readonly id: string; readonly tier: number };
    }>("payload", {
      maxDynamicPaths: 256,
      maxDynamicTypes: 16,
      typeHints: {
        user_id: ckType.uint64(),
        "session.tier": ckType.uint8(),
      },
      skip: ["debug"],
      skipRegexp: ["^_tmp_"],
    }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.received_at, table.id],
  }),
);

export const scenarioSchema = {
  clickhouseLogPlatform,
  cdpUserEvents,
  cdpOrders,
  newjsonEvents,
};

export type ScenarioSchema = typeof scenarioSchema;
