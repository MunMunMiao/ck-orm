// Real-world ClickHouse scenarios sourced from public ClickHouse blog posts,
// project READMEs and conference talks. Each table is a faithful (but
// down-scoped) replica of a production schema; the references in JSDoc let you
// jump back to the source.
//
// These tables are intentionally co-located with the examples so application
// developers can copy any single block into their own project and have a
// runnable ck-orm `ckTable` to start from. The same definitions are imported
// by the e2e suite so we exercise them against a real ClickHouse server.
import { ckSql, ckTable, ckType, type StandardSchemaV1 } from "../ck-orm";

// ---------------------------------------------------------------------------
// A. Observability — Logs / Traces / Metrics
// ---------------------------------------------------------------------------

/**
 * ClickHouse's own 19 PiB OpenTelemetry logging platform.
 *
 * Source: https://clickhouse.com/blog/building-a-logging-platform-with-clickhouse-and-saving-millions-over-datadog
 *
 * The production schema uses SharedMergeTree (ClickHouse Cloud only) — we
 * substitute MergeTree so it runs on a single-node ClickHouse.
 */
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
    comment: "OpenTelemetry log table — based on ClickHouse's internal 19 PiB platform",
  }),
);

/**
 * SigNoz distributed traces (signoz_index_v3).
 *
 * Source: https://signoz.io/docs/userguide/writing-clickhouse-traces-query/
 */
export const signozTraces = ckTable(
  "scenario_signoz_traces",
  {
    ts_bucket_start: ckType.uint64().codec(ckSql`DoubleDelta, LZ4`),
    resource_fingerprint: ckType.string().codec(ckSql`ZSTD(1)`),
    timestamp: ckType.dateTime64({ precision: 9, timezone: "UTC" }).codec(ckSql`DoubleDelta, LZ4`),
    trace_id: ckType.fixedString({ length: 32 }).codec(ckSql`ZSTD(1)`),
    span_id: ckType.string().codec(ckSql`ZSTD(1)`),
    parent_span_id: ckType.string().codec(ckSql`ZSTD(1)`),
    flags: ckType.uint32().codec(ckSql`T64, ZSTD(1)`),
    name: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    kind: ckType.int8().codec(ckSql`T64, ZSTD(1)`),
    duration_nano: ckType.uint64().codec(ckSql`T64, ZSTD(1)`),
    status_code: ckType.int16().codec(ckSql`T64, ZSTD(1)`),
    status_message: ckType.string().codec(ckSql`ZSTD(1)`),
    service_name: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    has_error: ckType.bool().codec(ckSql`T64, ZSTD(1)`),
    response_status_code: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    http_url: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    http_method: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    http_host: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    db_name: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    db_operation: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    attributes_string: ckType.map(ckType.string(), ckType.string()).codec(ckSql`ZSTD(1)`),
    attributes_number: ckType.map(ckType.string(), ckType.float64()).codec(ckSql`ZSTD(1)`),
    attributes_bool: ckType.map(ckType.string(), ckType.bool()).codec(ckSql`ZSTD(1)`),
    resources_string: ckType.map(ckType.string(), ckType.string()).codec(ckSql`ZSTD(1)`),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`toDate(${ckSql.identifier("timestamp")})`,
    orderBy: [table.ts_bucket_start, table.resource_fingerprint, table.has_error, table.name, table.timestamp],
    ttl: ckSql`toDateTime(${ckSql.identifier("timestamp")}) + toIntervalDay(30)`,
    settings: { index_granularity: 8192, ttl_only_drop_parts: 1 },
  }),
);

/**
 * OpenTelemetry Collector ClickHouse exporter trace table.
 *
 * Source: https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse
 *
 * The original schema uses Nested Array(Tuple(...)) for events/links. We
 * keep the same array-of-tuple shape using ckType.array(ckType.tuple(...)).
 */
export const otelTraces = ckTable(
  "scenario_otel_traces",
  {
    timestamp: ckType.dateTime64({ precision: 9, timezone: "UTC" }).codec(ckSql`Delta(8), ZSTD(1)`),
    trace_id: ckType.string().codec(ckSql`ZSTD(1)`),
    span_id: ckType.string().codec(ckSql`ZSTD(1)`),
    parent_span_id: ckType.string().codec(ckSql`ZSTD(1)`),
    trace_state: ckType.string().codec(ckSql`ZSTD(1)`),
    span_name: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    span_kind: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    service_name: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    resource_attributes: ckType.map(ckType.string(), ckType.string()).codec(ckSql`ZSTD(1)`),
    span_attributes: ckType.map(ckType.string(), ckType.string()).codec(ckSql`ZSTD(1)`),
    duration_ns: ckType.int64().codec(ckSql`ZSTD(1)`),
    status_code: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    status_message: ckType.string().codec(ckSql`ZSTD(1)`),
    event_timestamps: ckType.array(ckType.dateTime64({ precision: 9, timezone: "UTC" })).codec(ckSql`ZSTD(1)`),
    event_names: ckType.array(ckType.lowCardinality(ckType.string())).codec(ckSql`ZSTD(1)`),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`toDate(${ckSql.identifier("timestamp")})`,
    orderBy: [
      table.service_name,
      table.span_name,
      ckSql`toUnixTimestamp(${ckSql.identifier("timestamp")})`,
      table.trace_id,
    ],
    ttl: ckSql`toDateTime(${ckSql.identifier("timestamp")}) + toIntervalDay(3)`,
    settings: { index_granularity: 8192, ttl_only_drop_parts: 1 },
  }),
);

/**
 * SigNoz metric samples (Prometheus-compatible time series store).
 *
 * Source: https://signoz.io/docs/userguide/write-a-metrics-clickhouse-query/
 */
export const signozMetricsSamples = ckTable(
  "scenario_signoz_metrics_samples",
  {
    env: ckType.lowCardinality(ckType.string()).default("'default'"),
    temporality: ckType.lowCardinality(ckType.string()).default("'Unspecified'"),
    metric_name: ckType.lowCardinality(ckType.string()),
    fingerprint: ckType.uint64().codec(ckSql`Delta(8), ZSTD(1)`),
    unix_milli: ckType.int64().codec(ckSql`DoubleDelta, ZSTD(1)`),
    value: ckType.float64().codec(ckSql`Gorilla, ZSTD(1)`),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`toDate(fromUnixTimestamp64Milli(${ckSql.identifier("unix_milli")}))`,
    orderBy: [table.env, table.temporality, table.metric_name, table.fingerprint, table.unix_milli],
    ttl: ckSql`toDateTime(fromUnixTimestamp64Milli(${ckSql.identifier("unix_milli")})) + toIntervalDay(30)`,
    settings: { index_granularity: 8192 },
  }),
);

/**
 * Highlight.io application logs.
 *
 * Source: https://highlight.io/blog/how-we-built-logging-with-clickhouse
 */
export const highlightLogs = ckTable(
  "scenario_highlight_logs",
  {
    timestamp: ckType.dateTime64({ precision: 9, timezone: "UTC" }),
    trace_id: ckType.string(),
    span_id: ckType.string(),
    trace_flags: ckType.uint32(),
    severity_text: ckType.lowCardinality(ckType.string()),
    severity_number: ckType.int32(),
    service_name: ckType.lowCardinality(ckType.string()),
    body: ckType.string(),
    resource_attributes: ckType.map(ckType.string(), ckType.string()),
    log_attributes: ckType.map(ckType.string(), ckType.string()),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`toDate(${ckSql.identifier("timestamp")})`,
    orderBy: [
      table.service_name,
      table.severity_text,
      ckSql`toUnixTimestamp(${ckSql.identifier("timestamp")})`,
      table.trace_id,
    ],
    ttl: ckSql`${ckSql.identifier("timestamp")} + toIntervalDay(30)`,
    settings: { ttl_only_drop_parts: 1 },
  }),
);

/**
 * Uber's schema-agnostic log platform with parallel KV arrays.
 *
 * Source: https://www.uber.com/en-NL/blog/logging/
 *
 * The hot fields (request_id, user_id, trip_id) are materialized columns
 * extracted from the `string_values[indexOf(string_keys, '...')]` pattern.
 */
export const uberSchemaAgnosticLogs = ckTable(
  "scenario_uber_logs",
  {
    // Uber's real schema uses `_source` / `_namespace` / `level` as physical
    // column names. ck-orm logical keys allow leading underscores, so we keep
    // them identical to the physical names to avoid any indirection.
    _source: ckType.string().codec(ckSql`ZSTD(3)`),
    _namespace: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    timestamp: ckType.dateTime64({ precision: 9, timezone: "UTC" }).codec(ckSql`DoubleDelta, ZSTD(1)`),
    level: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    service: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    host: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(1)`),
    string_keys: ckType.array(ckType.lowCardinality(ckType.string())).codec(ckSql`ZSTD(1)`),
    string_values: ckType.array(ckType.string()).codec(ckSql`ZSTD(1)`),
    number_keys: ckType.array(ckType.lowCardinality(ckType.string())).codec(ckSql`ZSTD(1)`),
    number_values: ckType.array(ckType.float64()).codec(ckSql`Gorilla, ZSTD(1)`),
    request_id: ckType
      .string()
      .materialized(ckSql`string_values[indexOf(string_keys, 'request_id')]`)
      .codec(ckSql`ZSTD(1)`),
    user_id: ckType.string().materialized(ckSql`string_values[indexOf(string_keys, 'user_id')]`).codec(ckSql`ZSTD(1)`),
    trip_id: ckType.string().materialized(ckSql`string_values[indexOf(string_keys, 'trip_id')]`).codec(ckSql`ZSTD(1)`),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`toDate(${ckSql.identifier("timestamp")})`,
    orderBy: [table._namespace, table.service, table.timestamp],
    ttl: ckSql`toDateTime(${ckSql.identifier("timestamp")}) + toIntervalDay(90)`,
    settings: { index_granularity: 8192, ttl_only_drop_parts: 1 },
  }),
);

// ---------------------------------------------------------------------------
// B. Product Analytics — User behaviour & funnels
// ---------------------------------------------------------------------------

/**
 * PostHog sharded_events.
 *
 * Source: https://posthog.com/handbook/engineering/clickhouse/schema/sharded-events
 */
export const posthogEvents = ckTable(
  "scenario_posthog_events",
  {
    uuid: ckType.uuid(),
    event: ckType.string(),
    properties: ckType.string().codec(ckSql`ZSTD(3)`),
    timestamp: ckType.dateTime64({ precision: 6, timezone: "UTC" }),
    team_id: ckType.int64(),
    distinct_id: ckType.string(),
    elements_chain: ckType.string(),
    created_at: ckType.dateTime64({ precision: 6, timezone: "UTC" }),
    person_id: ckType.uuid(),
    person_created_at: ckType.dateTime64({ precision: 6, timezone: "UTC" }),
    person_properties: ckType.string().codec(ckSql`ZSTD(3)`),
    // The real PostHog schema uses `$session_id` / `$window_id` (a Postgres-style
    // identifier with a leading `$`). ck-orm's identifier validator does not allow
    // `$`, so we materialize to underscore-prefixed names instead.
    session_id_materialized: ckType
      .string("session_id_materialized")
      .materialized(ckSql`replaceRegexpAll(JSONExtractRaw(properties, '$session_id'), '^"|"$', '')`),
    window_id_materialized: ckType
      .string("window_id_materialized")
      .materialized(ckSql`replaceRegexpAll(JSONExtractRaw(properties, '$window_id'), '^"|"$', '')`),
    _timestamp: ckType.dateTime(),
    _offset: ckType.uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    versionColumn: table._timestamp,
    partitionBy: ckSql`toYYYYMM(${ckSql.identifier("timestamp")})`,
    orderBy: [
      table.team_id,
      ckSql`toDate(${ckSql.identifier("timestamp")})`,
      table.event,
      ckSql`cityHash64(${ckSql.identifier("distinct_id")})`,
      ckSql`cityHash64(${ckSql.identifier("uuid")})`,
    ],
    sampleBy: ckSql`cityHash64(${ckSql.identifier("distinct_id")})`,
  }),
);

/**
 * Yandex.Metrica hits — the canonical web clickstream wide table that gave
 * birth to ClickHouse. ClickBench uses a near-identical subset of these
 * columns as the standard OLAP benchmark.
 *
 * Source: https://clickhouse.com/blog/evolution-of-data-structures-in-yandexmetrica
 */
export const metricaHits = ckTable(
  "scenario_metrica_hits",
  {
    watch_id: ckType.uint64(),
    event_time: ckType.dateTime(),
    event_date: ckType.date(),
    counter_id: ckType.uint32(),
    client_ip: ckType.uint32(),
    region_id: ckType.uint32(),
    user_id: ckType.uint64(),
    user_agent: ckType.uint8(),
    os: ckType.uint8(),
    url: ckType.string(),
    referer: ckType.string(),
    is_refresh: ckType.uint8(),
    referer_category_id: ckType.uint16(),
    url_category_id: ckType.uint16(),
    resolution_width: ckType.uint16(),
    resolution_height: ckType.uint16(),
    mobile_phone_model: ckType.lowCardinality(ckType.string()),
    search_phrase: ckType.string(),
    utm_source: ckType.lowCardinality(ckType.string()),
    utm_medium: ckType.lowCardinality(ckType.string()),
    utm_campaign: ckType.string(),
    http_error: ckType.uint16(),
    send_timing: ckType.int32(),
    dns_timing: ckType.int32(),
    connect_timing: ckType.int32(),
    age: ckType.uint8(),
    interests: ckType.uint16(),
    robotness: ckType.uint8(),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`toYYYYMM(${ckSql.identifier("event_date")})`,
    orderBy: [table.counter_id, table.event_date, ckSql`intHash32(${ckSql.identifier("user_id")})`],
    sampleBy: ckSql`intHash32(${ckSql.identifier("user_id")})`,
    settings: { index_granularity: 8192 },
  }),
);

/**
 * Mux video QoE views — CollapsingMergeTree mutable playback state.
 *
 * Source: https://www.mux.com/blog/from-russia-with-love-how-clickhouse-saved-our-data
 */
export const muxVideoQoe = ckTable(
  "scenario_mux_video_views",
  {
    view_id: ckType.uuid(),
    customer_id: ckType.uuid(),
    sign: ckType.int8(),
    view_time: ckType.dateTime(),
    operating_system: ckType.lowCardinality(ckType.string()),
    browser: ckType.lowCardinality(ckType.string()),
    player_name: ckType.lowCardinality(ckType.string()),
    cdn: ckType.lowCardinality(ckType.string()),
    country: ckType.lowCardinality(ckType.string()),
    video_id: ckType.string(),
    stream_type: ckType.enum8({ vod: 1, live: 2, dvr: 3 }),
    rebuffer_count: ckType.uint32(),
    rebuffer_duration_ms: ckType.uint32(),
    startup_time_ms: ckType.uint32(),
    watch_time_ms: ckType.uint64(),
    video_startup_failure: ckType.uint8(),
    exit_before_video_start: ckType.uint8(),
    avg_bitrate: ckType.uint32(),
    video_title: ckType.string(),
    view_end_time: ckType.nullable(ckType.dateTime()),
    error_type_id: ckType.nullable(ckType.uint16()),
  },
  (table) => ({
    engine: ckSql`CollapsingMergeTree(${ckSql.identifier("sign")})`,
    partitionBy: ckSql`toYYYYMMDD(${ckSql.identifier("view_time")})`,
    orderBy: [table.customer_id, table.view_time, table.view_id],
    ttl: ckSql`${ckSql.identifier("view_time")} + toIntervalDay(90)`,
  }),
);

/**
 * Snowplow Analytics canonical events.
 *
 * Source: https://docs.snowplow.io/docs/fundamentals/canonical-event/
 */
export const snowplowEvents = ckTable(
  "scenario_snowplow_events",
  {
    event_id: ckType.uuid(),
    collector_tstamp: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    dvce_created_tstamp: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    event: ckType.lowCardinality(ckType.string()),
    app_id: ckType.lowCardinality(ckType.string()),
    platform: ckType.lowCardinality(ckType.string()),
    user_id: ckType.string(),
    domain_userid: ckType.string(),
    network_userid: ckType.string(),
    session_id: ckType.string(),
    page_url: ckType.string(),
    page_urlhost: ckType.lowCardinality(ckType.string()),
    page_urlpath: ckType.string(),
    page_title: ckType.string(),
    referrer: ckType.string(),
    refr_medium: ckType.lowCardinality(ckType.string()),
    refr_source: ckType.lowCardinality(ckType.string()),
    geo_country: ckType.lowCardinality(ckType.string()),
    geo_city: ckType.string(),
    os_name: ckType.lowCardinality(ckType.string()),
    br_name: ckType.lowCardinality(ckType.string()),
    br_family: ckType.lowCardinality(ckType.string()),
    unstruct_event: ckType.string().codec(ckSql`ZSTD(3)`),
    contexts: ckType.string().codec(ckSql`ZSTD(3)`),
    derived_contexts: ckType.string().codec(ckSql`ZSTD(3)`),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    versionColumn: table.collector_tstamp,
    partitionBy: ckSql`toDate(${ckSql.identifier("collector_tstamp")})`,
    orderBy: [table.app_id, table.event, table.collector_tstamp, table.event_id],
    ttl: ckSql`toDate(${ckSql.identifier("collector_tstamp")}) + toIntervalDay(180)`,
    settings: { index_granularity: 8192 },
  }),
);

// ---------------------------------------------------------------------------
// C. Marketing / E-commerce
// ---------------------------------------------------------------------------

/**
 * Cloudflare HTTP analytics 1-minute aggregate (SummingMergeTree).
 *
 * Source: https://blog.cloudflare.com/http-analytics-for-6m-requests-per-second-using-clickhouse/
 */
export const cloudflareHttpRequests = ckTable(
  "scenario_cloudflare_requests_1m",
  {
    request_date: ckType.date(),
    zone_id: ckType.uint32(),
    timestamp: ckType.dateTime(),
    status: ckType.uint16(),
    country: ckType.lowCardinality(ckType.string()),
    content_type: ckType.lowCardinality(ckType.string()),
    colo_id: ckType.uint16(),
    threat_type: ckType.lowCardinality(ckType.string()),
    requests: ckType.uint64(),
    bytes: ckType.uint64(),
    cached_requests: ckType.uint64(),
    ssl_requests: ckType.uint64(),
  },
  (table) => ({
    engine: ckSql`SummingMergeTree((${ckSql.identifier("requests")}, ${ckSql.identifier("bytes")}, ${ckSql.identifier("cached_requests")}, ${ckSql.identifier("ssl_requests")}))`,
    partitionBy: table.request_date,
    orderBy: [table.zone_id, table.timestamp, table.status, table.country, table.content_type, table.colo_id],
    settings: { index_granularity: 32 },
  }),
);

/**
 * GrowthBook A/B test experiment exposures + conversion events.
 *
 * Source: https://clickhouse.com/blog/how-growthbook-and-clickhouse-make-enterprise-grade-ab-testing-easy
 */
export const growthbookExposures = ckTable(
  "scenario_growthbook_exposures",
  {
    exposure_id: ckType.uuid(),
    timestamp: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    experiment_id: ckType.lowCardinality(ckType.string()),
    variation_id: ckType.lowCardinality(ckType.string()),
    user_id: ckType.string(),
    anonymous_id: ckType.string(),
    attributes: ckType.map(ckType.string(), ckType.string()),
    session_id: ckType.string(),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`toYYYYMM(${ckSql.identifier("timestamp")})`,
    orderBy: [table.experiment_id, table.variation_id, table.timestamp, table.user_id],
    settings: { index_granularity: 8192 },
  }),
);

export const growthbookConversions = ckTable(
  "scenario_growthbook_conversions",
  {
    event_id: ckType.uuid(),
    timestamp: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    user_id: ckType.string(),
    anonymous_id: ckType.string(),
    event_name: ckType.lowCardinality(ckType.string()),
    properties: ckType.map(ckType.string(), ckType.string()),
    revenue: ckType.nullable(ckType.decimal({ precision: 18, scale: 4 })),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`toYYYYMM(${ckSql.identifier("timestamp")})`,
    orderBy: [table.event_name, table.timestamp, table.user_id],
  }),
);

/**
 * RTB DSP ad impressions and matching clicks.
 *
 * Source: https://oneuptime.com/blog/post/2026-03-31-clickhouse-track-ad-impressions-and-clicks/view
 */
export const rtbAdImpressions = ckTable(
  "scenario_rtb_ad_impressions",
  {
    impression_id: ckType.string(),
    event_time: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    event_date: ckType.date().materialized(ckSql`toDate(event_time)`),
    ad_id: ckType.uint32(),
    campaign_id: ckType.uint32(),
    advertiser_id: ckType.uint32(),
    publisher_id: ckType.uint32(),
    placement_id: ckType.uint32(),
    user_id: ckType.uint64(),
    device_type: ckType.lowCardinality(ckType.string()),
    os: ckType.lowCardinality(ckType.string()),
    country: ckType.lowCardinality(ckType.string()),
    city: ckType.lowCardinality(ckType.string()),
    bid_price: ckType.decimal({ precision: 10, scale: 6 }),
    win_price: ckType.decimal({ precision: 10, scale: 6 }),
    ad_format: ckType.lowCardinality(ckType.string()),
    creative_id: ckType.uint32(),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: table.event_date,
    orderBy: [table.campaign_id, table.advertiser_id, table.event_date, table.ad_id],
    ttl: ckSql`${ckSql.identifier("event_date")} + toIntervalDay(180)`,
  }),
);

export const rtbAdClicks = ckTable(
  "scenario_rtb_ad_clicks",
  {
    click_id: ckType.string(),
    impression_id: ckType.string(),
    event_time: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    event_date: ckType.date().materialized(ckSql`toDate(event_time)`),
    ad_id: ckType.uint32(),
    campaign_id: ckType.uint32(),
    user_id: ckType.uint64(),
    device_type: ckType.lowCardinality(ckType.string()),
    country: ckType.lowCardinality(ckType.string()),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: table.event_date,
    orderBy: [table.campaign_id, table.impression_id, table.event_time],
  }),
);

/**
 * Email marketing platform events (Mailchimp / SendGrid style).
 *
 * Source: https://mailchimp.com/developer/transactional/docs/activity-reports/
 */
export const mailchimpEmailEvents = ckTable(
  "scenario_email_events",
  {
    event_id: ckType.uuid(),
    event_time: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    event_date: ckType.date().materialized(ckSql`toDate(event_time)`),
    event_type: ckType.enum8({
      sent: 1,
      delivered: 2,
      bounced: 3,
      opened: 4,
      clicked: 5,
      unsubscribed: 6,
      complained: 7,
    }),
    campaign_id: ckType.uint32(),
    account_id: ckType.uint32(),
    recipient_email: ckType.lowCardinality(ckType.string()),
    recipient_id: ckType.uint64(),
    list_id: ckType.uint32(),
    country: ckType.lowCardinality(ckType.string()),
    user_agent: ckType.lowCardinality(ckType.string()),
    link_url: ckType.string(),
    bounce_type: ckType.lowCardinality(ckType.string()),
    message_id: ckType.string(),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: table.event_date,
    orderBy: [table.account_id, table.campaign_id, table.event_type, table.event_time],
    ttl: ckSql`${ckSql.identifier("event_date")} + toIntervalDay(365)`,
  }),
);

/**
 * E-commerce CDP user behaviour events.
 *
 * Source: https://github.com/RafaelAdao/cdp-clickhouse
 */
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

// ---------------------------------------------------------------------------
// D. Finance / Crypto / Time Series
// ---------------------------------------------------------------------------

/**
 * Stock tick trades (Polygon/Alpaca-style market data store).
 *
 * Source: https://rafalkwasny.com/clickhouse-tick-store
 */
export const stockTickTrades = ckTable(
  "scenario_stock_trades",
  {
    symbol: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(9)`),
    exchange: ckType.lowCardinality(ckType.string()).codec(ckSql`ZSTD(9)`),
    trade_id: ckType.uint64().codec(ckSql`ZSTD(9)`),
    price: ckType.decimal({ precision: 18, scale: 6 }).codec(ckSql`ZSTD(9)`),
    trade_size: ckType.decimal({ precision: 38, scale: 8 }).codec(ckSql`ZSTD(9)`),
    conditions: ckType.array(ckType.fixedString({ length: 2 })).codec(ckSql`ZSTD(9)`),
    tape: ckType.enum8({ NYSE: 1, AMEX: 2, NASDAQ: 3 }),
    event_ts: ckType.dateTime64({ precision: 9, timezone: "UTC" }).codec(ckSql`DoubleDelta, ZSTD(1)`),
    received_ts: ckType.dateTime64({ precision: 9, timezone: "UTC" }).codec(ckSql`DoubleDelta, ZSTD(1)`),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    versionColumn: table.received_ts,
    partitionBy: ckSql`toDate(${ckSql.identifier("event_ts")})`,
    orderBy: [table.symbol, table.event_ts, table.trade_id],
    settings: { index_granularity: 8192 },
  }),
);

/**
 * Coinhall cross-chain DEX swaps.
 *
 * Source: https://clickhouse.com/blog/trade-secrets-how-coinhall-uses-clickhouse-to-power-its-blockchain-data-platform
 */
export const dexSwaps = ckTable(
  "scenario_dex_swaps",
  {
    chain_id: ckType.lowCardinality(ckType.string()),
    pool_address: ckType.string(),
    tx_hash: ckType.fixedString({ length: 66 }),
    block_height: ckType.uint64(),
    block_ts: ckType.dateTime64({ precision: 3, timezone: "UTC" }).codec(ckSql`DoubleDelta, ZSTD(1)`),
    token_in: ckType.lowCardinality(ckType.string()),
    token_out: ckType.lowCardinality(ckType.string()),
    amount_in: ckType.decimal({ precision: 38, scale: 18 }),
    amount_out: ckType.decimal({ precision: 38, scale: 18 }),
    amount_in_usd: ckType.decimal({ precision: 20, scale: 6 }),
    amount_out_usd: ckType.decimal({ precision: 20, scale: 6 }),
    sender: ckType.string(),
    recipient: ckType.string(),
    fee_tier: ckType.uint32(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    versionColumn: table.block_height,
    partitionBy: ckSql`(${ckSql.identifier("chain_id")}, toYYYYMM(${ckSql.identifier("block_ts")}))`,
    orderBy: [table.chain_id, table.token_in, table.token_out, table.block_ts, table.tx_hash],
    settings: { index_granularity: 8192 },
  }),
);

/**
 * CryptoHouse Solana transactions with nested token balance changes.
 *
 * Source: https://clickhouse.com/blog/announcing-cryptohouse-free-blockchain-analytics
 */
export const solanaTransactions = ckTable(
  "scenario_solana_transactions",
  {
    block_slot: ckType.uint64(),
    block_hash: ckType.fixedString({ length: 44 }),
    block_time: ckType.dateTime64({ precision: 3, timezone: "UTC" }).codec(ckSql`DoubleDelta, ZSTD(1)`),
    tx_index: ckType.uint32(),
    signature: ckType.fixedString({ length: 88 }),
    success: ckType.bool(),
    fee: ckType.uint64(),
    compute_units_used: ckType.uint64(),
    compute_units_limit: ckType.uint64(),
    recent_blockhash: ckType.fixedString({ length: 44 }),
    account_keys: ckType.array(ckType.string()),
    log_messages: ckType.array(ckType.string()),
    pre_balances: ckType.array(ckType.uint64()),
    post_balances: ckType.array(ckType.uint64()),
    token_balance_changes: ckType.nested({
      account_index: ckType.uint8(),
      mint: ckType.string(),
      owner: ckType.string(),
      amount_pre: ckType.uint64(),
      amount_post: ckType.uint64(),
      decimals: ckType.uint8(),
    }),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    versionColumn: table.block_slot,
    partitionBy: ckSql`toYYYYMM(${ckSql.identifier("block_time")})`,
    orderBy: [table.block_slot, table.tx_index, table.signature],
    settings: { index_granularity: 8192 },
  }),
);

/**
 * NYC TLC taxi trips — the canonical ClickHouse geo time-series demo.
 *
 * Source: https://clickhouse.com/docs/getting-started/example-datasets/nyc-taxi
 */
export const nycTaxiTrips = ckTable(
  "scenario_nyc_taxi_trips",
  {
    trip_id: ckType.uint32(),
    vendor_id: ckType.lowCardinality(ckType.string()),
    pickup_datetime: ckType.dateTime().codec(ckSql`DoubleDelta, ZSTD(1)`),
    dropoff_datetime: ckType.dateTime().codec(ckSql`DoubleDelta, ZSTD(1)`),
    store_and_fwd_flag: ckType.uint8(),
    rate_code_id: ckType.uint8(),
    pickup_longitude: ckType.float32(),
    pickup_latitude: ckType.float32(),
    dropoff_longitude: ckType.float32(),
    dropoff_latitude: ckType.float32(),
    passenger_count: ckType.uint8(),
    trip_distance: ckType.float32(),
    fare_amount: ckType.decimal({ precision: 8, scale: 2 }),
    extra: ckType.decimal({ precision: 8, scale: 2 }),
    mta_tax: ckType.decimal({ precision: 8, scale: 2 }),
    tip_amount: ckType.decimal({ precision: 8, scale: 2 }),
    tolls_amount: ckType.decimal({ precision: 8, scale: 2 }),
    total_amount: ckType.decimal({ precision: 8, scale: 2 }),
    payment_type: ckType.enum8({ CSH: 1, CRE: 2, NOC: 3, DIS: 4, UNK: 5 }),
    pickup_ntaname: ckType.lowCardinality(ckType.string()),
    dropoff_ntaname: ckType.lowCardinality(ckType.string()),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`toYYYYMM(${ckSql.identifier("pickup_datetime")})`,
    orderBy: [table.vendor_id, table.pickup_datetime],
    settings: { index_granularity: 8192 },
  }),
);

// ---------------------------------------------------------------------------
// E. IoT / Gaming / SaaS / ML
// ---------------------------------------------------------------------------

/**
 * EMQ MQTT industrial IoT sensor telemetry.
 *
 * Source: https://clickhouse.com/blog/emq-ai-assisted-analytics
 */
export const iotTelemetry = ckTable(
  "scenario_iot_telemetry",
  {
    device_id: ckType.lowCardinality(ckType.string()),
    plant_id: ckType.lowCardinality(ckType.string()),
    protocol: ckType.lowCardinality(ckType.string()),
    ts: ckType.dateTime64({ precision: 3, timezone: "UTC" }).codec(ckSql`DoubleDelta, ZSTD(1)`),
    metric_name: ckType.lowCardinality(ckType.string()),
    value_float: ckType.nullable(ckType.float64()).codec(ckSql`Gorilla, ZSTD(1)`),
    value_int: ckType.nullable(ckType.int64()).codec(ckSql`ZSTD(9)`),
    value_str: ckType.nullable(ckType.string()),
    quality_code: ckType.uint8().default("192"),
    tags: ckType.map(ckType.string(), ckType.string()),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`(${ckSql.identifier("plant_id")}, toYYYYMMDD(${ckSql.identifier("ts")}))`,
    orderBy: [table.device_id, table.metric_name, table.ts],
    ttl: ckSql`${ckSql.identifier("ts")} + toIntervalDay(90)`,
    settings: { index_granularity: 8192 },
  }),
);

/**
 * Azur Games mobile game telemetry.
 *
 * Source: https://aws.amazon.com/blogs/gametech/azur-games-migrates-all-game-analytics-data-to-clickhouse-cloud-on-aws/
 */
export const gameEvents = ckTable(
  "scenario_game_events",
  {
    game_id: ckType.lowCardinality(ckType.string()),
    event_type: ckType.lowCardinality(ckType.string()),
    player_id: ckType.uint64(),
    session_id: ckType.uint64(),
    event_ts: ckType.dateTime64({ precision: 3, timezone: "UTC" }).codec(ckSql`DoubleDelta, ZSTD(1)`),
    platform: ckType.enum8({ ios: 1, android: 2, pc: 3, console: 4 }),
    country: ckType.lowCardinality(ckType.fixedString({ length: 2 })),
    level_id: ckType.uint32(),
    score: ckType.uint32(),
    duration_ms: ckType.uint32(),
    is_win: ckType.nullable(ckType.bool()),
    revenue_usd: ckType.nullable(ckType.decimal({ precision: 10, scale: 4 })),
    properties: ckType.map(ckType.string(), ckType.string()),
    ab_variant: ckType.lowCardinality(ckType.string()),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`(${ckSql.identifier("game_id")}, toYYYYMM(${ckSql.identifier("event_ts")}))`,
    orderBy: [table.game_id, table.player_id, table.event_ts],
    settings: { index_granularity: 8192 },
  }),
);

/**
 * OpenMeter usage-based billing meter events.
 *
 * Source: https://clickhouse.com/blog/openmeter-real-time-usage-based-billing-powered-by-clickhouse-cloud
 */
export const meterEvents = ckTable(
  "scenario_meter_events",
  {
    id: ckType.uuid().default("generateUUIDv4()"),
    customer_id: ckType.lowCardinality(ckType.string()),
    meter_slug: ckType.lowCardinality(ckType.string()),
    event_ts: ckType.dateTime64({ precision: 3, timezone: "UTC" }).codec(ckSql`DoubleDelta, ZSTD(1)`),
    idempotency_key: ckType.string(),
    value: ckType.float64(),
    properties: ckType.map(ckType.string(), ckType.string()),
  },
  (table) => ({
    engine: "MergeTree",
    partitionBy: ckSql`toYYYYMM(${ckSql.identifier("event_ts")})`,
    orderBy: [table.customer_id, table.meter_slug, table.event_ts],
    settings: { index_granularity: 8192 },
  }),
);

/**
 * ML feature store — fraud detection raw user events.
 *
 * Source: https://clickhouse.com/blog/modeling-machine-learning-data-in-clickhouse
 */
export const mlUserEvents = ckTable(
  "scenario_ml_user_events",
  {
    user_id: ckType.uint64(),
    domain: ckType.lowCardinality(ckType.string()),
    url: ckType.string(),
    client_ip: ckType.uint32(),
    user_agent: ckType.uint8(),
    referer_cat_id: ckType.uint16(),
    url_cat_id: ckType.uint16(),
    fetch_timing_ms: ckType.uint32(),
    is_bounce: ckType.uint8(),
    event_ts: ckType.dateTime64({ precision: 3, timezone: "UTC" }).codec(ckSql`DoubleDelta, ZSTD(1)`),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.domain, table.user_id, table.event_ts],
  }),
);

// ---------------------------------------------------------------------------
// Standard Schema mocks (used by validator-driven examples / e2e)
// ---------------------------------------------------------------------------

export const severityValidator: StandardSchemaV1<
  "trace" | "debug" | "info" | "warn" | "error" | "fatal",
  "trace" | "debug" | "info" | "warn" | "error" | "fatal"
> = {
  "~standard": {
    version: 1,
    vendor: "ck-orm-scenarios",
    validate(value) {
      if (
        value === "trace" ||
        value === "debug" ||
        value === "info" ||
        value === "warn" ||
        value === "error" ||
        value === "fatal"
      ) {
        return { value };
      }
      return { issues: [{ message: `Unknown severity_text: ${String(value)}` }] };
    },
  },
};

// ---------------------------------------------------------------------------
// Schema bundle — used by e2e suite to create every table during seed
// ---------------------------------------------------------------------------

export const scenarioSchema = {
  // Observability
  clickhouseLogPlatform,
  signozTraces,
  otelTraces,
  signozMetricsSamples,
  highlightLogs,
  uberSchemaAgnosticLogs,
  // Analytics
  posthogEvents,
  metricaHits,
  muxVideoQoe,
  snowplowEvents,
  // Marketing / E-commerce
  cloudflareHttpRequests,
  growthbookExposures,
  growthbookConversions,
  rtbAdImpressions,
  rtbAdClicks,
  mailchimpEmailEvents,
  cdpUserEvents,
  cdpOrders,
  // Finance / Crypto / Time Series
  stockTickTrades,
  dexSwaps,
  solanaTransactions,
  nycTaxiTrips,
  // IoT / Gaming / SaaS / ML
  iotTelemetry,
  gameEvents,
  meterEvents,
  mlUserEvents,
};

export type ScenarioSchema = typeof scenarioSchema;
