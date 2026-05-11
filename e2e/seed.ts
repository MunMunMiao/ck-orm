import { sql } from "../src/sql";
import { ckSql } from "./ck-orm";
import {
  auditEvents,
  auditLogTyped,
  buildCreateTableStatement,
  buildDropTableStatement,
  chainedColumns,
  createAdminDb,
  createE2EDb,
  datasetCounts,
  ddlBrand,
  getE2EConfig,
  ioSplit,
  pets,
  quoteSnapshots,
  rewardEvents,
  scenarioSchema,
  schemaAggregates,
  schemaCompound,
  schemaGeo,
  schemaJsonAdvanced,
  schemaPrimitives,
  tradeFills,
  userDailySummary,
  userProfileTyped,
  users,
  validatorStrict,
  validatorTransform,
  webEvents,
  writePathBigInts,
} from "./shared";

const scenarioTables = [
  users,
  pets,
  webEvents,
  rewardEvents,
  tradeFills,
  quoteSnapshots,
  userDailySummary,
  auditEvents,
  writePathBigInts,
];
const schemaTables = [schemaPrimitives, schemaCompound, schemaAggregates, schemaGeo, schemaJsonAdvanced];
const typeOverrideTables = [
  auditLogTyped,
  userProfileTyped,
  validatorStrict,
  validatorTransform,
  ioSplit,
  ddlBrand,
  chainedColumns,
];
const scenarioCaseTables = Object.values(scenarioSchema);

const recreateDatabase = async () => {
  const adminDb = createAdminDb();
  const { database } = getE2EConfig();

  await adminDb.command(ckSql`DROP DATABASE IF EXISTS ${ckSql.identifier(database)}`);
  await adminDb.command(ckSql`CREATE DATABASE ${ckSql.identifier(database)}`);
};

const createTables = async () => {
  const db = createE2EDb();

  for (const table of [...scenarioTables, ...schemaTables, ...typeOverrideTables, ...scenarioCaseTables]) {
    await db.command(sql(buildDropTableStatement(table.originalName)));
    await db.command(sql(buildCreateTableStatement(table)));
  }
};

const insertUsers = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO users
    SELECT
      toInt32(number + 1) AS id,
      multiIf(
        number = 0, 'alice',
        number = 1, 'bob',
        number = 2, 'charlie',
        concat('user_', toString(number + 1))
      ) AS name,
      multiIf(number % 7 = 0, 'vip', number % 3 = 0, 'standard', 'trial') AS tier,
      addSeconds(toDateTime64('2026-01-01 00:00:00', 3), toInt32(number)) AS created_at
    FROM numbers(${datasetCounts.users})
  `);
};

const insertPets = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO pets
    SELECT
      toInt32(number + 101) AS id,
      toInt32((number % 4000) + 1) AS owner_id,
      multiIf(
        number = 0, 'milo',
        number = 1, 'luna',
        concat('pet_', toString(number + 1))
      ) AS pet_name,
      addSeconds(toDateTime64('2026-01-01 01:00:00', 3), toInt32(number)) AS created_at
    FROM numbers(${datasetCounts.pets})
  `);
};

const insertWebEvents = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO web_events
    SELECT
      toUInt64(number + 1) AS event_id,
      toInt32((number % ${datasetCounts.users}) + 1) AS user_id,
      arrayElement(['US', 'SG', 'AU', 'GB'], (number % 4) + 1) AS country,
      arrayElement(['ios', 'android', 'web'], (number % 3) + 1) AS device_type,
      addSeconds(toDateTime64('2026-02-01 00:00:00', 3), toInt32(number * 37)) AS viewed_at,
      CAST((number % 2500) / 10.0 AS Decimal(18, 2)) AS revenue,
      [concat('tag_', toString(number % 10)), concat('segment_', toString(number % 5))] AS tags,
      [toUInt8((number % 10) + 1), toUInt8(((number + 3) % 10) + 1)] AS tag_scores
    FROM numbers(${datasetCounts.webEvents})
  `);
};

const insertRewardEvents = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO reward_events
    SELECT *
    FROM
    (
      SELECT
        toInt32(number + 1) AS id,
        concat('user_', toString((number % ${datasetCounts.users}) + 1)) AS user_id,
        CAST((number % 3000) / 100.0 AS Decimal(20, 5)) AS reward_points,
        toInt64(900000 + number + 1) AS order_id,
        toInt32(if(number % 2 = 0, 2, 1)) AS channel,
        addSeconds(toDateTime64('2026-03-01 00:00:00', 3), toInt32(number)) AS created_at,
        addSeconds(toDateTime64('2026-03-01 00:00:05', 3), toInt32(number)) AS _peerdb_synced_at,
        toUInt8(0) AS _peerdb_is_deleted,
        toUInt64(1) AS _peerdb_version
      FROM numbers(20000)

      UNION ALL

      SELECT
        toInt32(number + 1) AS id,
        concat('user_', toString((number % ${datasetCounts.users}) + 1)) AS user_id,
        CAST(((number % 3000) / 100.0) + 1.25 AS Decimal(20, 5)) AS reward_points,
        toInt64(900000 + number + 1) AS order_id,
        toInt32(2) AS channel,
        addSeconds(toDateTime64('2026-03-01 00:00:00', 3), toInt32(number)) AS created_at,
        addSeconds(toDateTime64('2026-03-01 04:00:05', 3), toInt32(number)) AS _peerdb_synced_at,
        toUInt8(0) AS _peerdb_is_deleted,
        toUInt64(2) AS _peerdb_version
      FROM numbers(3000)

      UNION ALL

      SELECT
        toInt32(number + 3001) AS id,
        concat('user_', toString(((number + 3000) % ${datasetCounts.users}) + 1)) AS user_id,
        CAST(((number % 3000) / 100.0) + 0.75 AS Decimal(20, 5)) AS reward_points,
        toInt64(930000 + number + 1) AS order_id,
        toInt32(1) AS channel,
        addSeconds(toDateTime64('2026-03-01 00:00:00', 3), toInt32(number + 3000)) AS created_at,
        addSeconds(toDateTime64('2026-03-01 08:00:05', 3), toInt32(number)) AS _peerdb_synced_at,
        toUInt8(1) AS _peerdb_is_deleted,
        toUInt64(2) AS _peerdb_version
      FROM numbers(1000)
    )
  `);
};

const insertTradeFills = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO trade_fills
    SELECT
      toUInt64(number + 1) AS trade_id,
      toInt32((number % ${datasetCounts.users}) + 1) AS user_id,
      arrayElement(['EURUSD', 'XAUUSD', 'BTCUSD', 'AAPL', 'TSLA'], (number % 5) + 1) AS symbol,
      addSeconds(toDateTime64('2026-04-01 00:00:00', 3), toInt32((number * 12) + 7)) AS filled_at,
      CAST(((number % 40) + 1) / 2.0 AS Decimal(18, 2)) AS quantity
    FROM numbers(${datasetCounts.tradeFills})
  `);
};

const insertQuoteSnapshots = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO quote_snapshots
    SELECT
      arrayElement(['EURUSD', 'XAUUSD', 'BTCUSD', 'AAPL', 'TSLA'], (number % 5) + 1) AS symbol,
      addSeconds(toDateTime64('2026-04-01 00:00:00', 3), toInt32(number * 6)) AS quote_time,
      CAST(
        multiIf(
          number % 5 = 0, 1.10000,
          number % 5 = 1, 2350.00000,
          number % 5 = 2, 65000.00000,
          number % 5 = 3, 180.00000,
          290.00000
        ) + ((number % 100) / 1000.0)
        AS Decimal(18, 5)
      ) AS bid
    FROM numbers(${datasetCounts.quoteSnapshots})
  `);
};

const insertSchemaPrimitives = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO schema_primitives
    SELECT
      toInt32(1),
      toInt8(-8),
      toInt16(-16),
      toInt32(-32),
      toInt64(-64),
      toUInt8(8),
      toUInt16(16),
      toUInt32(32),
      toUInt64(64),
      CAST(3.25 AS Float32),
      CAST(6.5 AS Float64),
      toBFloat16(1.75),
      'hello world',
      CAST('ABCD' AS FixedString(4)),
      CAST(1234.56 AS Decimal(18, 2)),
      toDate('2026-01-10'),
      toDate32('2026-01-11'),
      CAST('12:34:56' AS Time),
      CAST('12:34:56.789' AS Time64(3)),
      toDateTime('2026-01-12 01:02:03'),
      toDateTime64('2026-01-12 01:02:03.456', 3),
      toBool(1),
      toUUID('123e4567-e89b-12d3-a456-426614174000'),
      toIPv4('192.168.10.1'),
      toIPv6('2001:db8::1'),
      CAST('{"id":1,"label":"json-value"}' AS JSON),
      CAST('dynamic-value' AS Dynamic),
      CAST([1, 2, 3, 4, 5, 6, 7, 8] AS QBit(Float32, 8)),
      CAST('active' AS Enum8('active' = 1, 'paused' = 2)),
      CAST('silver' AS Enum16('bronze' = 1000, 'silver' = 2000))
  `);
};

const insertSchemaCompound = async () => {
  const db = createE2EDb();
  await db.insert(schemaCompound).values({
    id: 1,
    nullable_value: null,
    array_value: ["alpha", "beta"],
    tuple_value: ["login", 42],
    map_value: { a: 1, b: 2 },
    variant_value: 7,
    low_cardinality_value: "vip",
    nested_value: [
      { name: "first", score: 10 },
      { name: "second", score: 20 },
    ],
  });
};

const insertSchemaAggregates = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO schema_aggregates
    SELECT
      toUInt32(1),
      sumState(toUInt64(7)),
      toInt64(11)
  `);
};

const insertSchemaGeo = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO schema_geo
    SELECT
      toInt32(1),
      (1.5, 2.5),
      [(0., 0.), (1., 0.), (1., 1.), (0., 0.)],
      [(0., 0.), (1., 1.)],
      [[(0., 0.), (1., 1.)], [(2., 2.), (3., 3.)]],
      [[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]],
      [[[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]]
  `);
};

const insertSchemaJsonAdvanced = async () => {
  // Use raw SQL (not the builder) so the test exercises CK's typed-path
  // subcolumn semantics without re-encoding through ck-orm — keeps the
  // seed independent of the path that the test itself validates.
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO schema_json_advanced (id, payload, payload_with_default) VALUES
      (1, '{"user_id":"999","tag":"alpha","nested":{"score":42}}', '{"note":"first"}'),
      (2, '{"user_id":"1000","tag":"beta","nested":{"score":5}}', DEFAULT)
  `);
};

const insertAuditLogTyped = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO audit_log_typed (id, actor_id, action, actor_role, created_at, note) VALUES
      (1, 1001, 'login', 'admin', toDateTime64('2026-05-01 09:00:00.000', 3), 'first login of the day'),
      (2, 1002, 'password_reset', 'user', toDateTime64('2026-05-01 09:30:00.000', 3), ''),
      (3, 1003, 'role_change', 'admin', toDateTime64('2026-05-01 10:00:00.000', 3), 'promoted to admin')
  `);
};

const insertUserProfileTyped = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO user_profile_typed (id, display_name, preferences, signup_at) VALUES
      (
        1,
        'alice',
        CAST('{"theme":"dark","locale":"en-US","betaFeatures":["search-v2","inline-edit"]}' AS JSON),
        toDateTime64('2026-01-15 08:00:00.000', 3)
      ),
      (
        2,
        'bob',
        CAST('{"theme":"light","locale":"zh-CN","betaFeatures":[]}' AS JSON),
        toDateTime64('2026-02-20 11:30:00.000', 3)
      )
  `);
};

const insertValidatorStrict = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO validator_strict (id, status) VALUES
      (1, 'admin'),
      (2, 'user')
  `);
};

const insertValidatorTransform = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO validator_transform (id, occurred_at) VALUES
      (1, '2026-04-21T00:00:00.000Z')
  `);
};

const insertIoSplit = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO io_split (id, created_at) VALUES
      (1, toDateTime('2026-04-21 00:00:00'))
  `);
};

const insertChainedColumns = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO chained_columns (id) VALUES (1)
  `);
};

// -- Real-world scenario seed data -------------------------------------------

const seedClickhouseLogPlatform = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_clickhouse_log_platform (
      timestamp, event_date, event_time, trace_id, span_id, trace_flags,
      severity_text, severity_number, service_name, body, namespace, cell,
      cloud_provider, region, container_name, pod_name, logger_name, log_level,
      scope_attributes, resource_attributes, log_attributes
    ) VALUES
      (toDateTime64('2026-05-11 10:00:00.000000000', 9), toDate('2026-05-11'),
       toDateTime('2026-05-11 10:00:00'), 'trace-aaa1', 'span-001', 1,
       'INFO', 9, 'orders-service', 'order created', 'production', 'us-east-1a',
       'aws', 'us-east-1', 'orders-7c8d', 'orders-7c8d-xyz', 'app.orders',
       'INFO', map('module','order'), map('service.version','1.4.2'), map('order_id','o-1001')),
      (toDateTime64('2026-05-11 10:05:00.000000000', 9), toDate('2026-05-11'),
       toDateTime('2026-05-11 10:05:00'), 'trace-aaa2', 'span-002', 1,
       'ERROR', 17, 'payments-service', 'card declined', 'production', 'us-east-1b',
       'aws', 'us-east-1', 'pay-9f1', 'pay-9f1-abc', 'app.payments',
       'ERROR', map('module','charge'), map('service.version','3.2.0'), map('error_code','CARD_DECLINED'))
  `);
};

const seedSignozTraces = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_signoz_traces (
      ts_bucket_start, resource_fingerprint, timestamp, trace_id, span_id, parent_span_id,
      flags, name, kind, duration_nano, status_code, status_message, service_name,
      has_error, response_status_code, http_url, http_method, http_host, db_name, db_operation,
      attributes_string, attributes_number, attributes_bool, resources_string
    ) VALUES
      (1746957600, 'fp-checkout-1', toDateTime64('2026-05-11 12:00:00.123456789', 9),
       toFixedString('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 32), 'span-s1', '',
       1, 'GET /checkout', 2, 12500000, 0, '', 'checkout-svc',
       false, '200', '/checkout', 'GET', 'shop.example.com', '', '',
       map('http.route','/checkout'), map('http.request.body.size', 0.0), map('cache_hit', true),
       map('service.name','checkout-svc')),
      (1746957600, 'fp-payments-1', toDateTime64('2026-05-11 12:00:01.234567891', 9),
       toFixedString('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 32), 'span-s2', 'span-s1',
       1, 'POST /charge', 3, 845000000, 2, 'card declined', 'payments-svc',
       true, '500', '/charge', 'POST', 'pay.example.com', '', '',
       map('http.route','/charge'), map('http.request.body.size', 1024.0), map('retry', false),
       map('service.name','payments-svc'))
  `);
};

const seedOtelTraces = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_otel_traces (
      timestamp, trace_id, span_id, parent_span_id, trace_state, span_name, span_kind,
      service_name, resource_attributes, span_attributes, duration_ns, status_code,
      status_message, event_timestamps, event_names
    ) VALUES
      (toDateTime64('2026-05-11 12:00:00.000000000', 9), 'otel-trace-1', 'otel-span-1', '',
       '', 'GET /api/users', 'SERVER', 'gateway',
       map('service.name','gateway'), map('http.url','/api/users','db.system',''),
       25000000, 'OK', '',
       [toDateTime64('2026-05-11 12:00:00.001000000', 9)], ['handler.start']),
      (toDateTime64('2026-05-11 12:00:00.010000000', 9), 'otel-trace-1', 'otel-span-2', 'otel-span-1',
       '', 'SELECT users', 'CLIENT', 'gateway',
       map('service.name','gateway'), map('db.system','postgres','db.statement','SELECT id,name FROM users'),
       8000000, 'OK', '', [], [])
  `);
};

const seedSignozMetricsSamples = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_signoz_metrics_samples (env, temporality, metric_name, fingerprint, unix_milli, value) VALUES
      ('production', 'Cumulative', 'http_requests_total', 11111, toUnixTimestamp64Milli(toDateTime64('2026-05-11 09:00:00.000', 3)), 100.0),
      ('production', 'Cumulative', 'http_requests_total', 11111, toUnixTimestamp64Milli(toDateTime64('2026-05-11 09:01:00.000', 3)), 125.0),
      ('production', 'Cumulative', 'http_requests_total', 11111, toUnixTimestamp64Milli(toDateTime64('2026-05-11 09:02:00.000', 3)), 156.0),
      ('production', 'Cumulative', 'http_requests_total', 22222, toUnixTimestamp64Milli(toDateTime64('2026-05-11 09:00:00.000', 3)), 50.0)
  `);
};

const seedHighlightLogs = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_highlight_logs (
      timestamp, trace_id, span_id, trace_flags, severity_text, severity_number,
      service_name, body, resource_attributes, log_attributes
    ) VALUES
      (toDateTime64('2026-05-11 14:00:00.000000000', 9), 'hl-trace-1', 'hl-span-1', 1,
       'ERROR', 17, 'frontend',
       'NullPointerException at handleClick',
       map('environment','production','version','2.3.1'),
       map('user_id','user-42','session_id','s-9988')),
      (toDateTime64('2026-05-11 14:00:05.000000000', 9), 'hl-trace-2', 'hl-span-2', 1,
       'INFO', 9, 'frontend', 'user clicked checkout',
       map('environment','production'), map('user_id','user-43'))
  `);
};

const seedUberLogs = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_uber_logs (
      _source, _namespace, timestamp, level, service, host,
      string_keys, string_values, number_keys, number_values
    ) VALUES
      ('{"msg":"trip started"}', 'rides',
       toDateTime64('2026-05-11 15:00:00.000000000', 9), 'INFO', 'trip-svc', 'trip-001',
       ['request_id','user_id','trip_id'], ['req-aaa','u-1001','trip-9001'],
       ['fare','distance_km'], [12.5, 4.2]),
      ('{"msg":"trip ended"}', 'rides',
       toDateTime64('2026-05-11 15:30:00.000000000', 9), 'INFO', 'trip-svc', 'trip-001',
       ['request_id','user_id','trip_id'], ['req-bbb','u-1001','trip-9001'],
       ['fare','distance_km'], [18.0, 6.7])
  `);
};

const seedPosthogEvents = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_posthog_events (
      uuid, event, properties, timestamp, team_id, distinct_id, elements_chain,
      created_at, person_id, person_created_at, person_properties,
      _timestamp, _offset
    ) VALUES
      (generateUUIDv4(), 'user_signed_up',
       '{"$session_id":"sess-1","plan":"pro"}',
       toDateTime64('2026-05-01 10:00:00.000000', 6), 1, 'distinct-1', '',
       toDateTime64('2026-05-01 10:00:00.000000', 6), generateUUIDv4(),
       toDateTime64('2026-05-01 10:00:00.000000', 6), '{}',
       toDateTime('2026-05-01 10:00:00'), 1),
      (generateUUIDv4(), 'checkout_started',
       '{"$session_id":"sess-1","plan":"pro"}',
       toDateTime64('2026-05-01 10:05:00.000000', 6), 1, 'distinct-1', '',
       toDateTime64('2026-05-01 10:05:00.000000', 6), generateUUIDv4(),
       toDateTime64('2026-05-01 10:00:00.000000', 6), '{}',
       toDateTime('2026-05-01 10:05:00'), 2),
      (generateUUIDv4(), 'payment_succeeded',
       '{"$session_id":"sess-1","plan":"pro"}',
       toDateTime64('2026-05-01 10:10:00.000000', 6), 1, 'distinct-1', '',
       toDateTime64('2026-05-01 10:10:00.000000', 6), generateUUIDv4(),
       toDateTime64('2026-05-01 10:00:00.000000', 6), '{}',
       toDateTime('2026-05-01 10:10:00'), 3),
      (generateUUIDv4(), 'user_signed_up',
       '{"$session_id":"sess-2","plan":"free"}',
       toDateTime64('2026-05-01 11:00:00.000000', 6), 1, 'distinct-2', '',
       toDateTime64('2026-05-01 11:00:00.000000', 6), generateUUIDv4(),
       toDateTime64('2026-05-01 11:00:00.000000', 6), '{}',
       toDateTime('2026-05-01 11:00:00'), 4)
  `);
};

const seedMetricaHits = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_metrica_hits (
      watch_id, event_time, event_date, counter_id, client_ip, region_id, user_id,
      user_agent, os, url, referer, is_refresh, referer_category_id, url_category_id,
      resolution_width, resolution_height, mobile_phone_model, search_phrase,
      utm_source, utm_medium, utm_campaign, http_error, send_timing, dns_timing,
      connect_timing, age, interests, robotness
    ) VALUES
      (1001, toDateTime('2026-05-11 09:00:00'), toDate('2026-05-11'), 42,
       3232235521, 213, 9001, 1, 1, 'https://shop.example.com/', '',
       0, 0, 1, 1920, 1080, '', 'best deals',
       'google', 'cpc', 'spring_sale', 0, 120, 8, 24, 28, 5, 2),
      (1002, toDateTime('2026-05-11 09:05:00'), toDate('2026-05-11'), 42,
       3232235522, 213, 9002, 1, 2, 'https://shop.example.com/cart', 'https://google.com',
       0, 0, 2, 1366, 768, 'iPhone 14', '',
       'newsletter', 'email', 'spring_sale', 0, 90, 12, 30, 35, 3, 0)
  `);
};

const seedMuxVideoQoe = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_mux_video_views (
      view_id, customer_id, sign, view_time, operating_system, browser,
      player_name, cdn, country, video_id, stream_type, rebuffer_count,
      rebuffer_duration_ms, startup_time_ms, watch_time_ms, video_startup_failure,
      exit_before_video_start, avg_bitrate, video_title, view_end_time, error_type_id
    ) VALUES
      (toUUID('11111111-1111-1111-1111-111111111111'),
       toUUID('22222222-2222-2222-2222-222222222222'), 1,
       toDateTime('2026-05-11 18:00:00'), 'iOS', 'Safari',
       'mux-player', 'cloudfront', 'US', 'vid-abc', 'vod', 0,
       0, 800, 600000, 0, 0, 2500000, 'Demo VOD', toDateTime('2026-05-11 18:10:00'), NULL),
      (toUUID('33333333-3333-3333-3333-333333333333'),
       toUUID('22222222-2222-2222-2222-222222222222'), 1,
       toDateTime('2026-05-11 18:30:00'), 'Android', 'Chrome',
       'mux-player', 'akamai', 'GB', 'vid-def', 'live', 3,
       2400, 1200, 120000, 0, 0, 1800000, 'Live Event', NULL, NULL)
  `);
};

const seedSnowplowEvents = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_snowplow_events (
      event_id, collector_tstamp, dvce_created_tstamp, event, app_id, platform,
      user_id, domain_userid, network_userid, session_id,
      page_url, page_urlhost, page_urlpath, page_title, referrer,
      refr_medium, refr_source, geo_country, geo_city, os_name, br_name, br_family,
      unstruct_event, contexts, derived_contexts
    ) VALUES
      (generateUUIDv4(), toDateTime64('2026-05-11 09:00:00.000', 3),
       toDateTime64('2026-05-11 09:00:00.000', 3), 'page_view',
       'shop-web', 'web', 'user-42', 'cookie-abc', 'net-xyz', 'sess-1',
       'https://shop.example.com/', 'shop.example.com', '/', 'Home', '',
       'direct', '', 'US', 'New York', 'macOS', 'Chrome', 'chrome',
       '{}', '[]', '[]'),
      (generateUUIDv4(), toDateTime64('2026-05-11 09:05:00.000', 3),
       toDateTime64('2026-05-11 09:05:00.000', 3), 'unstruct',
       'shop-web', 'web', 'user-42', 'cookie-abc', 'net-xyz', 'sess-1',
       'https://shop.example.com/p/widget', 'shop.example.com', '/p/widget', 'Widget', '',
       'search', 'google', 'US', 'New York', 'macOS', 'Chrome', 'chrome',
       '{"schema":"iglu:com.shop/product_view/jsonschema/1-0-0"}', '[]', '[]')
  `);
};

const seedCloudflareRequests = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_cloudflare_requests_1m (
      request_date, zone_id, timestamp, status, country, content_type, colo_id,
      threat_type, requests, bytes, cached_requests, ssl_requests
    ) VALUES
      (toDate('2026-05-11'), 12345, toDateTime('2026-05-11 09:00:00'), 200,
       'US', 'text/html', 1, '', 1000, 5242880, 800, 1000),
      (toDate('2026-05-11'), 12345, toDateTime('2026-05-11 09:01:00'), 200,
       'US', 'text/html', 1, '', 1200, 6291456, 950, 1200),
      (toDate('2026-05-11'), 12345, toDateTime('2026-05-11 09:00:00'), 404,
       'GB', 'text/html', 2, '', 12, 24576, 0, 12),
      (toDate('2026-05-11'), 12345, toDateTime('2026-05-11 09:00:00'), 200,
       'GB', 'application/json', 2, '', 500, 1048576, 100, 500)
  `);
};

const seedGrowthbookExposures = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_growthbook_exposures (
      exposure_id, timestamp, experiment_id, variation_id, user_id, anonymous_id,
      attributes, session_id
    ) VALUES
      (generateUUIDv4(), toDateTime64('2026-05-11 10:00:00.000', 3),
       'checkout_v2', 'control', 'user-1', 'anon-1',
       map('country','US','plan','pro'), 'sess-1'),
      (generateUUIDv4(), toDateTime64('2026-05-11 10:01:00.000', 3),
       'checkout_v2', 'treatment', 'user-2', 'anon-2',
       map('country','US','plan','free'), 'sess-2'),
      (generateUUIDv4(), toDateTime64('2026-05-11 10:02:00.000', 3),
       'checkout_v2', 'treatment', 'user-3', 'anon-3',
       map('country','GB','plan','pro'), 'sess-3')
  `);
  await db.command(ckSql`
    INSERT INTO scenario_growthbook_conversions (
      event_id, timestamp, user_id, anonymous_id, event_name, properties, revenue
    ) VALUES
      (generateUUIDv4(), toDateTime64('2026-05-11 10:30:00.000', 3),
       'user-2', 'anon-2', 'purchase', map('plan','free'), 49.99),
      (generateUUIDv4(), toDateTime64('2026-05-11 10:45:00.000', 3),
       'user-3', 'anon-3', 'purchase', map('plan','pro'), 199.00)
  `);
};

const seedRtbAds = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_rtb_ad_impressions (
      impression_id, event_time, ad_id, campaign_id, advertiser_id, publisher_id,
      placement_id, user_id, device_type, os, country, city, bid_price, win_price,
      ad_format, creative_id
    ) VALUES
      ('imp-1', toDateTime64('2026-05-11 12:00:00.000', 3), 100, 42, 7, 901, 50001, 9001,
       'mobile', 'iOS', 'US', 'New York', 0.005000, 0.004200, 'banner', 8001),
      ('imp-2', toDateTime64('2026-05-11 12:00:01.000', 3), 101, 42, 7, 902, 50002, 9002,
       'desktop', 'Windows', 'US', 'San Francisco', 0.008000, 0.007100, 'video', 8002),
      ('imp-3', toDateTime64('2026-05-11 12:00:02.000', 3), 100, 42, 7, 901, 50001, 9003,
       'mobile', 'Android', 'GB', 'London', 0.006000, 0.005500, 'banner', 8001)
  `);
  await db.command(ckSql`
    INSERT INTO scenario_rtb_ad_clicks (
      click_id, impression_id, event_time, ad_id, campaign_id, user_id, device_type, country
    ) VALUES
      ('clk-1', 'imp-1', toDateTime64('2026-05-11 12:00:05.000', 3), 100, 42, 9001, 'mobile', 'US'),
      ('clk-2', 'imp-3', toDateTime64('2026-05-11 12:00:08.000', 3), 100, 42, 9003, 'mobile', 'GB')
  `);
};

const seedMailchimpEmailEvents = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_email_events (
      event_id, event_time, event_type, campaign_id, account_id, recipient_email,
      recipient_id, list_id, country, user_agent, link_url, bounce_type, message_id
    ) VALUES
      (generateUUIDv4(), toDateTime64('2026-05-11 08:00:00.000', 3),
       'sent', 1, 1, 'alice@example.com', 1001, 10, 'US', '', '', '', 'msg-1'),
      (generateUUIDv4(), toDateTime64('2026-05-11 08:00:30.000', 3),
       'delivered', 1, 1, 'alice@example.com', 1001, 10, 'US', '', '', '', 'msg-1'),
      (generateUUIDv4(), toDateTime64('2026-05-11 09:15:00.000', 3),
       'opened', 1, 1, 'alice@example.com', 1001, 10, 'US', 'Gmail/iOS', '', '', 'msg-1'),
      (generateUUIDv4(), toDateTime64('2026-05-11 09:16:00.000', 3),
       'clicked', 1, 1, 'alice@example.com', 1001, 10, 'US', 'Gmail/iOS',
       'https://shop.example.com/promo', '', 'msg-1'),
      (generateUUIDv4(), toDateTime64('2026-05-11 08:00:00.000', 3),
       'sent', 1, 1, 'bob@example.com', 1002, 10, 'GB', '', '', '', 'msg-2'),
      (generateUUIDv4(), toDateTime64('2026-05-11 08:00:30.000', 3),
       'bounced', 1, 1, 'bob@example.com', 1002, 10, 'GB', '', '', 'hard', 'msg-2')
  `);
};

const seedCdp = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_cdp_user_events (
      event_id, event_time, user_id, session_id, event_type, page_url, referrer,
      product_id, category_id, search_query, device_type, country, properties
    ) VALUES
      (generateUUIDv4(), toDateTime64('2026-05-11 10:00:00.000', 3), 1001, 'sess-1',
       'page_view', '/', 'google', NULL, NULL, '', 'mobile', 'US', map()),
      (generateUUIDv4(), toDateTime64('2026-05-11 10:01:00.000', 3), 1001, 'sess-1',
       'add_to_cart', '/p/widget', '', 5001, 12, '', 'mobile', 'US', map('qty','2')),
      (generateUUIDv4(), toDateTime64('2026-05-11 10:02:00.000', 3), 1001, 'sess-1',
       'checkout', '/checkout', '', NULL, NULL, '', 'mobile', 'US', map()),
      (generateUUIDv4(), toDateTime64('2026-05-11 10:03:00.000', 3), 1001, 'sess-1',
       'purchase_complete', '/thanks', '', NULL, NULL, '', 'mobile', 'US', map())
  `);
  await db.command(ckSql`
    INSERT INTO scenario_cdp_orders (
      order_id, updated_at, user_id, order_date, status, total_amount, currency,
      item_count, items
    ) VALUES
      ('ord-1', toDateTime64('2026-05-11 10:03:30.000', 3), 1001,
       toDate('2026-05-11'), 'paid', 79.99, 'USD', 2, [5001, 5002]),
      ('ord-2', toDateTime64('2026-04-20 14:15:00.000', 3), 1002,
       toDate('2026-04-20'), 'paid', 199.00, 'USD', 1, [5001]),
      ('ord-3', toDateTime64('2026-03-10 11:00:00.000', 3), 1001,
       toDate('2026-03-10'), 'paid', 25.50, 'USD', 1, [5002])
  `);
};

const seedStockTrades = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_stock_trades (
      symbol, exchange, trade_id, price, trade_size, conditions, tape, event_ts, received_ts
    ) VALUES
      ('AAPL', 'NASDAQ', 1, 195.500000, 100.00000000, [toFixedString('@T',2)],
       'NASDAQ', toDateTime64('2026-05-11 13:30:00.000000001', 9),
       toDateTime64('2026-05-11 13:30:00.000000050', 9)),
      ('AAPL', 'NASDAQ', 2, 195.520000, 50.00000000, [toFixedString('@T',2)],
       'NASDAQ', toDateTime64('2026-05-11 13:30:00.500000000', 9),
       toDateTime64('2026-05-11 13:30:00.500000040', 9)),
      ('MSFT', 'NASDAQ', 3, 412.300000, 200.00000000, [toFixedString('@T',2)],
       'NASDAQ', toDateTime64('2026-05-11 13:30:00.100000000', 9),
       toDateTime64('2026-05-11 13:30:00.100000060', 9))
  `);
};

const seedDexSwaps = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_dex_swaps (
      chain_id, pool_address, tx_hash, block_height, block_ts, token_in, token_out,
      amount_in, amount_out, amount_in_usd, amount_out_usd, sender, recipient, fee_tier
    ) VALUES
      ('ethereum', '0xUNISWAPV3-pool-aaa',
       toFixedString('0x11111111111111111111111111111111111111111111111111111111111111aa', 66),
       19000000, toDateTime64('2026-05-11 09:00:00.000', 3),
       'ETH', 'USDC', 1.500000000000000000, 4500.000000000000000000,
       4500.000000, 4500.000000, '0xsender-1', '0xrecipient-1', 3000),
      ('arbitrum', '0xCAMELOT-pool-bbb',
       toFixedString('0x22222222222222222222222222222222222222222222222222222222222222bb', 66),
       150000000, toDateTime64('2026-05-11 09:00:01.000', 3),
       'ETH', 'USDC', 2.000000000000000000, 5990.000000000000000000,
       6000.000000, 5990.000000, '0xsender-2', '0xrecipient-2', 500)
  `);
};

const seedSolanaTransactions = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_solana_transactions (
      block_slot, block_hash, block_time, tx_index, signature, success, fee,
      compute_units_used, compute_units_limit, recent_blockhash,
      account_keys, log_messages, pre_balances, post_balances,
      \`token_balance_changes.account_index\`, \`token_balance_changes.mint\`,
      \`token_balance_changes.owner\`, \`token_balance_changes.amount_pre\`,
      \`token_balance_changes.amount_post\`, \`token_balance_changes.decimals\`
    ) VALUES
      (250000000, toFixedString('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 44),
       toDateTime64('2026-05-11 09:00:00.000', 3), 0,
       toFixedString('sig-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 88),
       true, 5000, 200000, 200000,
       toFixedString('blkhash-1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 44),
       ['9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'], ['Program log: success'],
       [1000000000], [999995000],
       [0], ['So11111111111111111111111111111111111111112'], ['owner-1'],
       [1000], [950], [9])
  `);
};

const seedNycTaxiTrips = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_nyc_taxi_trips (
      trip_id, vendor_id, pickup_datetime, dropoff_datetime, store_and_fwd_flag,
      rate_code_id, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
      passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount,
      tolls_amount, total_amount, payment_type, pickup_ntaname, dropoff_ntaname
    ) VALUES
      (1, 'CMT', toDateTime('2026-04-10 08:00:00'), toDateTime('2026-04-10 08:15:00'),
       0, 1, -73.985, 40.758, -73.969, 40.785, 1, 1.6,
       8.50, 0.50, 0.50, 2.00, 0.00, 11.50, 'CRE', 'Midtown', 'Upper East Side'),
      (2, 'VTS', toDateTime('2026-04-10 09:00:00'), toDateTime('2026-04-10 09:25:00'),
       0, 1, -73.991, 40.749, -74.005, 40.715, 2, 3.5,
       15.00, 1.00, 0.50, 3.00, 0.00, 19.50, 'CRE', 'Chelsea', 'Tribeca'),
      (3, 'CMT', toDateTime('2026-04-10 10:00:00'), toDateTime('2026-04-10 10:10:00'),
       0, 1, -73.978, 40.752, -73.964, 40.770, 1, 1.2,
       7.00, 0.00, 0.50, 0.00, 0.00, 7.50, 'CSH', 'Midtown', 'Upper East Side')
  `);
};

const seedIotTelemetry = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_iot_telemetry (
      device_id, plant_id, protocol, ts, metric_name, value_float, value_int,
      value_str, quality_code, tags
    ) VALUES
      ('CNC-LINE3-07', 'shanghai-plant-1', 'OPC-UA',
       toDateTime64('2026-05-11 12:00:00.000', 3), 'temperature', 72.5, NULL, NULL,
       192, map('zone','assembly')),
      ('CNC-LINE3-07', 'shanghai-plant-1', 'OPC-UA',
       toDateTime64('2026-05-11 12:00:05.000', 3), 'temperature', 86.2, NULL, NULL,
       192, map('zone','assembly')),
      ('CNC-LINE3-08', 'shanghai-plant-1', 'Modbus',
       toDateTime64('2026-05-11 12:00:00.000', 3), 'vibration_rms', 0.42, NULL, NULL,
       192, map('zone','milling')),
      ('CNC-LINE3-08', 'shanghai-plant-1', 'Modbus',
       toDateTime64('2026-05-11 12:00:00.000', 3), 'output_count', NULL, 1000, NULL,
       192, map('zone','milling'))
  `);
};

const seedGameEvents = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_game_events (
      game_id, event_type, player_id, session_id, event_ts, platform, country,
      level_id, score, duration_ms, is_win, revenue_usd, properties, ab_variant
    ) VALUES
      ('subway-runner', 'level_complete', 9001, 100001,
       toDateTime64('2026-05-11 14:00:00.000', 3), 'ios', toFixedString('US',2),
       1, 1500, 45000, true, NULL, map('difficulty','normal'), 'control'),
      ('subway-runner', 'level_complete', 9001, 100001,
       toDateTime64('2026-05-11 14:01:00.000', 3), 'ios', toFixedString('US',2),
       2, 1800, 50000, false, NULL, map('difficulty','normal'), 'control'),
      ('subway-runner', 'purchase', 9001, 100001,
       toDateTime64('2026-05-11 14:05:00.000', 3), 'ios', toFixedString('US',2),
       0, 0, 0, NULL, 4.99, map('item','coin_pack'), 'control'),
      ('subway-runner', 'purchase', 9002, 100002,
       toDateTime64('2026-05-11 14:10:00.000', 3), 'android', toFixedString('GB',2),
       0, 0, 0, NULL, 9.99, map('item','remove_ads'), 'treatment')
  `);
};

const seedMeterEvents = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_meter_events (
      customer_id, meter_slug, event_ts, idempotency_key, value, properties
    ) VALUES
      ('cus_alpha', 'api_calls', toDateTime64('2026-05-11 10:00:00.000', 3),
       'idem-1', 1.0, map('endpoint','/v1/chat')),
      ('cus_alpha', 'api_calls', toDateTime64('2026-05-11 10:01:00.000', 3),
       'idem-2', 1.0, map('endpoint','/v1/chat')),
      ('cus_alpha', 'tokens', toDateTime64('2026-05-11 10:00:00.000', 3),
       'idem-3', 2400.0, map('model','gpt-4')),
      ('cus_beta', 'api_calls', toDateTime64('2026-05-11 10:30:00.000', 3),
       'idem-4', 1.0, map('endpoint','/v1/images')),
      ('cus_beta', 'storage_gb', toDateTime64('2026-05-11 10:30:00.000', 3),
       'idem-5', 12.5, map())
  `);
};

const seedMlUserEvents = async () => {
  const db = createE2EDb();
  await db.command(ckSql`
    INSERT INTO scenario_ml_user_events (
      user_id, domain, url, client_ip, user_agent, referer_cat_id, url_cat_id,
      fetch_timing_ms, is_bounce, event_ts
    ) VALUES
      (42, 'shop.example.com', '/', 3232235521, 1, 0, 1, 150, 0,
       toDateTime64('2026-05-11 08:00:00.000', 3)),
      (42, 'shop.example.com', '/cart', 3232235521, 1, 0, 2, 220, 0,
       toDateTime64('2026-05-11 08:01:00.000', 3)),
      (43, 'shop.example.com', '/', 3232235522, 1, 0, 1, 95, 1,
       toDateTime64('2026-05-11 08:05:00.000', 3))
  `);
};

const seedScenarioCases = async () => {
  await seedClickhouseLogPlatform();
  await seedSignozTraces();
  await seedOtelTraces();
  await seedSignozMetricsSamples();
  await seedHighlightLogs();
  await seedUberLogs();
  await seedPosthogEvents();
  await seedMetricaHits();
  await seedMuxVideoQoe();
  await seedSnowplowEvents();
  await seedCloudflareRequests();
  await seedGrowthbookExposures();
  await seedRtbAds();
  await seedMailchimpEmailEvents();
  await seedCdp();
  await seedStockTrades();
  await seedDexSwaps();
  await seedSolanaTransactions();
  await seedNycTaxiTrips();
  await seedIotTelemetry();
  await seedGameEvents();
  await seedMeterEvents();
  await seedMlUserEvents();
};

const seed = async () => {
  await recreateDatabase();
  await createTables();
  await insertUsers();
  await insertPets();
  await insertWebEvents();
  await insertRewardEvents();
  await insertTradeFills();
  await insertQuoteSnapshots();
  await insertSchemaPrimitives();
  await insertSchemaCompound();
  await insertSchemaAggregates();
  await insertSchemaGeo();
  await insertSchemaJsonAdvanced();
  await insertAuditLogTyped();
  await insertUserProfileTyped();
  await insertValidatorStrict();
  await insertValidatorTransform();
  await insertIoSplit();
  await insertChainedColumns();
  await seedScenarioCases();
};

await seed();
