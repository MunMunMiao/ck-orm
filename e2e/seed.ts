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

const seedScenarioCases = async () => {
  await seedClickhouseLogPlatform();
  await seedCdp();
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
