import { sql } from "./ck-orm";
import {
  auditEvents,
  buildCreateTableStatement,
  buildDropTableStatement,
  createAdminDb,
  createE2EDb,
  datasetCounts,
  getE2EConfig,
  pets,
  quoteSnapshots,
  rewardEvents,
  schemaAggregates,
  schemaCompound,
  schemaGeo,
  schemaPrimitives,
  tradeFills,
  userDailySummary,
  users,
  webEvents,
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
];
const schemaTables = [schemaPrimitives, schemaCompound, schemaAggregates, schemaGeo];

const recreateDatabase = async () => {
  const adminDb = createAdminDb();
  const { database } = getE2EConfig();

  await adminDb.command(sql`DROP DATABASE IF EXISTS ${sql.identifier(database)}`);
  await adminDb.command(sql`CREATE DATABASE ${sql.identifier(database)}`);
};

const createTables = async () => {
  const db = createE2EDb();

  for (const table of [...scenarioTables, ...schemaTables]) {
    await db.command(buildDropTableStatement(table.originalName));
    await db.command(buildCreateTableStatement(table));
  }
};

const insertUsers = async () => {
  const db = createE2EDb();
  await db.command(`
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
  await db.command(`
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
  await db.command(`
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
  await db.command(`
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
  await db.command(`
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
  await db.command(`
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
  await db.command(`
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
  await db.insertJsonEachRow("schema_compound", [
    {
      id: 1,
      nullable_value: null,
      array_value: ["alpha", "beta"],
      tuple_value: ["login", 42],
      map_value: { a: 1, b: 2 },
      variant_value: 7,
      low_cardinality_value: "vip",
      "nested_value.name": ["first", "second"],
      "nested_value.score": [10, 20],
    },
  ]);
};

const insertSchemaAggregates = async () => {
  const db = createE2EDb();
  await db.command(`
    INSERT INTO schema_aggregates
    SELECT
      toUInt32(1),
      sumState(toUInt64(7)),
      toInt64(11)
  `);
};

const insertSchemaGeo = async () => {
  const db = createE2EDb();
  await db.command(`
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
};

await seed();
