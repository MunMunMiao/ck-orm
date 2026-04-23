export { buildCreateTableStatement, buildDropTableStatement } from "../src/schema-ddl";

import { alias, type ClickHouseClientConfig, chTable, chType, clickhouseClient } from "./ck-orm";

const requiredEnv = (name: string) => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required e2e env: ${name}`);
  }
  return value;
};

export const hasE2EEnv =
  Boolean(process.env.CLICKHOUSE_E2E_URL) &&
  Boolean(process.env.CLICKHOUSE_E2E_DATABASE) &&
  Boolean(process.env.CLICKHOUSE_E2E_USERNAME) &&
  Boolean(process.env.CLICKHOUSE_E2E_PASSWORD);

export const transportRoleFixtures = {
  analyst: process.env.CLICKHOUSE_E2E_ROLE_ANALYST,
  auditor: process.env.CLICKHOUSE_E2E_ROLE_AUDITOR,
  username: process.env.CLICKHOUSE_E2E_ROLE_USERNAME,
  password: process.env.CLICKHOUSE_E2E_ROLE_PASSWORD,
} as const;

export const hasTransportRoleFixtures =
  Boolean(transportRoleFixtures.analyst) &&
  Boolean(transportRoleFixtures.auditor) &&
  Boolean(transportRoleFixtures.username) &&
  Boolean(transportRoleFixtures.password);

export const experimentalSettings = {
  allow_experimental_json_type: 1,
  allow_experimental_dynamic_type: 1,
  allow_experimental_qbit_type: 1,
} as const;

export const datasetCounts = {
  users: 5_000,
  pets: 8_000,
  webEvents: 100_000,
  rewardEventsPhysicalRows: 24_000,
  tradeFills: 20_000,
  quoteSnapshots: 40_000,
} as const;

export const users = chTable(
  "users",
  {
    id: chType.int32(),
    name: chType.string(),
    tier: chType.string(),
    created_at: chType.dateTime64({ precision: 3 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const pets = chTable(
  "pets",
  {
    id: chType.int32(),
    owner_id: chType.int32(),
    pet_name: chType.string(),
    created_at: chType.dateTime64({ precision: 3 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.owner_id, table.id],
  }),
);

export const webEvents = chTable(
  "web_events",
  {
    event_id: chType.uint64(),
    user_id: chType.int32(),
    country: chType.lowCardinality(chType.string()),
    device_type: chType.lowCardinality(chType.string()),
    viewed_at: chType.dateTime64({ precision: 3 }),
    revenue: chType.decimal({ precision: 18, scale: 2 }),
    tags: chType.array(chType.string()),
    tag_scores: chType.array(chType.uint8()),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.viewed_at, table.user_id, table.event_id],
  }),
);

export const rewardEvents = chTable(
  "reward_events",
  {
    id: chType.int32(),
    user_id: chType.string(),
    reward_points: chType.decimal({ precision: 20, scale: 5 }),
    order_id: chType.int64(),
    channel: chType.int32(),
    created_at: chType.dateTime64({ precision: 3 }),
    _peerdb_synced_at: chType.dateTime64({ precision: 3 }),
    _peerdb_is_deleted: chType.uint8(),
    _peerdb_version: chType.uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.user_id, table.id],
    versionColumn: table._peerdb_version,
  }),
);

export const tradeFills = chTable(
  "trade_fills",
  {
    trade_id: chType.uint64(),
    user_id: chType.int32(),
    symbol: chType.lowCardinality(chType.string()),
    filled_at: chType.dateTime64({ precision: 3 }),
    quantity: chType.decimal({ precision: 18, scale: 2 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.symbol, table.filled_at, table.trade_id],
  }),
);

export const quoteSnapshots = chTable(
  "quote_snapshots",
  {
    symbol: chType.lowCardinality(chType.string()),
    quote_time: chType.dateTime64({ precision: 3 }),
    bid: chType.decimal({ precision: 18, scale: 5 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.symbol, table.quote_time],
  }),
);

export const userDailySummary = chTable(
  "user_daily_summary",
  {
    day: chType.date(),
    user_id: chType.int32(),
    total_events: chType.uint64(),
    total_revenue: chType.decimal({ precision: 18, scale: 2 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.day, table.user_id],
  }),
);

export const auditEvents = chTable(
  "audit_events",
  {
    id: chType.int32(),
    user_id: chType.int32(),
    event_name: chType.string(),
    created_at: chType.dateTime64({ precision: 3 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const writePathBigInts = chTable(
  "write_path_bigints",
  {
    id: chType.int32(),
    label: chType.string(),
    int64_value: chType.int64(),
    uint64_value: chType.uint64(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const schemaPrimitives = chTable(
  "schema_primitives",
  {
    id: chType.int32(),
    int8_value: chType.int8(),
    int16_value: chType.int16(),
    int32_value: chType.int32(),
    int64_value: chType.int64(),
    uint8_value: chType.uint8(),
    uint16_value: chType.uint16(),
    uint32_value: chType.uint32(),
    uint64_value: chType.uint64(),
    float32_value: chType.float32(),
    float64_value: chType.float64(),
    bfloat16_value: chType.bfloat16(),
    string_value: chType.string(),
    fixed_string_value: chType.fixedString({ length: 4 }),
    decimal_value: chType.decimal({ precision: 18, scale: 2 }),
    date_value: chType.date(),
    date32_value: chType.date32(),
    time_value: chType.time(),
    time64_value: chType.time64({ precision: 3 }),
    date_time_value: chType.dateTime(),
    date_time64_value: chType.dateTime64({ precision: 3 }),
    bool_value: chType.bool(),
    uuid_value: chType.uuid(),
    ipv4_value: chType.ipv4(),
    ipv6_value: chType.ipv6(),
    json_value: chType.json<{ id: number; label: string }>(),
    dynamic_value: chType.dynamic<unknown>(),
    qbit_value: chType.qbit(chType.float32(), { dimensions: 8 }),
    enum8_value: chType.enum8({ active: 1, paused: 2 }),
    enum16_value: chType.enum16({ bronze: 1000, silver: 2000 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const schemaCompound = chTable(
  "schema_compound",
  {
    id: chType.int32(),
    nullable_value: chType.nullable(chType.string()),
    array_value: chType.array(chType.string()),
    tuple_value: chType.tuple(chType.string(), chType.int32()),
    map_value: chType.map(chType.string(), chType.int32()),
    variant_value: chType.variant(chType.int32(), chType.string()),
    low_cardinality_value: chType.lowCardinality(chType.string()),
    nested_value: chType.nested({
      name: chType.string(),
      score: chType.int32(),
    }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const schemaAggregates = chTable(
  "schema_aggregates",
  {
    id: chType.uint32(),
    agg_sum_state: chType.aggregateFunction<string>("sum", chType.uint64()),
    simple_sum_value: chType.simpleAggregateFunction<number>("sum", chType.int64()),
  },
  (table) => ({
    engine: "AggregatingMergeTree",
    orderBy: [table.id],
  }),
);

export const schemaGeo = chTable(
  "schema_geo",
  {
    id: chType.int32(),
    point_value: chType.point(),
    ring_value: chType.ring(),
    line_value: chType.lineString(),
    multi_line_value: chType.multiLineString(),
    polygon_value: chType.polygon(),
    multi_polygon_value: chType.multiPolygon(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const e2eSchema = {
  users,
  pets,
  webEvents,
  rewardEvents,
  tradeFills,
  quoteSnapshots,
  userDailySummary,
  auditEvents,
  writePathBigInts,
  schemaPrimitives,
  schemaCompound,
  schemaAggregates,
  schemaGeo,
};

export type E2ESchema = typeof e2eSchema;

export const getE2EConfig = () => {
  return {
    host: requiredEnv("CLICKHOUSE_E2E_URL"),
    database: requiredEnv("CLICKHOUSE_E2E_DATABASE"),
    username: requiredEnv("CLICKHOUSE_E2E_USERNAME"),
    password: requiredEnv("CLICKHOUSE_E2E_PASSWORD"),
  };
};

export const getE2EDatabaseUrl = () => {
  const config = getE2EConfig();
  const url = new URL(config.host);
  url.username = config.username;
  url.password = config.password;
  url.pathname = `/${config.database}`;
  return url.toString();
};

export const createE2EDb = (
  overrides?: Omit<
    ClickHouseClientConfig<E2ESchema>,
    "databaseUrl" | "host" | "database" | "username" | "password" | "schema"
  >,
) => {
  const config = getE2EConfig();
  return clickhouseClient({
    host: config.host,
    database: config.database,
    username: config.username,
    password: config.password,
    schema: e2eSchema,
    clickhouse_settings: {
      ...experimentalSettings,
      ...(overrides?.clickhouse_settings ?? {}),
    },
    ...(overrides ?? {}),
  });
};

export const createAdminDb = () => {
  const config = getE2EConfig();
  return clickhouseClient({
    host: config.host,
    database: "default",
    username: config.username,
    password: config.password,
    schema: {},
    clickhouse_settings: experimentalSettings,
  });
};

export const createTempTableName = (prefix: string) => {
  const suffix = Math.random().toString(36).slice(2, 8);
  return `${prefix}_${Date.now()}_${suffix}`;
};

export const aliasedUsers = alias(users, "u");
