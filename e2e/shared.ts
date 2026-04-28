export { buildCreateTableStatement, buildDropTableStatement } from "../src/schema-ddl";

import { type ClickHouseClientConfig, ckAlias, ckTable, ckType, clickhouseClient } from "./ck-orm";

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

export const users = ckTable(
  "users",
  {
    id: ckType.int32(),
    name: ckType.string(),
    tier: ckType.string(),
    created_at: ckType.dateTime64({ precision: 3 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const pets = ckTable(
  "pets",
  {
    id: ckType.int32(),
    owner_id: ckType.int32(),
    pet_name: ckType.string(),
    created_at: ckType.dateTime64({ precision: 3 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.owner_id, table.id],
  }),
);

export const webEvents = ckTable(
  "web_events",
  {
    event_id: ckType.uint64(),
    user_id: ckType.int32(),
    country: ckType.lowCardinality(ckType.string()),
    device_type: ckType.lowCardinality(ckType.string()),
    viewed_at: ckType.dateTime64({ precision: 3 }),
    revenue: ckType.decimal({ precision: 18, scale: 2 }),
    tags: ckType.array(ckType.string()),
    tag_scores: ckType.array(ckType.uint8()),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.viewed_at, table.user_id, table.event_id],
  }),
);

export const rewardEvents = ckTable(
  "reward_events",
  {
    id: ckType.int32(),
    user_id: ckType.string(),
    reward_points: ckType.decimal({ precision: 20, scale: 5 }),
    order_id: ckType.int64(),
    channel: ckType.int32(),
    created_at: ckType.dateTime64({ precision: 3 }),
    _peerdb_synced_at: ckType.dateTime64({ precision: 3 }),
    _peerdb_is_deleted: ckType.uint8(),
    _peerdb_version: ckType.uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.user_id, table.id],
    versionColumn: table._peerdb_version,
  }),
);

export const tradeFills = ckTable(
  "trade_fills",
  {
    trade_id: ckType.uint64(),
    user_id: ckType.int32(),
    symbol: ckType.lowCardinality(ckType.string()),
    filled_at: ckType.dateTime64({ precision: 3 }),
    quantity: ckType.decimal({ precision: 18, scale: 2 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.symbol, table.filled_at, table.trade_id],
  }),
);

export const quoteSnapshots = ckTable(
  "quote_snapshots",
  {
    symbol: ckType.lowCardinality(ckType.string()),
    quote_time: ckType.dateTime64({ precision: 3 }),
    bid: ckType.decimal({ precision: 18, scale: 5 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.symbol, table.quote_time],
  }),
);

export const userDailySummary = ckTable(
  "user_daily_summary",
  {
    day: ckType.date(),
    user_id: ckType.int32(),
    total_events: ckType.uint64(),
    total_revenue: ckType.decimal({ precision: 18, scale: 2 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.day, table.user_id],
  }),
);

export const auditEvents = ckTable(
  "audit_events",
  {
    id: ckType.int32(),
    user_id: ckType.int32(),
    event_name: ckType.string(),
    created_at: ckType.dateTime64({ precision: 3 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const writePathBigInts = ckTable(
  "write_path_bigints",
  {
    id: ckType.int32(),
    label: ckType.string(),
    int64_value: ckType.int64(),
    uint64_value: ckType.uint64(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const schemaPrimitives = ckTable(
  "schema_primitives",
  {
    id: ckType.int32(),
    int8_value: ckType.int8(),
    int16_value: ckType.int16(),
    int32_value: ckType.int32(),
    int64_value: ckType.int64(),
    uint8_value: ckType.uint8(),
    uint16_value: ckType.uint16(),
    uint32_value: ckType.uint32(),
    uint64_value: ckType.uint64(),
    float32_value: ckType.float32(),
    float64_value: ckType.float64(),
    bfloat16_value: ckType.bfloat16(),
    string_value: ckType.string(),
    fixed_string_value: ckType.fixedString({ length: 4 }),
    decimal_value: ckType.decimal({ precision: 18, scale: 2 }),
    date_value: ckType.date(),
    date32_value: ckType.date32(),
    time_value: ckType.time(),
    time64_value: ckType.time64({ precision: 3 }),
    date_time_value: ckType.dateTime(),
    date_time64_value: ckType.dateTime64({ precision: 3 }),
    bool_value: ckType.bool(),
    uuid_value: ckType.uuid(),
    ipv4_value: ckType.ipv4(),
    ipv6_value: ckType.ipv6(),
    json_value: ckType.json<{ id: number; label: string }>(),
    dynamic_value: ckType.dynamic<unknown>(),
    qbit_value: ckType.qbit(ckType.float32(), { dimensions: 8 }),
    enum8_value: ckType.enum8({ active: 1, paused: 2 }),
    enum16_value: ckType.enum16({ bronze: 1000, silver: 2000 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const schemaCompound = ckTable(
  "schema_compound",
  {
    id: ckType.int32(),
    nullable_value: ckType.nullable(ckType.string()),
    array_value: ckType.array(ckType.string()),
    tuple_value: ckType.tuple(ckType.string(), ckType.int32()),
    map_value: ckType.map(ckType.string(), ckType.int32()),
    variant_value: ckType.variant(ckType.int32(), ckType.string()),
    low_cardinality_value: ckType.lowCardinality(ckType.string()),
    nested_value: ckType.nested({
      name: ckType.string(),
      score: ckType.int32(),
    }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const schemaAggregates = ckTable(
  "schema_aggregates",
  {
    id: ckType.uint32(),
    agg_sum_state: ckType.aggregateFunction<string>("sum", ckType.uint64()),
    simple_sum_value: ckType.simpleAggregateFunction<number>("sum", ckType.int64()),
  },
  (table) => ({
    engine: "AggregatingMergeTree",
    orderBy: [table.id],
  }),
);

export const schemaGeo = ckTable(
  "schema_geo",
  {
    id: ckType.int32(),
    point_value: ckType.point(),
    ring_value: ckType.ring(),
    line_value: ckType.lineString(),
    multi_line_value: ckType.multiLineString(),
    polygon_value: ckType.polygon(),
    multi_polygon_value: ckType.multiPolygon(),
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

export const aliasedUsers = ckAlias(users, "u");
