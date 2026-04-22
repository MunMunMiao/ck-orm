import type { AnyColumn } from "../src/columns";
import type { AnyTable, Table } from "../src/schema";
import {
  aggregateFunction,
  alias,
  array,
  bfloat16,
  bool,
  type ClickHouseClientConfig,
  chTable,
  clickhouseClient,
  date,
  date32,
  dateTime,
  dateTime64,
  decimal,
  dynamic,
  enum8,
  enum16,
  fixedString,
  float32,
  float64,
  int8,
  int16,
  int32,
  int64,
  ipv4,
  ipv6,
  json,
  lineString,
  lowCardinality,
  map,
  multiLineString,
  multiPolygon,
  nested,
  nullable,
  point,
  polygon,
  qbit,
  ring,
  simpleAggregateFunction,
  string,
  time,
  time64,
  tuple,
  uint8,
  uint16,
  uint32,
  uint64,
  uuid,
  variant,
} from "./ck-orm";

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
    id: int32(),
    name: string(),
    tier: string(),
    created_at: dateTime64(3),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const pets = chTable(
  "pets",
  {
    id: int32(),
    owner_id: int32(),
    pet_name: string(),
    created_at: dateTime64(3),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.owner_id, table.id],
  }),
);

export const webEvents = chTable(
  "web_events",
  {
    event_id: uint64(),
    user_id: int32(),
    country: lowCardinality(string()),
    device_type: lowCardinality(string()),
    viewed_at: dateTime64(3),
    revenue: decimal(18, 2),
    tags: array(string()),
    tag_scores: array(uint8()),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.viewed_at, table.user_id, table.event_id],
  }),
);

export const rewardEvents = chTable(
  "reward_events",
  {
    id: int32(),
    user_id: string(),
    reward_points: decimal(20, 5),
    order_id: int64(),
    channel: int32(),
    created_at: dateTime64(3),
    _peerdb_synced_at: dateTime64(3),
    _peerdb_is_deleted: uint8(),
    _peerdb_version: uint64(),
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
    trade_id: uint64(),
    user_id: int32(),
    symbol: lowCardinality(string()),
    filled_at: dateTime64(3),
    quantity: decimal(18, 2),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.symbol, table.filled_at, table.trade_id],
  }),
);

export const quoteSnapshots = chTable(
  "quote_snapshots",
  {
    symbol: lowCardinality(string()),
    quote_time: dateTime64(3),
    bid: decimal(18, 5),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.symbol, table.quote_time],
  }),
);

export const userDailySummary = chTable(
  "user_daily_summary",
  {
    day: date(),
    user_id: int32(),
    total_events: uint64(),
    total_revenue: decimal(18, 2),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.day, table.user_id],
  }),
);

export const auditEvents = chTable(
  "audit_events",
  {
    id: int32(),
    user_id: int32(),
    event_name: string(),
    created_at: dateTime64(3),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const schemaPrimitives = chTable(
  "schema_primitives",
  {
    id: int32(),
    int8_value: int8(),
    int16_value: int16(),
    int32_value: int32(),
    int64_value: int64(),
    uint8_value: uint8(),
    uint16_value: uint16(),
    uint32_value: uint32(),
    uint64_value: uint64(),
    float32_value: float32(),
    float64_value: float64(),
    bfloat16_value: bfloat16(),
    string_value: string(),
    fixed_string_value: fixedString(4),
    decimal_value: decimal(18, 2),
    date_value: date(),
    date32_value: date32(),
    time_value: time(),
    time64_value: time64(3),
    date_time_value: dateTime(),
    date_time64_value: dateTime64(3),
    bool_value: bool(),
    uuid_value: uuid(),
    ipv4_value: ipv4(),
    ipv6_value: ipv6(),
    json_value: json<{ id: number; label: string }>(),
    dynamic_value: dynamic<unknown>(),
    qbit_value: qbit(float32(), 8),
    enum8_value: enum8({ active: 1, paused: 2 }),
    enum16_value: enum16({ bronze: 1000, silver: 2000 }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

export const schemaCompound = chTable(
  "schema_compound",
  {
    id: int32(),
    nullable_value: nullable(string()),
    array_value: array(string()),
    tuple_value: tuple(string(), int32()),
    map_value: map(string(), int32()),
    variant_value: variant(int32(), string()),
    low_cardinality_value: lowCardinality(string()),
    nested_value: nested({
      name: string(),
      score: int32(),
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
    id: uint32(),
    agg_sum_state: aggregateFunction<string>("sum", uint64()),
    simple_sum_value: simpleAggregateFunction<number>("sum", int64()),
  },
  (table) => ({
    engine: "AggregatingMergeTree",
    orderBy: [table.id],
  }),
);

export const schemaGeo = chTable(
  "schema_geo",
  {
    id: int32(),
    point_value: point(),
    ring_value: ring(),
    line_value: lineString(),
    multi_line_value: multiLineString(),
    polygon_value: polygon(),
    multi_polygon_value: multiPolygon(),
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

const escapeIdentifier = (value: string) => {
  return `\`${value.replaceAll("`", "``")}\``;
};

const renderColumnName = (column: AnyColumn) => {
  if (!column.name) {
    throw new Error(`Expected bound column name for ${column.sqlType}`);
  }
  return escapeIdentifier(column.name);
};

const renderEngineClause = (table: AnyTable) => {
  const engine = table.options.engine ?? "MergeTree";
  if (engine === "ReplacingMergeTree" && table.options.versionColumn?.name) {
    return `${engine}(${escapeIdentifier(table.options.versionColumn.name)})`;
  }
  return engine;
};

export const buildCreateTableStatement = (table: Table<Record<string, AnyColumn>>) => {
  const columnDefinitions = Object.values(table.columns)
    .map((column) => `  ${renderColumnName(column)} ${column.sqlType}`)
    .join(",\n");

  const orderByColumns = table.options.orderBy?.length
    ? table.options.orderBy.map(renderColumnName).join(", ")
    : "tuple()";

  return `
    CREATE TABLE ${table.originalName}
    (
${columnDefinitions}
    )
    ENGINE = ${renderEngineClause(table)}
    ORDER BY (${orderByColumns})
  `.trim();
};

export const buildDropTableStatement = (tableName: string) => `DROP TABLE IF EXISTS ${escapeIdentifier(tableName)}`;

export const aliasedUsers = alias(users, "u");
