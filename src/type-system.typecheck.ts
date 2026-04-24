import {
  alias,
  type Column,
  chTable,
  chType,
  ck,
  clickhouseClient,
  csql,
  fn,
  type InferInsertModel,
  type InferSelectModel,
  type Selection,
} from "./index";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Expect<T extends true> = T;
type DataOf<TValue> = TValue extends Column<infer TData> ? TData : never;
type InferBuilderResult<TValue> = Awaited<TValue> extends Array<infer TResult> ? TResult : never;

const chTypeNameMatrix = {
  aggregateFunction: [
    chType.aggregateFunction("sum", chType.uint64()),
    chType.aggregateFunction("sum_state", { name: "sum", args: [chType.uint64()] }),
  ],
  array: [chType.array(chType.string()), chType.array("tag_names", chType.string())],
  bfloat16: [chType.bfloat16(), chType.bfloat16("score_bf16")],
  bool: [chType.bool(), chType.bool("is_active")],
  date: [chType.date(), chType.date("event_date")],
  date32: [chType.date32(), chType.date32("event_date32")],
  dateTime: [chType.dateTime(), chType.dateTime("created_at")],
  dateTime64: [
    chType.dateTime64({ precision: 9, timezone: "UTC" }),
    chType.dateTime64("created_at_64", { precision: 9, timezone: "UTC" }),
  ],
  decimal: [chType.decimal({ precision: 20, scale: 5 }), chType.decimal("reward_points", { precision: 20, scale: 5 })],
  dynamic: [chType.dynamic<{ label: string }>(), chType.dynamic<{ label: string }>("payload_dynamic")],
  enum8: [
    chType.enum8<"open" | "closed">({ open: 1, closed: 2 }),
    chType.enum8<"open" | "closed">("status_8", { open: 1, closed: 2 }),
  ],
  enum16: [
    chType.enum16<"small" | "large">({ small: 1, large: 1000 }),
    chType.enum16<"small" | "large">("status_16", { small: 1, large: 1000 }),
  ],
  fixedString: [chType.fixedString({ length: 8 }), chType.fixedString("code", { length: 8 })],
  float32: [chType.float32(), chType.float32("ratio_32")],
  float64: [chType.float64(), chType.float64("ratio_64")],
  int8: [chType.int8(), chType.int8("i8")],
  int16: [chType.int16(), chType.int16("i16")],
  int32: [chType.int32(), chType.int32("i32")],
  int64: [chType.int64(), chType.int64("i64")],
  ipv4: [chType.ipv4(), chType.ipv4("ip_v4")],
  ipv6: [chType.ipv6(), chType.ipv6("ip_v6")],
  json: [chType.json<{ id: number }>(), chType.json<{ id: number }>("payload_json")],
  lineString: [chType.lineString(), chType.lineString("line_value")],
  lowCardinality: [chType.lowCardinality(chType.string()), chType.lowCardinality("region", chType.string())],
  map: [chType.map(chType.string(), chType.int32()), chType.map("attrs", chType.string(), chType.int32())],
  multiLineString: [chType.multiLineString(), chType.multiLineString("multi_line_value")],
  multiPolygon: [chType.multiPolygon(), chType.multiPolygon("multi_polygon_value")],
  nested: [
    chType.nested({ id: chType.int32(), name: chType.string() }),
    chType.nested("profiles", { id: chType.int32(), name: chType.string() }),
  ],
  nullable: [chType.nullable(chType.string()), chType.nullable("optional_note", chType.string())],
  point: [chType.point(), chType.point("point_value")],
  polygon: [chType.polygon(), chType.polygon("polygon_value")],
  qbit: [
    chType.qbit(chType.float32(), { dimensions: 8 }),
    chType.qbit("embedding", chType.float32(), { dimensions: 8 }),
  ],
  ring: [chType.ring(), chType.ring("ring_value")],
  simpleAggregateFunction: [
    chType.simpleAggregateFunction("sum", chType.uint64()),
    chType.simpleAggregateFunction("sum_value", { name: "sum", value: chType.uint64() }),
  ],
  string: [chType.string(), chType.string("user_id")],
  time: [chType.time(), chType.time("event_time")],
  time64: [chType.time64({ precision: 6 }), chType.time64("event_time_64", { precision: 6 })],
  tuple: [chType.tuple(chType.int32(), chType.string()), chType.tuple("point_pair", chType.int32(), chType.string())],
  uint8: [chType.uint8(), chType.uint8("u8")],
  uint16: [chType.uint16(), chType.uint16("u16")],
  uint32: [chType.uint32(), chType.uint32("u32")],
  uint64: [chType.uint64(), chType.uint64("u64")],
  uuid: [chType.uuid(), chType.uuid("entity_uuid")],
  variant: [
    chType.variant(chType.string(), chType.int32()),
    chType.variant("variant_value", chType.string(), chType.int32()),
  ],
} satisfies { readonly [K in keyof typeof chType]: readonly [unknown, unknown] };

const columnTypeMatrix = {
  aggregateFunction: chType.aggregateFunction<number>("sum", chType.uint64()),
  array: chType.array(chType.string()),
  bfloat16: chType.bfloat16(),
  bool: chType.bool(),
  date: chType.date(),
  date32: chType.date32(),
  dateTime: chType.dateTime(),
  dateTime64: chType.dateTime64({ precision: 9 }),
  decimal: chType.decimal({ precision: 20, scale: 5 }),
  dynamic: chType.dynamic<{ label: string }>(),
  enum8: chType.enum8<"open" | "closed">({ open: 1, closed: 2 }),
  enum16: chType.enum16<"small" | "large">({ small: 1, large: 1000 }),
  fixedString: chType.fixedString({ length: 8 }),
  float32: chType.float32(),
  float64: chType.float64(),
  int8: chType.int8(),
  int16: chType.int16(),
  int32: chType.int32(),
  int64: chType.int64(),
  ipv4: chType.ipv4(),
  ipv6: chType.ipv6(),
  json: chType.json<{ id: number }>(),
  lineString: chType.lineString(),
  lowCardinality: chType.lowCardinality(chType.string()),
  map: chType.map(chType.string(), chType.int32()),
  multiLineString: chType.multiLineString(),
  multiPolygon: chType.multiPolygon(),
  nested: chType.nested({ id: chType.int32(), name: chType.string() }),
  nullable: chType.nullable(chType.string()),
  point: chType.point(),
  polygon: chType.polygon(),
  qbit: chType.qbit(chType.float32(), { dimensions: 8 }),
  ring: chType.ring(),
  simpleAggregateFunction: chType.simpleAggregateFunction<number>("sum", chType.uint64()),
  string: chType.string(),
  time: chType.time(),
  time64: chType.time64({ precision: 6 }),
  tuple: chType.tuple(chType.int32(), chType.string()),
  uint8: chType.uint8(),
  uint16: chType.uint16(),
  uint32: chType.uint32(),
  uint64: chType.uint64(),
  uuid: chType.uuid(),
  variant: chType.variant(chType.string(), chType.int32()),
} satisfies { readonly [K in keyof typeof chType]: Column };

type _ChTypeDataMatrix = Expect<
  Equal<
    { readonly [K in keyof typeof columnTypeMatrix]: DataOf<(typeof columnTypeMatrix)[K]> },
    {
      readonly aggregateFunction: number;
      readonly array: string[];
      readonly bfloat16: number;
      readonly bool: boolean;
      readonly date: Date;
      readonly date32: Date;
      readonly dateTime: Date;
      readonly dateTime64: Date;
      readonly decimal: string;
      readonly dynamic: { label: string };
      readonly enum8: "open" | "closed";
      readonly enum16: "small" | "large";
      readonly fixedString: string;
      readonly float32: number;
      readonly float64: number;
      readonly int8: number;
      readonly int16: number;
      readonly int32: number;
      readonly int64: string;
      readonly ipv4: string;
      readonly ipv6: string;
      readonly json: { id: number };
      readonly lineString: readonly [number, number][];
      readonly lowCardinality: string;
      readonly map: Record<string, number>;
      readonly multiLineString: readonly [number, number][][];
      readonly multiPolygon: readonly [number, number][][][];
      readonly nested: { id: number; name: string }[];
      readonly nullable: string | null;
      readonly point: readonly [number, number];
      readonly polygon: readonly [number, number][][];
      readonly qbit: readonly number[];
      readonly ring: readonly [number, number][];
      readonly simpleAggregateFunction: number;
      readonly string: string;
      readonly time: Date;
      readonly time64: Date;
      readonly tuple: readonly [number, string];
      readonly uint8: number;
      readonly uint16: number;
      readonly uint32: number;
      readonly uint64: string;
      readonly uuid: string;
      readonly variant: string | number;
    }
  >
>;

// @ts-expect-error decimal config must stay object-shaped.
chType.decimal(20, 5);
// @ts-expect-error decimal named config must stay object-shaped.
chType.decimal("reward_points", 20, 5);
// @ts-expect-error fixedString config must stay object-shaped.
chType.fixedString(8);
// @ts-expect-error fixedString named config must stay object-shaped.
chType.fixedString("code", 8);
// @ts-expect-error dateTime64 config must stay object-shaped.
chType.dateTime64(9);
// @ts-expect-error time64 config must stay object-shaped.
chType.time64(6);
// @ts-expect-error qbit dimensions must stay object-shaped.
chType.qbit(chType.float32(), 8);
// @ts-expect-error simpleAggregateFunction named form requires a value column.
chType.simpleAggregateFunction("sum_value", { name: "sum" });

const logicalColumns = chTable(
  "logical_columns",
  {
    userId: chType.string("user_id"),
    rewardPoints: chType.decimal("reward_points", { precision: 20, scale: 5 }),
    createdAt: chType.dateTime64("created_at", { precision: 9 }),
    tags: chType.array("tag_names", chType.string()),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.userId, table.createdAt],
  }),
);

type _LogicalSelectKeys = Expect<
  Equal<
    InferSelectModel<typeof logicalColumns>,
    {
      userId: string;
      rewardPoints: string;
      createdAt: Date;
      tags: string[];
    }
  >
>;
type _LogicalInsertKeys = Expect<
  Equal<InferInsertModel<typeof logicalColumns>, InferSelectModel<typeof logicalColumns>>
>;

const reportDeals = chTable("report_deals", {
  instance_id: chType.string(),
  source: chType.enum8<"mt4" | "mt5">({ mt4: 1, mt5: 2 }),
  deal_ticket: chType.uint64(),
  order_ticket: chType.int32(),
  position_ticket: chType.int32(),
  login: chType.int32(),
  time_utc: chType.dateTime64({ precision: 6 }),
  entry: chType.int8(),
  action: chType.int8(),
  price: chType.decimal({ precision: 18, scale: 5 }),
  profit: chType.decimal({ precision: 18, scale: 5 }),
  _peerdb_is_deleted: chType.int8(),
  _peerdb_version: chType.uint64(),
});

const petOwners = chTable("pet_owners", {
  id: chType.int32(),
  name: chType.string(),
});
const pets = chTable("pets", {
  id: chType.int32(),
  ownerId: chType.int32("owner_id"),
  petName: chType.string("pet_name"),
});

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/type_system",
  schema: {
    logicalColumns,
    petOwners,
    pets,
    reportDeals,
  },
});

db.insert(logicalColumns).values({
  userId: "user_1",
  rewardPoints: "10.50000",
  createdAt: new Date("2026-04-24T00:00:00.000Z"),
  tags: ["vip"],
});
// @ts-expect-error insert values must use logical schema keys, not physical column names.
db.insert(logicalColumns).values({ user_id: "user_1", reward_points: "10.50000" });

const aliasedLogicalColumns = alias(logicalColumns, "lc");
const aliasLogicalSelect = db.select({ userId: aliasedLogicalColumns.userId }).from(aliasedLogicalColumns);
type _AliasLogicalSelectType = Expect<Equal<InferBuilderResult<typeof aliasLogicalSelect>, { userId: string }>>;

const functionTypeMatrix = {
  array: fn.array<string>("vip", "pro"),
  arrayConcat: fn.arrayConcat<string>(["vip"], ["pro"]),
  arrayElement: fn.arrayElement<string>(["vip"], 1),
  arrayElementOrNull: fn.arrayElementOrNull<string>(["vip"], 2),
  arrayFlatten: fn.arrayFlatten<string>([["vip"], ["pro"]]),
  arrayIntersect: fn.arrayIntersect<string>(["vip"], ["pro", "vip"]),
  arrayJoin: fn.arrayJoin<number>([1, 2]),
  arraySlice: fn.arraySlice<string>(["vip", "pro"], 1, 1),
  arrayZip: fn.arrayZip([1], ["vip"]),
  avg: fn.avg(reportDeals.login),
  call: fn.call<number>("abs", reportDeals.login),
  coalesce: fn.coalesce<string>(reportDeals.profit, "0"),
  count: fn.count(),
  countIf: fn.countIf(ck.eq(reportDeals.entry, 1)),
  indexOf: fn.indexOf(["vip"], "vip"),
  jsonExtract: fn.jsonExtract(csql`payload`, chType.array(chType.string()), "tags"),
  length: fn.length(["vip"]),
  max: fn.max<string>(reportDeals.instance_id),
  min: fn.min<number>(reportDeals.login),
  not: fn.not(ck.eq(reportDeals.entry, 1)),
  notEmpty: fn.notEmpty(["vip"]),
  sum: fn.sum(reportDeals.profit),
  sumIf: fn.sumIf(reportDeals.profit, ck.eq(reportDeals.entry, 1)),
  toDate: fn.toDate(reportDeals.time_utc),
  toDateTime: fn.toDateTime(reportDeals.time_utc, "UTC"),
  toStartOfMonth: fn.toStartOfMonth(reportDeals.time_utc),
  toString: fn.toString(reportDeals.login),
  tuple: fn.tuple(reportDeals.instance_id, reportDeals.login),
  tupleElement: fn.tupleElement<number>(fn.tuple(reportDeals.instance_id, reportDeals.login), 2),
  uniqExact: fn.uniqExact(reportDeals.login),
  withParams: fn.withParams<number>("quantile", [0.95], reportDeals.login),
} satisfies Omit<{ readonly [K in keyof typeof fn]: unknown }, "table">;

const tableFunctionTypeMatrix = {
  call: fn.table.call("numbers", 10),
} satisfies { readonly [K in keyof typeof fn.table]: unknown };

type _FunctionDataMatrix = Expect<
  Equal<
    {
      readonly [K in keyof typeof functionTypeMatrix]: (typeof functionTypeMatrix)[K] extends Selection<infer TData>
        ? TData
        : never;
    },
    {
      readonly array: string[];
      readonly arrayConcat: string[];
      readonly arrayElement: string;
      readonly arrayElementOrNull: string | null;
      readonly arrayFlatten: string[];
      readonly arrayIntersect: string[];
      readonly arrayJoin: number;
      readonly arraySlice: string[];
      readonly arrayZip: unknown[];
      readonly avg: number;
      readonly call: number;
      readonly coalesce: string;
      readonly count: string;
      readonly countIf: string;
      readonly indexOf: string;
      readonly jsonExtract: string[];
      readonly length: string;
      readonly max: string;
      readonly min: number;
      readonly not: boolean;
      readonly notEmpty: boolean;
      readonly sum: number | string;
      readonly sumIf: number | string;
      readonly toDate: Date;
      readonly toDateTime: Date;
      readonly toStartOfMonth: Date;
      readonly toString: string;
      readonly tuple: unknown[];
      readonly tupleElement: number;
      readonly uniqExact: string;
      readonly withParams: number;
    }
  >
>;

const petSubquery = db
  .select({
    ownerId: pets.ownerId,
    petName: pets.petName,
  })
  .from(pets)
  .as("pet_subquery");

const joinedOwners = db
  .select({
    ownerId: petOwners.id,
    ownerName: petOwners.name,
    petName: petSubquery.petName,
  })
  .from(petOwners)
  .leftJoin(petSubquery, ck.eq(petOwners.id, petSubquery.ownerId))
  .where(ck.and(ck.gt(petOwners.id, 0), ck.notLike(petOwners.name, "bot%")))
  .groupBy(petOwners.id, petOwners.name, petSubquery.petName)
  .having(ck.not(ck.eq(fn.count(), "0")))
  .orderBy(ck.asc(petOwners.id), ck.desc(petSubquery.petName))
  .limit(100)
  .offset(10);

type _JoinedOwnersType = Expect<
  Equal<InferBuilderResult<typeof joinedOwners>, { ownerId: number; ownerName: string; petName: string | null }>
>;

const countReportDeals = db.count(reportDeals, ck.eq(reportDeals._peerdb_is_deleted, 0));
type _CountReportDealsType = Expect<Equal<Awaited<typeof countReportDeals>, number>>;

const targetOrders = db.$with("target_orders").as(
  db.select({
    target: fn
      .arrayJoin<readonly [string, "mt4" | "mt5", number, number]>(
        fn.arrayZip(["instance_a"], ["mt5" as const], [9001], [10001]),
      )
      .as("target"),
  }),
);

const targetPairs = db.$with("target_pairs").as(
  db
    .select({
      instance_id: fn.tupleElement<string>(targetOrders.target, 1),
      source: fn.tupleElement<"mt4" | "mt5">(targetOrders.target, 2),
      order_ticket: fn.tupleElement<number>(targetOrders.target, 3),
      login: fn.tupleElement<number>(targetOrders.target, 4),
    })
    .from(targetOrders),
);

const scopedDedupDeals = db.$with("scoped_dedup_deals").as(
  db
    .select({
      instance_id: reportDeals.instance_id,
      source: reportDeals.source,
      deal_ticket: reportDeals.deal_ticket,
      order_ticket: reportDeals.order_ticket,
      position_ticket: reportDeals.position_ticket,
      login: reportDeals.login,
      time_utc: reportDeals.time_utc,
      entry: reportDeals.entry,
      action: reportDeals.action,
      price: reportDeals.price,
      profit: reportDeals.profit,
      _peerdb_is_deleted: reportDeals._peerdb_is_deleted,
      _peerdb_version: reportDeals._peerdb_version,
    })
    .from(reportDeals)
    .where(
      ck.inArray(
        fn.tuple(reportDeals.instance_id, reportDeals.source, reportDeals.order_ticket, reportDeals.login),
        targetPairs,
      ),
    )
    .orderBy(ck.desc(reportDeals._peerdb_version), ck.desc(reportDeals.time_utc), ck.desc(reportDeals.deal_ticket))
    .limitBy([reportDeals.instance_id, reportDeals.source, reportDeals.deal_ticket], 1),
);

const outDealProfitSummary = db.$with("out_deal_profit_summary").as(
  db
    .select({
      instance_id: scopedDedupDeals.instance_id,
      source: scopedDedupDeals.source,
      login: scopedDedupDeals.login,
      order_ticket: scopedDedupDeals.order_ticket,
      total_profit: fn.sum(scopedDedupDeals.profit).as("total_profit"),
    })
    .from(scopedDedupDeals)
    .where(
      ck.eq(scopedDedupDeals._peerdb_is_deleted, 0),
      ck.inArray(scopedDedupDeals.entry, [1, 2, 3]),
      ck.inArray(scopedDedupDeals.action, [0, 1]),
    )
    .groupBy(
      scopedDedupDeals.instance_id,
      scopedDedupDeals.source,
      scopedDedupDeals.login,
      scopedDedupDeals.order_ticket,
    ),
);

const latestOutDeals = db.$with("latest_out_deals").as(
  db
    .select({
      instance_id: scopedDedupDeals.instance_id,
      source: scopedDedupDeals.source,
      login: scopedDedupDeals.login,
      order_ticket: scopedDedupDeals.order_ticket,
      position_ticket: scopedDedupDeals.position_ticket,
      close_price: scopedDedupDeals.price,
      close_time_utc: scopedDedupDeals.time_utc,
      deal_ticket: scopedDedupDeals.deal_ticket,
    })
    .from(scopedDedupDeals)
    .where(
      ck.eq(scopedDedupDeals._peerdb_is_deleted, 0),
      ck.inArray(scopedDedupDeals.entry, [1, 2, 3]),
      ck.inArray(scopedDedupDeals.action, [0, 1]),
    )
    .orderBy(ck.desc(scopedDedupDeals.time_utc), ck.desc(scopedDedupDeals.deal_ticket))
    .limitBy(
      [scopedDedupDeals.instance_id, scopedDedupDeals.source, scopedDedupDeals.login, scopedDedupDeals.order_ticket],
      1,
    ),
);

const commissionReportTradeSnapshots = db
  .with(targetOrders, targetPairs, scopedDedupDeals, outDealProfitSummary, latestOutDeals)
  .select({
    instanceId: latestOutDeals.instance_id.as("instanceId"),
    source: latestOutDeals.source.as("source"),
    login: latestOutDeals.login.as("login"),
    orderId: latestOutDeals.order_ticket.as("orderId"),
    closePrice: latestOutDeals.close_price.as("closePrice"),
    closeTimeUtc: fn.toString(latestOutDeals.close_time_utc).as("closeTimeUtc"),
    profit: fn.coalesce<number | string>(outDealProfitSummary.total_profit, 0).as("profit"),
  })
  .from(latestOutDeals)
  .leftJoin(
    outDealProfitSummary,
    ck.and(
      ck.eq(latestOutDeals.instance_id, outDealProfitSummary.instance_id),
      ck.eq(latestOutDeals.source, outDealProfitSummary.source),
      ck.eq(latestOutDeals.login, outDealProfitSummary.login),
      ck.eq(latestOutDeals.order_ticket, outDealProfitSummary.order_ticket),
    ),
  );

type _CommissionReportTradeSnapshotsType = Expect<
  Equal<
    InferBuilderResult<typeof commissionReportTradeSnapshots>,
    {
      instanceId: string;
      source: "mt4" | "mt5";
      login: number;
      orderId: number;
      closePrice: string;
      closeTimeUtc: string;
      profit: number | string;
    }
  >
>;

type _CteReferenceTypes = Expect<
  Equal<
    [
      typeof targetPairs.login,
      typeof scopedDedupDeals.instance_id,
      typeof latestOutDeals.login,
      typeof latestOutDeals.order_ticket,
    ],
    [
      Selection<number, "target_pairs">,
      Selection<string, "scoped_dedup_deals">,
      Selection<number, "latest_out_deals">,
      Selection<number, "latest_out_deals">,
    ]
  >
>;

const ckApiMatrix = {
  and: ck.and(ck.eq(reportDeals.login, 10001), ck.ne(reportDeals.entry, 9)),
  asc: ck.asc(reportDeals.login),
  between: ck.between(reportDeals.login, 1, 100),
  contains: ck.contains(reportDeals.instance_id, "instance"),
  containsIgnoreCase: ck.containsIgnoreCase(reportDeals.instance_id, "INSTANCE"),
  createSessionId: ck.createSessionId(),
  decodeRow: ck.decodeRow,
  desc: ck.desc(reportDeals.login),
  endsWith: ck.endsWith(reportDeals.instance_id, "_a"),
  endsWithIgnoreCase: ck.endsWithIgnoreCase(reportDeals.instance_id, "_A"),
  eq: ck.eq(reportDeals.login, 10001),
  exists: ck.exists(db.select({ login: reportDeals.login }).from(reportDeals)),
  expr: ck.expr<boolean>(csql`1`, { decoder: (value) => Number(value) === 1, sqlType: "UInt8" }),
  fn: ck.fn,
  gt: ck.gt(reportDeals.login, 0),
  gte: ck.gte(reportDeals.login, 0),
  has: ck.has(chTypeNameMatrix.array[0], "vip"),
  hasAll: ck.hasAll(chTypeNameMatrix.array[0], ["vip"]),
  hasAny: ck.hasAny(chTypeNameMatrix.array[0], ["vip"]),
  ilike: ck.ilike(reportDeals.instance_id, "%instance%"),
  inArray: ck.inArray(reportDeals.entry, [0, 1]),
  like: ck.like(reportDeals.instance_id, "%instance%"),
  lt: ck.lt(reportDeals.login, 100),
  lte: ck.lte(reportDeals.login, 100),
  ne: ck.ne(reportDeals.login, 10002),
  not: ck.not(ck.eq(reportDeals.login, 10001)),
  notExists: ck.notExists(db.select({ login: reportDeals.login }).from(reportDeals)),
  notIlike: ck.notIlike(reportDeals.instance_id, "%test%"),
  notInArray: ck.notInArray(reportDeals.entry, [8, 9]),
  notLike: ck.notLike(reportDeals.instance_id, "%test%"),
  or: ck.or(ck.eq(reportDeals.entry, 0), ck.eq(reportDeals.entry, 1)),
  startsWith: ck.startsWith(reportDeals.instance_id, "instance"),
  startsWithIgnoreCase: ck.startsWithIgnoreCase(reportDeals.instance_id, "INSTANCE"),
} satisfies { readonly [K in keyof typeof ck]: unknown };

const csqlMatrix = {
  identifier: csql.identifier({ table: "report_deals", column: "login" }),
  join: csql.join([csql`select 1`, csql`select 2`], csql`, `),
  tagged: csql`select ${reportDeals.login}`,
};

void chTypeNameMatrix;
void columnTypeMatrix;
void functionTypeMatrix;
void tableFunctionTypeMatrix;
void ckApiMatrix;
void csqlMatrix;
