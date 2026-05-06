import { describe, expect, it } from "bun:test";
import {
  aggregateFunction,
  array,
  bfloat16,
  bool,
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
} from "./columns";
import { type DecodeError, isDecodeError } from "./errors";
import { compileSql, sql } from "./sql";

const buildContext = () => ({
  params: {},
  nextParamIndex: 0,
});

const normalizeSql = (value: string) => value.replace(/\s+/g, " ").trim();

describe("ck-orm columns", function describeClickHouseORMColumns() {
  it("decodes integer columns including range and type-mismatch rejection", function testIntegerColumns() {
    const intColumn = int32();
    expect(intColumn.mapFromDriverValue(1)).toBe(1);
    expect(intColumn.mapFromDriverValue("2")).toBe(2);
    expect(intColumn.mapFromDriverValue(3n)).toBe(3);
    expect(() => intColumn.mapFromDriverValue({})).toThrow("Cannot convert value to number");
    expect(() => intColumn.mapFromDriverValue(Number.NaN)).toThrow("Cannot convert value to finite number");
    expect(() => int8().mapFromDriverValue(128)).toThrow("Cannot convert value to integer in range -128..127");
    expect(() => uint32().mapFromDriverValue(-1)).toThrow("Cannot convert value to integer in range 0..4294967295");

    const int64Column = int64();
    expect(int64Column.mapFromDriverValue(4n)).toBe("4");
    expect(int64Column.mapFromDriverValue(4)).toBe("4");
    expect(int64Column.mapFromDriverValue("5")).toBe("5");
    expect(int64Column.mapToDriverValue("6")).toBe("6");
    expect(int64Column.mapToDriverValue(6 as never)).toBe("6");
    expect(int64Column.mapToDriverValue(6n as never)).toBe("6");
    expect(() => int64Column.mapFromDriverValue(false)).toThrow("Cannot convert value to string");

    const uint64Column = uint64();
    expect(uint64Column.mapFromDriverValue(7n)).toBe("7");
    expect(uint64Column.mapFromDriverValue(7)).toBe("7");
    expect(uint64Column.mapFromDriverValue("8")).toBe("8");
    expect(uint64Column.mapToDriverValue("9")).toBe("9");
    expect(uint64Column.mapToDriverValue(9 as never)).toBe("9");
    expect(uint64Column.mapToDriverValue(9n as never)).toBe("9");
    expect(() => uint64Column.mapFromDriverValue(false)).toThrow("Cannot convert value to string");
    expect(() => uint64Column.mapFromDriverValue(Number.MAX_SAFE_INTEGER + 1)).toThrow(
      "Cannot convert value to integer string",
    );
    expect(() => uint64Column.mapFromDriverValue("-1")).toThrow("Cannot convert value to integer string");
  });

  it("decodes string columns including type coercion and rejection", function testStringColumns() {
    const stringColumn = string();
    expect(stringColumn.mapFromDriverValue("plain")).toBe("plain");
    expect(stringColumn.mapFromDriverValue(6)).toBe("6");
    expect(stringColumn.mapFromDriverValue(true)).toBe("true");
    expect(stringColumn.mapFromDriverValue(new Date("2026-04-21T00:00:00.000Z"))).toBe("2026-04-21T00:00:00.000Z");
    expect(() => stringColumn.mapFromDriverValue({})).toThrow("Cannot convert value to string");
  });

  it("decodes date/time columns and rejects malformed temporal inputs", function testTemporalDecoders() {
    const dateColumn = dateTime64({ precision: 3, timezone: "UTC" });
    const originalDate = new Date("2026-04-21T00:00:00.000Z");
    expect(dateColumn.mapFromDriverValue(originalDate)).toBe(originalDate);
    const parsedDate = dateColumn.mapFromDriverValue("2026-04-21T00:00:00.000Z");
    expect(parsedDate).toBeInstanceOf(Date);
    expect(dateTime().mapFromDriverValue("2026-04-21 01:02:03").toISOString()).toBe("2026-04-21T01:02:03.000Z");
    expect(dateColumn.mapFromDriverValue(1_710_000_000_000 as never).toISOString()).toBe("2024-03-09T16:00:00.000Z");
    expect(() => dateColumn.mapFromDriverValue(false)).toThrow("Cannot convert value to Date");
    expect(() => dateColumn.mapFromDriverValue("not-a-date")).toThrow("Cannot convert value to valid Date");
    expect(() => dateTime().mapFromDriverValue("2026-02-30 01:02:03")).toThrow("Cannot convert value to valid Date");

    // Time/Time64 映射为 string——ClickHouse Time 可负、可超 24h，
    // JS Date 无法表达。读 / 写都用 'HH:MM:SS[.fff]' 字符串。
    expect(time().mapFromDriverValue("12:34:56")).toBe("12:34:56");
    expect(time().mapFromDriverValue("-01:30:00")).toBe("-01:30:00");
    expect(time().mapFromDriverValue("999:59:59")).toBe("999:59:59");
    expect(time64({ precision: 3 }).mapFromDriverValue("12:34:56.789")).toBe("12:34:56.789");
    expect(time64({ precision: 6 }).mapFromDriverValue("12:34:56.789123")).toBe("12:34:56.789123");
    expect(time64({ precision: 9 }).mapFromDriverValue("12:34:56.789123456")).toBe("12:34:56.789123456");
    // Time/Time64 mapToDriverValue 接受字符串和数字直通；拒 Date
    expect(time().mapToDriverValue("12:34:56")).toBe("12:34:56");
    expect(time().mapToDriverValue(5400 as never)).toBe(5400);
    expect(() => time().mapToDriverValue(new Date() as never)).toThrow(
      /Time column values must be a 'HH:MM:SS' string or integer; JS Date is not appropriate/,
    );
    expect(() => time64({ precision: 3 }).mapToDriverValue(new Date() as never)).toThrow(
      /Time64 column values must be a 'HH:MM:SS' string or integer/,
    );
  });

  it("encodes Date/Date32 columns by extracting UTC YYYY-MM-DD from Date inputs", function testDateEncoders() {
    // Date 直通：UTC 抽 YYYY-MM-DD（与 toISOString() 第一段一致）
    expect(date().mapToDriverValue(new Date("2026-06-15T23:59:00.000Z"))).toBe("2026-06-15");
    expect(date32().mapToDriverValue(new Date("2026-06-16T01:00:00.000Z"))).toBe("2026-06-16");
    // 字符串/数字直通（库不再做客户端格式校验，交给 ClickHouse 服务端）
    expect(date().mapToDriverValue("2026-06-17" as never)).toBe("2026-06-17");
    expect(date32().mapToDriverValue(0 as never)).toBe(0);
    // 拒绝其他类型
    expect(() => date().mapToDriverValue({} as never)).toThrow("Date column values must be a Date, string, or number");
    expect(() => date32().mapToDriverValue(true as never)).toThrow(
      "Date32 column values must be a Date, string, or number",
    );
    // 拒绝 Invalid Date
    expect(() => date().mapToDriverValue(new Date("not-a-date"))).toThrow(
      "Date column values must be a Date, string, or number",
    );
  });

  it("decodes boolean columns strictly, rejecting NaN/Infinity/object", function testBooleanDecoder() {
    const booleanColumn = bool();
    expect(booleanColumn.mapFromDriverValue(true)).toBe(true);
    expect(booleanColumn.mapFromDriverValue(0)).toBe(false);
    expect(booleanColumn.mapFromDriverValue("TRUE")).toBe(true);
    expect(() => booleanColumn.mapFromDriverValue({})).toThrow("Cannot convert value to boolean");
    // Non-finite numbers should be rejected rather than silently coerced to true.
    expect(() => booleanColumn.mapFromDriverValue(Number.NaN)).toThrow("Cannot convert non-finite number to boolean");
    expect(() => booleanColumn.mapFromDriverValue(Number.POSITIVE_INFINITY)).toThrow(
      "Cannot convert non-finite number to boolean",
    );
    expect(() => booleanColumn.mapFromDriverValue(Number.NEGATIVE_INFINITY)).toThrow(
      "Cannot convert non-finite number to boolean",
    );
  });

  it("compiles bound columns with table and alias prefixes", function testColumnBinding() {
    const intColumn = int32();
    expect(() => intColumn.compile(buildContext())).toThrow("Unbound column cannot be compiled: Int32");

    const bound = intColumn.bind({
      name: "id",
      tableName: "orders",
    });
    expect(normalizeSql(compileSql(sql`${bound}`).query)).toBe("`orders`.`id`");

    const aliased = intColumn.bind({
      name: "id",
      tableAlias: "o",
      tableName: "orders",
    });
    expect(normalizeSql(compileSql(sql`${aliased}`).query)).toBe("`o`.`id`");
  });

  it("covers scalar builder sqlType strings and driver encoders", function testScalarBuilderTypes() {
    expect(int8().sqlType).toBe("Int8");
    expect(int16().sqlType).toBe("Int16");
    expect(int16().mapFromDriverValue("16")).toBe(16);
    expect(int32().sqlType).toBe("Int32");
    expect(int64().sqlType).toBe("Int64");
    expect(uint8().sqlType).toBe("UInt8");
    expect(uint8().mapFromDriverValue("8")).toBe(8);
    expect(uint16().sqlType).toBe("UInt16");
    expect(uint16().mapFromDriverValue("16")).toBe(16);
    expect(uint32().sqlType).toBe("UInt32");
    expect(uint64().sqlType).toBe("UInt64");
    expect(float32().sqlType).toBe("Float32");
    expect(float64().sqlType).toBe("Float64");
    expect(bfloat16().sqlType).toBe("BFloat16");
    expect(fixedString({ length: 8 }).sqlType).toBe("FixedString(8)");
    expect(decimal({ precision: 18, scale: 5 }).sqlType).toBe("Decimal(18, 5)");
    expect(date().sqlType).toBe("Date");
    expect(date32().sqlType).toBe("Date32");
    expect(time().sqlType).toBe("Time");
    expect(time64({ precision: 6 }).sqlType).toBe("Time64(6)");
    expect(dateTime().sqlType).toBe("DateTime");
    expect(dateTime64({ precision: 6 }).sqlType).toBe("DateTime64(6)");
    expect(dateTime64({ precision: 6, timezone: "Asia/Shanghai" }).sqlType).toBe("DateTime64(6, 'Asia/Shanghai')");

    // dateTime/dateTime64 mapToDriverValue passes Date through unchanged.
    // Each transport path serialises it natively: JSONEachRow uses JSON.stringify
    // (→ ISO 8601 with Z, accepted via auto-enabled best_effort) while SQL
    // parameter binding uses formatQueryParamValue (→ Unix seconds, timezone-agnostic).
    // Strings/numbers pass through verbatim; other types are rejected.
    const driverDate = new Date("2026-04-21T12:34:56.789Z");
    expect(dateTime().mapToDriverValue(driverDate)).toBe(driverDate);
    expect(dateTime64({ precision: 3 }).mapToDriverValue(driverDate)).toBe(driverDate);
    expect(dateTime64({ precision: 6 }).mapToDriverValue(driverDate)).toBe(driverDate);
    expect(dateTime().mapToDriverValue("2026-04-21 12:34:56" as never)).toBe("2026-04-21 12:34:56");
    expect(dateTime64({ precision: 3 }).mapToDriverValue(1_700_000_000 as never)).toBe(1_700_000_000);
    expect(() => dateTime().mapToDriverValue({} as never)).toThrow(
      "DateTime column values must be a Date, string, or number",
    );
    expect(() => dateTime64({ precision: 3 }).mapToDriverValue(true as never)).toThrow(
      "DateTime64 column values must be a Date, string, or number",
    );
    // Invalid Date is rejected with the same error path.
    expect(() => dateTime().mapToDriverValue(new Date(Number.NaN))).toThrow(
      "DateTime column values must be a Date, string, or number",
    );
    expect(uuid().sqlType).toBe("UUID");
    expect(ipv4().sqlType).toBe("IPv4");
    expect(ipv6().sqlType).toBe("IPv6");
    expect(json<{ id: number }>().sqlType).toBe("JSON");
    expect(dynamic<{ label: string }>().sqlType).toBe("Dynamic");
    expect(qbit(float32(), { dimensions: 8 }).sqlType).toBe("QBit(Float32, 8)");
    expect(json<{ id: number }>().mapFromDriverValue({ id: 1 })).toEqual({
      id: 1,
    });
    expect(json<{ id: number }>().mapToDriverValue({ id: 1 })).toEqual({
      id: 1,
    });
    expect(dynamic<{ label: string }>().mapFromDriverValue({ label: "dynamic" })).toEqual({ label: "dynamic" });
    expect(dynamic<{ label: string }>().mapToDriverValue({ label: "dynamic" })).toEqual({ label: "dynamic" });
    expect(qbit(float32(), { dimensions: 8 }).mapFromDriverValue([1, 2, 3])).toEqual([1, 2, 3]);
    expect(qbit(float32(), { dimensions: 8 }).mapToDriverValue([1, 2, 3])).toEqual([1, 2, 3]);
    expect(() => qbit(float32(), { dimensions: 8 }).mapFromDriverValue("bad")).toThrow(
      "Cannot convert value to qbit array",
    );

    expect(decimal({ precision: 18, scale: 5 }).mapToDriverValue("12.50000")).toBe("12.50000");
    // JS number is rejected to prevent silent precision loss (e.g. `0.1 + 0.2`
    // would become `"0.30000000000000004"`, and `1e30` becomes `"1e+30"` which
    // ClickHouse cannot parse as Decimal).
    expect(() => decimal({ precision: 18, scale: 5 }).mapToDriverValue(12.5 as never)).toThrow(
      /requires a string or bigint value to preserve precision/,
    );
    // BigInt is accepted (it has exact decimal representation in JS for ints).
    expect(decimal({ precision: 18, scale: 5 }).mapToDriverValue(BigInt(123) as never)).toBe("123");

    const amount = decimal({ precision: 18, scale: 5 });
    expect(amount.decimalConfig).toEqual({ precision: 18, scale: 5 });
    expect(() => amount.mapToDriverValue({ toFixed: () => "1.00" } as never)).toThrow(
      /expects string \| number; got an object/,
    );
    const cast = amount.bind({ name: "amount", tableName: "ledger" }).cast(20, 2);
    expect(compileSql(cast).query).toBe("CAST(`ledger`.`amount` AS Decimal(20, 2))");

    const nullableAmount = nullable(amount);
    expect(nullableAmount.decimalConfig).toEqual({ precision: 18, scale: 5 });
    expect(nullableAmount.mapToDriverValue(null)).toBeNull();
    expect(nullableAmount.mapToDriverValue("9.00000")).toBe("9.00000");
    expect(() => nullableAmount.mapToDriverValue({ toFixed: () => "1.00" } as never)).toThrow(
      /expects string \| number; got an object/,
    );

    const lowCardAmount = lowCardinality(decimal({ precision: 12, scale: 4 }));
    expect(lowCardAmount.decimalConfig).toEqual({ precision: 12, scale: 4 });
  });

  it("preserves logical keys separately from configured database column names", function testConfiguredColumnNames() {
    const rewardPoints = decimal("reward_points", { precision: 20, scale: 5 });
    expect(rewardPoints.configuredName).toBe("reward_points");
    expect(decimal({ precision: 20, scale: 5 }).configuredName).toBeUndefined();
    expect(fixedString("code", { length: 4 }).configuredName).toBe("code");
    expect(dateTime64("created_at", { precision: 9 }).configuredName).toBe("created_at");
    const namedQBit = qbit("embedding", float32(), { dimensions: 8 });
    expect(namedQBit.configuredName).toBe("embedding");
    expect(namedQBit.mapToDriverValue([1, 2, 3])).toEqual([1, 2, 3]);
    expect(() => qbit("embedding", float32() as never)).toThrow("qbit() requires two values after the column name");

    const bound = rewardPoints.bind({
      key: "rewardPoints",
      name: "reward_points",
      tableName: "order_reward_log",
    });

    expect(bound.key).toBe("rewardPoints");
    expect(bound.name).toBe("reward_points");
    expect(normalizeSql(compileSql(sql`${bound}`).query)).toBe("`order_reward_log`.`reward_points`");
    expect(() => string("bad-name")).toThrow("Invalid SQL identifier: bad-name");
    expect(() => decimal("reward_points" as never)).toThrow("decimal() requires a config object after the column name");
    expect(() => decimal(20 as never)).toThrow("decimal() requires a config object");
  });

  it("covers enum, nullable, array, tuple, map, variant and nested types", function testCompositeColumns() {
    const status8 = enum8({ active: 1, paused: 2 });
    expect(status8.sqlType).toBe("Enum8('active' = 1, 'paused' = 2)");
    expect(status8.mapFromDriverValue("active")).toBe("active");
    expect(status8.mapToDriverValue("active")).toBe("active");

    const status16 = enum16({ archived: 1000 });
    expect(status16.sqlType).toBe("Enum16('archived' = 1000)");
    expect(status16.mapToDriverValue("archived")).toBe("archived");

    const nullableString = nullable(string());
    expect(nullableString.mapFromDriverValue(null)).toBeNull();
    // `undefined` is rejected because ClickHouse JSON never emits it.
    // Treating it as null would mask driver bugs.
    expect(() => nullableString.mapFromDriverValue(undefined)).toThrow(
      "Nullable column received undefined from the driver",
    );
    expect(nullableString.mapFromDriverValue(7)).toBe("7");
    expect(nullableString.mapToDriverValue(null)).toBeNull();
    expect(nullableString.mapToDriverValue("ok")).toBe("ok");
    expect(nullable("optional_note", string()).configuredName).toBe("optional_note");
    expect(() => nullable("optional_note" as never)).toThrow("nullable() requires a value after the column name");
    expect(() => nullable(array(string()))).toThrow(
      "Nullable(Array(String)) is not supported by ClickHouse; wrap Nullable around fields inside the composite type instead",
    );
    expect(() => nullable(map(string(), int32()))).toThrow(
      "Nullable(Map(String, Int32)) is not supported by ClickHouse",
    );
    expect(() => nullable(tuple(string(), int32()))).toThrow("Nullable(Tuple(String, Int32)) is not supported");

    const stringArray = array(string());
    expect(stringArray.mapFromDriverValue(["a", 1])).toEqual(["a", "1"]);
    expect(stringArray.mapToDriverValue(["a", "b"])).toEqual(["a", "b"]);
    expect(() => stringArray.mapFromDriverValue("bad")).toThrow("Cannot convert value to array");
    const namedStringArray = array("tags", string());
    expect(namedStringArray.configuredName).toBe("tags");
    expect(namedStringArray.sqlType).toBe("Array(String)");
    const nullableStringArray = array(nullable(string()));
    expect(nullableStringArray.sqlType).toBe("Array(Nullable(String))");
    // ClickHouse never emits undefined; the array passes through nulls but
    // wraps any stray undefined element as a decode error with a clear path.
    expect(() => nullableStringArray.mapFromDriverValue(["a", null, undefined])).toThrow(
      /Nullable column received undefined from the driver/,
    );

    const tupleColumn = tuple(string(), int32());
    expect(tupleColumn.sqlType).toBe("Tuple(String, Int32)");
    expect(tupleColumn.mapFromDriverValue(["login", "42"])).toEqual(["login", 42]);
    expect(tupleColumn.mapToDriverValue(["login", 42])).toEqual(["login", 42]);
    expect(() => tupleColumn.mapFromDriverValue("bad")).toThrow("Cannot convert value to tuple");
    expect(() => tupleColumn.mapFromDriverValue(["login"])).toThrow("expected 2 items, got 1");
    expect(() => tupleColumn.mapToDriverValue("bad" as never)).toThrow("Cannot convert value to tuple");
    expect(() => tupleColumn.mapToDriverValue(["login"] as never)).toThrow("expected 2 items, got 1");

    const mapColumn = map(string(), int32());
    expect(mapColumn.sqlType).toBe("Map(String, Int32)");
    expect(mapColumn.mapFromDriverValue({ a: "1", b: 2 })).toEqual({
      a: 1,
      b: 2,
    });
    expect(mapColumn.mapToDriverValue({ a: 1, b: 2 })).toEqual({ a: 1, b: 2 });
    expect(() => mapColumn.mapFromDriverValue(null)).toThrow("Cannot convert value to map");
    expect(() => map(int32(), string())).toThrow("ckType.map() currently supports only String keys");

    const variantColumn = variant(string(), int32());
    expect(variantColumn.sqlType).toBe("Variant(String, Int32)");
    expect(variantColumn.mapFromDriverValue("a")).toBe("a");
    expect(variantColumn.mapToDriverValue("b")).toBe("b");

    const lowCardinalityString = lowCardinality(string());
    expect(lowCardinalityString.sqlType).toBe("LowCardinality(String)");
    expect(lowCardinalityString.mapFromDriverValue(9)).toBe("9");
    expect(lowCardinalityString.mapToDriverValue("x")).toBe("x");
    expect(lowCardinality(uint32()).sqlType).toBe("LowCardinality(UInt32)");
    expect(lowCardinality(dateTime()).sqlType).toBe("LowCardinality(DateTime)");
    expect(lowCardinality(nullable(string())).sqlType).toBe("LowCardinality(Nullable(String))");

    const nestedColumn = nested({
      id: int32(),
      name: string(),
    });
    expect(nestedColumn.sqlType).toBe("Nested(id Int32, name String)");
    expect(nestedColumn.nestedShape).toBeTruthy();
    const nestedValue = [{ id: 1, name: "name" }] as Array<{
      id: number;
      name: string;
    }>;
    expect(nestedColumn.mapFromDriverValue(nestedValue)).toEqual(nestedValue);
    expect(nestedColumn.mapToDriverValue(nestedValue)).toEqual(nestedValue);
    expect(() => nestedColumn.mapToDriverValue([null] as never)).toThrow("Cannot convert nested item: null");
    expect(() => nestedColumn.mapToDriverValue([{ id: 1 } as never])).toThrow(
      'Nested item is missing required field "name"',
    );
    expect(() => nestedColumn.mapFromDriverValue("bad")).toThrow("Cannot convert value to nested");
    expect(() =>
      nestedColumn.mapFromDriverValue([{ id: 1, name: "ok" }, 1] as unknown[] as typeof nestedValue),
    ).toThrow("Cannot convert nested item");
  });

  it("covers aggregate and geo builders", function testAggregateAndGeoColumns() {
    const aggregate = aggregateFunction("sum", string(), "UInt64");
    expect(aggregate.sqlType).toBe("AggregateFunction(sum, String, UInt64)");
    expect(aggregate.mapFromDriverValue("state")).toBe("state");
    expect(aggregate.mapToDriverValue("state")).toBe("state");

    const namedAggregate = aggregateFunction("agg_sum_state", {
      name: "sum",
      args: [uint64()],
    });
    expect(namedAggregate.configuredName).toBe("agg_sum_state");
    expect(namedAggregate.sqlType).toBe("AggregateFunction(sum, UInt64)");

    const quantileAggregate = aggregateFunction("quantile(0.5)", float64());
    expect(quantileAggregate.sqlType).toBe("AggregateFunction(quantile(0.5), Float64)");

    const namedTopKAggregate = aggregateFunction("agg_top_tags", {
      name: "topK(10)",
      args: ["LowCardinality(String)"],
    });
    expect(namedTopKAggregate.configuredName).toBe("agg_top_tags");
    expect(namedTopKAggregate.sqlType).toBe("AggregateFunction(topK(10), LowCardinality(String))");

    const simpleAggregate = simpleAggregateFunction("sum", int32());
    expect(simpleAggregate.sqlType).toBe("SimpleAggregateFunction(sum, Int32)");
    expect(simpleAggregate.mapFromDriverValue("value")).toBe("value");
    expect(simpleAggregate.mapToDriverValue("value")).toBe("value");

    const namedSimpleAggregate = simpleAggregateFunction("sum_value", {
      name: "sum",
      value: int32(),
    });
    expect(namedSimpleAggregate.configuredName).toBe("sum_value");
    expect(namedSimpleAggregate.sqlType).toBe("SimpleAggregateFunction(sum, Int32)");

    const pointValue: [number, number] = [1, 2];
    const lineValue: [number, number][] = [pointValue];
    const multiLineValue: [number, number][][] = [lineValue];
    const polygonValue: [number, number][][] = [lineValue];
    const multiPolygonValue: [number, number][][][] = [polygonValue];

    expect(point().mapFromDriverValue(pointValue)).toBe(pointValue);
    expect(point().mapToDriverValue(pointValue)).toBe(pointValue);
    expect(ring().mapFromDriverValue(lineValue)).toBe(lineValue);
    expect(ring().mapToDriverValue(lineValue)).toBe(lineValue);
    expect(lineString().mapFromDriverValue(lineValue)).toBe(lineValue);
    expect(lineString().mapToDriverValue(lineValue)).toBe(lineValue);
    expect(multiLineString().mapFromDriverValue(multiLineValue)).toBe(multiLineValue);
    expect(multiLineString().mapToDriverValue(multiLineValue)).toBe(multiLineValue);
    expect(polygon().mapFromDriverValue(polygonValue)).toBe(polygonValue);
    expect(polygon().mapToDriverValue(polygonValue)).toBe(polygonValue);
    expect(multiPolygon().mapFromDriverValue(multiPolygonValue)).toBe(multiPolygonValue);
    expect(multiPolygon().mapToDriverValue(multiPolygonValue)).toBe(multiPolygonValue);
  });

  it("preserves column ddl metadata across fluent builders and bind()", function testColumnDdlMetadata() {
    const codec = sql`ZSTD(1)`;
    const ttl = sql`now() + INTERVAL 1 DAY`;
    const note = string().comment("session-local note").codec(codec).ttl(ttl);
    expect(note.ddl?.comment).toBe("session-local note");
    expect(note.ddl?.codec).toBe(codec);
    expect(note.ddl?.ttl).toBe(ttl);

    const bound = note.bind({
      name: "note",
      tableName: "tmp_scope",
    });
    expect(bound.ddl).toEqual(note.ddl);
  });

  it("annotates container decode failures with the offending path", function testContainerDecodePath() {
    const items = array(int32());
    let arrErr: unknown;
    try {
      items.mapFromDriverValue([1, true] as unknown);
    } catch (error) {
      arrErr = error;
    }
    expect(isDecodeError(arrErr)).toBe(true);
    expect((arrErr as DecodeError).path).toBe("[1]");
    expect((arrErr as DecodeError).message).toMatch(/\(at \[1\]\)$/);

    const userTuple = tuple(int32(), string());
    let tupleErr: unknown;
    try {
      userTuple.mapFromDriverValue([1, Symbol("oops")] as unknown);
    } catch (error) {
      tupleErr = error;
    }
    expect(isDecodeError(tupleErr)).toBe(true);
    expect((tupleErr as DecodeError).path).toBe("[1]");

    const profiles = nested({ id: int32(), email: string() });
    let nestedErr: unknown;
    try {
      profiles.mapFromDriverValue([
        { id: 1, email: "a@b" },
        { id: 2, email: Symbol("nope") },
      ] as unknown);
    } catch (error) {
      nestedErr = error;
    }
    expect(isDecodeError(nestedErr)).toBe(true);
    expect((nestedErr as DecodeError).path).toBe("[1].email");
    expect((nestedErr as DecodeError).message).toContain("[ck-orm]");
  });

  it("annotates map decode failures and wraps non-DecodeError throws by message", function testMapAndNonDecodeErrorRethrow() {
    // Map decode error path: inner value column throws DecodeError with a coerced value.
    const counters = map(string(), int32());
    let mapErr: unknown;
    try {
      counters.mapFromDriverValue({ ok: 1, broken: Symbol("nope") } as unknown);
    } catch (error) {
      mapErr = error;
    }
    expect(isDecodeError(mapErr)).toBe(true);
    expect((mapErr as DecodeError).path).toBe('["broken"]');

    // Non-DecodeError path: swap the inner column's decoder to throw a plain Error,
    // so rethrowDecodeWithPath falls into the `error instanceof Error ? error.message : String(error)` branch.
    const inner = string();
    const arr = array(inner);
    const original = inner.mapFromDriverValue;
    (inner as { mapFromDriverValue: (v: unknown) => unknown }).mapFromDriverValue = () => {
      throw new TypeError("boom");
    };
    let plainErr: unknown;
    try {
      arr.mapFromDriverValue(["x"]);
    } catch (error) {
      plainErr = error;
    } finally {
      (inner as { mapFromDriverValue: (v: unknown) => unknown }).mapFromDriverValue = original;
    }
    expect(isDecodeError(plainErr)).toBe(true);
    expect((plainErr as DecodeError).message).toContain("boom");
    expect((plainErr as DecodeError).path).toBe("[0]");

    // String(error) branch: inner throws a non-Error value.
    (inner as { mapFromDriverValue: (v: unknown) => unknown }).mapFromDriverValue = () => {
      throw "string-thrown";
    };
    let strErr: unknown;
    try {
      arr.mapFromDriverValue(["x"]);
    } catch (error) {
      strErr = error;
    } finally {
      (inner as { mapFromDriverValue: (v: unknown) => unknown }).mapFromDriverValue = original;
    }
    expect(isDecodeError(strErr)).toBe(true);
    expect((strErr as DecodeError).message).toContain("string-thrown");
  });
});
