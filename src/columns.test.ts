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
import { DecodeError } from "./errors";
import { compileSql, sql } from "./sql";

const buildContext = () => ({
  params: {},
  nextParamIndex: 0,
});

const normalizeSql = (value: string) => value.replace(/\s+/g, " ").trim();

describe("ck-orm columns", function describeClickHouseOrmColumns() {
  it("converts scalar values, binds columns, and rejects invalid inputs", function testScalarColumns() {
    const intColumn = int32();
    expect(intColumn.mapFromDriverValue(1)).toBe(1);
    expect(intColumn.mapFromDriverValue("2")).toBe(2);
    expect(intColumn.mapFromDriverValue(3n)).toBe(3);
    expect(() => intColumn.mapFromDriverValue({})).toThrow("Cannot convert value to number");

    const bigintColumn = int64();
    expect(bigintColumn.mapFromDriverValue(4n)).toBe(4n);
    expect(bigintColumn.mapFromDriverValue(4)).toBe(4n);
    expect(bigintColumn.mapFromDriverValue("5")).toBe(5n);
    expect(() => bigintColumn.mapFromDriverValue(false)).toThrow("Cannot convert value to bigint");

    const stringColumn = string();
    expect(stringColumn.mapFromDriverValue("plain")).toBe("plain");
    expect(stringColumn.mapFromDriverValue(6)).toBe("6");
    expect(stringColumn.mapFromDriverValue(true)).toBe("true");
    expect(stringColumn.mapFromDriverValue(new Date("2026-04-21T00:00:00.000Z"))).toBe("2026-04-21T00:00:00.000Z");
    expect(() => stringColumn.mapFromDriverValue({})).toThrow("Cannot convert value to string");

    const dateColumn = dateTime64(3, "UTC");
    const originalDate = new Date("2026-04-21T00:00:00.000Z");
    expect(dateColumn.mapFromDriverValue(originalDate)).toBe(originalDate);
    const parsedDate = dateColumn.mapFromDriverValue("2026-04-21T00:00:00.000Z");
    expect(parsedDate).toBeInstanceOf(Date);
    expect(() => dateColumn.mapFromDriverValue(false)).toThrow("Cannot convert value to Date");

    const booleanColumn = bool();
    expect(booleanColumn.mapFromDriverValue(true)).toBe(true);
    expect(booleanColumn.mapFromDriverValue(0)).toBe(false);
    expect(booleanColumn.mapFromDriverValue("TRUE")).toBe(true);
    expect(() => booleanColumn.mapFromDriverValue({})).toThrow("Cannot convert value to boolean");

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
    expect(int32().sqlType).toBe("Int32");
    expect(int64().sqlType).toBe("Int64");
    expect(uint8().sqlType).toBe("UInt8");
    expect(uint16().sqlType).toBe("UInt16");
    expect(uint32().sqlType).toBe("UInt32");
    expect(uint64().sqlType).toBe("UInt64");
    expect(float32().sqlType).toBe("Float32");
    expect(float64().sqlType).toBe("Float64");
    expect(bfloat16().sqlType).toBe("BFloat16");
    expect(fixedString(8).sqlType).toBe("FixedString(8)");
    expect(decimal(18, 5).sqlType).toBe("Decimal(18, 5)");
    expect(date().sqlType).toBe("Date");
    expect(date32().sqlType).toBe("Date32");
    expect(time().sqlType).toBe("Time");
    expect(time64(6).sqlType).toBe("Time64(6)");
    expect(dateTime().sqlType).toBe("DateTime");
    expect(dateTime64(6).sqlType).toBe("DateTime64(6)");
    expect(dateTime64(6, "Asia/Shanghai").sqlType).toBe("DateTime64(6, 'Asia/Shanghai')");
    expect(uuid().sqlType).toBe("UUID");
    expect(ipv4().sqlType).toBe("IPv4");
    expect(ipv6().sqlType).toBe("IPv6");
    expect(json<{ id: number }>().sqlType).toBe("JSON");
    expect(dynamic<{ label: string }>().sqlType).toBe("Dynamic");
    expect(qbit(float32(), 8).sqlType).toBe("QBit(Float32, 8)");
    expect(json<{ id: number }>().mapFromDriverValue({ id: 1 })).toEqual({
      id: 1,
    });
    expect(json<{ id: number }>().mapToDriverValue({ id: 1 })).toEqual({
      id: 1,
    });
    expect(dynamic<{ label: string }>().mapFromDriverValue({ label: "dynamic" })).toEqual({ label: "dynamic" });
    expect(dynamic<{ label: string }>().mapToDriverValue({ label: "dynamic" })).toEqual({ label: "dynamic" });
    expect(qbit(float32(), 8).mapFromDriverValue([1, 2, 3])).toEqual([1, 2, 3]);
    expect(qbit(float32(), 8).mapToDriverValue([1, 2, 3])).toEqual([1, 2, 3]);
    expect(() => qbit(float32(), 8).mapFromDriverValue("bad")).toThrow("Cannot convert value to qbit array");

    expect(decimal(18, 5).mapToDriverValue("12.50000")).toBe("12.50000");
    expect(decimal(18, 5).mapToDriverValue(12.5 as never)).toBe("12.5");
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
    expect(nullableString.mapFromDriverValue(undefined)).toBeNull();
    expect(nullableString.mapFromDriverValue(7)).toBe("7");
    expect(nullableString.mapToDriverValue(null)).toBeNull();
    expect(nullableString.mapToDriverValue("ok")).toBe("ok");

    const stringArray = array(string());
    expect(stringArray.mapFromDriverValue(["a", 1])).toEqual(["a", "1"]);
    expect(stringArray.mapToDriverValue(["a", "b"])).toEqual(["a", "b"]);
    expect(() => stringArray.mapFromDriverValue("bad")).toThrow("Cannot convert value to array");

    const tupleColumn = tuple(string(), int32());
    expect(tupleColumn.sqlType).toBe("Tuple(String, Int32)");
    expect(tupleColumn.mapFromDriverValue(["login", "42"])).toEqual(["login", 42]);
    expect(tupleColumn.mapToDriverValue(["login", 42])).toEqual(["login", 42]);
    expect(() => tupleColumn.mapFromDriverValue("bad")).toThrow("Cannot convert value to tuple");

    const mapColumn = map(string(), int32());
    expect(mapColumn.sqlType).toBe("Map(String, Int32)");
    expect(mapColumn.mapFromDriverValue({ a: "1", b: 2 })).toEqual({
      a: 1,
      b: 2,
    });
    expect(mapColumn.mapToDriverValue({ a: 1, b: 2 })).toEqual({ a: 1, b: 2 });
    expect(() => mapColumn.mapFromDriverValue(null)).toThrow("Cannot convert value to map");

    const variantColumn = variant(string(), int32());
    expect(variantColumn.sqlType).toBe("Variant(String, Int32)");
    expect(variantColumn.mapFromDriverValue("a")).toBe("a");
    expect(variantColumn.mapToDriverValue("b")).toBe("b");

    const lowCardinalityString = lowCardinality(string());
    expect(lowCardinalityString.sqlType).toBe("LowCardinality(String)");
    expect(lowCardinalityString.mapFromDriverValue(9)).toBe("9");
    expect(lowCardinalityString.mapToDriverValue("x")).toBe("x");

    const nestedColumn = nested({
      id: int32(),
      name: string(),
    });
    expect(nestedColumn.sqlType).toBe("Nested(id Int32, name String)");
    const nestedValue = [{ id: 1, name: "name" }] as Array<{
      id: number;
      name: string;
    }>;
    expect(nestedColumn.mapFromDriverValue(nestedValue)).toEqual(nestedValue);
    expect(nestedColumn.mapToDriverValue(nestedValue)).toEqual(nestedValue);
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

    const simpleAggregate = simpleAggregateFunction("sum", int32());
    expect(simpleAggregate.sqlType).toBe("SimpleAggregateFunction(sum, Int32)");
    expect(simpleAggregate.mapFromDriverValue("value")).toBe("value");
    expect(simpleAggregate.mapToDriverValue("value")).toBe("value");

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
      items.mapFromDriverValue([1, "not-a-number-but-actually-coerced", "abc"] as unknown);
    } catch (error) {
      arrErr = error;
    }
    // Strings get coerced via toNumber happily, so force an obvious failure with a non-coercible value:
    try {
      items.mapFromDriverValue([1, true] as unknown);
    } catch (error) {
      arrErr = error;
    }
    expect(arrErr).toBeInstanceOf(DecodeError);
    expect((arrErr as DecodeError).path).toBe("[1]");
    expect((arrErr as DecodeError).message).toMatch(/\(at \[1\]\)$/);

    const userTuple = tuple(int32(), string());
    let tupleErr: unknown;
    try {
      userTuple.mapFromDriverValue([1, Symbol("oops")] as unknown);
    } catch (error) {
      tupleErr = error;
    }
    expect(tupleErr).toBeInstanceOf(DecodeError);
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
    expect(nestedErr).toBeInstanceOf(DecodeError);
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
    expect(mapErr).toBeInstanceOf(DecodeError);
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
    expect(plainErr).toBeInstanceOf(DecodeError);
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
    expect(strErr).toBeInstanceOf(DecodeError);
    expect((strErr as DecodeError).message).toContain("string-thrown");
  });
});
