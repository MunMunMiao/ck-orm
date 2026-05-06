import { describe, expect, it } from "bun:test";
import {
  getArrayElementType,
  getTupleElementTypes,
  normalizeAggregateFunctionSignature,
  normalizeClickHouseTypeLiteral,
  splitTopLevelTypeList,
  unwrapNullableLowCardinalityType,
} from "./clickhouse-type";

describe("ck-orm internal ClickHouse type literals", function describeClickHouseTypeLiterals() {
  it("normalizes supported ClickHouse type literal families", function testSupportedTypeLiterals() {
    const validTypes = [
      "String",
      "Nullable(LowCardinality(String))",
      "Array(Tuple(name String, score UInt8, at DateTime64(3, 'UTC')))",
      "Tuple(name String, UInt8, tag LowCardinality(String))",
      "Variant(UInt8, String)",
      "Nested(name String, score UInt8)",
      "Map(String, Array(UInt8))",
      "FixedString(8)",
      "Decimal(10, 2)",
      "Decimal32(2)",
      "Decimal64(4)",
      "Decimal128(8)",
      "Decimal256(16)",
      "DateTime('UTC')",
      "DateTime64(3, 'UTC')",
      "Time64(6)",
      "Enum8('active' = 1, 'paused' = -2)",
      "Enum16('big' = 1000)",
      "AggregateFunction(sum, UInt64)",
      "AggregateFunction(quantile(0.5), Float64)",
      "AggregateFunction(topK(10), String)",
      "AggregateFunction(sequenceMatch('(?1)(?2)'), DateTime)",
      "SimpleAggregateFunction(sum, UInt64)",
      "Object('json')",
      "QBit(Float32, 128)",
    ] as const;

    for (const typeLiteral of validTypes) {
      expect(normalizeClickHouseTypeLiteral(` ${typeLiteral} `)).toBe(typeLiteral);
    }
  });

  it("splits and unwraps nested type arguments without crossing quoted or nested boundaries", function testTypeHelpers() {
    expect(splitTopLevelTypeList("Tuple(String, UInt8), DateTime64(3, 'UTC'), Enum8('a,b' = 1)")).toEqual([
      "Tuple(String, UInt8)",
      "DateTime64(3, 'UTC')",
      "Enum8('a,b' = 1)",
    ]);
    expect(unwrapNullableLowCardinalityType("Nullable(LowCardinality(String))")).toBe("String");
    expect(unwrapNullableLowCardinalityType("Nullable(UInt8, String)")).toBe("Nullable(UInt8, String)");
    expect(getArrayElementType(undefined)).toBeUndefined();
    expect(getArrayElementType("String")).toBeUndefined();
    expect(getArrayElementType("Nullable(Array(Tuple(name String, score UInt8)))")).toBe(
      "Tuple(name String, score UInt8)",
    );
    expect(getArrayElementType("Array(UInt8, String)")).toBeUndefined();
    expect(getTupleElementTypes(undefined)).toBeUndefined();
    expect(getTupleElementTypes("Array(UInt8)")).toBeUndefined();
    expect(getTupleElementTypes("LowCardinality(Tuple(name String, score UInt8))")).toEqual(["String", "UInt8"]);
  });

  it("rejects malformed and unsafe type literals", function testRejectedTypeLiterals() {
    const invalidTypes = [
      "\0String",
      "UInt8; DROP TABLE t",
      "Array(UInt8",
      "Array((UInt8)",
      "Array(UInt8) String",
      "Array(UInt8)String)",
      "Nullable(UInt8, String)",
      "Tuple('bad name' UInt8)",
      "Tuple(Array(UInt8) value)",
      "Tuple(1bad UInt8)",
      "Tuple(name Unknown)",
      "Nested(UInt8)",
      "Nested(name Unknown)",
      "Map(String)",
      "FixedString()",
      "FixedString(1, 2)",
      "FixedString(-1)",
      "Decimal(10)",
      "Decimal(0, 0)",
      "Decimal(2, 3)",
      "Decimal32(1, 2)",
      "Decimal32(10)",
      "DateTime('UTC', 'extra')",
      "DateTime('UTC' trailing)",
      "DateTime64(1, 'UTC', 'extra')",
      "DateTime64(10)",
      "Time64()",
      "Time64(1, 2)",
      "Time64(10)",
      "Enum8(active = 1)",
      "Enum8('bad\\\\)",
      "Enum8('active' = one)",
      "AggregateFunction(sum(), UInt64)",
      "AggregateFunction(sum)",
      "AggregateFunction(quantile(now()), Float64)",
      "AggregateFunction(quantile(0.5 + 0.1), Float64)",
      "SimpleAggregateFunction(quantile(0.5), Float64)",
      "Object('json', 'extra')",
      "QBit(Float32)",
      "QBit(Float32, nope)",
      "concat(String)",
    ] as const;

    for (const typeLiteral of invalidTypes) {
      expect(() => normalizeClickHouseTypeLiteral(typeLiteral)).toThrow();
    }

    expect(() => normalizeClickHouseTypeLiteral(1)).toThrow("ClickHouse type literal must be a non-empty string");
    expect(() => normalizeClickHouseTypeLiteral("")).toThrow("ClickHouse type literal must be a non-empty string");
    expect(() => normalizeAggregateFunctionSignature("")).toThrow(
      "AggregateFunction signature must be a non-empty string",
    );
    expect(() => splitTopLevelTypeList("'bad\\")).toThrow("Invalid ClickHouse type literal");
    expect(() => splitTopLevelTypeList("UInt8)")).toThrow("Invalid ClickHouse type literal");
    expect(() => splitTopLevelTypeList("UInt8,,String")).toThrow("Invalid ClickHouse type literal");
    expect(() => splitTopLevelTypeList("UInt8,")).toThrow("Invalid ClickHouse type literal");
    expect(() => splitTopLevelTypeList("Tuple(UInt8")).toThrow("Invalid ClickHouse type literal");
  });
});
