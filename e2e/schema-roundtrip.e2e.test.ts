import { expect, it } from "bun:test";
import { alias, csql } from "./ck-orm";
import { createE2EDb, schemaAggregates, schemaCompound, schemaGeo, schemaPrimitives, users } from "./shared";
import { describeE2E, expectDate, expectPresent } from "./test-helpers";

describeE2E("ck-orm e2e schema roundtrip", function describeSchemaRoundtrip() {
  it("round-trips all primitive schema factories against real clickhouse", async function testPrimitiveSchemaRoundtrip() {
    const db = createE2EDb();
    const [row] = await db.select().from(schemaPrimitives).orderBy(schemaPrimitives.id).limit(1);

    const presentRow = expectPresent(row, "schemaPrimitives row");
    expect(presentRow.int8_value).toBe(-8);
    expect(presentRow.int16_value).toBe(-16);
    expect(presentRow.int32_value).toBe(-32);
    expect(presentRow.int64_value).toBe("-64");
    expect(presentRow.uint8_value).toBe(8);
    expect(presentRow.uint16_value).toBe(16);
    expect(presentRow.uint32_value).toBe(32);
    expect(presentRow.uint64_value).toBe("64");
    expect(presentRow.float32_value).toBeCloseTo(3.25);
    expect(presentRow.float64_value).toBe(6.5);
    expect(presentRow.bfloat16_value).toBeCloseTo(1.75, 2);
    expect(presentRow.string_value).toBe("hello world");
    expect(presentRow.fixed_string_value).toBe("ABCD");
    expect(presentRow.decimal_value).toBe("1234.56");
    expectDate(presentRow.date_value);
    expectDate(presentRow.date32_value);
    expectDate(presentRow.time_value);
    expectDate(presentRow.time64_value);
    expectDate(presentRow.date_time_value);
    expectDate(presentRow.date_time64_value);
    expect(presentRow.bool_value).toBe(true);
    expect(presentRow.uuid_value).toBe("123e4567-e89b-12d3-a456-426614174000");
    expect(presentRow.ipv4_value).toBe("192.168.10.1");
    expect(presentRow.ipv6_value).toBe("2001:db8::1");
    expect(presentRow.json_value).toEqual({ id: "1", label: "json-value" });
    expect(presentRow.dynamic_value).toBe("dynamic-value");
    expect(presentRow.qbit_value).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(presentRow.enum8_value).toBe("active");
    expect(presentRow.enum16_value).toBe("silver");
  });

  it("round-trips compound schema factories including nested and variant", async function testCompoundSchemaRoundtrip() {
    const db = createE2EDb();
    const [row] = await db.select().from(schemaCompound).orderBy(schemaCompound.id).limit(1);

    expect(expectPresent(row, "schemaCompound row")).toEqual({
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
  });

  it("round-trips aggregate and geo schema factories", async function testAggregateAndGeoSchemaRoundtrip() {
    const db = createE2EDb();
    const [aggregateRow] = await db
      .select({
        id: schemaAggregates.id,
        aggValue: csql<number>`finalizeAggregation(${schemaAggregates.agg_sum_state})`
          .mapWith((value) => Number(value))
          .as("agg_value"),
        simpleValue: schemaAggregates.simple_sum_value,
      })
      .from(schemaAggregates)
      .orderBy(schemaAggregates.id)
      .limit(1);

    expect(expectPresent(aggregateRow, "aggregateRow")).toEqual({
      id: 1,
      aggValue: 7,
      simpleValue: "11",
    });

    const [geoRow] = await db.select().from(schemaGeo).orderBy(schemaGeo.id).limit(1);
    expect(expectPresent(geoRow, "geoRow")).toEqual({
      id: 1,
      point_value: [1.5, 2.5],
      ring_value: [
        [0, 0],
        [1, 0],
        [1, 1],
        [0, 0],
      ],
      line_value: [
        [0, 0],
        [1, 1],
      ],
      multi_line_value: [
        [
          [0, 0],
          [1, 1],
        ],
        [
          [2, 2],
          [3, 3],
        ],
      ],
      polygon_value: [
        [
          [0, 0],
          [1, 0],
          [1, 1],
          [0, 0],
        ],
      ],
      multi_polygon_value: [
        [
          [
            [0, 0],
            [1, 0],
            [1, 1],
            [0, 0],
          ],
        ],
      ],
    });
  });

  it("supports alias interpolation for schema sources", async function testAliasRoundtrip() {
    const db = createE2EDb();
    const aliasedUsers = alias(users, "u");
    const rows = await db.execute(csql`
      select ${aliasedUsers.id} as id, ${aliasedUsers.name} as name
      from ${aliasedUsers}
      where ${aliasedUsers.id} = ${1}
    `);

    expect(rows).toEqual([{ id: 1, name: "alice" }]);
  });
});
