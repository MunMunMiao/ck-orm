import { expect, it } from "bun:test";
import { ck, ckSql, fn } from "./ck-orm";
import { createE2EDb, datasetCounts, schemaAggregates, users } from "./shared";
import {
  describeE2E,
  expectNoMutationAfterRejectedInjection,
  expectPresent,
  expectRejectsWithClickhouseError,
} from "./test-helpers";

describeE2E("ck-orm e2e query parameter edge cases", function describeQueryParameterEdgeCases() {
  it("binds literal, array, map, DateTime64 and special floating-point query parameters", async function testComplexQueryParams() {
    const db = createE2EDb();
    const timestamp = new Date("2026-04-24T01:02:03.456Z");

    const [row] = await db.execute(
      ckSql`
      select
        {literal:String} as literal,
        {items:Array(Nullable(String))} as items,
        {scores:Map(String, UInt8)} as scores,
        toUnixTimestamp64Milli({ts:DateTime64(3)}) as ts_ms,
        isNaN({nan_value:Float64}) as is_nan,
        isInfinite({inf_value:Float64}) as is_inf
    `,
      {
        query_params: {
          literal: "quote ' slash \\ line\u2028paragraph\u2029done",
          items: ["vip", null, "trial"],
          scores: new Map([
            ["gold", 1],
            ["silver", 2],
          ]),
          ts: timestamp,
          nan_value: Number.NaN,
          inf_value: Number.POSITIVE_INFINITY,
        },
      },
    );

    const presentRow = expectPresent(row, "complex query params row");
    expect(presentRow.literal).toBe("quote ' slash \\ line\u2028paragraph\u2029done");
    expect(presentRow.items).toEqual(["vip", null, "trial"]);
    expect(presentRow.scores).toEqual({ gold: 1, silver: 2 });
    expect(Number(presentRow.ts_ms)).toBe(timestamp.getTime());
    expect(Number(presentRow.is_nan)).toBe(1);
    expect(Number(presentRow.is_inf)).toBe(1);
  });

  it("uses Identifier query parameters for table and column names without widening SQL text", async function testIdentifierQueryParams() {
    const db = createE2EDb();

    const rows = await db.execute(
      ckSql`
      select {selected_column:Identifier} as value
      from {target_table:Identifier}
      where {id_column:Identifier} = {user_id:Int32}
    `,
      {
        query_params: {
          selected_column: "name",
          target_table: "users",
          id_column: "id",
          user_id: 1,
        },
      },
    );

    expect(rows).toEqual([{ value: "alice" }]);
  });

  it("rejects malicious Identifier query parameter values and leaves seeded tables untouched", async function testMaliciousIdentifierQueryParams() {
    const db = createE2EDb();

    await expectRejectsWithClickhouseError(
      db.execute(
        ckSql`
        select count() as total
        from {target_table:Identifier}
      `,
        {
          query_params: {
            target_table: "users; DROP TABLE users; --",
          },
        },
      ),
      {
        kind: "request_failed",
        executionState: "rejected",
      },
    );

    await expectNoMutationAfterRejectedInjection();
  });

  it("filters rows with inArray, notInArray and eq on a typed tuple", async function testTupleInArray() {
    const db = createE2EDb();
    const pairs = [
      [1, "alice"],
      [2, "bob"],
    ] as const;

    const rows = await db
      .select({ id: users.id, name: users.name })
      .from(users)
      .where(ck.inArray(fn.tuple(users.id, users.name), pairs))
      .orderBy(users.id);
    expect(rows).toEqual([
      { id: 1, name: "alice" },
      { id: 2, name: "bob" },
    ]);

    const [excluded] = await db
      .select({ rows: fn.count() })
      .from(users)
      .where(ck.notInArray(fn.tuple(users.id, users.name), pairs));
    expect(excluded?.rows).toBe(datasetCounts.users - 2);

    const matched = await db
      .select({ id: users.id, name: users.name })
      .from(users)
      .where(ck.eq(fn.tuple(users.id, users.name), [1, "alice"]));
    expect(matched).toEqual([{ id: 1, name: "alice" }]);
  });

  it("filters with inArray/notInArray lists longer than ClickHouse's 1,000 HTTP form fields", async function testLargeInArrayLists() {
    const db = createE2EDb();
    const ids = Array.from({ length: 2_000 }, (_, index) => index + 1);

    const [included] = await db.select({ rows: fn.count() }).from(users).where(ck.inArray(users.id, ids));
    const [excluded] = await db.select({ rows: fn.count() }).from(users).where(ck.notInArray(users.id, ids));

    expect(included?.rows).toBe(2_000);
    expect(excluded?.rows).toBe(datasetCounts.users - 2_000);
  });

  it("filters SimpleAggregateFunction(sum, Int64) values with inArray of numeric strings", async function testSimpleAggregateInArray() {
    const db = createE2EDb();
    const rows = await db
      .select({
        id: schemaAggregates.id,
        simpleSumValue: schemaAggregates.simple_sum_value,
      })
      .from(schemaAggregates)
      .where(ck.inArray(schemaAggregates.simple_sum_value, ["11", "20"]));

    expect(rows).toEqual([{ id: 1, simpleSumValue: "11" }]);
  });

  it("filters inArray and notInArray lists larger than one HTTP form field", async function testOversizedInArrayField() {
    const db = createE2EDb();
    const matchedIds = ["1", "2", "42"];
    const longStrings = [
      ...matchedIds,
      ...Array.from({ length: 700 }, (_, index) => `${index.toString().padStart(3, "0")}${"x".repeat(197)}`),
    ];
    const uuidStrings = [
      ...matchedIds,
      ...Array.from({ length: 5_000 }, (_, index) => `00000000-0000-4000-8000-${index.toString(16).padStart(12, "0")}`),
    ];

    const [longMatch] = await db
      .select({ rows: fn.count() })
      .from(users)
      .where(ck.inArray(fn.toString(users.id), longStrings));
    const [uuidMatch] = await db
      .select({ rows: fn.count() })
      .from(users)
      .where(ck.inArray(fn.toString(users.id), uuidStrings));
    const [uuidMiss] = await db
      .select({ rows: fn.count() })
      .from(users)
      .where(ck.notInArray(fn.toString(users.id), uuidStrings));

    expect(longMatch?.rows).toBe(matchedIds.length);
    expect(uuidMatch?.rows).toBe(matchedIds.length);
    expect(uuidMiss?.rows).toBe(datasetCounts.users - matchedIds.length);
  });
});
