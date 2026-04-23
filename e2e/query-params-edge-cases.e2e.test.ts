import { expect, it } from "bun:test";
import { csql } from "./ck-orm";
import { createE2EDb } from "./shared";
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
      csql`
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
      csql`
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
        csql`
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
});
