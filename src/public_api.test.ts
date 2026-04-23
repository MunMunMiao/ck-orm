import { describe, expect, it } from "bun:test";
import type * as RootApi from "ck-orm";
import * as publicApi from "./index";

describe("ck-orm public api", function describePublicApi() {
  it("keeps internal runtime helpers out of the package root", function testPrivateRuntimeHelpers() {
    expect("renderTableIdentifier" in publicApi).toBe(false);
    // @ts-expect-error ClickHouseTableEngine should stay private to the schema module
    expectType<RootApi.ClickHouseTableEngine | undefined>(undefined);
    // @ts-expect-error TableOptions should stay private to the schema module
    expectType<RootApi.TableOptions | undefined>(undefined);
  });

  it("keeps core schema and query builders available from the package root", function testPublicBuilders() {
    expect("ck" in publicApi).toBe(true);
    expect("chType" in publicApi).toBe(true);
    expect("chTable" in publicApi).toBe(true);
    expect("clickhouseClient" in publicApi).toBe(true);
    expect("Grouping" in publicApi).toBe(false);
    expect("Order" in publicApi).toBe(false);
    expect("Predicate" in publicApi).toBe(false);
    expect("Selection" in publicApi).toBe(false);
    expect("SqlExpression" in publicApi).toBe(false);
    expect("makeWhereCondition" in publicApi).toBe(false);
    expect("sql" in publicApi).toBe(false);
    expect("eq" in publicApi).toBe(false);
    expect("desc" in publicApi).toBe(false);
    expect("int32" in publicApi).toBe(false);
    expect("string" in publicApi).toBe(false);
    expect("decimal" in publicApi).toBe(false);
    expect("array" in publicApi).toBe(false);
    expect("tableFn" in publicApi).toBe(false);
    expect(typeof publicApi.chType.int32).toBe("function");
    expect(typeof publicApi.chType.string).toBe("function");
    expect(publicApi.ck.fn).toBe(publicApi.fn);
    expect(typeof publicApi.ck.eq).toBe("function");
    expect(typeof publicApi.ck.desc).toBe("function");
    expect(typeof publicApi.ck.sql.raw).toBe("function");
    expect(typeof publicApi.fn.table.call).toBe("function");
  });

  it("keeps advanced root-exported types aligned with public_api.ts", function testRootExportedTypes() {
    expectType<RootApi.AnyColumn | undefined>(undefined);
    expectType<RootApi.AnyTable | undefined>(undefined);
    expectType<RootApi.Order | undefined>(undefined);
    expectType<RootApi.Predicate | undefined>(undefined);
    expectType<RootApi.Selection | undefined>(undefined);
    expectType<RootApi.Session | undefined>(undefined);
    // @ts-expect-error Grouping should remain private to clause internals
    expectType<RootApi.Grouping | undefined>(undefined);
  });

  it("keeps error guards and compatibility exports available from the package root", function testRootErrorExports() {
    expect("ClickHouseOrmError" in publicApi).toBe(false);
    expect("DecodeError" in publicApi).toBe(false);
    expect("isClickHouseOrmError" in publicApi).toBe(true);
    expect("isDecodeError" in publicApi).toBe(true);

    expectType<RootApi.ClickHouseOrmError | undefined>(undefined);
    expectType<RootApi.DecodeError | undefined>(undefined);
  });
});

function expectType<TValue>(_value: TValue) {}
