import { describe, expect, it } from "bun:test";
import type { InferInsertModel, InferInsertSchema, InferSelectModel, InferSelectSchema } from "ck-orm";
import { int32, string } from "./columns";
import { ckAlias, ckTable, renderTableIdentifier } from "./schema";
import { compileSql } from "./sql";
import type {
  commerceSchema,
  customerInvoice,
  InvoiceInsertModel,
  InvoiceModel,
  OrderRewardLogInsertModel,
  OrderRewardLogModel,
  orderRewardLog,
} from "./test-schema/commerce";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Expect<T extends true> = T;

type _InvoiceSelectModel = Expect<Equal<InferSelectModel<typeof customerInvoice>, typeof customerInvoice.$inferSelect>>;
type _InvoiceInsertModel = Expect<Equal<InferInsertModel<typeof customerInvoice>, typeof customerInvoice.$inferInsert>>;
type _DirectInvoiceSelectModel = Expect<Equal<InvoiceModel, typeof customerInvoice.$inferSelect>>;
type _DirectInvoiceInsertModel = Expect<Equal<InvoiceInsertModel, typeof customerInvoice.$inferInsert>>;
type _DirectOrderRewardLogModel = Expect<Equal<OrderRewardLogModel, typeof orderRewardLog.$inferSelect>>;
type _DirectOrderRewardLogInsertModel = Expect<Equal<OrderRewardLogInsertModel, typeof orderRewardLog.$inferInsert>>;
type _CommerceSelectSchema = Expect<
  Equal<
    InferSelectSchema<typeof commerceSchema>,
    {
      customerInvoice: typeof customerInvoice.$inferSelect;
      orderRewardLog: typeof orderRewardLog.$inferSelect;
    }
  >
>;
type _CommerceInsertSchema = Expect<
  Equal<
    InferInsertSchema<typeof commerceSchema>,
    {
      customerInvoice: typeof customerInvoice.$inferInsert;
      orderRewardLog: typeof orderRewardLog.$inferInsert;
    }
  >
>;

const typeAssertions: [
  _InvoiceSelectModel,
  _InvoiceInsertModel,
  _DirectInvoiceSelectModel,
  _DirectInvoiceInsertModel,
  _DirectOrderRewardLogModel,
  _DirectOrderRewardLogInsertModel,
  _CommerceSelectSchema,
  _CommerceInsertSchema,
] = [true, true, true, true, true, true, true, true];

describe("ck-orm schema infer helpers", function describeClickHouseORMSchemaInferHelpers() {
  it("exposes stable model inference helpers for tables and schema objects", function testSchemaInferHelpers() {
    expect(typeAssertions).toEqual([true, true, true, true, true, true, true, true]);
  });

  it("keeps unmanaged orderBy columns stable when aliasing tables", function testAliasWithUnmanagedOrderByColumns() {
    const unmanagedOrderBy = int32();
    const table = ckTable(
      "events",
      {
        id: int32(),
      },
      {
        engine: "MergeTree",
        orderBy: [unmanagedOrderBy as never],
      },
    );

    const aliased = ckAlias(table, "e");
    expect(aliased.options.orderBy).toEqual([unmanagedOrderBy]);
  });

  it("rebinds managed orderBy and versionColumn to the aliased columns", function testAliasRebindsOrderByAndVersionColumn() {
    const events = ckTable("events", { id: int32(), version: int32() }, (t) => ({
      engine: "ReplacingMergeTree",
      orderBy: [t.id],
      versionColumn: t.version,
    }));

    const aliased = ckAlias(events, "e");
    expect(aliased.options.orderBy?.[0]).toBe(aliased.id as never);
    expect(aliased.options.versionColumn).toBe(aliased.version as never);
    expect((aliased.options.orderBy?.[0] as { tableAlias?: string } | undefined)?.tableAlias).toBe("e");
    expect((aliased.options.versionColumn as { tableAlias?: string } | undefined)?.tableAlias).toBe("e");
  });

  it("uses object keys as logical keys and rejects duplicate physical column names", function testPhysicalNames() {
    const events = ckTable("events", { userId: string("user_id"), createdAt: int32("created_at") }, (table) => ({
      engine: "MergeTree",
      orderBy: [table.userId, table.createdAt],
    }));

    expect(events.userId.key).toBe("userId");
    expect(events.userId.name).toBe("user_id");
    expect(events.createdAt.key).toBe("createdAt");
    expect(events.createdAt.name).toBe("created_at");

    const aliased = ckAlias(events, "e");
    expect(aliased.userId.key).toBe("userId");
    expect(aliased.userId.name).toBe("user_id");
    expect(aliased.userId.tableAlias).toBe("e");
    expect(aliased.options.orderBy?.[0]).toBe(aliased.userId as never);
    expect(aliased.options.orderBy?.[1]).toBe(aliased.createdAt as never);

    expect(() =>
      ckTable("duplicate_columns", {
        userId: string("user_id"),
        user_id: int32(),
      }),
    ).toThrow('Duplicate column name "user_id" in table "duplicate_columns"');
  });

  it("rebinds partitionBy and primaryKey arrays when aliasing tables", function testAliasRebindsExpressionLists() {
    const events = ckTable("events", { id: int32(), tenant_id: int32(), bucket_id: int32() }, (t) => ({
      engine: "MergeTree",
      partitionBy: [t.tenant_id, t.bucket_id],
      primaryKey: [t.id, t.tenant_id],
    }));

    const aliased = ckAlias(events, "e");
    expect(aliased.options.partitionBy).toEqual([aliased.tenant_id, aliased.bucket_id]);
    expect(aliased.options.primaryKey).toEqual([aliased.id, aliased.tenant_id]);
    expect(compileSql(renderTableIdentifier(aliased)).query).toBe("`events` as `e`");
  });

  it("rebinds single partitionBy and primaryKey expressions when aliasing tables", function testAliasRebindsSingleExpressions() {
    const events = ckTable("events", { id: int32(), tenant_id: int32() }, (t) => ({
      engine: "MergeTree",
      partitionBy: t.tenant_id,
      primaryKey: t.id,
    }));

    const aliased = ckAlias(events, "e");
    expect(aliased.options.partitionBy).toBe(aliased.tenant_id);
    expect(aliased.options.primaryKey).toBe(aliased.id);
  });

  it("rejects malformed alias names at construction time", function testAliasFailFast() {
    const events = ckTable("events", { id: int32() });
    // Previously these only failed at compile-time. Fail-fast keeps stack
    // traces meaningful and matches Drizzle's `alias()` behaviour.
    expect(() => ckAlias(events, "evil`; drop --" as never)).toThrow("Invalid SQL identifier");
    expect(() => ckAlias(events, "x y" as never)).toThrow("Invalid SQL identifier");
    expect(() => ckAlias(events, "" as never)).toThrow("Invalid SQL identifier");
  });

  it("warns about column keys that collide with reserved table metadata fields", function testReservedTableKeyCollision() {
    // `Object.assign({}, boundColumns, tableBase)` in ckTable silently shadows
    // user-supplied columns whose keys match the metadata field names. We
    // surface that as a one-shot console.warn rather than a hard throw — real
    // OpenTelemetry / SigNoz schemas legitimately use `kind` as a column,
    // and breaking those at construction time would force every consumer to
    // rewrite their schemas. The column ref stays reachable via
    // `table.columns[key]`.
    const originalWarn = console.warn;
    const warnings: string[] = [];
    console.warn = (...args: unknown[]) => {
      warnings.push(args.map(String).join(" "));
    };
    try {
      const t1 = ckTable("collision_table_a", { kind: int32() });
      expect(t1.columns.kind).toBeDefined();
      expect((t1 as { kind: unknown }).kind).toBe("table"); // metadata wins on top-level access
      expect(warnings.some((w) => w.includes("kind"))).toBe(true);

      // Second call to the same (table, key) is deduped — no extra warning.
      const lengthAfterFirst = warnings.length;
      ckTable("collision_table_a", { kind: int32(), id: int32() });
      expect(warnings.length).toBe(lengthAfterFirst);

      // Different table / different key still warns.
      ckTable("collision_table_b", { columns: int32() });
      expect(warnings.some((w) => w.includes("columns"))).toBe(true);
    } finally {
      console.warn = originalWarn;
    }
  });
});
