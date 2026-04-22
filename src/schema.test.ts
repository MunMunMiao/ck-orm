import { describe, expect, it } from "bun:test";
import type { InferInsertModel, InferInsertSchema, InferSelectModel, InferSelectSchema } from "ck-orm";
import { int32 } from "./columns";
import { alias, chTable } from "./schema";
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

describe("ck-orm schema infer helpers", function describeClickHouseOrmSchemaInferHelpers() {
  it("exposes stable model inference helpers for tables and schema objects", function testSchemaInferHelpers() {
    expect(typeAssertions).toEqual([true, true, true, true, true, true, true, true]);
  });

  it("keeps unmanaged orderBy columns stable when aliasing tables", function testAliasWithUnmanagedOrderByColumns() {
    const unmanagedOrderBy = int32();
    const table = chTable(
      "events",
      {
        id: int32(),
      },
      {
        engine: "MergeTree",
        orderBy: [unmanagedOrderBy as never],
      },
    );

    const aliased = alias(table, "e");
    expect(aliased.options.orderBy).toEqual([unmanagedOrderBy]);
  });

  it("rebinds managed orderBy and versionColumn to the aliased columns", function testAliasRebindsOrderByAndVersionColumn() {
    const events = chTable("events", { id: int32(), version: int32() }, (t) => ({
      engine: "ReplacingMergeTree",
      orderBy: [t.id],
      versionColumn: t.version,
    }));

    const aliased = alias(events, "e");
    expect(aliased.options.orderBy?.[0]).toBe(aliased.id as never);
    expect(aliased.options.versionColumn).toBe(aliased.version as never);
    expect((aliased.options.orderBy?.[0] as { tableAlias?: string } | undefined)?.tableAlias).toBe("e");
    expect((aliased.options.versionColumn as { tableAlias?: string } | undefined)?.tableAlias).toBe("e");
  });

  it("rebinds partitionBy and primaryKey arrays when aliasing tables", function testAliasRebindsExpressionLists() {
    const events = chTable("events", { id: int32(), tenant_id: int32(), bucket_id: int32() }, (t) => ({
      engine: "MergeTree",
      partitionBy: [t.tenant_id, t.bucket_id],
      primaryKey: [t.id, t.tenant_id],
    }));

    const aliased = alias(events, "e");
    expect(aliased.options.partitionBy).toEqual([aliased.tenant_id, aliased.bucket_id]);
    expect(aliased.options.primaryKey).toEqual([aliased.id, aliased.tenant_id]);
  });

  it("rebinds single partitionBy and primaryKey expressions when aliasing tables", function testAliasRebindsSingleExpressions() {
    const events = chTable("events", { id: int32(), tenant_id: int32() }, (t) => ({
      engine: "MergeTree",
      partitionBy: t.tenant_id,
      primaryKey: t.id,
    }));

    const aliased = alias(events, "e");
    expect(aliased.options.partitionBy).toBe(aliased.tenant_id);
    expect(aliased.options.primaryKey).toBe(aliased.id);
  });
});
