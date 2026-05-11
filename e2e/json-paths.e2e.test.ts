import { expect, it } from "bun:test";
import { ckSql, ckType, fn } from "./ck-orm";
import { createE2EDb, schemaJsonAdvanced } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

describeE2E("ck-orm e2e NewJSON path access", function describeJsonPathAccess() {
  it("renders parameterized JSON DDL with typeHints, SKIP, and SKIP REGEXP", async function testJsonDdlInSystemTables() {
    const db = createE2EDb();
    const rows = await db.execute<{ create_table_query: string }>(
      ckSql`SELECT create_table_query FROM system.tables WHERE name = 'schema_json_advanced'`,
    );
    const row = expectPresent(rows[0], "system.tables row");
    // typed paths and SKIP rules survive into the on-server DDL exactly as
    // ck-orm rendered them. ClickHouse may re-quote dotted typed paths with
    // backticks when echoing the DDL back via system.tables — accept either
    // form so the assertion stays stable across server versions.
    expect(row.create_table_query).toMatch(/max_dynamic_paths\s*=\s*128/);
    expect(row.create_table_query).toMatch(/max_dynamic_types\s*=\s*8/);
    expect(row.create_table_query).toMatch(/`?nested\.score`?\s+UInt32/);
    expect(row.create_table_query).toMatch(/user_id\s+UInt64/);
    expect(row.create_table_query).toMatch(/SKIP debug/);
    expect(row.create_table_query).toMatch(/SKIP REGEXP '\^_tmp'/);
  });

  it("selects typed sub-paths via .path()", async function testJsonPathSelect() {
    const db = createE2EDb();
    const rows = await db
      .select({
        id: schemaJsonAdvanced.id,
        uid: schemaJsonAdvanced.payload.path("user_id"),
        score: schemaJsonAdvanced.payload.path("nested.score"),
        tag: schemaJsonAdvanced.payload.path("tag"),
      })
      .from(schemaJsonAdvanced)
      .orderBy(schemaJsonAdvanced.id);

    expect(rows).toHaveLength(2);
    expect(expectPresent(rows[0], "row 1")).toEqual({ id: 1, uid: "999", score: 42, tag: "alpha" });
    expect(expectPresent(rows[1], "row 2")).toEqual({ id: 2, uid: "1000", score: 5, tag: "beta" });
  });

  it("filters by sub-path in WHERE", async function testJsonPathFilter() {
    const db = createE2EDb();
    const rows = await db
      .select({ id: schemaJsonAdvanced.id })
      .from(schemaJsonAdvanced)
      .where(ckSql`${schemaJsonAdvanced.payload.path("nested.score")} > ${10}`)
      .orderBy(schemaJsonAdvanced.id);
    expect(rows).toEqual([{ id: 1 }]);
  });

  it("casts dynamic paths via .castPath()", async function testJsonCastPath() {
    const db = createE2EDb();
    const rows = await db
      .select({
        id: schemaJsonAdvanced.id,
        tagUpper: schemaJsonAdvanced.payload.castPath("tag", ckType.string()),
      })
      .from(schemaJsonAdvanced)
      .orderBy(schemaJsonAdvanced.id);
    expect(expectPresent(rows[0], "castPath row 0").tagUpper).toBe("alpha");
    expect(expectPresent(rows[1], "castPath row 1").tagUpper).toBe("beta");
  });

  it("reads sub-objects via .subobject() as JSON values", async function testJsonSubobject() {
    const db = createE2EDb();
    const rows = await db
      .select({
        id: schemaJsonAdvanced.id,
        nested: schemaJsonAdvanced.payload.subobject("nested"),
      })
      .from(schemaJsonAdvanced)
      .orderBy(schemaJsonAdvanced.id);
    // `^nested` returns a JSON sub-object; ClickHouse 26.3 surfaces it as an
    // object whose `score` is the path-hint (UInt32 → number).
    const first = expectPresent(rows[0], "subobject row 0");
    expect(first.id).toBe(1);
    expect((first.nested as Record<string, unknown>).score).toBe(42);
  });

  it("works with fn.* JSON helpers for dynamic-path call sites", async function testJsonFnHelpers() {
    const db = createE2EDb();
    // ClickHouse rejects `.:Type` casts on *typed* paths (their type is
    // already fixed in the DDL) — castPath / jsonCast targets a dynamic
    // path, which is `tag` here.
    const rows = await db
      .select({
        id: schemaJsonAdvanced.id,
        uid: fn.jsonPath<string>(schemaJsonAdvanced.payload, "user_id"),
        tagCast: fn.jsonCast(schemaJsonAdvanced.payload, "tag", ckType.string()),
        kind: fn.dynamicType(fn.jsonPath(schemaJsonAdvanced.payload, "tag")),
      })
      .from(schemaJsonAdvanced)
      .orderBy(schemaJsonAdvanced.id);
    const first = expectPresent(rows[0], "fn helpers row 0");
    expect(first.uid).toBe("999");
    expect(first.tagCast).toBe("alpha");
    expect(first.kind).toBe("String");
  });

  it("rejects top-level non-object on INSERT (client-side guard)", async function testJsonClientGuard() {
    const db = createE2EDb();
    // The runtime guard inside `mapToDriverValue` throws synchronously while
    // the values() builder is being compiled, so wrap with `async () =>`
    // instead of relying on the builder's thenable surface.
    const arrayPayload: unknown = ["wrong"];
    const scalarPayload: unknown = "raw";

    await expect(async () => {
      await db.insert(schemaJsonAdvanced).values({
        id: 999_001,
        payload: arrayPayload as never,
      });
    }).toThrow(/plain object/);

    await expect(async () => {
      await db.insert(schemaJsonAdvanced).values({
        id: 999_002,
        payload: scalarPayload as never,
      });
    }).toThrow(/plain object/);
  });

  it("inserts a typed-hint row through the builder and reads it back", async function testJsonInsertThroughBuilder() {
    const db = createE2EDb();
    await db.insert(schemaJsonAdvanced).values({
      id: 1001,
      payload: { user_id: "55555", tag: "gamma", nested: { score: 7 } },
      payload_with_default: { note: "via builder" },
    });
    const rows = await db.select().from(schemaJsonAdvanced).where(ckSql`${schemaJsonAdvanced.id} = ${1001}`).limit(1);
    const row = expectPresent(rows[0], "builder-inserted row");
    expect(row.payload).toEqual({ user_id: "55555", tag: "gamma", nested: { score: 7 } });
    expect(row.payload_with_default).toEqual({ note: "via builder" });
    // Cleanup so re-running the suite doesn't accumulate rows
    await db.command(ckSql`ALTER TABLE schema_json_advanced DELETE WHERE id = ${1001}`);
  });
});
