import { expect, it } from "bun:test";
import { ck, ckSql, type InferInsertModel, type InferSelectModel } from "./ck-orm";
import {
  buildCreateTableStatement,
  chainedColumns,
  createE2EDb,
  ddlBrand,
  ioSplit,
  validatorStrict,
  validatorTransform,
} from "./shared";
import {
  describeE2E,
  expectClientValidationNotSent,
  expectPresent,
  expectRejectsWithClickhouseError,
} from "./test-helpers";

describeE2E("ck-orm e2e — developer-scenario type overrides", function describeDeveloperScenarioTypeOverrides() {
  it("decodes through $validator and surfaces failures as DecodeError", async function testValidatorDecodes() {
    const db = createE2EDb();

    // Happy path — seed inserted "admin" / "user", both should pass the schema.
    // Filter to ids 1-2 so a leftover illegal row from a previous run cannot
    // bleed in and trigger a decode error before we get to the failure case.
    const happyRows = await db
      .select()
      .from(validatorStrict)
      .where(ck.inArray(validatorStrict.id, [1, 2]))
      .orderBy(validatorStrict.id)
      .execute();
    expect(happyRows.length).toBe(2);
    const happyValues = happyRows.map((row) => row.status).sort();
    expect(happyValues).toEqual(["admin", "user"]);

    // Sneak an illegal row past the ORM encode path using raw SQL.
    await db.command(ckSql`INSERT INTO validator_strict (id, status) VALUES (999, 'banned')`);

    // Now SELECTing the illegal row should throw a DecodeError with the schema's message.
    // ck-orm wraps decoder errors in a generic "Failed to decode column" message
    // and tucks the original schema diagnostic into `responseText`.
    const decodeError = await expectRejectsWithClickhouseError(
      () => db.select().from(validatorStrict).where(ck.eq(validatorStrict.id, 999)).execute(),
      { kind: "decode", executionState: "rejected" },
    );
    expect(decodeError.message).toContain("Failed to decode column: status");
    expect(decodeError.responseText).toContain("Standard Schema validation failed");
    expect(decodeError.responseText).toContain('Expected "admin" or "user"');
  });

  it("rejects illegal $validator insert input as client_validation before sending", async function testValidatorInsertRejection() {
    const db = createE2EDb();
    const error = await expectClientValidationNotSent(() =>
      db.insert(validatorStrict).values({ id: 500, status: "invalid" as never }),
    );
    expect(error.message).toContain("Standard Schema validation failed");
  });

  it("applies $validator transform schema across encode + decode", async function testValidatorTransform() {
    const db = createE2EDb();

    // The seed inserted '2026-04-21T00:00:00.000Z' as a raw string. On decode
    // the $validator transforms it into a JS Date.
    const [seedRow] = await db.select().from(validatorTransform).where(ck.eq(validatorTransform.id, 1)).execute();
    const present = expectPresent(seedRow, "seed validator_transform row");
    expect(present.occurred_at).toBeInstanceOf(Date);
    expect((present.occurred_at as Date).toISOString()).toBe("2026-04-21T00:00:00.000Z");

    // ORM insert: the schema input is `string`, so TS accepts a string here.
    // The validator transforms the string into a Date; the underlying String
    // column's encoder (`toStringValue`) re-serialises the Date with
    // `toISOString()` so ClickHouse receives a string back.
    await db.insert(validatorTransform).values({
      id: 2,
      occurred_at: "2026-05-15T12:34:56.000Z",
    });

    const [round] = await db.select().from(validatorTransform).where(ck.eq(validatorTransform.id, 2)).execute();
    const roundPresent = expectPresent(round, "round-tripped validator_transform row");
    expect(roundPresent.occurred_at).toBeInstanceOf(Date);
    expect((roundPresent.occurred_at as Date).toISOString()).toBe("2026-05-15T12:34:56.000Z");

    // Type-level checks — InferInsert<>.occurred_at is string (schema input),
    // InferSelect<>.occurred_at is Date (schema output).
    type Insert = InferInsertModel<typeof validatorTransform>;
    type Select = InferSelectModel<typeof validatorTransform>;
    const _legalInsert: Insert = { id: 3, occurred_at: "2026-01-01T00:00:00.000Z" };
    void _legalInsert;
    const _selectShape: Select["occurred_at"] = new Date();
    void _selectShape;

    // @ts-expect-error insert side cannot accept Date — the schema input is string only
    const _bad: Insert = { id: 4, occurred_at: new Date() };
    void _bad;
  });

  it("supports $type<{ select, insert }> IO split on a DateTime column", async function testTypeIoSplit() {
    const db = createE2EDb();

    // Insert with a Date instance
    await db.insert(ioSplit).values({ id: 2, created_at: new Date("2026-04-22T00:00:00.000Z") });
    // Insert with a raw string — both are accepted on the insert side
    await db.insert(ioSplit).values({ id: 3, created_at: "2026-04-23 00:00:00" });

    const rows = await db.select().from(ioSplit).orderBy(ioSplit.id).execute();
    expect(rows.length).toBeGreaterThanOrEqual(3);

    for (const row of rows) {
      expect(row.created_at).toBeInstanceOf(Date);
    }

    // Type-level: InferInsert accepts string|Date, InferSelect is strictly Date
    type Insert = InferInsertModel<typeof ioSplit>;
    type Select = InferSelectModel<typeof ioSplit>;
    const _insertWithString: Insert = { id: 99, created_at: "2026-06-01 00:00:00" };
    const _insertWithDate: Insert = { id: 98, created_at: new Date() };
    void _insertWithString;
    void _insertWithDate;
    const _selectIsDate: Select["created_at"] = new Date();
    void _selectIsDate;

    // @ts-expect-error select side is strictly Date — a string is not assignable
    const _badSelect: Select["created_at"] = "2026-04-21T00:00:00.000Z";
    void _badSelect;
  });

  it("honors DDL brands: MATERIALIZED/ALIAS are dropped from insert, DEFAULT becomes optional", async function testDdlBrand() {
    const db = createE2EDb();
    // Truncate first so repeated test runs (the table is shared with other
    // scenarios and is never re-seeded between tests) cannot leave us with
    // duplicate rows that break the row-count assertion below.
    await db.command(ckSql`TRUNCATE TABLE ddl_brand`);
    const idA = 10001;
    const idB = 10002;

    // idA omits default_role entirely; ClickHouse must fall back to DEFAULT.
    await db.insert(ddlBrand).values({ id: idA });
    // idB explicitly overrides default_role.
    await db.insert(ddlBrand).values({ id: idB, default_role: "explicit" });

    const rows = await db
      .select()
      .from(ddlBrand)
      .where(ck.inArray(ddlBrand.id, [idA, idB]))
      .orderBy(ddlBrand.id)
      .execute();
    expect(rows.length).toBe(2);

    const first = expectPresent(rows[0], `ddl_brand id=${idA}`);
    expect(first.default_role).toBe("guest"); // DEFAULT fired
    expect(first.computed_label).toBe(`user-${idA}`); // MATERIALIZED fired
    expect(first.aliased_search).toBe("GUEST"); // ALIAS evaluated on read

    const second = expectPresent(rows[1], `ddl_brand id=${idB}`);
    expect(second.default_role).toBe("explicit");
    expect(second.computed_label).toBe(`user-${idB}`);
    expect(second.aliased_search).toBe("EXPLICIT");

    type Insert = InferInsertModel<typeof ddlBrand>;

    // @ts-expect-error MATERIALIZED columns are removed from the insert model
    const _writeMaterialized: Insert = { id: 10, computed_label: "user-10" };
    void _writeMaterialized;

    // @ts-expect-error ALIAS columns are removed from the insert model
    const _writeAlias: Insert = { id: 11, aliased_search: "ANY" };
    void _writeAlias;

    // default_role being absent is legal because it has a DEFAULT
    const _legalOmitDefault: Insert = { id: 12 };
    void _legalOmitDefault;
  });

  it("DDL brand round-trip leaves materialized/alias SQL exactly as written", function testDdlBrandSqlShape() {
    const ddl = buildCreateTableStatement(ddlBrand);
    expect(ddl).toContain("`default_role` String DEFAULT 'guest'");
    expect(ddl).toContain("`computed_label` String MATERIALIZED concat('user-', toString(id))");
    expect(ddl).toContain("`aliased_search` String ALIAS upper(default_role)");
  });

  it("supports chained $type + default + comment + codec without breaking round-trip", async function testChainedColumnsRoundtrip() {
    const db = createE2EDb();

    // The seed inserted id=1 with both DEFAULTs taking effect.
    const [seeded] = await db.select().from(chainedColumns).where(ck.eq(chainedColumns.id, 1)).execute();
    const present = expectPresent(seeded, "seeded chained_columns row");
    expect(present.status).toBe("a");
    expect(present.category).toBe("books");

    // ORM insert with explicit narrowed values
    await db.insert(chainedColumns).values({ id: 2, status: "b", category: "music" });
    const [explicit] = await db.select().from(chainedColumns).where(ck.eq(chainedColumns.id, 2)).execute();
    const explicitPresent = expectPresent(explicit, "explicit chained_columns row");
    expect(explicitPresent.status).toBe("b");
    expect(explicitPresent.category).toBe("music");

    // DDL keeps comment + codec + default — all three column modifiers compose
    const ddl = buildCreateTableStatement(chainedColumns);
    expect(ddl).toContain("`status` String DEFAULT 'a' COMMENT");
    expect(ddl).toContain("`category` String DEFAULT 'books' CODEC(ZSTD(3))");

    type Insert = InferInsertModel<typeof chainedColumns>;

    // @ts-expect-error status only accepts the narrowed "a" | "b" literals
    const _bad: Insert = { id: 99, status: "c", category: "books" };
    void _bad;

    // Both narrowed columns are optional thanks to their DEFAULT clauses
    const _legalOmit: Insert = { id: 3 };
    void _legalOmit;
  });
});
