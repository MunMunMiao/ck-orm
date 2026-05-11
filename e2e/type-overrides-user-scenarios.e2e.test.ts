import { expect, it } from "bun:test";
import { ck, type InferInsertModel, type InferSelectModel } from "./ck-orm";
import {
  auditLogTyped,
  buildCreateTableStatement,
  createE2EDb,
  type UserId,
  type UserRole,
  userProfileTyped,
} from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

// Helper to brand an arbitrary number as a UserId at the call site without
// allocating extra wrapper objects.
const userId = (value: number) => value as UserId;

describeE2E("ck-orm e2e — user-scenario type overrides", function describeUserScenarioTypeOverrides() {
  it("emits the same DDL whether or not $type is applied", function testTypeDoesNotAlterDdl() {
    const ddl = buildCreateTableStatement(auditLogTyped);
    // actor_id is Int32 even though TS sees it as UserId
    expect(ddl).toContain("`actor_id` Int32");
    // actor_role keeps its enum mapping; $type only widens the TS type
    expect(ddl).toContain("`actor_role` Enum8('guest' = 1, 'user' = 2, 'admin' = 3)");
    // action gets its full Enum8 mapping inferred from the values object
    expect(ddl).toContain("`action` Enum8('login' = 1, 'logout' = 2, 'password_reset' = 3, 'role_change' = 4)");
    // note carries the DEFAULT clause exactly as written in the schema
    expect(ddl).toContain("`note` String DEFAULT ''");
  });

  it("decodes enum columns into their narrowed literal unions", async function testEnumLiteralRoundtrip() {
    const db = createE2EDb();
    // Filter to the three seeded ids so leftover rows from earlier test
    // iterations cannot drift the ordering of the first row.
    const rows = await db
      .select()
      .from(auditLogTyped)
      .where(ck.inArray(auditLogTyped.id, [1, 2, 3]))
      .orderBy(auditLogTyped.id)
      .execute();

    expect(rows.length).toBe(3);

    const first = expectPresent(rows[0], "first audit row");
    // Runtime values come back as plain strings from ClickHouse
    expect(first.action).toBe("login");
    expect(first.actor_role).toBe("admin");
    expect(first.note).toBe("first login of the day");

    // Static type check: the inferred select model exposes the narrowed unions.
    type SelectRow = InferSelectModel<typeof auditLogTyped>;
    const _typedAction: SelectRow["action"] = first.action;
    const _typedRole: SelectRow["actor_role"] = first.actor_role;
    void _typedAction;
    void _typedRole;
  });

  it("makes DEFAULT-backed columns optional in the insert model", async function testDefaultColumnOptional() {
    const db = createE2EDb();

    await db.insert(auditLogTyped).values({
      id: 100,
      actor_id: userId(2001),
      action: "logout",
      actor_role: "user",
      created_at: new Date("2026-05-10T12:00:00.000Z"),
      // note is omitted — DEFAULT clause should kick in on ClickHouse side
    });

    const [row] = await db.select().from(auditLogTyped).where(ck.eq(auditLogTyped.id, 100)).execute();
    const present = expectPresent(row, "id=100 row");

    expect(present.action).toBe("logout");
    expect(present.note).toBe("");
    expect(present.actor_role).toBe("user");

    // Type-level: note is optional in the insert model (no `note` here compiles)
    type InsertRow = InferInsertModel<typeof auditLogTyped>;
    const _legalInsert: InsertRow = {
      id: 101,
      actor_id: userId(2002),
      action: "login",
      actor_role: "guest",
      created_at: new Date(),
    };
    void _legalInsert;
  });

  it("rejects enum values outside the inferred union at compile time", function testEnumLiteralRejection() {
    type InsertRow = InferInsertModel<typeof auditLogTyped>;

    // @ts-expect-error "delete" is not one of the inferred enum keys
    const _bad: InsertRow = {
      id: 200,
      actor_id: userId(3001),
      action: "delete",
      actor_role: "admin",
      created_at: new Date(),
    };
    void _bad;

    // @ts-expect-error "owner" is not one of the declared UserRole literals
    const _badRole: InsertRow = {
      id: 201,
      actor_id: userId(3002),
      action: "login",
      actor_role: "owner",
      created_at: new Date(),
    };
    void _badRole;

    // @ts-expect-error actor_id requires a UserId brand, plain number is rejected
    const _plainNumber: InsertRow = {
      id: 202,
      actor_id: 3003,
      action: "login",
      actor_role: "user",
      created_at: new Date(),
    };
    void _plainNumber;

    // Sanity: assigning a proper UserRole compiles
    const ok: UserRole = "admin";
    void ok;
  });

  it("round-trips JSON columns with $type-narrowed shapes", async function testJsonShapeRoundtrip() {
    const db = createE2EDb();
    // Restrict to the seeded rows so this test stays stable when the table
    // accumulates additional rows from previous iterations.
    const rows = await db
      .select()
      .from(userProfileTyped)
      .where(ck.inArray(userProfileTyped.id, [1, 2]))
      .orderBy(userProfileTyped.id)
      .execute();

    expect(rows.length).toBe(2);

    const alice = expectPresent(rows[0], "alice profile");
    expect(alice.display_name).toBe("alice");
    expect(alice.preferences.theme).toBe("dark");
    expect(alice.preferences.locale).toBe("en-US");
    expect(alice.preferences.betaFeatures).toEqual(["search-v2", "inline-edit"]);

    const bob = expectPresent(rows[1], "bob profile");
    expect(bob.preferences.theme).toBe("light");
    expect(bob.preferences.betaFeatures).toEqual([]);

    // For JSON columns ck-orm recommends `insertJsonEachRow`, which serializes
    // each row as a JSON document — `db.insert().values()` would render plain
    // objects in ClickHouse's Map literal form which is not valid JSON.
    // The `satisfies` clause still gives full $type narrowing on the row shape.
    const carolRow = {
      id: 100,
      display_name: "carol",
      preferences: {
        theme: "dark",
        locale: "ja-JP",
        betaFeatures: ["alpha-only"],
      },
      signup_at: new Date("2026-03-01T00:00:00.000Z"),
    } satisfies InferInsertModel<typeof userProfileTyped>;
    await db.insertJsonEachRow(userProfileTyped, [carolRow]);

    const [carol] = await db.select().from(userProfileTyped).where(ck.eq(userProfileTyped.id, 100)).execute();
    const carolPresent = expectPresent(carol, "carol profile");
    expect(carolPresent.preferences.theme).toBe("dark");
    expect(carolPresent.preferences.locale).toBe("ja-JP");
    expect(carolPresent.preferences.betaFeatures).toEqual(["alpha-only"]);

    // @ts-expect-error preferences must match the declared shape — wrong theme literal
    const _badRow: InferInsertModel<typeof userProfileTyped> = {
      id: 999,
      display_name: "rejected",
      preferences: { theme: "neon", locale: "en", betaFeatures: [] },
      signup_at: new Date(),
    };
    void _badRow;
  });
});
