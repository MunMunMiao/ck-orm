import { expect, it } from "bun:test";
import { compileQuerySymbol } from "../src/query";
import { createSessionId, decodeRow, desc, fn, gt, sql } from "./ck-orm";
import { aliasedUsers, createE2EDb, users } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

describeE2E("ck-orm e2e query basics", function describeQueryBasics() {
  it("supports system endpoint helpers", async function testSystemEndpointHelpers() {
    const db = createE2EDb();

    await db.ping();
    await db.replicasStatus();
  });

  it("supports raw string SQL, pure-string sql fragments, sql.raw, sql.join and sql.identifier", async function testRawSqlFactories() {
    const db = createE2EDb();

    expect(await db.execute("SELECT 1 AS one")).toEqual([{ one: 1 }]);
    expect(await db.execute(sql("SELECT 2 AS two"))).toEqual([{ two: 2 }]);
    expect(
      await db.execute(
        sql`select ${sql.join([sql.raw("1 as one"), sql.raw("2 as two")], sql.raw(", "))}, ${sql.raw("3 as three")}`,
      ),
    ).toEqual([{ one: 1, two: 2, three: 3 }]);
    expect(
      await db.execute(
        sql`select count() as total from ${sql.identifier("users")} where ${sql.identifier({ column: "id" })} <= ${3}`,
      ),
    ).toEqual([{ total: 3 }]);
  });

  it("supports interpolated SQL with tables, columns, sources, values and fn expressions", async function testInterpolatedSql() {
    const db = createE2EDb();

    const rows = await db.execute(sql`
      select
        ${aliasedUsers.id} as id,
        ${fn.call<string>("upper", aliasedUsers.name).mapWith((value) => String(value))} as upper_name
      from ${aliasedUsers}
      where ${aliasedUsers.id} > ${0}
      order by ${aliasedUsers.id}
      limit ${3}
    `);

    expect(rows).toEqual([
      { id: 1, upper_name: "ALICE" },
      { id: 2, upper_name: "BOB" },
      { id: 3, upper_name: "CHARLIE" },
    ]);
  });

  it("supports base builder flow, explicit execute() and createSessionId()", async function testBaseBuilderFlow() {
    const db = createE2EDb();
    const sessionId = createSessionId();

    expect(sessionId).toMatch(/^ck_orm_[0-9a-f_]+$/);

    const rows = await db
      .select({
        id: users.id,
        upperName: sql<string>`upper(${users.name})`.mapWith((value) => String(value)).as("upper_name"),
      })
      .from(users)
      .where(gt(users.id, 0))
      .orderBy(users.id, desc(users.name))
      .limit(3)
      .offset(1)
      .execute({
        session_id: sessionId,
      });

    expect(rows).toEqual([
      { id: 2, upperName: "BOB" },
      { id: 3, upperName: "CHARLIE" },
      { id: 4, upperName: "USER_4" },
    ]);
  });

  it("decodes compiled selection metadata through the public decodeRow helper", async function testDecodeRow() {
    const db = createE2EDb();
    const builder = db
      .select({
        id: users.id,
        upperName: sql<string>`upper(${users.name})`.mapWith((value) => String(value)).as("upper_name"),
      })
      .from(users)
      .orderBy(users.id)
      .limit(1);

    const compiled = builder[compileQuerySymbol]();
    const [idSelection, upperNameSelection] = compiled.selection;
    const [rawRow] = await db.execute(sql`
      select
        ${users.id} as ${sql.identifier(idSelection.sqlAlias)},
        upper(${users.name}) as ${sql.identifier(upperNameSelection.sqlAlias)}
      from ${users}
      order by ${users.id}
      limit 1
    `);

    expect(decodeRow<{ id: number; upperName: string }>(expectPresent(rawRow, "rawRow"), compiled.selection)).toEqual({
      id: 1,
      upperName: "ALICE",
    });
  });
});
