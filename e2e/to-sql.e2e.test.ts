import { expect, it } from "bun:test";
import { type BuiltQuery, type CompiledQuery, ck, ckSql, ckTable, ckType, fn } from "./ck-orm";
import { createE2EDb, createTempTableName, pets, users, webEvents } from "./shared";
import { describeE2E } from "./test-helpers";

type Db = ReturnType<typeof createE2EDb>;

// Sends what toSQL() returned, as a request of its own. Only string, number and array parameters are used here,
// because toSQL() has the raw values and not the ClickHouse type they are formatted with.
const send = (db: Db, built: BuiltQuery, mode: "query" | "command" = "query") =>
  db.executeCompiled({
    kind: "compiled-query",
    mode,
    statement: built.sql,
    params: built.params,
    selection: [],
    forcedSettings: built.forcedSettings,
    ...(built.body === undefined ? {} : { body: built.body }),
  } satisfies CompiledQuery<Record<string, unknown>>);

describeE2E("ck-orm e2e toSQL", function describeToSQL() {
  it("sends the same rows for the plain and the pretty statement of a select", async function testPrettySelect() {
    const db = createE2EDb();
    const monthly = db.$with("monthly").as(
      db
        .select({ userId: webEvents.user_id, total: fn.sum(webEvents.revenue).as("total") })
        .from(webEvents)
        .where(ck.lt(webEvents.user_id, 6))
        .groupBy(webEvents.user_id),
    );
    const queries = {
      "a CTE joined to a table": db
        .with(monthly)
        .select({ userId: monthly.userId, name: users.name })
        .from(monthly)
        .innerJoin(users, ck.eq(users.id, monthly.userId))
        .orderBy(monthly.userId),
      "and, or, not, between, in and like": db
        .select({ id: users.id, name: users.name })
        .from(users)
        .where(
          ck.and(
            ck.or(ck.eq(users.tier, "gold"), ck.eq(users.tier, "silver"), ck.like(users.name, "%a%")),
            ck.between(users.id, 1, 8),
            ck.inArray(users.id, [1, 2, 3, 4, 5, 6]),
            ck.not(ck.eq(users.name, "nobody")),
          ),
        )
        .orderBy(users.id),
      "a left join, which forces a setting": db
        .select()
        .from(users)
        .leftJoin(pets, ck.eq(users.id, pets.owner_id))
        .orderBy(users.id, pets.id),
      "a subquery in a condition": db
        .select({ id: users.id })
        .from(users)
        .where(ck.inArray(users.id, db.select({ id: pets.owner_id }).from(pets)))
        .orderBy(users.id),
    };

    for (const [label, query] of Object.entries(queries)) {
      const expected = await query;
      const plain = await send(db, query.toSQL());
      const pretty = await send(db, query.toSQL({ pretty: true }));

      expect(plain.length, label).toBeGreaterThan(0);
      expect(plain, label).toHaveLength(expected.length);
      expect(pretty, label).toEqual(plain);
    }
  });

  it("inserts the same rows from a pretty insert().values() and insert().fromSelect()", async function testPrettyInserts() {
    const db = createE2EDb();
    const sourceName = createTempTableName("tosql_source");
    const targetName = createTempTableName("tosql_target");
    const source = ckTable(sourceName, { id: ckType.uint32(), name: ckType.string() });
    const target = ckTable(targetName, { id: ckType.uint32(), name: ckType.string() });
    const rows = [
      { id: 1, name: "a, b" },
      { id: 2, name: "it's (x), (y)" },
      { id: 3, name: "line\nbreak" },
    ];

    try {
      for (const name of [sourceName, targetName]) {
        await db.command(ckSql`create table ${ckSql.identifier(name)} (id UInt32, name String) engine = Memory`);
      }

      const values = db.insert(source).values(rows).toSQL({ pretty: true });
      expect(values.body).toContain("),\n(");
      await send(db, values, "command");
      expect(await db.select().from(source).orderBy(source.id)).toEqual(rows);

      const copy = db
        .insert(target)
        .fromSelect(db.select({ id: source.id, name: source.name }).from(source).where(ck.gte(source.id, 2)))
        .toSQL({ pretty: true });
      expect(copy.sql).toContain("\nselect\n");
      await send(db, copy, "command");
      expect(await db.select().from(target).orderBy(target.id)).toEqual(rows.slice(1));
    } finally {
      for (const name of [sourceName, targetName]) {
        await db.command(ckSql`drop table if exists ${ckSql.identifier(name)}`);
      }
    }
  });
});
