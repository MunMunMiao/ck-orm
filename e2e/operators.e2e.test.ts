import { expect, it } from "bun:test";
import { ck, ckSql, ckTable, ckType } from "./ck-orm";
import { createE2EDb, createTempTableName, pets, users, webEvents } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

describeE2E("ck-orm e2e operators", function describeOperators() {
  it("supports comparison operators and boolean combinators", async function testComparisonsAndBooleanOperators() {
    const db = createE2EDb();

    const [eqRow] = await db.select({ total: users.id }).from(users).where(ck.eq(users.id, 1)).limit(1);
    const [neRow] = await db
      .select({ total: users.id })
      .from(users)
      .where(ck.ne(users.id, 1))
      .orderBy(users.id)
      .limit(1);
    const [gtRow] = await db
      .select({ total: users.id })
      .from(users)
      .where(ck.gt(users.id, 3))
      .orderBy(users.id)
      .limit(1);
    const [gteRow] = await db
      .select({ total: users.id })
      .from(users)
      .where(ck.gte(users.id, 3))
      .orderBy(users.id)
      .limit(1);
    const [ltRow] = await db
      .select({ total: users.id })
      .from(users)
      .where(ck.lt(users.id, 3))
      .orderBy(ck.desc(users.id))
      .limit(1);
    const [lteRow] = await db
      .select({ total: users.id })
      .from(users)
      .where(ck.lte(users.id, 3))
      .orderBy(ck.desc(users.id))
      .limit(1);

    expect(expectPresent(eqRow, "eqRow").total).toBe(1);
    expect(expectPresent(neRow, "neRow").total).toBe(2);
    expect(expectPresent(gtRow, "gtRow").total).toBe(4);
    expect(expectPresent(gteRow, "gteRow").total).toBe(3);
    expect(expectPresent(ltRow, "ltRow").total).toBe(2);
    expect(expectPresent(lteRow, "lteRow").total).toBe(3);

    const booleanRows = await db
      .select({ id: users.id })
      .from(users)
      .where(
        ck.and(
          ck.between(users.id, 1, 6),
          ck.or(ck.eq(users.tier, "vip"), ck.eq(users.tier, "trial")),
          ck.not(ck.eq(users.name, "user_4")),
        ),
      )
      .orderBy(users.id);

    expect(booleanRows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 5 }, { id: 6 }]);
  });

  it("supports inArray, notInArray, exists and notExists with subqueries", async function testSetOperators() {
    const db = createE2EDb();

    const petOwners = db
      .select({
        ownerId: pets.owner_id,
      })
      .from(pets)
      .where(ck.lte(pets.owner_id, 3))
      .as("pet_owners");

    const inRows = await db
      .select({ id: users.id })
      .from(users)
      .where(ck.inArray(users.id, petOwners))
      .orderBy(users.id);

    const notInRows = await db
      .select({ id: users.id })
      .from(users)
      .where(ck.notInArray(users.id, [1, 2, 3]))
      .orderBy(users.id)
      .limit(3);

    const existsRows = await db
      .select({ id: users.id })
      .from(users)
      .where(
        ck.and(
          ck.lte(users.id, 3),
          ck.exists(db.select({ ownerId: pets.owner_id }).from(pets).where(ck.eq(pets.owner_id, 1)).limit(1)),
        ),
      )
      .orderBy(users.id);

    const notExistsRows = await db
      .select({ id: users.id })
      .from(users)
      .where(
        ck.and(
          ck.gt(users.id, 4000),
          ck.notExists(db.select({ ownerId: pets.owner_id }).from(pets).where(ck.eq(pets.owner_id, -1)).limit(1)),
        ),
      )
      .orderBy(users.id)
      .limit(3);

    expect(inRows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    expect(notInRows).toEqual([{ id: 4 }, { id: 5 }, { id: 6 }]);
    expect(existsRows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    expect(notExistsRows).toEqual([{ id: 4001 }, { id: 4002 }, { id: 4003 }]);
  });

  it("uses explicit NULL predicates and rejects nullish predicate values before requests", async function testNullPredicates() {
    const db = createE2EDb();

    const rows = await db
      .select({ id: users.id })
      .from(users)
      .where(ck.and(ck.isNull(ckSql`NULL`), ck.isNotNull(users.name)))
      .orderBy(users.id)
      .limit(1);

    expect(rows).toEqual([{ id: 1 }]);
    expect(() => ck.eq(users.tier, null as never)).toThrow("does not accept bare null");
    expect(() => ck.inArray(users.tier, ["vip", null] as never)).toThrow("does not accept bare null");
    expect(() => ck.like(users.tier, undefined as never)).toThrow("does not accept bare undefined");
  });

  it("supports has, hasAll, hasAny and hasSubstr against array columns", async function testHasOperators() {
    const db = createE2EDb();

    const hasRows = await db
      .select({ id: webEvents.event_id })
      .from(webEvents)
      .where(ck.has(webEvents.tags, "tag_0"))
      .orderBy(webEvents.event_id)
      .limit(3);

    const hasAllRows = await db
      .select({ id: webEvents.event_id })
      .from(webEvents)
      .where(ck.hasAll(webEvents.tags, ["tag_0", "segment_0"]))
      .orderBy(webEvents.event_id)
      .limit(3);

    const hasAnyRows = await db
      .select({ id: webEvents.event_id })
      .from(webEvents)
      .where(ck.hasAny(webEvents.tags, ["tag_1", "segment_0"]))
      .orderBy(webEvents.event_id)
      .limit(5);

    const hasSubstrRows = await db
      .select({ id: webEvents.event_id })
      .from(webEvents)
      .where(ck.hasSubstr(webEvents.tags, ["tag_0", "segment_0"]))
      .orderBy(webEvents.event_id)
      .limit(3);

    expect(hasRows).toEqual([{ id: "1" }, { id: "11" }, { id: "21" }]);
    expect(hasAllRows).toEqual([{ id: "1" }, { id: "11" }, { id: "21" }]);
    expect(hasAnyRows).toEqual([{ id: "1" }, { id: "2" }, { id: "6" }, { id: "11" }, { id: "12" }]);
    expect(hasSubstrRows).toEqual([{ id: "1" }, { id: "11" }, { id: "21" }]);
  });

  it("uses array column encoders for date arrays and keeps boolean predicates explicit", async function testArrayDateAndBooleanPredicates() {
    const db = createE2EDb();
    const tempTable = createTempTableName("array_predicate_scope");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      active: ckType.bool(),
      business_days: ckType.array(ckType.date({ encode: "utc" })),
      local_days: ckType.array(ckType.date32({ encode: (value) => value.toISOString().slice(0, 10) })),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);
      await session.insertJsonEachRow(scope, [
        {
          id: 1,
          active: false,
          business_days: ["2026-06-15"],
          local_days: ["2026-06-16"],
        },
        {
          id: 2,
          active: true,
          business_days: ["2026-06-20"],
          local_days: ["2026-06-21"],
        },
      ]);

      const dateRows = await session
        .select({ id: scope.id })
        .from(scope)
        .where(
          ck.has(scope.business_days, new Date("2026-06-15T23:00:00.000Z")),
          ck.hasAny(scope.local_days, [new Date("2026-06-16T01:00:00.000Z")]),
          ck.hasAll(scope.business_days, [new Date("2026-06-15T00:00:00.000Z")]),
        )
        .orderBy(scope.id);

      const falseRows = await session
        .select({ id: scope.id })
        .from(scope)
        .where(ck.eq(scope.active, false))
        .orderBy(scope.id);

      const trueRows = await session.select({ id: scope.id }).from(scope).where(scope.active).orderBy(scope.id);

      expect(dateRows).toEqual([{ id: 1 }]);
      expect(falseRows).toEqual([{ id: 1 }]);
      expect(trueRows).toEqual([{ id: 2 }]);
    });
  });

  it("supports asc, desc and expr in ordered builder queries", async function testOrderByAndExpr() {
    const db = createE2EDb();

    const rows = await db
      .select({
        id: webEvents.event_id,
        country: webEvents.country,
      })
      .from(webEvents)
      .where(ck.expr(ckSql`(${webEvents.event_id} % 2) = ${0}`))
      .orderBy(ck.asc(webEvents.country), ck.desc(webEvents.event_id))
      .limit(4);

    expect(rows).toEqual([
      { id: "100000", country: "GB" },
      { id: "99996", country: "GB" },
      { id: "99992", country: "GB" },
      { id: "99988", country: "GB" },
    ]);
  });
});
