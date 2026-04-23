import { expect, it } from "bun:test";
import { ck, csql } from "./ck-orm";
import { createE2EDb, pets, users, webEvents } from "./shared";
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

  it("supports has, hasAll and hasAny against array columns", async function testHasOperators() {
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

    expect(hasRows).toEqual([{ id: "1" }, { id: "11" }, { id: "21" }]);
    expect(hasAllRows).toEqual([{ id: "1" }, { id: "11" }, { id: "21" }]);
    expect(hasAnyRows).toEqual([{ id: "1" }, { id: "2" }, { id: "6" }, { id: "11" }, { id: "12" }]);
  });

  it("supports asc, desc and expr in ordered builder queries", async function testOrderByAndExpr() {
    const db = createE2EDb();

    const rows = await db
      .select({
        id: webEvents.event_id,
        country: webEvents.country,
      })
      .from(webEvents)
      .where(ck.expr(csql`(${webEvents.event_id} % 2) = ${0}`))
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
