import { expect, it } from "bun:test";
import {
  and,
  asc,
  between,
  desc,
  eq,
  exists,
  expr,
  gt,
  gte,
  inArray,
  lt,
  lte,
  ne,
  not,
  notExists,
  notInArray,
  or,
  sql,
} from "./ck-orm";
import { createE2EDb, pets, users, webEvents } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

describeE2E("ck-orm e2e operators", function describeOperators() {
  it("supports comparison operators and boolean combinators", async function testComparisonsAndBooleanOperators() {
    const db = createE2EDb();

    const [eqRow] = await db.select({ total: users.id }).from(users).where(eq(users.id, 1)).limit(1);
    const [neRow] = await db.select({ total: users.id }).from(users).where(ne(users.id, 1)).orderBy(users.id).limit(1);
    const [gtRow] = await db.select({ total: users.id }).from(users).where(gt(users.id, 3)).orderBy(users.id).limit(1);
    const [gteRow] = await db
      .select({ total: users.id })
      .from(users)
      .where(gte(users.id, 3))
      .orderBy(users.id)
      .limit(1);
    const [ltRow] = await db
      .select({ total: users.id })
      .from(users)
      .where(lt(users.id, 3))
      .orderBy(desc(users.id))
      .limit(1);
    const [lteRow] = await db
      .select({ total: users.id })
      .from(users)
      .where(lte(users.id, 3))
      .orderBy(desc(users.id))
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
        and(between(users.id, 1, 6), or(eq(users.tier, "vip"), eq(users.tier, "trial")), not(eq(users.name, "user_4"))),
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
      .where(lte(pets.owner_id, 3))
      .as("pet_owners");

    const inRows = await db.select({ id: users.id }).from(users).where(inArray(users.id, petOwners)).orderBy(users.id);

    const notInRows = await db
      .select({ id: users.id })
      .from(users)
      .where(notInArray(users.id, [1, 2, 3]))
      .orderBy(users.id)
      .limit(3);

    const existsRows = await db
      .select({ id: users.id })
      .from(users)
      .where(
        and(
          lte(users.id, 3),
          exists(db.select({ ownerId: pets.owner_id }).from(pets).where(eq(pets.owner_id, 1)).limit(1)),
        ),
      )
      .orderBy(users.id);

    const notExistsRows = await db
      .select({ id: users.id })
      .from(users)
      .where(
        and(
          gt(users.id, 4000),
          notExists(db.select({ ownerId: pets.owner_id }).from(pets).where(eq(pets.owner_id, -1)).limit(1)),
        ),
      )
      .orderBy(users.id)
      .limit(3);

    expect(inRows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    expect(notInRows).toEqual([{ id: 4 }, { id: 5 }, { id: 6 }]);
    expect(existsRows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    expect(notExistsRows).toEqual([{ id: 4001 }, { id: 4002 }, { id: 4003 }]);
  });

  it("supports asc, desc and expr in ordered builder queries", async function testOrderByAndExpr() {
    const db = createE2EDb();

    const rows = await db
      .select({
        id: webEvents.event_id,
        country: webEvents.country,
      })
      .from(webEvents)
      .where(expr(sql`(${webEvents.event_id} % 2) = ${0}`))
      .orderBy(asc(webEvents.country), desc(webEvents.event_id))
      .limit(4);

    expect(rows).toEqual([
      { id: 100000n, country: "GB" },
      { id: 99996n, country: "GB" },
      { id: 99992n, country: "GB" },
      { id: 99988n, country: "GB" },
    ]);
  });
});
