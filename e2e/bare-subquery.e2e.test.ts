import { expect, it } from "bun:test";
import { ck, ckTable, ckType, fn } from "./ck-orm";
import { createE2EDb, createTempTableName, pets, users, webEvents } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

// E2E verification for PR-A/B/C: bare SelectBuilder as subquery source.
// Each pattern runs an actual SQL query against a seeded ClickHouse instance
// and asserts on real rows — catches any regressions in auto-alias rendering,
// column ref compile, or callback wiring that unit tests cannot detect.
//
// Seeded baselines (see e2e/seed.ts + datasetCounts):
//   users:     5 000 rows, id = 1..5000
//   pets:      8 000 rows, owner_id = (number % 4000) + 1  → owners 1..4000 each have 2 pets
//   webEvents: 100 000 rows, user_id = (number % 5000) + 1 → each user has 20 events

describeE2E("ck-orm e2e bare SelectBuilder subqueries", function describeBareSubquery() {
  it("Pattern A: variable-bound bare builder exposes column refs in the outer query", async function testBareVariableBind() {
    const db = createE2EDb();

    const petCounts = db
      .select({
        owner_id: pets.owner_id,
        pet_count: fn.count(pets.id).as("pet_count"),
      })
      .from(pets)
      .groupBy(pets.owner_id);

    const top = await db
      .select({
        ownerId: petCounts.owner_id,
        petCount: petCounts.pet_count,
      })
      .from(petCounts)
      .orderBy(ck.desc(petCounts.pet_count), petCounts.owner_id)
      .limit(3);

    // Every owner_id 1..4000 has exactly 2 pets (8000 % 4000 == 0). Tie-broken
    // by ASC owner_id, so the top 3 are owners 1, 2, 3 each with 2 pets.
    expect(top).toEqual([
      { ownerId: 1, petCount: 2 },
      { ownerId: 2, petCount: 2 },
      { ownerId: 3, petCount: 2 },
    ]);
  });

  it("Pattern B: callback-style innerJoin with inline bare builder", async function testBareCallbackInnerJoin() {
    const db = createE2EDb();

    const rows = await db
      .select({
        userId: users.id,
        userName: users.name,
      })
      .from(users)
      .innerJoin(
        db
          .select({
            pet_owner: pets.owner_id,
            pet_count: fn.count(pets.id).as("pet_count"),
          })
          .from(pets)
          .groupBy(pets.owner_id),
        (joined) => ck.eq(users.id, joined.pet_owner),
      )
      .orderBy(users.id)
      .limit(3);

    // First 3 users that have pets — owner_id 1..4000 are all covered, so it's
    // simply users 1/2/3 in id order. Names from the seed are lowercase.
    expect(rows).toEqual([
      { userId: 1, userName: "alice" },
      { userId: 2, userName: "bob" },
      { userId: 3, userName: "charlie" },
    ]);
  });

  it("Pattern C: bare builder inside inArray() filters the outer query", async function testBareInArray() {
    const db = createE2EDb();

    const [eventCount] = await db
      .select({ total: fn.count(webEvents.event_id) })
      .from(webEvents)
      .where(
        ck.inArray(
          webEvents.user_id,
          // Bare subquery: pick users 1..5 (no .as() needed — inArray reads
          // the subquery as a value-set, outer never names its alias).
          db.select({ id: users.id }).from(users).where(ck.lte(users.id, 5)),
        ),
      );

    // Each user has 20 events (100 000 / 5 000), so users 1..5 → 100 events.
    expect(expectPresent(eventCount, "eventCount").total).toBe(100);
  });

  it("Pattern D: named .as() subquery + callback-style join keeps the literal alias", async function testNamedAsWithCallback() {
    const db = createE2EDb();

    const namedPetCounts = db
      .select({
        owner: pets.owner_id,
        pc: fn.count(pets.id).as("pc"),
      })
      .from(pets)
      .groupBy(pets.owner_id)
      .as("pet_counts");

    const rows = await db
      .select({
        userId: users.id,
        userName: users.name,
        petCount: namedPetCounts.pc,
      })
      .from(users)
      .innerJoin(namedPetCounts, (joined) => ck.eq(users.id, joined.owner))
      .orderBy(users.id)
      .limit(3);

    expect(rows).toEqual([
      { userId: 1, userName: "alice", petCount: 2 },
      { userId: 2, userName: "bob", petCount: 2 },
      { userId: 3, userName: "charlie", petCount: 2 },
    ]);
  });

  it("Pattern E: leftJoin of a bare builder or anonymous CTE decodes unmatched rows as null", async function testBareLeftJoinNulls() {
    const db = createE2EDb();
    const petCountQuery = () =>
      db
        .select({ ownerId: pets.owner_id, petCount: fn.count(pets.id).as("pet_count") })
        .from(pets)
        .groupBy(pets.owner_id);
    // Owners 1..4000 have 2 pets each; users 4001..5000 have none.
    const expected = [
      { userId: 3999, petCount: 2 },
      { userId: 4000, petCount: 2 },
      { userId: 4001, petCount: null },
      { userId: 4002, petCount: null },
    ];

    const bare = petCountQuery();
    expect(
      await db
        .select({ userId: users.id, petCount: bare.petCount })
        .from(users)
        .leftJoin(bare, ck.eq(users.id, bare.ownerId))
        .where(ck.between(users.id, 3999, 4002))
        .orderBy(users.id),
    ).toEqual(expected);

    const cte = db.$with().as(petCountQuery());
    expect(
      await db
        .with(cte)
        .select({ userId: users.id, petCount: cte.petCount })
        .from(users)
        .leftJoin(cte, ck.eq(users.id, cte.ownerId))
        .where(ck.between(users.id, 3999, 4002))
        .orderBy(users.id),
    ).toEqual(expected);
  });

  it("decodes an unmatched left join as null when the bare-builder ref is aliased with .as()", async function testBareLeftJoinAliasedRef() {
    const db = createE2EDb();
    const bare = db
      .select({ ownerId: pets.owner_id, petCount: fn.count(pets.id).as("pet_count") })
      .from(pets)
      .groupBy(pets.owner_id);

    expect(
      await db
        .select({ userId: users.id, petCount: bare.petCount.as("pets") })
        .from(users)
        .leftJoin(bare, ck.eq(users.id, bare.ownerId))
        .where(ck.between(users.id, 4000, 4001))
        .orderBy(users.id),
    ).toEqual([
      { userId: 4000, petCount: 2 },
      { userId: 4001, petCount: null },
    ]);
  });

  it("decodes NULL from computed expressions over an unmatched left join and a nullable column", async function testComputedExpressionNull() {
    const db = createE2EDb();
    const bareTotals = db.select({ userId: users.id, spent: users.id }).from(users).where(ck.eq(users.id, 1));
    const namedTotals = db
      .select({ userId: users.id, cnt: users.id })
      .from(users)
      .where(ck.eq(users.id, 1))
      .as("totals");

    const rows = await db
      .select({
        id: users.id,
        plusSpent: fn.plus<number | null>(bareTotals.spent, 1),
        cntText: fn.toString(namedTotals.cnt),
      })
      .from(users)
      .leftJoin(bareTotals, ck.eq(users.id, bareTotals.userId))
      .leftJoin(namedTotals, ck.eq(users.id, namedTotals.userId))
      .where(ck.inArray(users.id, [1, 5000]))
      .orderBy(users.id);

    const decoded: ReadonlyArray<{ id: number; plusSpent: number | null; cntText: string | null }> = rows;
    expect(decoded).toEqual([
      { id: 1, plusSpent: 2, cntText: "1" },
      { id: 5000, plusSpent: null, cntText: null },
    ]);

    const metrics = ckTable(createTempTableName("tmp_nullable_plus"), {
      id: ckType.int32(),
      nullableInt: ckType.nullable(ckType.int32()),
    });
    await db.runInSession(async (session) => {
      await session.createTemporaryTable(metrics);
      await session.insert(metrics).values([
        { id: 1, nullableInt: 2 },
        { id: 2, nullableInt: null },
      ]);
      expect(
        await session
          .select({ id: metrics.id, v: fn.plus<number | null>(metrics.nullableInt, 1) })
          .from(metrics)
          .orderBy(metrics.id),
      ).toEqual([
        { id: 1, v: 3 },
        { id: 2, v: null },
      ]);
    });
  });
});
