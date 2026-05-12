import { expect, it } from "bun:test";
import { ck, ckSql, ckType, fn, isClickHouseORMError } from "./ck-orm";
import { createE2EDb, createTempTableName, datasetCounts, pets, users, webEvents } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

// E2E verification for the anonymous-CTE feature (`db.$with()` with no name).
// Each pattern runs a real query against a seeded ClickHouse instance and
// asserts on returned rows — catches regressions in the per-compile
// `__cte_N` alias generation, lazy column ref resolution, and renderCtes /
// renderSource branch wiring that unit tests on the compiled SQL string
// cannot detect by themselves.
//
// Seeded baselines (see e2e/seed.ts + datasetCounts):
//   users:     5 000 rows, id = 1..5000
//   pets:      8 000 rows, owner_id = (number % 4000) + 1 → owners 1..4000 each have 2 pets
//   webEvents: 100 000 rows, user_id = (number % 5000) + 1 → each user has 20 events

describeE2E("ck-orm e2e anonymous CTE via $with()", function describeAnonymousCte() {
  it("basic: single anonymous CTE feeds the outer SELECT and returns expected rows", async function testAnonBasic() {
    const db = createE2EDb();

    const ranked = db.$with().as(
      db
        .select({
          owner_id: pets.owner_id,
          pet_count: fn.count(pets.id).as("pet_count"),
        })
        .from(pets)
        .groupBy(pets.owner_id),
    );

    const top = await db
      .with(ranked)
      .select({
        ownerId: ranked.owner_id,
        petCount: ranked.pet_count,
      })
      .from(ranked)
      .orderBy(ck.desc(ranked.pet_count), ranked.owner_id)
      .limit(3);

    // Every owner_id 1..4000 has exactly 2 pets (8000 % 4000 == 0). Tie-broken
    // by ASC owner_id, so the top 3 are owners 1, 2, 3 each with 2 pets.
    expect(top).toEqual([
      { ownerId: 1, petCount: 2 },
      { ownerId: 2, petCount: 2 },
      { ownerId: 3, petCount: 2 },
    ]);
  });

  it("CTE → join: anonymous CTE joined against a real table delivers correct combined rows", async function testAnonJoin() {
    const db = createE2EDb();

    const petCounts = db.$with().as(
      db
        .select({
          owner_id: pets.owner_id,
          pet_count: fn.count(pets.id).as("pet_count"),
        })
        .from(pets)
        .groupBy(pets.owner_id),
    );

    const rows = await db
      .with(petCounts)
      .select({
        userId: users.id,
        userName: users.name,
        petCount: petCounts.pet_count,
      })
      .from(users)
      .innerJoin(petCounts, ck.eq(users.id, petCounts.owner_id))
      .orderBy(users.id)
      .limit(3);

    expect(rows).toEqual([
      { userId: 1, userName: "alice", petCount: 2 },
      { userId: 2, userName: "bob", petCount: 2 },
      { userId: 3, userName: "charlie", petCount: 2 },
    ]);
  });

  it("multiple anonymous CTEs chained: each gets its own __cte_N and inner refs resolve correctly", async function testAnonChainedCtes() {
    const db = createE2EDb();

    const userSlice = db
      .$with()
      .as(db.select({ id: users.id, name: users.name }).from(users).where(ck.lte(users.id, 5)));

    const eventCounts = db.$with().as(
      db
        .select({
          user_id: webEvents.user_id,
          events: fn.count(webEvents.event_id).as("events"),
        })
        .from(webEvents)
        .groupBy(webEvents.user_id),
    );

    const rows = await db
      .with(userSlice, eventCounts)
      .select({
        id: userSlice.id,
        userName: userSlice.columns.name,
        events: eventCounts.events,
      })
      .from(userSlice)
      .innerJoin(eventCounts, ck.eq(userSlice.id, eventCounts.user_id))
      .orderBy(userSlice.id);

    // Each user has exactly 20 events (100_000 / 5_000).
    expect(rows).toHaveLength(5);
    for (const row of rows) {
      expect(row.events).toBe(20);
    }
    expect(rows.map((r) => r.id)).toEqual([1, 2, 3, 4, 5]);
  });

  it("anonymous CTE used in WHERE-side semi-join via inArray returns the right slice", async function testAnonInArray() {
    const db = createE2EDb();

    // Anonymous CTE shaped as a value-set for inArray. The outer query
    // doesn't expose the CTE alias in its SELECT, but the WITH clause is
    // still produced because the value source is the CTE.
    const lowIds = db.$with().as(db.select({ id: users.id }).from(users).where(ck.lte(users.id, 5)));

    const [eventCount] = await db
      .with(lowIds)
      .select({ total: fn.count(webEvents.event_id) })
      .from(webEvents)
      .where(ck.inArray(webEvents.user_id, db.select({ id: lowIds.id }).from(lowIds)));

    // Users 1..5 → 100 events total (each user has 20 events).
    expect(expectPresent(eventCount, "eventCount").total).toBe(100);
  });

  it("mixed named + anonymous CTEs in one query: both render and outer query returns expected data", async function testAnonMixedNamed() {
    const db = createE2EDb();

    const sliceNamed = db
      .$with("user_slice")
      .as(db.select({ id: users.id, name: users.name }).from(users).where(ck.eq(users.id, 1)));
    const eventCounts = db.$with().as(
      db
        .select({
          user_id: webEvents.user_id,
          events: fn.count(webEvents.event_id).as("events"),
        })
        .from(webEvents)
        .where(ck.eq(webEvents.user_id, 1))
        .groupBy(webEvents.user_id),
    );

    const rows = await db
      .with(sliceNamed, eventCounts)
      .select({
        id: sliceNamed.id,
        userName: sliceNamed.columns.name,
        events: eventCounts.events,
      })
      .from(sliceNamed)
      .innerJoin(eventCounts, ck.eq(sliceNamed.id, eventCounts.user_id));

    expect(rows).toEqual([{ id: 1, userName: "alice", events: 20 }]);
  });

  it("nested anonymous CTE: cte2's body references cte1 (which is also anonymous) — alias propagates", async function testAnonNested() {
    const db = createE2EDb();

    const cte1 = db.$with().as(db.select({ id: users.id, tier: users.tier }).from(users).where(ck.lte(users.id, 50)));
    // cte2's inner select reads from cte1. Both are anonymous; the compile
    // must assign cte1 → __cte_1 and cte2 → __cte_2 and the inner FROM
    // inside cte2's definition must use __cte_1.
    const cte2 = db.$with().as(db.select({ id: cte1.id, tier: cte1.tier }).from(cte1).where(ck.lte(cte1.id, 5)));

    const rows = await db.with(cte1, cte2).select({ id: cte2.id, tier: cte2.tier }).from(cte2).orderBy(cte2.id);

    expect(rows).toHaveLength(5);
    expect(rows.map((r) => r.id)).toEqual([1, 2, 3, 4, 5]);
  });

  it("count(anonymousCte) with a predicate returns the correct number", async function testAnonCount() {
    const db = createE2EDb();

    const lowIds = db.$with().as(db.select({ id: users.id }).from(users));
    const total = await db.with(lowIds).count(lowIds, ck.lte(lowIds.id, 100)).execute();

    expect(total).toBe(100);
  });

  it("anonymous CTE + LEFT JOIN: unmatched outer rows survive with NULLs (coalesced to 0)", async function testAnonLeftJoin() {
    const db = createE2EDb();

    // Outer scope: users {1, 4999}. eventCounts CTE filters webEvents to
    // only user_id=1 — so user 4999 has no matching row. The leftJoin must
    // keep both ids; user 4999's events column will be NULL on the wire,
    // which we coalesce to 0 for a stable assertion.
    const userScope = db.$with().as(
      db
        .select({ id: users.id })
        .from(users)
        .where(ck.inArray(users.id, [1, 4999])),
    );
    const eventCounts = db.$with().as(
      db
        .select({
          user_id: webEvents.user_id,
          events: fn.count(webEvents.event_id).as("events"),
        })
        .from(webEvents)
        .where(ck.eq(webEvents.user_id, 1))
        .groupBy(webEvents.user_id),
    );

    const rows = await db
      .with(userScope, eventCounts)
      .select({
        id: userScope.id,
        events: ck.expr<bigint | string>(ckSql`coalesce(${eventCounts.events}, 0)`).as("events"),
      })
      .from(userScope)
      .leftJoin(eventCounts, ck.eq(userScope.id, eventCounts.user_id))
      .orderBy(userScope.id);

    expect(rows).toHaveLength(2);
    expect(rows[0]?.id).toBe(1);
    expect(Number(rows[0]?.events as unknown as string)).toBe(20);
    expect(rows[1]?.id).toBe(4999);
    expect(Number(rows[1]?.events as unknown as string)).toBe(0);
  });

  it("anonymous CTE inside insert().fromSelect() materialises into a temp table", async function testAnonInsertFromSelect() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_anon_cte_materialise");
    const { ckTable } = await import("./ck-orm");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      tier: ckType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      const sliceCte = session.$with().as(
        session
          .select({ id: users.id, tier: users.tier })
          .from(users)
          .where(ck.inArray(users.id, [1, 2, 3])),
      );

      await session
        .insert(scope)
        .fromSelect(session.with(sliceCte).select({ id: sliceCte.id, tier: sliceCte.tier }).from(sliceCte));

      const rows = await session.select({ id: scope.id, tier: scope.tier }).from(scope).orderBy(scope.id);
      expect(rows.map((r) => r.id)).toEqual([1, 2, 3]);
    });
  });

  it("real-world: per-tier top-N analytics via anonymous CTE + window-style ranking", async function testAnonTierAnalytics() {
    const db = createE2EDb();

    // Production-shaped query: produce per-tier top-2 users by id within
    // the first 20 users. The anonymous CTE projects (id, tier) for the
    // slice; outer query uses limitBy(tier, 2). This mirrors the canonical
    // "top-N per group" reporting query.
    const tierSlice = db
      .$with()
      .as(db.select({ id: users.id, tier: users.tier }).from(users).where(ck.lte(users.id, 20)));

    const rows = await db
      .with(tierSlice)
      .select({ id: tierSlice.id, tier: tierSlice.tier })
      .from(tierSlice)
      .orderBy(tierSlice.tier, tierSlice.id)
      .limitBy([tierSlice.tier], 2);

    // Within ids 1..20, every tier present must contribute at most 2 rows.
    const tierGroups = new Map<string, number[]>();
    for (const row of rows) {
      const ids = tierGroups.get(row.tier) ?? [];
      ids.push(row.id);
      tierGroups.set(row.tier, ids);
    }
    for (const ids of tierGroups.values()) {
      expect(ids.length).toBeLessThanOrEqual(2);
    }
    expect(rows.length).toBeGreaterThan(0);
  });

  it("stable __cte_N counter: identical query compiled twice via execute() yields identical SQL", async function testAnonStableSql() {
    const startEvents: string[] = [];
    const db = createE2EDb({
      instrumentation: [
        {
          onQueryStart(event) {
            startEvents.push(event.statement);
          },
        },
      ],
    });

    const cte = db.$with().as(db.select({ id: users.id }).from(users).where(ck.lte(users.id, 3)));
    const query = () => db.with(cte).select({ id: cte.id }).from(cte).orderBy(cte.id);

    const first = await query();
    const second = await query();

    // Both runs hit the same source SQL — verifies the counter resets per
    // compile (each top-level execute() gets its own BuildContext).
    expect(first).toEqual(second);
    expect(startEvents.length).toBeGreaterThanOrEqual(2);
    expect(startEvents[0]).toBe(startEvents[1] ?? "");
    expect(startEvents[0]).toContain("`__cte_1`");
  });

  it("error path: server-side error inside an anonymous CTE bubbles up as a ClickHouseORMError", async function testAnonServerError() {
    const db = createE2EDb();

    // Build an anonymous CTE that triggers a server-side runtime error
    // (intDiv by literal zero). The outer query should fail with a
    // ClickHouseORMError, and the alias rendering should not be the cause
    // of the failure (i.e. the WITH clause itself must be syntactically
    // valid).
    const bad = db.$with().as(
      db
        .select({
          id: users.id,
          ratio: fn.intDiv(users.id, ck.expr<number>(ckSql`toInt32(0)`)).as("ratio"),
        })
        .from(users)
        .where(ck.eq(users.id, 1)),
    );

    let caught: unknown;
    try {
      await db.with(bad).select({ id: bad.id, ratio: bad.ratio }).from(bad);
    } catch (e) {
      caught = e;
    }

    expect(caught).toBeDefined();
    expect(isClickHouseORMError(caught)).toBe(true);
  });

  it("anonymous CTE columns flow through outer WHERE/GROUP BY/HAVING/ORDER BY end-to-end", async function testAnonAllClauses() {
    const db = createE2EDb();

    // Realistic shape: aggregate events per user → anonymous CTE → outer
    // query filters via WHERE on the CTE column, re-groups, applies HAVING,
    // and ORDER BYs by CTE column. Verifies the lazy alias resolver returns
    // a stable alias across all five clauses.
    const perUser = db.$with().as(
      db
        .select({
          user_id: webEvents.user_id,
          events: fn.count(webEvents.event_id).as("events"),
        })
        .from(webEvents)
        .groupBy(webEvents.user_id),
    );

    const rows = await db
      .with(perUser)
      .select({
        bucket: ck.expr<number>(ckSql`${perUser.user_id} % 3`).as("bucket"),
        ids: fn.count(perUser.user_id).as("ids"),
      })
      .from(perUser)
      .where(ck.lte(perUser.user_id, 30))
      .groupBy(ck.expr(ckSql`${perUser.user_id} % 3`) as never)
      .having(ck.gt(fn.count(perUser.user_id), 0))
      .orderBy(ck.expr(ckSql`bucket`) as never);

    // Buckets 0..2 across user_id 1..30 each must contribute non-zero rows.
    expect(rows.length).toBeGreaterThanOrEqual(2);
    let totalIds = 0;
    for (const row of rows) {
      totalIds += Number(row.ids as unknown as string);
    }
    expect(totalIds).toBe(30);
  });

  // Sanity: ensure the seeded dataset assumption is in place (matches the
  // datasetCounts comment block at the top of the file). Keeps this file
  // self-checking in case the seed contract shifts.
  it("dataset baseline: 5_000 users, 8_000 pets, 100_000 web_events", async function testDatasetBaseline() {
    const db = createE2EDb();
    const [u, p, w] = await Promise.all([db.count(users), db.count(pets), db.count(webEvents)]);
    expect(expectPresent(u, "users")).toBe(datasetCounts.users);
    expect(expectPresent(p, "pets")).toBe(datasetCounts.pets);
    expect(expectPresent(w, "webEvents")).toBe(datasetCounts.webEvents);
  });
});
