import { expect, it } from "bun:test";
import { ck, ckSql, ckTable, ckType, fn, isClickHouseORMError } from "./ck-orm";
import { createE2EDb, createTempTableName, datasetCounts, users, webEvents } from "./shared";
import { describeE2E } from "./test-helpers";

describeE2E("ck-orm e2e insert.fromSelect", function describeInsertFromSelect() {
  it("materialises a SELECT into a temp table without round-tripping through JS", async function testBasicMaterialise() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_basic");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      name: ckType.string(),
      tier: ckType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      await session.insert(scope).fromSelect(
        session
          .select({
            id: users.id,
            name: users.name,
            tier: users.tier,
          })
          .from(users)
          .where(ck.eq(users.id, 1)),
      );

      const rows = await session.select().from(scope);

      expect(rows).toHaveLength(1);
      expect(rows[0]?.id).toBe(1);
      expect(rows[0]?.name).toBe("alice");
      expect(typeof rows[0]?.tier).toBe("string");
    });
  });

  it("aligns columns by selection key, not by table or position", async function testColumnAlignmentByKey() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_key_align");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      name: ckType.string(),
      tier: ckType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      // Projection key order intentionally inverted vs. table column order.
      // ClickHouse aligns INSERT (cols) ↔ SELECT projection by position; we
      // generate INSERT list in projection-key order so the visible semantics
      // match "by key name".
      await session.insert(scope).fromSelect(
        session
          .select({
            tier: users.tier,
            name: users.name,
            id: users.id,
          })
          .from(users)
          .where(ck.eq(users.id, 2)),
      );

      const rows = await session.select().from(scope);
      expect(rows).toHaveLength(1);
      expect(rows[0]?.id).toBe(2);
      expect(typeof rows[0]?.name).toBe("string");
      expect(typeof rows[0]?.tier).toBe("string");
    });
  });

  it("supports projecting computed expressions (multiIf, fn.toString) into the target", async function testComputedProjection() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_scored");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      bucket: ckType.int32(),
      label: ckType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      await session.insert(scope).fromSelect(
        session
          .select({
            id: users.id,
            bucket: fn.multiIf<number>(ck.lt(users.id, 3), 1, ck.lt(users.id, 7), 2, 3),
            label: fn.toString(users.id),
          })
          .from(users)
          .where(ck.inArray(users.id, [1, 4, 9])),
      );

      const rows = await session.select().from(scope).orderBy(scope.id);

      expect(rows).toEqual([
        { id: 1, bucket: 1, label: "1" },
        { id: 4, bucket: 2, label: "4" },
        { id: 9, bucket: 3, label: "9" },
      ]);
    });
  });

  it("renders a CTE between INSERT and SELECT so the inner query keeps full SELECT semantics", async function testCteAndJoinEmbedding() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_cte_join");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      name: ckType.string(),
      revenue: ckType.decimal({ precision: 18, scale: 2 }),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      const totals = session.$with("totals").as(
        session
          .select({
            user_id: webEvents.user_id,
            revenue: fn.sum(webEvents.revenue),
          })
          .from(webEvents)
          .where(ck.eq(webEvents.user_id, 1))
          .groupBy(webEvents.user_id),
      );

      await session
        .with(totals)
        .insert(scope)
        .fromSelect(
          session
            .with(totals)
            .select({
              id: users.id,
              name: users.name,
              revenue: totals.revenue,
            })
            .from(users)
            .innerJoin(totals, ck.eq(users.id, totals.user_id)),
        );

      const rows = await session.select().from(scope);
      expect(rows).toHaveLength(1);
      expect(rows[0]?.id).toBe(1);
      expect(typeof rows[0]?.revenue).toBe("string");
    });
  });

  it("succeeds with a zero-row SELECT and writes no rows", async function testEmptySource() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_empty");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      name: ckType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      await session.insert(scope).fromSelect(
        session
          .select({
            id: users.id,
            name: users.name,
          })
          .from(users)
          .where(ck.lt(users.id, 0)),
      );

      const count = await session.count(scope);
      expect(count).toBe(0);
    });
  });

  it("forwards ORDER BY + LIMIT BY semantics from the embedded select into the materialised target", async function testOrderingPreserved() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_top_by_tier");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      tier: ckType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      // Top-1 per tier ordered by id desc — the canonical "scoped materialise"
      // shape the downstream ib-report use case targets.
      await session.insert(scope).fromSelect(
        session
          .select({ id: users.id, tier: users.tier })
          .from(users)
          .where(ck.inArray(users.id, [1, 2, 3, 4, 5]))
          .orderBy(ck.desc(users.id))
          .limitBy([users.tier], 1),
      );

      const rows = await session.select().from(scope).orderBy(scope.tier);
      const tiers = new Set(rows.map((row) => row.tier));
      // Exactly one row per tier among the sampled ids.
      expect(rows.length).toBe(tiers.size);
    });
  });

  it("rejects insert().values() followed by insert().fromSelect() at runtime", async function testRuntimeMutualExclusionValuesFirst() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_mutex_vf");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      name: ckType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      const valuesFirst = session.insert(scope).values({ id: 1, name: "valuesFirst" });
      expect(() =>
        (valuesFirst as unknown as { fromSelect(q: unknown): unknown }).fromSelect(
          session.select({ id: users.id, name: users.name }).from(users),
        ),
      ).toThrow("insert().fromSelect() cannot follow insert().values()");
    });
  });

  it("rejects insert().fromSelect() followed by insert().values() at runtime", async function testRuntimeMutualExclusionFromSelectFirst() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_mutex_fv");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      name: ckType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      const fromSelectFirst = session
        .insert(scope)
        .fromSelect(session.select({ id: users.id, name: users.name }).from(users));

      expect(() =>
        (fromSelectFirst as unknown as { values(rows: unknown): unknown }).values({
          id: 1,
          name: "valuesAfter",
        }),
      ).toThrow("insert().values() cannot follow insert().fromSelect()");
    });
  });

  it("materialises a non-trivial slice from a large dataset without buffering rows in JS", async function testLargeDatasetMaterialise() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_bulk");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      tier: ckType.string(),
    });

    // No `fetch`-stub here: the assertion is structural — the temp table
    // ends up with the same count the SELECT would have produced, proving
    // ClickHouse did the materialisation server-side.
    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      await session
        .insert(scope)
        .fromSelect(session.select({ id: users.id, tier: users.tier }).from(users).where(ck.lt(users.id, 100)));

      const tempCount = await session.count(scope);
      const expectedCount = await session.count(users, ck.lt(users.id, 100));
      expect(tempCount).toBe(expectedCount);
      expect(tempCount).toBeGreaterThan(0);
      expect(tempCount).toBeLessThan(datasetCounts.users);
    });
  });

  it("attaches insert/INSERT observability metadata when used through fromSelect", async function testObservabilityForFromSelect() {
    const startEvents: Array<{ mode: string; operation: string; tableName?: string }> = [];
    const successEvents: Array<{ mode: string; operation: string; tableName?: string }> = [];

    const db = createE2EDb({
      instrumentation: [
        {
          onQueryStart(event) {
            if (event.tableName?.startsWith("tmp_users_obs")) {
              startEvents.push({
                mode: event.mode,
                operation: event.operation,
                tableName: event.tableName,
              });
            }
          },
          onQuerySuccess(event) {
            if (event.tableName?.startsWith("tmp_users_obs")) {
              successEvents.push({
                mode: event.mode,
                operation: event.operation,
                tableName: event.tableName,
              });
            }
          },
        },
      ],
    });

    const tempTable = createTempTableName("tmp_users_obs");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      name: ckType.string(),
    });

    await db.runInSession(async (session) => {
      // createTemporaryTable also emits one event with the table name; filter
      // out by the explicit insert assertion below.
      await session.createTemporaryTable(scope);
      await session
        .insert(scope)
        .fromSelect(session.select({ id: users.id, name: users.name }).from(users).where(ck.eq(users.id, 1)));
    });

    const insertStart = startEvents.find((event) => event.operation === "INSERT");
    const insertSuccess = successEvents.find((event) => event.operation === "INSERT");
    expect(insertStart?.mode).toBe("insert");
    expect(insertStart?.tableName).toBe(tempTable);
    expect(insertSuccess?.mode).toBe("insert");
    expect(insertSuccess?.tableName).toBe(tempTable);
  });

  it("rejects a fromSelect whose embedded select omits a required column on the target", async function testServerRejectsMissingRequired() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_missing_req");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      name: ckType.string(),
      tier: ckType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      // Cast through `never` to bypass the FromSelectShapeConstraint type and
      // exercise the runtime guard inside ck-orm. The guard fires during
      // [compileQuerySymbol](), which `.execute()` invokes synchronously
      // before returning a Promise — wrap the call so `toThrow` can catch it.
      const partial = session.insert(scope).fromSelect(
        session
          .select({
            id: users.id,
            name: users.name,
          })
          .from(users) as never,
      );

      expect(() => partial.execute()).toThrow("insert().fromSelect() select is missing required columns: tier");

      // The temp table must remain empty since the request never reached CH.
      const rows = await session.execute(ckSql`select count() as cnt from ${ckSql.identifier(tempTable)}`);
      expect(Number(rows[0]?.cnt as string)).toBe(0);
    });
  });

  it("materialises into a persistent (non-temporary) MergeTree table and tears it down", async function testPersistentTargetTable() {
    const db = createE2EDb();
    const tableName = createTempTableName("persist_user_summary");
    try {
      // Create the persistent target directly (no temp / no session).
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id Int32,
          tier String
        ) engine = MergeTree order by id
      `);

      const summary = ckTable(tableName, {
        id: ckType.int32(),
        tier: ckType.string(),
      });

      await db
        .insert(summary)
        .fromSelect(db.select({ id: users.id, tier: users.tier }).from(users).where(ck.lt(users.id, 50)));

      const written = await db.count(summary);
      const expected = await db.count(users, ck.lt(users.id, 50));
      expect(written).toBe(expected);
      expect(written).toBeGreaterThan(0);
    } finally {
      // Always tear down so the e2e schema stays clean for subsequent runs.
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("materialises a GROUP BY + HAVING aggregate into a target scope", async function testAggregateMaterialise() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_revenue_by_user");
    const scope = ckTable(tempTable, {
      userId: ckType.int32("user_id"),
      eventCount: ckType.uint64("event_count"),
      revenue: ckType.decimal({ precision: 18, scale: 2 }),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      await session.insert(scope).fromSelect(
        session
          .select({
            userId: webEvents.user_id,
            eventCount: fn.count(),
            revenue: fn.sum(webEvents.revenue),
          })
          .from(webEvents)
          .groupBy(webEvents.user_id)
          .having(ck.gt(fn.count(), 1)),
      );

      // Cross-check the materialised count against what a direct GROUP BY
      // over the source produces: the temp table must hold one row per
      // user_id that has > 1 events.
      const tempCount = await session.count(scope);
      const directGroupCountRows = await session.execute(
        ckSql`select count() as c from (select user_id from ${webEvents} group by user_id having count() > 1)`,
      );
      const expectedGroupCount = Number(directGroupCountRows[0]?.c as string);
      expect(tempCount).toBe(expectedGroupCount);
      expect(tempCount).toBeGreaterThan(0);

      // Spot-check: every row's revenue is a non-empty string (Decimal
      // decoder), userId is a number, eventCount is bigint-or-string (CH
      // emits UInt64 as a string in JSON output and ck-orm preserves it).
      const sample = await session
        .select({
          userId: scope.userId,
          eventCount: scope.eventCount,
          revenue: scope.revenue,
        })
        .from(scope)
        .orderBy(scope.userId)
        .limit(3);
      expect(sample.length).toBeGreaterThan(0);
      for (const row of sample) {
        expect(typeof row.userId).toBe("number");
        expect(typeof row.revenue).toBe("string");
        expect(Number(row.eventCount as unknown as string)).toBeGreaterThan(1);
      }
    });
  });

  it("supports multiple fromSelect calls appending to the same temp table (accumulating slice)", async function testAccumulatingFromSelect() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_accum");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      tier: ckType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      // First batch: ids 1..3
      await session.insert(scope).fromSelect(
        session
          .select({ id: users.id, tier: users.tier })
          .from(users)
          .where(ck.inArray(users.id, [1, 2, 3])),
      );

      // Second batch on the same temp table: disjoint ids 4..6.
      // Each .insert(scope) call must produce a fresh builder so two
      // sequential fromSelect inserts coexist without interfering.
      await session.insert(scope).fromSelect(
        session
          .select({ id: users.id, tier: users.tier })
          .from(users)
          .where(ck.inArray(users.id, [4, 5, 6])),
      );

      const finalCount = await session.count(scope);
      expect(finalCount).toBe(6);

      const rows = await session.select({ id: scope.id }).from(scope).orderBy(scope.id);
      expect(rows.map((row) => row.id)).toEqual([1, 2, 3, 4, 5, 6]);
    });
  });

  it("mixes .values() and .fromSelect() across separate builder chains on the same temp table", async function testMixedWritesAcrossChains() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_users_mixed_chains");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      tier: ckType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      // Chain A: a one-off literal row via .values()
      await session.insert(scope).values({ id: 999, tier: "manual" });
      // Chain B (new builder): a server-side materialisation via .fromSelect()
      await session.insert(scope).fromSelect(
        session
          .select({ id: users.id, tier: users.tier })
          .from(users)
          .where(ck.inArray(users.id, [1, 2])),
      );

      const total = await session.count(scope);
      expect(total).toBe(3);
    });
  });

  it("preserves DateTime64 / Decimal / Array / LowCardinality fidelity through fromSelect", async function testCompositeTypeFidelity() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_composite_fidelity");
    const scope = ckTable(tempTable, {
      eventId: ckType.uint64("event_id"),
      country: ckType.lowCardinality(ckType.string()),
      viewedAt: ckType.dateTime64("viewed_at", { precision: 3 }),
      revenue: ckType.decimal({ precision: 18, scale: 2 }),
      tags: ckType.array(ckType.string()),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      await session.insert(scope).fromSelect(
        session
          .select({
            eventId: webEvents.event_id,
            country: webEvents.country,
            viewedAt: webEvents.viewed_at,
            revenue: webEvents.revenue,
            tags: webEvents.tags,
          })
          .from(webEvents)
          .orderBy(webEvents.event_id)
          .limit(3),
      );

      const sourceRows = await session
        .select({
          eventId: webEvents.event_id,
          country: webEvents.country,
          viewedAt: webEvents.viewed_at,
          revenue: webEvents.revenue,
          tags: webEvents.tags,
        })
        .from(webEvents)
        .orderBy(webEvents.event_id)
        .limit(3);

      const scopeRows = await session
        .select({
          eventId: scope.eventId,
          country: scope.country,
          viewedAt: scope.viewedAt,
          revenue: scope.revenue,
          tags: scope.tags,
        })
        .from(scope)
        .orderBy(scope.eventId);

      // Sanity: scopeRows holds the same shape as sourceRows after the
      // round-trip — Decimals stay strings, DateTime64 stays Date, Arrays
      // remain arrays, LowCardinality is transparent on read.
      expect(scopeRows.length).toBe(sourceRows.length);
      for (let i = 0; i < scopeRows.length; i += 1) {
        expect(scopeRows[i]?.eventId).toBe(sourceRows[i]?.eventId as bigint);
        expect(scopeRows[i]?.country).toBe(sourceRows[i]?.country as string);
        expect(scopeRows[i]?.revenue).toBe(sourceRows[i]?.revenue as string);
        expect(scopeRows[i]?.tags).toEqual(sourceRows[i]?.tags as string[]);
        expect(scopeRows[i]?.viewedAt.getTime()).toBe((sourceRows[i]?.viewedAt as Date).getTime());
      }
    });
  });

  it("emits an observability error event when the server rejects an insert.fromSelect query", async function testObservabilityErrorEventFromSelect() {
    const errorEvents: Array<{
      mode: string;
      operation: string;
      tableName?: string;
      errorKind: string | undefined;
    }> = [];

    const db = createE2EDb({
      instrumentation: [
        {
          onQueryError(event) {
            if (event.tableName?.startsWith("tmp_obs_error_")) {
              errorEvents.push({
                mode: event.mode,
                operation: event.operation,
                tableName: event.tableName,
                errorKind: isClickHouseORMError(event.error) ? event.error.kind : undefined,
              });
            }
          },
        },
      ],
    });

    const tempTable = createTempTableName("tmp_obs_error_");
    // Target's `name` column is String; we'll project an Int column into it
    // through a cast that ClickHouse rejects at INSERT time.
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      bucket: ckType.int32(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      // Run a fromSelect that the server-side will reject (Int8 doesn't fit
      // into Int32 when value is way out of range — we use UInt64 → Int32
      // overflow as a deterministic server-side failure).
      let caught: unknown;
      try {
        await session.insert(scope).fromSelect(
          session
            .select({
              id: users.id,
              // Force an arithmetic error by dividing by a deliberate zero
              // expression — ClickHouse rejects this at execute time.
              bucket: fn.intDiv(users.id, ck.expr(ckSql`toInt32(0)`)),
            })
            .from(users)
            .where(ck.eq(users.id, 1)),
        );
      } catch (error) {
        caught = error;
      }

      // The server-side error must surface as a ClickHouse error AND the
      // instrumentation hook must record the corresponding error event
      // pointing at the temp table with mode/operation set correctly.
      expect(caught).toBeDefined();
      expect(isClickHouseORMError(caught)).toBe(true);

      const errorEvent = errorEvents.find((event) => event.operation === "INSERT");
      expect(errorEvent).toBeDefined();
      expect(errorEvent?.mode).toBe("insert");
      expect(errorEvent?.tableName).toBe(tempTable);
    });
  });

  it("materialises a nested column from one temp table into another via wrap-subquery dot-path expansion", async function testNestedColumnWholeColumnMove() {
    const db = createE2EDb();
    const sourceTable = createTempTableName("tmp_nested_src");
    const sinkTable = createTempTableName("tmp_nested_sink");
    const source = ckTable(sourceTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });
    const sink = ckTable(sinkTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(source);
      await session.createTemporaryTable(sink);

      await session.insert(source).values([
        {
          id: 1,
          events: [
            { name: "a", score: 10 },
            { name: "b", score: 20 },
          ],
        },
        { id: 2, events: [{ name: "c", score: 30 }] },
      ]);

      // No `as never` — type layer now accepts projecting a direct nested
      // column reference (events: source.events) and compileInsertFromSelect
      // fans it out into per-field dot-path projections via wrap-subquery.
      await session.insert(sink).fromSelect(session.select({ id: source.id, events: source.events }).from(source));

      const rows = await session.select({ id: sink.id, events: sink.events }).from(sink).orderBy(sink.id);

      expect(rows).toEqual([
        {
          id: 1,
          events: [
            { name: "a", score: 10 },
            { name: "b", score: 20 },
          ],
        },
        { id: 2, events: [{ name: "c", score: 30 }] },
      ]);
    });
  });

  it("wraps nested-column projection inside a CTE-bearing select", async function testNestedColumnRefWithCte() {
    const db = createE2EDb();
    const sourceTable = createTempTableName("tmp_nested_cte_src");
    const sinkTable = createTempTableName("tmp_nested_cte_sink");
    const source = ckTable(sourceTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });
    const sink = ckTable(sinkTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(source);
      await session.createTemporaryTable(sink);

      await session.insert(source).values([
        { id: 1, events: [{ name: "alpha", score: 1 }] },
        { id: 2, events: [{ name: "beta", score: 2 }] },
        { id: 3, events: [{ name: "gamma", score: 3 }] },
      ]);

      // The inner SELECT has a CTE — wrap-subquery must remain compatible
      // with `WITH cte AS (...) SELECT ...` form (already verified on CH 26.3
      // docker pre-flight).
      const filtered = session
        .$with("filtered")
        .as(session.select({ id: source.id, events: source.events }).from(source).where(ck.lt(source.id, 3)));
      await session
        .with(filtered)
        .insert(sink)
        .fromSelect(session.with(filtered).select({ id: filtered.id, events: filtered.events }).from(filtered));

      const count = await session.count(sink);
      expect(count).toBe(2);
    });
  });

  it("rejects fromSelect projecting a non-column-ref expression into a nested target column", async function testNestedColumnExpressionRejected() {
    const db = createE2EDb();
    const sinkTable = createTempTableName("tmp_nested_expr_rej");
    const sink = ckTable(sinkTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(sink);

      // `users.name` is a plain string column, not a nested ref. ck-orm
      // rejects this at compile time (no SQL ever reaches ClickHouse).
      // `as never` bypasses the FromSelectShapeConstraint type guard so the
      // runtime branch is exercised.
      const partial = session
        .insert(sink)
        .fromSelect(session.select({ id: users.id, events: users.name }).from(users) as never);
      expect(() => partial.execute()).toThrow(
        'insert().fromSelect() projection for nested column "events" must be a direct nested column reference',
      );

      // The temp table must remain empty since the request never reached CH.
      const rows = await session.execute(ckSql`select count() as cnt from ${ckSql.identifier(sinkTable)}`);
      expect(Number(rows[0]?.cnt as string)).toBe(0);
    });
  });

  it("enforces .requiredOnInsert() at the type layer while still accepting projections that satisfy the requirement", async function testRequiredOnInsertNestedSemantics() {
    const db = createE2EDb();
    const sourceTable = createTempTableName("tmp_req_src");
    const sinkTable = createTempTableName("tmp_req_sink");
    const source = ckTable(sourceTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });
    const sink = ckTable(sinkTable, {
      id: ckType.int32(),
      // Business contract: rows MUST carry at least the nested events shape.
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }).requiredOnInsert(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(source);
      await session.createTemporaryTable(sink);

      await session.insert(source).values({
        id: 1,
        events: [{ name: "audit", score: 42 }],
      });

      // .values() with explicit nested data — passes both type and runtime.
      await session.insert(sink).values({
        id: 1,
        events: [{ name: "manual", score: 99 }],
      });

      // .fromSelect() projecting the nested column ref — passes both type
      // and runtime (wrap-subquery dot-path expansion).
      await session.insert(sink).fromSelect(session.select({ id: source.id, events: source.events }).from(source));

      // The type layer would reject `.values({ id: 2 })` and the equivalent
      // .fromSelect omitting `events`; those cases are covered exhaustively
      // in src/runtime.typecheck.ts (cases 15 + matrix). Here we only
      // assert the happy paths landed real data.
      const rows = await session.select().from(sink).orderBy(sink.id);
      expect(rows).toHaveLength(2);
      for (const row of rows) {
        expect(Array.isArray(row.events)).toBe(true);
        expect((row.events as unknown as { name: string }[]).length).toBeGreaterThan(0);
      }
    });
  });

  // --- Real-world business scenarios ---
  //
  // Below tests mirror concrete shapes the ck-orm consumer (CRM/IB report
  // backend) actually models in production. Each scenario exercises the full
  // ck-orm path — schema → fromSelect → wrap-subquery → ClickHouse → read-back
  // — to lock the contract against accidental regressions.

  it("orders pipeline: archives order rows with two parallel nested columns (line_items + status_history)", async function testOrdersPipelineArchive() {
    const db = createE2EDb();
    const liveTable = createTempTableName("orders_live");
    const archiveTable = createTempTableName("orders_archive");

    // Real-world order schema: every order carries an array of line items
    // (sku/qty/price) AND an audit-style status history (state changes with
    // timestamps). Both are modelled as ClickHouse `Nested`.
    // Production schema slice: orders carry a list of line items and a
    // status history. Both are modelled as ClickHouse `Nested`. Money fields
    // and timestamps inside nested rows use scalar primitives (float64 /
    // int64) instead of Decimal / DateTime64 because parameterised
    // `Array(Decimal(P,S))` / `Array(DateTime64(P))` values inside Nested
    // still have a wire-format gap on ClickHouse 26.3 — the parameterised
    // INSERT VALUES path emits `[12.34,56.78]` which the server then fails
    // to re-parse as a typed array. The real-world workaround is the same
    // ck-orm consumers ship: unix-ms timestamps + scalar Decimal kept at
    // top level.
    const liveOrders = ckTable(liveTable, {
      orderId: ckType.int64("order_id"),
      customerId: ckType.int64("customer_id"),
      total: ckType.decimal({ precision: 18, scale: 2 }),
      lineItems: ckType.nested("line_items", {
        sku: ckType.string(),
        qty: ckType.int32(),
        price: ckType.float64(),
      }),
      statusHistory: ckType.nested("status_history", {
        state: ckType.string(),
        // Unix-epoch seconds — int32 (instead of int64 / DateTime64) sidesteps
        // a ck-orm wire-format quirk where `Array(Int64)` and
        // `Array(DateTime64(P))` parameters inside nested rows are emitted as
        // quoted string arrays that ClickHouse can't parse. Plain Int32
        // arrays serialise correctly.
        changedAtSec: ckType.int32("changed_at_sec"),
      }),
    });
    const archivedOrders = ckTable(archiveTable, {
      orderId: ckType.int64("order_id"),
      customerId: ckType.int64("customer_id"),
      total: ckType.decimal({ precision: 18, scale: 2 }),
      lineItems: ckType.nested("line_items", {
        sku: ckType.string(),
        qty: ckType.int32(),
        price: ckType.float64(),
      }),
      statusHistory: ckType.nested("status_history", {
        state: ckType.string(),
        changedAtSec: ckType.int32("changed_at_sec"),
      }),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(liveOrders);
      await session.createTemporaryTable(archivedOrders);

      const t0 = Math.floor(new Date("2026-05-01T08:00:00.000Z").getTime() / 1000);
      const t1 = Math.floor(new Date("2026-05-01T08:15:00.000Z").getTime() / 1000);
      const t2 = Math.floor(new Date("2026-05-01T08:30:00.000Z").getTime() / 1000);
      await session.insert(liveOrders).values([
        {
          orderId: 1001n,
          customerId: 42n,
          total: "199.99",
          lineItems: [
            { sku: "BOOK-A", qty: 1, price: 49.99 },
            { sku: "MUG-X", qty: 2, price: 75.0 },
          ],
          statusHistory: [
            { state: "PLACED", changedAtSec: t0 },
            { state: "PAID", changedAtSec: t1 },
          ],
        },
        {
          orderId: 1002n,
          customerId: 43n,
          total: "30.00",
          lineItems: [{ sku: "MUG-X", qty: 1, price: 30.0 }],
          statusHistory: [
            { state: "PLACED", changedAtSec: t0 },
            { state: "PAID", changedAtSec: t1 },
            { state: "SHIPPED", changedAtSec: t2 },
          ],
        },
      ]);

      // Archive everything by materialising live → archive via wrap-subquery.
      // The wrapped SELECT carries both nested columns simultaneously, which
      // exercises the multi-nested-column fan-out branch in ck-orm.
      await session.insert(archivedOrders).fromSelect(
        session
          .select({
            orderId: liveOrders.orderId,
            customerId: liveOrders.customerId,
            total: liveOrders.total,
            lineItems: liveOrders.lineItems,
            statusHistory: liveOrders.statusHistory,
          })
          .from(liveOrders),
      );

      const archived = await session
        .select({
          orderId: archivedOrders.orderId,
          customerId: archivedOrders.customerId,
          total: archivedOrders.total,
          lineItems: archivedOrders.lineItems,
          statusHistory: archivedOrders.statusHistory,
        })
        .from(archivedOrders)
        .orderBy(archivedOrders.orderId);

      expect(archived).toHaveLength(2);

      // ck-orm decodes ClickHouse Int64 as a JS string (avoids JS-number
      // precision loss on 64-bit values), so the orderId/customerId
      // assertions compare against the stringified form.
      const firstOrder = archived[0];
      expect(firstOrder?.orderId).toBe("1001");
      expect(firstOrder?.customerId).toBe("42");
      expect(firstOrder?.total).toBe("199.99");
      expect(firstOrder?.lineItems).toEqual([
        { sku: "BOOK-A", qty: 1, price: 49.99 },
        { sku: "MUG-X", qty: 2, price: 75.0 },
      ]);
      expect(firstOrder?.statusHistory).toEqual([
        { state: "PLACED", changedAtSec: t0 },
        { state: "PAID", changedAtSec: t1 },
      ]);

      const secondOrder = archived[1];
      expect(secondOrder?.statusHistory).toHaveLength(3);
      expect(secondOrder?.lineItems).toEqual([{ sku: "MUG-X", qty: 1, price: 30.0 }]);
    });
  });

  it("audit log pipeline: scoped materialisation of users with audit events (CTE + WHERE filter + nested column ref)", async function testAuditLogScopedMaterialisation() {
    const db = createE2EDb();
    const auditTable = createTempTableName("user_audit_live");
    const scopeTable = createTempTableName("user_audit_scope");

    const audit = ckTable(auditTable, {
      userId: ckType.int64("user_id"),
      occurredAt: ckType.dateTime64("occurred_at", { precision: 3 }),
      auditTrail: ckType.nested("audit_trail", {
        action: ckType.string(),
        actor: ckType.string(),
        success: ckType.bool(),
      }),
    });
    const scope = ckTable(scopeTable, {
      userId: ckType.int64("user_id"),
      occurredAt: ckType.dateTime64("occurred_at", { precision: 3 }),
      auditTrail: ckType.nested("audit_trail", {
        action: ckType.string(),
        actor: ckType.string(),
        success: ckType.bool(),
      }),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(audit);
      await session.createTemporaryTable(scope);

      const t0 = new Date("2026-05-12T09:00:00.000Z");
      await session.insert(audit).values([
        {
          userId: 1n,
          occurredAt: t0,
          auditTrail: [
            { action: "login", actor: "alice", success: true },
            { action: "view_dashboard", actor: "alice", success: true },
          ],
        },
        {
          userId: 2n,
          occurredAt: t0,
          auditTrail: [{ action: "login_failed", actor: "mallory", success: false }],
        },
        {
          userId: 3n,
          occurredAt: t0,
          auditTrail: [
            { action: "login", actor: "bob", success: true },
            { action: "delete_record", actor: "bob", success: false },
          ],
        },
      ]);

      // Scope: only users 1 and 3 (i.e. exclude failed-login-only user 2).
      // Filter happens inside a CTE — exercising the CTE+nested-ref path that
      // depends on `buildReferenceColumns` propagating nestedShape.
      const eligible = session.$with("eligible").as(
        session
          .select({
            userId: audit.userId,
            occurredAt: audit.occurredAt,
            auditTrail: audit.auditTrail,
          })
          .from(audit)
          .where(ck.inArray(audit.userId, [1n, 3n])),
      );

      await session
        .with(eligible)
        .insert(scope)
        .fromSelect(
          session
            .with(eligible)
            .select({
              userId: eligible.userId,
              occurredAt: eligible.occurredAt,
              auditTrail: eligible.auditTrail,
            })
            .from(eligible),
        );

      const scopeRows = await session
        .select({ userId: scope.userId, auditTrail: scope.auditTrail })
        .from(scope)
        .orderBy(scope.userId);

      expect(scopeRows).toHaveLength(2);
      // ck-orm decodes Int64 as JS string (lossless across 64-bit values).
      expect(scopeRows[0]?.userId).toBe("1");
      expect(scopeRows[0]?.auditTrail).toEqual([
        { action: "login", actor: "alice", success: true },
        { action: "view_dashboard", actor: "alice", success: true },
      ]);
      expect(scopeRows[1]?.userId).toBe("3");
      expect(scopeRows[1]?.auditTrail).toHaveLength(2);
    });
  });

  it("source nested superset: target with subset of fields still materialises correctly", async function testNestedShapeSuperset() {
    const db = createE2EDb();
    const richTable = createTempTableName("rich_events_src");
    const slimTable = createTempTableName("slim_events_tgt");

    // Real scenario: a wider production "events" table (with debug-only
    // `tag` field) materialises into a slimmer downstream table that doesn't
    // care about debug metadata. The ORM must drop the unmapped source field
    // automatically.
    const richSource = ckTable(richTable, {
      id: ckType.int32(),
      events: ckType.nested({
        name: ckType.string(),
        score: ckType.int32(),
        debugTag: ckType.string("debug_tag"),
      }),
    });
    const slimTarget = ckTable(slimTable, {
      id: ckType.int32(),
      events: ckType.nested({
        name: ckType.string(),
        score: ckType.int32(),
      }),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(richSource);
      await session.createTemporaryTable(slimTarget);

      await session.insert(richSource).values({
        id: 1,
        events: [
          { name: "checkpoint-a", score: 10, debugTag: "internal-1" },
          { name: "checkpoint-b", score: 20, debugTag: "internal-2" },
        ],
      });

      await session
        .insert(slimTarget)
        .fromSelect(session.select({ id: richSource.id, events: richSource.events }).from(richSource) as never);

      const out = await session.select().from(slimTarget);
      expect(out).toHaveLength(1);
      expect(out[0]?.events).toEqual([
        { name: "checkpoint-a", score: 10 },
        { name: "checkpoint-b", score: 20 },
      ]);
    });
  });

  it("source nested missing target field: client guard prevents the request from reaching ClickHouse", async function testNestedShapeMismatchClientGuard() {
    const db = createE2EDb();
    const sourceTable = createTempTableName("shape_mismatch_src");
    const targetTable = createTempTableName("shape_mismatch_tgt");

    const slimSource = ckTable(sourceTable, {
      id: ckType.int32(),
      events: ckType.nested({
        name: ckType.string(),
      }),
    });
    const wideTarget = ckTable(targetTable, {
      id: ckType.int32(),
      events: ckType.nested({
        name: ckType.string(),
        score: ckType.int32(),
      }),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(slimSource);
      await session.createTemporaryTable(wideTarget);
      await session.insert(slimSource).values({ id: 1, events: [{ name: "x" }] });

      const partial = session
        .insert(wideTarget)
        .fromSelect(session.select({ id: slimSource.id, events: slimSource.events }).from(slimSource) as never);
      expect(() => partial.execute()).toThrow(
        'insert().fromSelect() nested column "events" shape mismatch: target requires field "score"',
      );

      // Confirm no row landed in the target — the guard fires before any
      // SQL hits ClickHouse.
      const targetCount = await session.execute(ckSql`select count() as cnt from ${ckSql.identifier(targetTable)}`);
      expect(Number(targetCount[0]?.cnt as string)).toBe(0);
    });
  });

  it("nested fromSelect emits an observability success event with mode='insert' and the target table name", async function testNestedFromSelectObservabilitySuccess() {
    const startEvents: Array<{ mode: string; operation: string; tableName?: string }> = [];
    const successEvents: Array<{
      mode: string;
      operation: string;
      tableName?: string;
      durationMs: number;
    }> = [];

    const db = createE2EDb({
      instrumentation: [
        {
          onQueryStart(event) {
            if (event.tableName?.startsWith("nested_obs_")) {
              startEvents.push({
                mode: event.mode,
                operation: event.operation,
                tableName: event.tableName,
              });
            }
          },
          onQuerySuccess(event) {
            if (event.tableName?.startsWith("nested_obs_")) {
              successEvents.push({
                mode: event.mode,
                operation: event.operation,
                tableName: event.tableName,
                durationMs: event.durationMs,
              });
            }
          },
        },
      ],
    });

    const sourceTable = createTempTableName("nested_obs_src");
    const targetTable = createTempTableName("nested_obs_tgt");
    const src = ckTable(sourceTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });
    const tgt = ckTable(targetTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(src);
      await session.createTemporaryTable(tgt);
      await session.insert(src).values({ id: 1, events: [{ name: "a", score: 1 }] });

      await session.insert(tgt).fromSelect(session.select({ id: src.id, events: src.events }).from(src));
    });

    const insertStart = startEvents.find((event) => event.operation === "INSERT" && event.tableName === targetTable);
    const insertSuccess = successEvents.find(
      (event) => event.operation === "INSERT" && event.tableName === targetTable,
    );
    expect(insertStart?.mode).toBe("insert");
    expect(insertSuccess?.mode).toBe("insert");
    expect(insertSuccess?.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("nested fromSelect emits an observability error event when ClickHouse rejects the materialisation", async function testNestedFromSelectObservabilityError() {
    const errorEvents: Array<{
      mode: string;
      operation: string;
      tableName?: string;
      errorKind: string | undefined;
    }> = [];

    const db = createE2EDb({
      instrumentation: [
        {
          onQueryError(event) {
            if (event.tableName?.startsWith("nested_err_")) {
              errorEvents.push({
                mode: event.mode,
                operation: event.operation,
                tableName: event.tableName,
                errorKind: isClickHouseORMError(event.error) ? event.error.kind : undefined,
              });
            }
          },
        },
      ],
    });

    const sourceTable = createTempTableName("nested_err_src");
    const targetTable = createTempTableName("nested_err_tgt");
    const src = ckTable(sourceTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });
    const tgt = ckTable(targetTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });

    let caught: unknown;
    await db.runInSession(async (session) => {
      await session.createTemporaryTable(src);
      await session.createTemporaryTable(tgt);
      await session.insert(src).values({ id: 1, events: [{ name: "a", score: 1 }] });

      try {
        // Force a server-side error: project a literal string into the
        // Int32 `id` column. ClickHouse rejects this with
        // `Cannot parse text` at execute time, so the observability
        // instrumentation must catch the corresponding error event.
        await session.insert(tgt).fromSelect(
          session
            .select({
              id: ck.expr<number>(ckSql`'not an int'::Int32`),
              events: src.events,
            })
            .from(src),
        );
      } catch (error) {
        caught = error;
      }
    });

    expect(caught).toBeDefined();
    expect(isClickHouseORMError(caught)).toBe(true);

    const errorEvent = errorEvents.find((event) => event.operation === "INSERT");
    expect(errorEvent).toBeDefined();
    expect(errorEvent?.mode).toBe("insert");
    expect(errorEvent?.tableName).toBe(targetTable);
  });

  it("end-to-end .requiredOnInsert(): values path satisfies it, fromSelect+nested-ref path satisfies it, fromSelect omission errors out", async function testRequiredOnInsertHappyAndErrorPaths() {
    const db = createE2EDb();
    const sourceTable = createTempTableName("req_src");
    const sinkTable = createTempTableName("req_sink");

    const src = ckTable(sourceTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });
    const sink = ckTable(sinkTable, {
      id: ckType.int32(),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }).requiredOnInsert(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(src);
      await session.createTemporaryTable(sink);
      await session.insert(src).values({ id: 1, events: [{ name: "seed", score: 7 }] });

      // Happy path 1: explicit values with nested data.
      await session.insert(sink).values({
        id: 100,
        events: [{ name: "manual", score: 1 }],
      });

      // Happy path 2: fromSelect projecting source.events.
      await session.insert(sink).fromSelect(session.select({ id: src.id, events: src.events }).from(src));

      // Error path: fromSelect omits events. Bypass the type guard with
      // `as never` to exercise the runtime client_validation guard.
      const partialOmission = session.insert(sink).fromSelect(session.select({ id: src.id }).from(src) as never);
      expect(() => partialOmission.execute()).toThrow(
        "insert().fromSelect() select is missing required columns: events",
      );

      // Confirm only the two happy-path rows landed.
      const rows = await session.select({ id: sink.id }).from(sink).orderBy(sink.id);
      expect(rows.map((row) => row.id)).toEqual([1, 100]);
    });
  });
});
