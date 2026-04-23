import { expect, it } from "bun:test";
import { chTable, chType, ck } from "./ck-orm";
import { createE2EDb, createTempTableName, pets, rewardEvents, users } from "./shared";
import { describeE2E, expectPresent, takeAsync } from "./test-helpers";

describeE2E("ck-orm e2e session, cdc and stream", function describeSessionCdcAndStream() {
  it("supports builder.iterator and raw stream on large datasets", async function testStreamingPaths() {
    const db = createE2EDb();

    const builderRows = await takeAsync(
      db
        .select({
          id: users.id,
          name: users.name,
        })
        .from(users)
        .orderBy(users.id)
        .iterator(),
      5,
    );

    const rawRows = await takeAsync(
      db.stream(ck.sql`select ${users.id} as id, ${users.name} as name from ${users} order by ${users.id}`),
      5,
    );

    expect(builderRows).toEqual([
      { id: 1, name: "alice" },
      { id: 2, name: "bob" },
      { id: 3, name: "charlie" },
      { id: 4, name: "user_4" },
      { id: 5, name: "user_5" },
    ]);
    expect(rawRows).toEqual(builderRows);
  });

  it("supports leftJoin null semantics and join_use_nulls opt-out", async function testJoinNullModes() {
    const db = createE2EDb();

    const defaultRows = await db
      .select()
      .from(users)
      .leftJoin(pets, ck.eq(users.id, pets.owner_id))
      .orderBy(users.id)
      .limit(3);
    expect(defaultRows[0]?.users.name).toBe("alice");
    expect(defaultRows[0]?.pets).not.toBeNull();

    const [missingPetRow] = await db
      .select()
      .from(users)
      .leftJoin(pets, ck.eq(users.id, pets.owner_id))
      .where(ck.eq(users.id, 4001))
      .limit(1);

    expect(missingPetRow?.pets).toBeNull();

    const [noNullMissingPetRow] = await db
      .withSettings({
        join_use_nulls: 0 as const,
      })
      .select()
      .from(users)
      .leftJoin(pets, ck.eq(users.id, pets.owner_id))
      .where(ck.eq(users.id, 4001))
      .limit(1);

    expect(noNullMissingPetRow?.pets).toEqual({
      id: 0,
      owner_id: 0,
      pet_name: "",
      created_at: new Date(0),
    });
  });

  it("supports FINAL and deleted-row filtering for cdc tables", async function testFinalAndDeletedFiltering() {
    const db = createE2EDb();

    const [physicalRows] = await db
      .select({
        total: ck.sql<number>`count()`.mapWith((value) => Number(value)).as("total"),
      })
      .from(rewardEvents);
    const [logicalRows] = await db
      .select({
        total: ck.sql<number>`count()`.mapWith((value) => Number(value)).as("total"),
      })
      .from(rewardEvents)
      .final()
      .where(ck.eq(rewardEvents._peerdb_is_deleted, 0));

    const physicalTotal = Number(expectPresent(physicalRows, "physicalRows").total);
    const logicalTotal = Number(expectPresent(logicalRows, "logicalRows").total);
    expect(physicalTotal).toBeGreaterThan(logicalTotal);
    expect(logicalTotal).toBe(19_000);
  });

  it("supports runInSession, registerTempTable and createTemporaryTable with cleanup", async function testSessionTempTables() {
    const db = createE2EDb();
    const manualTempTable = createTempTableName("manual_scope");
    const helperTempTable = createTempTableName("helper_scope");
    const helperTempScope = chTable(helperTempTable, { user_id: chType.int32() });
    const sessionId = ck.createSessionId();

    const scopedRows = await db.runInSession(
      async (session) => {
        await session.command(`CREATE TEMPORARY TABLE ${manualTempTable} (user_id Int32)`);
        session.registerTempTable(manualTempTable);
        await session.insertJsonEachRow(manualTempTable, [{ user_id: 1 }, { user_id: 2 }]);

        await session.createTemporaryTable(helperTempScope);
        await session.insertJsonEachRow(helperTempScope, [{ user_id: 3 }]);

        return await session
          .select({
            id: users.id,
            name: users.name,
          })
          .from(users)
          .where(
            ck.expr(ck.sql`
              ${users.id} in (
                select user_id from ${ck.sql.identifier(manualTempTable)}
                union all
                select user_id from ${ck.sql.identifier(helperTempTable)}
              )
            `),
          )
          .orderBy(users.id);
      },
      { session_id: sessionId },
    );

    expect(scopedRows).toEqual([
      { id: 1, name: "alice" },
      { id: 2, name: "bob" },
      { id: 3, name: "charlie" },
    ]);

    await expect(
      db.execute(ck.sql`select count() as total from ${ck.sql.identifier(manualTempTable)}`, {
        session_id: sessionId,
      }),
    ).rejects.toThrow(/doesn't exist|unknown table/i);

    await expect(
      db.execute(ck.sql`select count() as total from ${ck.sql.identifier(helperTempTable)}`, {
        session_id: sessionId,
      }),
    ).rejects.toThrow(/doesn't exist|unknown table/i);
  });
});
