import { expect, it } from "bun:test";
import { chTable, chType, ck } from "./ck-orm";
import { createE2EDb, createTempTableName, users } from "./shared";
import { describeE2E } from "./test-helpers";

describeE2E("ck-orm e2e injection values", function describeInjectionValues() {
  it("parameterizes set-membership payloads instead of widening the result set", async function testInArrayPayloads() {
    const db = createE2EDb();
    const payloads = [
      "'; DROP TABLE users; --",
      "' OR '1'='1",
      "' UNION SELECT version() --",
      "admin'--",
      "hello\u2028world",
      "hello\u2029world",
    ];

    for (const payload of payloads) {
      const rows = await db
        .select({
          id: users.id,
          name: users.name,
        })
        .from(users)
        .where(ck.inArray(users.name, [payload, "alice"]))
        .orderBy(users.id)
        .limit(2);

      expect(rows).toEqual([{ id: 1, name: "alice" }]);
    }
  });

  it("parameterizes LIKE and ILIKE payloads instead of changing pattern semantics", async function testLikePayloads() {
    const db = createE2EDb();
    const payloads = [
      "'; DROP TABLE users; --",
      "' UNION SELECT null, version(), null --",
      "admin'--",
      "hello\u2028world",
      "hello\u2029world",
    ];

    for (const payload of payloads) {
      expect(
        await db
          .select({
            id: users.id,
          })
          .from(users)
          .where(ck.like(users.name, payload))
          .limit(1),
      ).toEqual([]);

      expect(
        await db
          .select({
            id: users.id,
          })
          .from(users)
          .where(ck.and(ck.eq(users.id, 1), ck.notLike(users.name, payload)))
          .limit(1),
      ).toEqual([{ id: 1 }]);

      expect(
        await db
          .select({
            id: users.id,
          })
          .from(users)
          .where(ck.ilike(users.name, payload))
          .limit(1),
      ).toEqual([]);

      expect(
        await db
          .select({
            id: users.id,
          })
          .from(users)
          .where(ck.and(ck.eq(users.id, 1), ck.notIlike(users.name, payload)))
          .limit(1),
      ).toEqual([{ id: 1 }]);
    }
  });

  it("requires escapeLike to match literal wildcard characters safely", async function testEscapeLikeLiteralWildcards() {
    const db = createE2EDb();
    const tempTable = createTempTableName("pattern_scope");
    const patternScope = chTable(tempTable, {
      name: chType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(patternScope);
      await session.insertJsonEachRow(patternScope, [
        { name: "price100%real" },
        { name: "price100Xreal" },
        { name: "tag_user_1" },
        { name: "tag_userA1" },
      ]);

      const unescapedPercentRows = await session
        .select({
          name: patternScope.name,
        })
        .from(patternScope)
        .where(ck.like(patternScope.name, "price100%real"))
        .orderBy(patternScope.name);
      expect(unescapedPercentRows.map((row) => row.name)).toEqual(["price100%real", "price100Xreal"]);

      const escapedPercentRows = await session
        .select({
          name: patternScope.name,
        })
        .from(patternScope)
        .where(ck.like(patternScope.name, ck.escapeLike("price100%real")))
        .orderBy(patternScope.name);
      expect(escapedPercentRows.map((row) => row.name)).toEqual(["price100%real"]);

      const unescapedUnderscoreRows = await session
        .select({
          name: patternScope.name,
        })
        .from(patternScope)
        .where(ck.like(patternScope.name, "tag_user_1"))
        .orderBy(patternScope.name);
      expect(unescapedUnderscoreRows.map((row) => row.name)).toEqual(["tag_userA1", "tag_user_1"]);

      const escapedUnderscoreRows = await session
        .select({
          name: patternScope.name,
        })
        .from(patternScope)
        .where(ck.like(patternScope.name, ck.escapeLike("tag_user_1")))
        .orderBy(patternScope.name);
      expect(escapedUnderscoreRows.map((row) => row.name)).toEqual(["tag_user_1"]);
    });
  });
});
