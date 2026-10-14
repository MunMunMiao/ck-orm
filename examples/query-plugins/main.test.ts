import { expect, test } from "bun:test";
import { clickhouseClient } from "ck-orm";
import { requestLog, run } from "./main";

const databaseUrl = process.env.CLICKHOUSE_EXAMPLES_URL ?? "";
const realTest = test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL);

// Stands in for ClickHouse: answers every request with no rows, or fails the SELECT.
const fakeDb = (options: { failSelect?: boolean } = {}) => {
  const { plugin, statements } = requestLog();
  const sources: (string | null)[] = [];
  const db = clickhouseClient({
    host: "http://clickhouse.test",
    plugins: [plugin],
    fetch: async (request) => {
      sources.push(request.headers.get("x-request-source"));
      // The SELECT is the third request of the session: CREATE, INSERT, SELECT.
      if (options.failSelect && sources.length === 3)
        return new Response("boom", { status: 500 });
      return Response.json({ data: [] });
    },
  });
  return { db, statements, sources };
};

test("logs every request, including the session's CREATE and DROP, and tags each one", async () => {
  const { db, statements, sources } = fakeDb();

  expect(await run(db)).toEqual([]);

  expect(statements).toEqual(["CREATE", "INSERT readings", "SELECT readings", "DROP"]);
  expect(sources).toEqual(Array(4).fill("billing-worker"));
});

test("logs a failed request and still sees the cleanup", async () => {
  const { db, statements } = fakeDb({ failSelect: true });

  await expect(run(db)).rejects.toThrow();

  expect(statements).toEqual([
    "CREATE",
    "INSERT readings",
    "SELECT readings failed",
    "DROP",
  ]);
});

realTest("averages the readings per probe against ClickHouse", async () => {
  const { plugin, statements } = requestLog();
  const db = clickhouseClient({ databaseUrl, plugins: [plugin] });

  expect(await run(db)).toEqual([
    { probeId: "alpha", average: 2 },
    { probeId: "beta", average: 4 },
  ]);
  expect(statements).toEqual(["CREATE", "INSERT readings", "SELECT readings", "DROP"]);
});
