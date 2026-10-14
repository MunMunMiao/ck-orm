import { expect, test } from "bun:test";
import { clickhouseClient } from "ck-orm";
import { run } from "./main";

const realTest = test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL);

const requestSql = async (request: Request) => {
  const copy = request.clone();
  const isForm = copy.headers.get("content-type")?.startsWith("multipart/form-data");
  const query = isForm
    ? String((await copy.formData()).get("query"))
    : (new URL(copy.url).searchParams.get("query") ?? "");
  return [query, isForm ? "" : await copy.text()].filter(Boolean).join("\n");
};

test("sends the seed batches and both rollup steps without JavaScript row data", async () => {
  const requests: string[] = [];
  const fakeDb = clickhouseClient({
    host: "http://clickhouse.test",
    fetch: async (request: Request) => {
      requests.push(await requestSql(request));
      return Response.json({ data: [] });
    },
  });

  expect(await run(fakeDb)).toEqual([]);

  const seeds = requests.filter((sql) => sql.startsWith("insert into `orders_cdc`"));
  expect(seeds).toHaveLength(2);
  for (const sql of seeds) expect(sql).toContain("format Values");

  const current = requests.filter((sql) =>
    sql.startsWith("insert into `current_orders`"),
  );
  expect(current).toHaveLength(1);
  expect(current[0]).toContain("from `orders_cdc` final where");
  expect(current[0]).not.toContain("format Values");

  const daily = requests.filter((sql) => sql.startsWith("insert into `daily_revenue`"));
  expect(daily).toHaveLength(1);
  expect(daily[0]).toContain("toString(count())");
  expect(daily[0]).toContain("group by");
  expect(daily[0]).not.toContain("format Values");
});

// Expected rows:
// - order 1 is the only paid order on 2026-02-01, because order 2 was refunded;
// - order 3 was deleted, so it is excluded from the rollup;
// - order 4 was updated from pending to paid, so it counts on 2026-02-02.
// Order 4 is at 23:59:59.999 UTC, so it stays on 2026-02-02 in any session timezone:
// toDate uses the column's UTC time zone.
const expectedRows = [
  { day: new Date("2026-02-01T00:00:00.000Z"), orders: "1", revenue: "120" },
  { day: new Date("2026-02-02T00:00:00.000Z"), orders: "1", revenue: "10" },
];

realTest("rolls up CDC rows in ClickHouse", async () => {
  const db = clickhouseClient({ databaseUrl: process.env.CLICKHOUSE_EXAMPLES_URL ?? "" });
  expect(await run(db)).toEqual(expectedRows);
});

realTest("returns the same rows in a non-UTC session timezone", async () => {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_EXAMPLES_URL ?? "",
    clickhouse_settings: { session_timezone: "America/Los_Angeles" },
  });
  expect(await run(db)).toEqual(expectedRows);
});
