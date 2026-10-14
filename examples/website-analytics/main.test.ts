import { expect, test } from "bun:test";
import { clickhouseClient } from "ck-orm";
import { run } from "./main";

test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL)(
  "counts repeated pageviews within one site's UTC [start, end) window",
  async () => {
    const db = clickhouseClient({
      databaseUrl: process.env.CLICKHOUSE_EXAMPLES_URL ?? "",
    });

    // Alice visits / twice and /pricing once; Bob visits / at end minus 1 ms.
    // Other sites, custom events, start minus 1 ms and the exact end are excluded.
    expect(await run(db)).toEqual([
      { pathname: "/", pageviews: 3, visitors: 2 },
      { pathname: "/pricing", pageviews: 1, visitors: 1 },
    ]);
  },
);
