import { expect, test } from "bun:test";
import { clickhouseClient } from "ck-orm";
import { run } from "./main";

test("sums a Decimal column into an exact string", async () => {
  const statements: string[] = [];
  // Stands in for ClickHouse: the statement is the `query` parameter when set,
  // otherwise the multipart field or raw body. Answers the SELECT the way the server would.
  const fetch = async (request: Request) => {
    const isForm = request.headers.get("content-type")?.startsWith("multipart/form-data");
    const sql =
      new URL(request.url).searchParams.get("query") ??
      (isForm ? String((await request.formData()).get("query")) : await request.text());
    statements.push(sql);
    return Response.json({
      data: sql.startsWith("select") ? [{ probeId: "alpha", total: "3.30003" }] : [],
    });
  };

  const rows = await run(clickhouseClient({ host: "http://clickhouse.test", fetch }));

  expect(rows).toEqual([{ probeId: "alpha", total: "3.30003" }]);
  expect(statements.map((sql) => sql.split(" ")[0].toLowerCase())).toEqual([
    "create",
    "insert",
    "select",
    "drop",
  ]);
  expect(statements[2]).toContain("CAST(sum(`probes`.`signal`) AS Decimal(38, 5))");
});
