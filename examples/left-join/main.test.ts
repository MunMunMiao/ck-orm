import { expect, test } from "bun:test";
import { clickhouseClient } from "ck-orm";
import { run } from "./main";

test("keeps unmatched right-side columns as null", async () => {
  const selects: URL[] = [];
  // Stands in for ClickHouse: records the SELECT request and answers it the way the server would.
  const fetch = async (request: Request) => {
    const isForm = request.headers.get("content-type")?.startsWith("multipart/form-data");
    const sql = isForm
      ? String((await request.formData()).get("query"))
      : await request.text();
    if (!sql.startsWith("select")) return Response.json({ data: [] });
    selects.push(new URL(request.url));
    return Response.json({
      data: [
        { probeId: "alpha", mission: "Voyager" },
        { probeId: "beta", mission: null },
      ],
    });
  };

  const rows = await run(clickhouseClient({ host: "http://clickhouse.test", fetch }));

  expect(rows).toEqual([
    { probeId: "alpha", mission: "Voyager" },
    { probeId: "beta", mission: null },
  ]);
  expect(selects).toHaveLength(1);
  expect(selects[0].searchParams.get("join_use_nulls")).toBe("1");
});
