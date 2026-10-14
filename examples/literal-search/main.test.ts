import { expect, test } from "bun:test";
import { clickhouseClient } from "ck-orm";
import { run } from "./main";

test("sends the search text as an escaped parameter", async () => {
  const selects: FormData[] = [];
  // Stands in for ClickHouse: records the SELECT form and answers it the way the server would.
  const fetch = async (request: Request) => {
    const isForm = request.headers.get("content-type")?.startsWith("multipart/form-data");
    const form = isForm ? await request.formData() : undefined;
    if (!String(form?.get("query")).startsWith("select"))
      return Response.json({ data: [] });
    selects.push(form as FormData);
    return Response.json({ data: [{ probeId: "alpha%1" }] });
  };

  const rows = await run(clickhouseClient({ host: "http://clickhouse.test", fetch }));

  expect(rows).toEqual([{ probeId: "alpha%1" }]);
  expect(selects).toHaveLength(1);
  expect(String(selects[0].get("query"))).not.toContain("alpha");
  expect(selects[0].get("param_orm_param1")).toBe("%alpha\\\\%%");
});
