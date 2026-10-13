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

test("main.sql is the SQL it sends", async () => {
  const statements: string[] = [];
  // Stands in for ClickHouse: a request's statement is the `query` URL parameter and/or the body,
  // or the `query` field of a multipart form.
  const fetch = async (request: Request) => {
    const isForm = request.headers.get("content-type")?.startsWith("multipart/form-data");
    const query = isForm
      ? String((await request.formData()).get("query"))
      : (new URL(request.url).searchParams.get("query") ?? "");
    statements.push(
      [query, isForm ? "" : await request.text()].filter(Boolean).join("\n"),
    );
    return Response.json({ data: [] });
  };

  await run(clickhouseClient({ host: "http://clickhouse.test", fetch }));

  // The docs show main.sql as it is, so this test is what keeps it true. Multipart form fields carry
  // CRLF line breaks and a checkout may convert main.sql, so both sides are compared with LF.
  const lf = (text: string) => text.replaceAll("\r\n", "\n");
  expect(lf(`${statements.join("\n\n")}\n`)).toBe(
    lf(await Bun.file(new URL("./main.sql", import.meta.url)).text()),
  );
});
