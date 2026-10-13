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
