import { type ClickHouseORMClient, ckSql, ckTable, ckType, clickhouseClient } from "ck-orm";
import { format } from "sql-formatter";
import * as asofPrice from "../../examples/asof-price/main";
import * as blueskyEvents from "../../examples/bluesky-events/main";
import * as cdcRollup from "../../examples/cdc-rollup/main";
import * as decimalSum from "../../examples/decimal-sum/main";
import * as dynamicSignals from "../../examples/dynamic-signals/main";
import * as leftJoin from "../../examples/left-join/main";
import * as literalSearch from "../../examples/literal-search/main";
import * as otelTracing from "../../examples/otel-tracing/main";
import * as queryPlugins from "../../examples/query-plugins/main";
import * as requestLatency from "../../examples/request-latency/main";
import * as supportTickets from "../../examples/support-tickets/main";
import * as traceList from "../../examples/trace-list/main";
import * as usageMetering from "../../examples/usage-metering/main";
import * as websiteAnalytics from "../../examples/website-analytics/main";

// The examples page shows each example's main.ts as it is, and the SQL that running it sends to ClickHouse.
export type Example = {
  /** Folder name in examples/, also the anchor on the examples page. */
  id: string;
  /** Repository path of the runnable file. */
  path: string;
  source: string;
  sql: string;
};

type Run = (db: ClickHouseORMClient) => unknown;

// What a request carries: its statement (the `query` URL parameter and/or the body, or the `query` field of a
// multipart form), the values of its typed parameters (`param_<name>`, which ck-orm sends apart from the SQL),
// and its settings (the other URL parameters).
const requestOf = async (request: Request) => {
  const url = new URL(request.url);
  const form = request.headers.get("content-type")?.startsWith("multipart/form-data")
    ? await request.formData()
    : undefined;
  const query = form ? String(form.get("query")) : (url.searchParams.get("query") ?? "");
  const fields = [...url.searchParams, ...(form ?? [])].map(([key, value]) => [key, String(value)] as const);
  return {
    statement: [query, form ? "" : await request.text()].filter(Boolean).join("\n"),
    params: fields.filter(([key]) => key.startsWith("param_")).map(([key, value]) => [key.slice(6), value] as const),
    settings: fields.filter(([key]) => key !== "query" && !key.startsWith("param_")),
  };
};

type Request_ = Awaited<ReturnType<typeof requestOf>>;

// Settings that every plain query, session query and insert carries (the wire contract) are not worth showing,
// and the ids differ on every run. What is left on an example's requests is what that example asks for.
const HIDDEN_SETTINGS = new Set(["query_id", "session_id"]);
const wireSettings = await (async () => {
  const seen: Request_[] = [];
  const db = clickhouseClient({
    host: "http://clickhouse.test",
    fetch: async (request) => {
      seen.push(await requestOf(request));
      return Response.json({ data: [] });
    },
  });
  await db.execute(ckSql`select 1`);
  await db.runInSession((session) => session.execute(ckSql`select 1`));
  await db.insert(ckTable("wire", { id: ckType.string() })).values([{ id: "x" }]);
  return new Set(seen.flatMap(({ settings }) => settings.map(([key, value]) => `${key}=${value}`)));
})();

// Single-quoted SQL string literal, with backslash escapes and doubled quotes.
const STRING = /('(?:[^'\\]|\\.|'')*')/;
// Applies a fix to the SQL between string literals only.
const outsideStrings = (sql: string, fix: (code: string) => string) =>
  sql
    .split(STRING)
    .map((part, index) => (index % 2 ? part : fix(part)))
    .join("");
// The SQL without the whitespace between tokens, to check that a layout changed nothing else.
const squash = (sql: string) => outsideStrings(sql, (code) => code.replace(/\s+/g, ""));

// Lays a statement out for reading, without changing it beyond whitespace:
// - a SELECT is one line as ck-orm sends it, so it goes one clause per line;
// - INSERT rows go one per line (rows are joined with "), (", cells inside a row with "," and no space);
// - DDL and everything else stay as the example wrote them.
const layout = (statement: string) => {
  let laidOut = statement;
  if (/^insert\b/i.test(statement)) {
    laidOut = statement.replaceAll("), (", "),\n(");
  } else if (/^(select|with)\b/i.test(statement)) {
    let formatted: string;
    try {
      formatted = format(statement, { language: "clickhouse", keywordCase: "preserve", tabWidth: 2 });
    } catch {
      // sql-formatter cannot parse some ClickHouse syntax, such as JSON subcolumns (`json.^path`, `json.path.:Type`).
      return statement;
    }
    const lines = formatted.split("\n").map((line) => line.trimEnd());
    // sql-formatter puts every clause body on its own line; keep one-line bodies next to their keyword.
    const compact: string[] = [];
    for (let index = 0; index < lines.length; index++) {
      const [line, next, after] = [lines[index], lines[index + 1], lines[index + 2]];
      if (/^[a-z ]+$/i.test(line) && next?.startsWith("  ") && !after?.startsWith("  ")) {
        compact.push(`${line} ${next.trim()}`);
        index++;
      } else {
        compact.push(line);
      }
    }
    // sql-formatter 15.9.0 puts a space before the parenthesis of aggregate function combinators (countMerge, sumIf, ...),
    // before the second parenthesis of quantile(0.9)(x) and before the subscript of a quoted identifier, and
    // splits "ASOF LEFT JOIN" in two. The replacements below fix that, and only whitespace changes.
    laidOut = outsideStrings(compact.join("\n"), (code) =>
      code
        .replace(/\b(\w+(?:Merge|State|If|Array|OrNull|OrDefault|ForEach|Distinct)) \(/g, "$1(")
        .replace(/(\b\w+\(\d[\d., ]*\)) \(/g, "$1(")
        .replace(/` \[/g, "`[")
        .replace(/(\S) ASOF\n(\s*)LEFT JOIN/g, "$1\n$2ASOF LEFT JOIN"),
    );
  }
  // The page must show exactly what is sent: if anything but whitespace between tokens changed, show the statement as sent.
  return squash(laidOut) === squash(statement) ? laidOut : statement;
};

// Runs the example against a fake ClickHouse that records each request and answers with an empty result,
// so no server is needed. ck-orm checks the SQL fragments it is given against its own module, and an example
// imports ck-orm from its own folder, so the client must come from that same copy.
const sqlOf = async (id: string, run: Run) => {
  const folder = new URL(`../../examples/${id}/`, import.meta.url).pathname;
  const { clickhouseClient } = (await import(Bun.resolveSync("ck-orm", folder))) as typeof import("ck-orm");
  const requests: Request_[] = [];
  const fetch = async (request: Request) => {
    requests.push(await requestOf(request));
    return Response.json({ data: [] });
  };
  await run(clickhouseClient({ host: "http://clickhouse.test", fetch }));
  if (requests.length === 0) throw new Error(`examples/${id}/main.ts sent no request`);
  // Multipart form fields carry CRLF line breaks, so line endings are normalised first.
  const lf = (text: string) => text.replaceAll("\r\n", "\n");
  // Each statement is followed, as comments, by the values of its parameters and the settings it asks for.
  return requests
    .map(({ statement, params, settings }) =>
      [
        layout(lf(statement)),
        ...params.map(([name, value]) => `-- param ${name} = ${lf(value).replaceAll("\n", "\\n")}`),
        ...settings
          .filter(([key, value]) => !HIDDEN_SETTINGS.has(key) && !wireSettings.has(`${key}=${value}`))
          .map(([key, value]) => `-- setting ${key} = ${value}`),
      ].join("\n"),
    )
    .join("\n\n");
};

const example = async (id: string, run: Run): Promise<Example> => ({
  id,
  path: `examples/${id}/main.ts`,
  source: (await Bun.file(new URL(`../../examples/${id}/main.ts`, import.meta.url)).text()).trimEnd(),
  sql: await sqlOf(id, run),
});

// Written by hand, in display order.
export const examples = [
  await example("decimal-sum", decimalSum.run),
  await example("literal-search", literalSearch.run),
  await example("left-join", leftJoin.run),
  await example("trace-list", traceList.run),
  await example("support-tickets", supportTickets.run),
  await example("website-analytics", websiteAnalytics.run),
  await example("asof-price", asofPrice.run),
  await example("dynamic-signals", dynamicSignals.run),
  await example("bluesky-events", blueskyEvents.run),
  await example("cdc-rollup", cdcRollup.run),
  await example("usage-metering", usageMetering.run),
  await example("request-latency", requestLatency.run),
  await example("query-plugins", queryPlugins.run),
  await example("otel-tracing", otelTracing.run),
];
