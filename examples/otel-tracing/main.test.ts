import { expect, test } from "bun:test";
import {
  InMemorySpanExporter,
  NodeTracerProvider,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-node";
import { clickhouseClient } from "ck-orm";
import { otelTracing } from "ck-orm/otel";
import { run } from "./main";

const databaseUrl = process.env.CLICKHOUSE_EXAMPLES_URL ?? "";
const realTest = test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL);

// A provider of its own, passed to the plugin as `tracer`: the test registers nothing globally.
const tracing = () => {
  const exporter = new InMemorySpanExporter();
  const provider = new NodeTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
  const plugin = otelTracing({
    tracer: provider.getTracer("test"),
    includeStatement: false,
  });
  return { exporter, plugin };
};

test("records one span per request and leaves the SQL out", async () => {
  const { exporter, plugin } = tracing();
  const db = clickhouseClient({
    host: "http://clickhouse.test",
    plugins: [plugin],
    fetch: async () => Response.json({ data: [] }),
  });

  await run(db);

  const spans = exporter.getFinishedSpans();
  expect(spans.map((span) => span.name)).toEqual([
    "clickhouse CREATE",
    "clickhouse INSERT",
    "clickhouse QUERY",
    "clickhouse DROP",
  ]);
  expect(spans.map((span) => span.attributes["db.query.summary"])).toEqual([
    "CREATE",
    "INSERT readings",
    "SELECT readings",
    "DROP",
  ]);
  for (const span of spans) {
    expect(span.attributes["db.system"]).toBe("clickhouse");
    expect(span.attributes["db.statement"]).toBeUndefined();
    expect(span.attributes["db.query.text"]).toBeUndefined();
  }
});

test("ends the span of a failed request with an error status", async () => {
  const { exporter, plugin } = tracing();
  let requests = 0;
  const db = clickhouseClient({
    host: "http://clickhouse.test",
    plugins: [plugin],
    // The SELECT is the third request of the session: CREATE, INSERT, SELECT.
    fetch: async () =>
      ++requests === 3
        ? new Response("boom", { status: 500 })
        : Response.json({ data: [] }),
  });

  await expect(run(db)).rejects.toThrow();

  const failed = exporter.getFinishedSpans().filter((span) => span.status.code === 2);
  expect(failed.map((span) => span.name)).toEqual(["clickhouse QUERY"]);
});

realTest("traces the queries it sends to ClickHouse", async () => {
  const { exporter, plugin } = tracing();
  const db = clickhouseClient({ databaseUrl, plugins: [plugin] });

  expect(await run(db)).toEqual([
    { probeId: "alpha", average: 2 },
    { probeId: "beta", average: 4 },
  ]);
  expect(exporter.getFinishedSpans()).toHaveLength(4);
});
