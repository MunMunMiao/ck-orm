/**
 * OpenTelemetry tracing
 *
 * `otelTracing()` from `ck-orm/otel` records one span per request. ck-orm only creates
 * the spans: the application starts an OpenTelemetry SDK once, at startup, and the SDK
 * decides where the spans go.
 */
import { trace } from "@opentelemetry/api";
import {
  InMemorySpanExporter,
  NodeTracerProvider,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-node";
import type { ClickHouseORMClient } from "ck-orm";
import { ckTable, ckType, clickhouseClient, fn } from "ck-orm";
import { otelTracing } from "ck-orm/otel";

const readings = ckTable("readings", {
  probeId: ckType.string("probe_id"),
  signal: ckType.float64(),
});

// A session temporary table: the example needs no tables on the server.
export const run = (db: ClickHouseORMClient) =>
  db.runInSession(async (session) => {
    await session.createTemporaryTable(readings);
    await session.insert(readings).values([
      { probeId: "alpha", signal: 1.5 },
      { probeId: "alpha", signal: 2.5 },
      { probeId: "beta", signal: 4 },
    ]);

    return session
      .select({
        probeId: readings.probeId,
        average: fn.avg(readings.signal).as("average"),
      })
      .from(readings)
      .groupBy(readings.probeId)
      .orderBy(readings.probeId);
  });

if (import.meta.main) {
  // Start the SDK before the first query. A real application exports to a collector
  // (NodeSDK with an OTLP exporter); an in-memory exporter keeps this example
  // self-contained. register() also sets the context manager and the W3C propagator.
  const exporter = new InMemorySpanExporter();
  new NodeTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  }).register();

  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
    plugins: [otelTracing({ includeStatement: false })],
  });

  // ck-orm's spans nest under the active span, such as the one a web framework opens
  // for each request.
  const tracer = trace.getTracer("report-job");
  const rows = await tracer.startActiveSpan("report", async (span) => {
    try {
      return await run(db);
    } finally {
      span.end();
    }
  });
  console.log(rows);
  // [{ probeId: "alpha", average: 2 }, { probeId: "beta", average: 4 }]

  const spans = exporter.getFinishedSpans();
  const names = new Map(spans.map((span) => [span.spanContext().spanId, span.name]));
  console.log(
    spans.map((span) => ({
      name: span.name,
      parent: names.get(span.parentSpanContext?.spanId ?? ""),
      summary: span.attributes["db.query.summary"],
    })),
  );
  // [
  //   { name: "clickhouse CREATE", parent: "report", summary: "CREATE" },
  //   { name: "clickhouse INSERT", parent: "report", summary: "INSERT readings" },
  //   { name: "clickhouse QUERY", parent: "report", summary: "SELECT readings" },
  //   { name: "clickhouse DROP", parent: "report", summary: "DROP" },
  //   { name: "report", parent: undefined, summary: undefined }
  // ]
}
