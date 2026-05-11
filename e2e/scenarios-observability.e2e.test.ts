import { expect, it } from "bun:test";
import { ck, fn } from "./ck-orm";
import { createE2EDb, scenarioSchema } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

const { clickhouseLogPlatform, signozTraces, otelTraces, signozMetricsSamples, highlightLogs, uberSchemaAgnosticLogs } =
  scenarioSchema;

describeE2E("ck-orm e2e — observability scenarios", function describeObservabilityScenarios() {
  it("ClickHouse OTel log platform: top services by error count", async function testClickhouseLogPlatform() {
    const db = createE2EDb();
    const rows = await db
      .select({
        service: clickhouseLogPlatform.service_name,
        errorCount: fn.count().as("error_count"),
      })
      .from(clickhouseLogPlatform)
      .where(ck.inArray(clickhouseLogPlatform.severity_text, ["ERROR", "CRITICAL", "FATAL"]))
      .groupBy(clickhouseLogPlatform.service_name)
      .orderBy(ck.desc(fn.count()))
      .execute();
    expect(rows.length).toBeGreaterThan(0);
    const payments = rows.find((row) => row.service === "payments-service");
    expect(expectPresent(payments, "payments-service row").errorCount).toBeGreaterThan(0);
  });

  it("SigNoz traces: error counts per service grouped by service name", async function testSignozTraceErrors() {
    const db = createE2EDb();
    // fn.countIf evaluates the predicate inline in SQL; we can't pass a plain
    // boolean column directly here because ck-orm builds the SQL via a generic
    // value compile path that rejects bare expressions. Use ck.eq instead.
    const rows = await db
      .select({
        service: signozTraces.service_name,
        errors: fn.countIf(ck.eq(signozTraces.has_error, true)).as("errors"),
        total: fn.count().as("total"),
      })
      .from(signozTraces)
      .groupBy(signozTraces.service_name)
      .orderBy(signozTraces.service_name)
      .execute();
    expect(rows.length).toBeGreaterThan(0);
    const payments = rows.find((row) => row.service === "payments-svc");
    expect(expectPresent(payments, "payments-svc trace row").errors).toBeGreaterThan(0);

    const avgDuration = await db
      .select({
        service: signozTraces.service_name,
        avgNs: fn.avg(signozTraces.duration_nano).as("avg_ns"),
      })
      .from(signozTraces)
      .groupBy(signozTraces.service_name)
      .execute();
    expect(avgDuration.length).toBeGreaterThan(0);
  });

  it("OTel traces: trace stitching by trace_id", async function testOtelTraceStitching() {
    const db = createE2EDb();
    const rows = await db
      .select()
      .from(otelTraces)
      .where(ck.eq(otelTraces.trace_id, "otel-trace-1"))
      .orderBy(otelTraces.timestamp)
      .execute();
    expect(rows.length).toBe(2);
    expect(rows[0]?.span_kind).toBe("SERVER");
    expect(rows[1]?.span_kind).toBe("CLIENT");
    expect(rows[1]?.parent_span_id).toBe("otel-span-1");
  });

  it("SigNoz metric samples: time-bucket aggregation over fingerprint", async function testSignozMetrics() {
    const db = createE2EDb();
    const rows = await db
      .select({
        fingerprint: signozMetricsSamples.fingerprint,
        samples: fn.count().as("samples"),
        avgValue: fn.avg(signozMetricsSamples.value).as("avg_value"),
      })
      .from(signozMetricsSamples)
      .where(ck.eq(signozMetricsSamples.metric_name, "http_requests_total"))
      .groupBy(signozMetricsSamples.fingerprint)
      .orderBy(ck.desc(fn.count()))
      .execute();
    expect(rows.length).toBe(2);
    const top = rows[0];
    expect(expectPresent(top, "top metric row").samples).toBeGreaterThanOrEqual(3);
  });

  it("Highlight.io logs: severity breakdown and trace correlation", async function testHighlightLogs() {
    const db = createE2EDb();
    const breakdown = await db
      .select({
        severity: highlightLogs.severity_text,
        cnt: fn.count().as("cnt"),
      })
      .from(highlightLogs)
      .groupBy(highlightLogs.severity_text)
      .execute();
    expect(breakdown.length).toBeGreaterThanOrEqual(2);

    const errorLog = await db.select().from(highlightLogs).where(ck.eq(highlightLogs.trace_id, "hl-trace-1")).execute();
    expect(errorLog.length).toBe(1);
    expect(errorLog[0]?.severity_text).toBe("ERROR");
  });

  it("Uber schema-agnostic logs: materialized request_id is searchable", async function testUberLogs() {
    const db = createE2EDb();
    const tripStart = await db
      .select()
      .from(uberSchemaAgnosticLogs)
      .where(ck.eq(uberSchemaAgnosticLogs.request_id, "req-aaa"))
      .execute();
    expect(tripStart.length).toBe(1);
    expect(tripStart[0]?._namespace).toBe("rides");
    expect(tripStart[0]?.user_id).toBe("u-1001");

    const allRides = await db
      .select({ namespaceName: uberSchemaAgnosticLogs._namespace, total: fn.count().as("total") })
      .from(uberSchemaAgnosticLogs)
      .groupBy(uberSchemaAgnosticLogs._namespace)
      .execute();
    const rides = allRides.find((row) => row.namespaceName === "rides");
    expect(expectPresent(rides, "rides namespace").total).toBe(2);
  });
});
