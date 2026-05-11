import { expect, it } from "bun:test";
import { ck, ckSql, fn } from "./ck-orm";
import { createE2EDb, scenarioSchema } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

const { iotTelemetry, gameEvents, meterEvents, mlUserEvents } = scenarioSchema;

describeE2E("ck-orm e2e — IoT / Gaming / SaaS / ML scenarios", function describeIotSaasScenarios() {
  it("IoT telemetry: temperature anomaly detection over threshold", async function testIotAnomalies() {
    const db = createE2EDb();
    const rows = await db
      .select({
        deviceId: iotTelemetry.device_id,
        anomalies: fn.count().as("anomalies"),
        peakTemp: fn.max(iotTelemetry.value_float).as("peak_temp"),
      })
      .from(iotTelemetry)
      .where(ck.and(ck.eq(iotTelemetry.metric_name, "temperature"), ck.gt(iotTelemetry.value_float, 80.0)))
      .groupBy(iotTelemetry.device_id)
      .execute();
    expect(rows.length).toBe(1);
    const anomaly = expectPresent(rows[0], "anomaly row");
    expect(anomaly.deviceId).toBe("CNC-LINE3-07");
    expect(anomaly.anomalies).toBe(1);
  });

  it("Game events: ARPU per ab_variant", async function testGameArpu() {
    const db = createE2EDb();
    const rows = await db
      .select({
        variant: gameEvents.ab_variant,
        users: fn.uniqExact(gameEvents.player_id).as("users"),
        totalRevenue: fn.sum(gameEvents.revenue_usd).as("total_revenue"),
      })
      .from(gameEvents)
      .where(ck.eq(gameEvents.event_type, "purchase"))
      .groupBy(gameEvents.ab_variant)
      .orderBy(gameEvents.ab_variant)
      .execute();
    expect(rows.length).toBe(2);
    const treatment = rows.find((row) => row.variant === "treatment");
    expect(expectPresent(treatment, "treatment variant").users).toBe(1);
  });

  it("Game events: level pass rate from level_complete events", async function testGameLevels() {
    const db = createE2EDb();
    const rows = await db
      .select({
        level: gameEvents.level_id,
        wins: fn.countIf(ck.eq(gameEvents.is_win, true)).as("wins"),
        attempts: fn.count().as("attempts"),
      })
      .from(gameEvents)
      .where(ck.eq(gameEvents.event_type, "level_complete"))
      .groupBy(gameEvents.level_id)
      .orderBy(gameEvents.level_id)
      .execute();
    expect(rows.length).toBe(2);
    const level1 = rows.find((row) => row.level === 1);
    expect(expectPresent(level1, "level 1").wins).toBe(1);
  });

  it("Meter events: usage roll-up per customer + meter", async function testMeterEvents() {
    const db = createE2EDb();
    const rows = await db
      .select({
        customer: meterEvents.customer_id,
        meter: meterEvents.meter_slug,
        usage: fn.sum(meterEvents.value).as("usage"),
        events: fn.count().as("events"),
      })
      .from(meterEvents)
      .groupBy(meterEvents.customer_id, meterEvents.meter_slug)
      .orderBy(meterEvents.customer_id, meterEvents.meter_slug)
      .execute();
    expect(rows.length).toBe(4);
    const alphaApi = rows.find((row) => row.customer === "cus_alpha" && row.meter === "api_calls");
    expect(expectPresent(alphaApi, "cus_alpha api_calls").events).toBe(2);
    const alphaTokens = rows.find((row) => row.customer === "cus_alpha" && row.meter === "tokens");
    expect(expectPresent(alphaTokens, "cus_alpha tokens").usage).toBe(2400);
  });

  it("ML feature: unique IPs and bounce rate per domain over 1h window", async function testMlFeatureStore() {
    const db = createE2EDb();
    const rows = await db
      .select({
        domain: mlUserEvents.domain,
        uniqueIps: fn.uniqExact(mlUserEvents.client_ip).as("unique_ips"),
        bounceRate: ck
          .expr<number>(ckSql`avg(${mlUserEvents.is_bounce})`, {
            decoder: (value) => Number(value),
            sqlType: "Float64",
          })
          .as("bounce_rate"),
        events: fn.count().as("events"),
      })
      .from(mlUserEvents)
      .groupBy(mlUserEvents.domain)
      .execute();
    expect(rows.length).toBe(1);
    const domain = expectPresent(rows[0], "domain row");
    expect(domain.uniqueIps).toBe(2);
    expect(domain.events).toBe(3);
  });
});
