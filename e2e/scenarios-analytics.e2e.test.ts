import { expect, it } from "bun:test";
import { ck, ckSql, fn } from "./ck-orm";
import { createE2EDb, scenarioSchema } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

const { posthogEvents, metricaHits, muxVideoQoe, snowplowEvents } = scenarioSchema;

describeE2E("ck-orm e2e — product analytics scenarios", function describeAnalyticsScenarios() {
  it("PostHog events: funnel windowFunnel over a user session", async function testPosthogFunnel() {
    const db = createE2EDb();
    const rows = await db
      .select({
        // windowFunnel is a parameterized aggregate (`windowFunnel(86400)(...)`);
        // raw ckSql renders the call shape ClickHouse expects. The timestamp
        // column is DateTime64(6) but windowFunnel only accepts Date/DateTime/
        // unsigned int — cast it to DateTime first.
        steps: ck
          .expr<number>(
            ckSql`windowFunnel(86400)(toDateTime(${posthogEvents.timestamp}),
              ${posthogEvents.event} = 'user_signed_up',
              ${posthogEvents.event} = 'checkout_started',
              ${posthogEvents.event} = 'payment_succeeded')`,
            { decoder: (value) => Number(value), sqlType: "UInt8" },
          )
          .as("steps"),
      })
      .from(posthogEvents)
      .where(ck.and(ck.eq(posthogEvents.team_id, 1), ck.eq(posthogEvents.distinct_id, "distinct-1")))
      .groupBy(posthogEvents.distinct_id)
      .execute();
    expect(rows.length).toBe(1);
    expect(rows[0]?.steps).toBe(3);

    const eventCounts = await db
      .select({ event: posthogEvents.event, cnt: fn.count().as("cnt") })
      .from(posthogEvents)
      .where(ck.eq(posthogEvents.team_id, 1))
      .groupBy(posthogEvents.event)
      .execute();
    expect(eventCounts.length).toBe(3);
  });

  it("Metrica hits: top UTM campaigns and bounce metric", async function testMetricaHits() {
    const db = createE2EDb();
    const rows = await db
      .select({
        campaign: metricaHits.utm_campaign,
        hits: fn.count().as("hits"),
        uniqueUsers: fn.uniqExact(metricaHits.user_id).as("unique_users"),
      })
      .from(metricaHits)
      .where(ck.and(ck.eq(metricaHits.counter_id, 42), ck.ne(metricaHits.utm_campaign, "")))
      .groupBy(metricaHits.utm_campaign)
      .orderBy(ck.desc(fn.count()))
      .execute();
    expect(rows.length).toBe(1);
    const top = expectPresent(rows[0], "top campaign");
    expect(top.campaign).toBe("spring_sale");
    expect(top.hits).toBe(2);
  });

  it("Mux video QoE: sign-aware average rebuffer per CDN", async function testMuxQoe() {
    const db = createE2EDb();
    const rows = await db
      .select({
        cdn: muxVideoQoe.cdn,
        weightedRebuffers: ck
          .expr<string>(ckSql`sum(${muxVideoQoe.rebuffer_count} * ${muxVideoQoe.sign})`, {
            decoder: (value) => String(value),
            sqlType: "Int64",
          })
          .as("weighted_rebuffers"),
        signSum: fn.sum(muxVideoQoe.sign).as("sign_sum"),
      })
      .from(muxVideoQoe)
      .groupBy(muxVideoQoe.cdn)
      .orderBy(muxVideoQoe.cdn)
      .execute();
    expect(rows.length).toBe(2);
    const cdns = rows.map((row) => row.cdn).sort();
    expect(cdns).toEqual(["akamai", "cloudfront"]);
  });

  it("Snowplow events: top referer mediums", async function testSnowplowEvents() {
    const db = createE2EDb();
    const rows = await db
      .select({
        medium: snowplowEvents.refr_medium,
        pageViews: fn.count().as("page_views"),
        uniqueVisitors: fn.uniqExact(snowplowEvents.domain_userid).as("unique_visitors"),
      })
      .from(snowplowEvents)
      .groupBy(snowplowEvents.refr_medium)
      .orderBy(ck.desc(fn.count()))
      .execute();
    expect(rows.length).toBe(2);
    const mediums = rows.map((row) => row.medium).sort();
    expect(mediums).toEqual(["direct", "search"]);
  });
});
