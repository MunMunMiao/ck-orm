import { expect, it } from "bun:test";
import { ck, fn } from "./ck-orm";
import { createE2EDb, scenarioSchema } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

const {
  cloudflareHttpRequests,
  growthbookExposures,
  growthbookConversions,
  rtbAdImpressions,
  rtbAdClicks,
  mailchimpEmailEvents,
  cdpUserEvents,
  cdpOrders,
} = scenarioSchema;

describeE2E("ck-orm e2e — marketing & e-commerce scenarios", function describeMarketingScenarios() {
  it("Cloudflare HTTP requests: SummingMergeTree pre-aggregated counts per country", async function testCloudflareSumming() {
    const db = createE2EDb();
    const rows = await db
      .select({
        country: cloudflareHttpRequests.country,
        totalRequests: fn.sum(cloudflareHttpRequests.requests).as("total_requests"),
        totalBytes: fn.sum(cloudflareHttpRequests.bytes).as("total_bytes"),
      })
      .from(cloudflareHttpRequests)
      .where(ck.eq(cloudflareHttpRequests.zone_id, 12345))
      .groupBy(cloudflareHttpRequests.country)
      .orderBy(ck.desc(fn.sum(cloudflareHttpRequests.requests)))
      .execute();
    expect(rows.length).toBe(2);
    const us = rows.find((row) => row.country === "US");
    expect(expectPresent(us, "US row").totalRequests).toBe("2200");
  });

  it("GrowthBook: exposures join conversions for conversion rate per variation", async function testGrowthbookConversion() {
    const db = createE2EDb();
    const rows = await db
      .select({
        variation: growthbookExposures.variation_id,
        exposed: fn.uniqExact(growthbookExposures.user_id).as("exposed"),
        purchases: fn.uniqExact(growthbookConversions.user_id).as("purchases"),
      })
      .from(growthbookExposures)
      .leftJoin(growthbookConversions, ck.eq(growthbookExposures.user_id, growthbookConversions.user_id))
      .where(ck.eq(growthbookExposures.experiment_id, "checkout_v2"))
      .groupBy(growthbookExposures.variation_id)
      .orderBy(growthbookExposures.variation_id)
      .execute();
    expect(rows.length).toBe(2);
    const treatment = rows.find((row) => row.variation === "treatment");
    expect(expectPresent(treatment, "treatment variation").exposed).toBeGreaterThanOrEqual(2);
  });

  it("RTB ads: impression → click join with CTR per campaign", async function testRtbCtr() {
    const db = createE2EDb();
    const rows = await db
      .select({
        campaign: rtbAdImpressions.campaign_id,
        impressions: fn.count().as("impressions"),
        clicks: fn.countIf(ck.isNotNull(rtbAdClicks.click_id)).as("clicks"),
      })
      .from(rtbAdImpressions)
      .leftJoin(rtbAdClicks, ck.eq(rtbAdImpressions.impression_id, rtbAdClicks.impression_id))
      .groupBy(rtbAdImpressions.campaign_id)
      .execute();
    expect(rows.length).toBe(1);
    const campaign = expectPresent(rows[0], "campaign row");
    expect(campaign.impressions).toBe(3);
    expect(campaign.clicks).toBe(2);
  });

  it("Email events: campaign funnel — sent / delivered / opened / clicked", async function testEmailFunnel() {
    const db = createE2EDb();
    const rows = await db
      .select({
        sent: fn.countIf(ck.eq(mailchimpEmailEvents.event_type, "sent")).as("sent"),
        delivered: fn.countIf(ck.eq(mailchimpEmailEvents.event_type, "delivered")).as("delivered"),
        opened: fn.countIf(ck.eq(mailchimpEmailEvents.event_type, "opened")).as("opened"),
        clicked: fn.countIf(ck.eq(mailchimpEmailEvents.event_type, "clicked")).as("clicked"),
        bounced: fn.countIf(ck.eq(mailchimpEmailEvents.event_type, "bounced")).as("bounced"),
      })
      .from(mailchimpEmailEvents)
      .where(ck.eq(mailchimpEmailEvents.campaign_id, 1))
      .execute();
    const summary = expectPresent(rows[0], "email funnel");
    expect(summary.sent).toBe(2);
    expect(summary.delivered).toBe(1);
    expect(summary.opened).toBe(1);
    expect(summary.clicked).toBe(1);
    expect(summary.bounced).toBe(1);
  });

  it("CDP: RFM-style aggregate per user across user_events + orders", async function testCdpRfm() {
    const db = createE2EDb();
    const userEventCount = await db
      .select({ total: fn.count().as("total") })
      .from(cdpUserEvents)
      .where(ck.eq(cdpUserEvents.user_id, 1001))
      .execute();
    expect(expectPresent(userEventCount[0], "user 1001 events").total).toBe(4);

    const orders = await db
      .select({
        userId: cdpOrders.user_id,
        orderCount: fn.count().as("order_count"),
        totalSpent: fn.sum(cdpOrders.total_amount).as("total_spent"),
      })
      .from(cdpOrders)
      .where(ck.eq(cdpOrders.status, "paid"))
      .groupBy(cdpOrders.user_id)
      .orderBy(ck.desc(fn.sum(cdpOrders.total_amount)))
      .execute();
    expect(orders.length).toBeGreaterThanOrEqual(2);
    const big = expectPresent(orders[0], "biggest spender");
    expect(big.orderCount).toBeGreaterThanOrEqual(1);
  });
});
