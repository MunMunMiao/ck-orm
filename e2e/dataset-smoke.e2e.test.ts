import { expect, it } from "bun:test";
import { ck, csql, fn } from "./ck-orm";
import { createE2EDb, datasetCounts, pets, quoteSnapshots, rewardEvents, tradeFills, users, webEvents } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

describeE2E("ck-orm e2e dataset smoke", function describeDatasetSmoke() {
  it("builds the seeded dataset with the expected row counts and invariants", async function testDatasetSmoke() {
    const db = createE2EDb();

    const [userCount] = await db.select({ total: fn.count(users.id) }).from(users);
    const [petCount] = await db.select({ total: fn.count(pets.id) }).from(pets);
    const [eventCount] = await db.select({ total: fn.count(webEvents.event_id) }).from(webEvents);
    const [tradeCount] = await db.select({ total: fn.count(tradeFills.trade_id) }).from(tradeFills);
    const [quoteCount] = await db.select({ total: fn.count(quoteSnapshots.symbol) }).from(quoteSnapshots);
    const [physicalCdcCount] = await db.select({ total: fn.count(rewardEvents.id) }).from(rewardEvents);
    const [logicalCdcCount] = await db
      .select({ total: fn.count(rewardEvents.id) })
      .from(rewardEvents)
      .final()
      .where(ck.eq(rewardEvents._peerdb_is_deleted, 0));

    expect(expectPresent(userCount, "userCount").total).toBe(datasetCounts.users);
    expect(expectPresent(petCount, "petCount").total).toBe(datasetCounts.pets);
    expect(expectPresent(eventCount, "eventCount").total).toBe(datasetCounts.webEvents);
    expect(expectPresent(tradeCount, "tradeCount").total).toBe(datasetCounts.tradeFills);
    expect(expectPresent(quoteCount, "quoteCount").total).toBe(datasetCounts.quoteSnapshots);
    const physicalCdcTotal = expectPresent(physicalCdcCount, "physicalCdcCount").total;
    const logicalCdcTotal = expectPresent(logicalCdcCount, "logicalCdcCount").total;
    expect(physicalCdcTotal).toBeGreaterThan(logicalCdcTotal);
    expect(physicalCdcTotal).toBeLessThanOrEqual(datasetCounts.rewardEventsPhysicalRows);
    expect(logicalCdcTotal).toBe(19_000);

    const [ownerStats] = await db.execute(csql`
      select
        uniqExact(owner_id) as distinct_owner_count,
        max(owner_id) as max_owner_id
      from ${pets}
    `);

    expect(expectPresent(ownerStats, "ownerStats")).toEqual({
      distinct_owner_count: "4000",
      max_owner_id: 4000,
    });

    const [eventSample] = await db
      .select({
        eventId: webEvents.event_id,
        userId: webEvents.user_id,
        country: webEvents.country,
        deviceType: webEvents.device_type,
      })
      .from(webEvents)
      .orderBy(webEvents.event_id)
      .limit(1);

    expect(expectPresent(eventSample, "eventSample")).toEqual({
      eventId: "1",
      userId: 1,
      country: "US",
      deviceType: "ios",
    });
  });
});
