// CDP RFM — see ./README.md.
import { ck, fn } from "../../ck-orm";
import { cdpOrders, cdpUserEvents } from "../../schema/scenarios";

export { cdpOrders, cdpUserEvents };

/** Total spent per user across paid orders. */
export const buildCdpTotalSpentExample = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        userId: cdpOrders.user_id,
        orderCount: fn.count().as("order_count"),
        totalSpent: fn.sum(cdpOrders.total_amount).as("total_spent"),
      })
      .from(cdpOrders)
      .where(ck.eq(cdpOrders.status, "paid"))
      .groupBy(cdpOrders.user_id),
});

/** Event-type counts for one user. */
export const buildCdpUserActivityExample = (userId: number) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        eventType: cdpUserEvents.event_type,
        cnt: fn.count().as("cnt"),
      })
      .from(cdpUserEvents)
      .where(ck.eq(cdpUserEvents.user_id, userId))
      .groupBy(cdpUserEvents.event_type),
});
