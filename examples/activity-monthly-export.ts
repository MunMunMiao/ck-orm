import { ck, ckAlias, clickhouseClient, fn } from "./ck-orm";
import { orderRewardLog } from "./schema/commerce";

const createCommerceDb = () => {
  return clickhouseClient({
    host: "http://127.0.0.1:8123",
    database: "demo_store",
    username: "default",
    password: "<password>",
    clickhouse_settings: {
      max_execution_time: 10,
    },
  });
};

export const createMonthlyRewardSummaryQuery = () => {
  const commerceDb = createCommerceDb();
  const rewardLog = ckAlias(orderRewardLog, "orl");

  const recentRewardEvents = commerceDb.$with("recent_reward_events").as(
    commerceDb
      .select({
        userId: rewardLog.userId,
        campaignId: rewardLog.campaignId,
        month: fn.toStartOfMonth(fn.toDateTime(rewardLog.createdAt)).as("month"),
        orderId: rewardLog.orderId,
        rewardPoints: rewardLog.rewardPoints,
      })
      .from(rewardLog)
      .where(ck.and(ck.eq(rewardLog.peerdbIsDeleted, 0), ck.eq(rewardLog.channel, 1)))
      .final(),
  );

  return commerceDb
    .with(recentRewardEvents)
    .select({
      month: recentRewardEvents.month,
      userId: recentRewardEvents.userId,
      campaignId: recentRewardEvents.campaignId,
      orderCount: fn.count(recentRewardEvents.orderId).as("order_count"),
      totalRewardPoints: fn.sum(recentRewardEvents.rewardPoints).as("total_reward_points"),
      avgRewardPoints: fn.avg(recentRewardEvents.rewardPoints).as("avg_reward_points"),
    })
    .from(recentRewardEvents)
    .groupBy(recentRewardEvents.month, recentRewardEvents.userId, recentRewardEvents.campaignId)
    .orderBy(ck.desc(recentRewardEvents.month), ck.asc(recentRewardEvents.userId));
};

export const streamMonthlyRewardSummary = () => {
  return createMonthlyRewardSummaryQuery().iterator({
    query_id: "monthly_reward_summary",
    clickhouse_settings: {
      max_threads: 2,
    },
  });
};
