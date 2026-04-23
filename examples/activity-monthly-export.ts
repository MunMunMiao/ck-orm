import { alias, ck, clickhouseClient, fn } from "./ck-orm";
import { commerceSchema, orderRewardLog } from "./schema/commerce";

const createCommerceDb = () => {
  return clickhouseClient({
    host: "http://127.0.0.1:8123",
    database: "demo_store",
    username: "default",
    password: "<password>",
    schema: commerceSchema,
    clickhouse_settings: {
      max_execution_time: 10,
    },
  });
};

export const createMonthlyRewardSummaryQuery = () => {
  const commerceDb = createCommerceDb();
  const rewardLog = alias(orderRewardLog, "orl");

  const recentRewardEvents = commerceDb.$with("recent_reward_events").as(
    commerceDb
      .select({
        userId: rewardLog.user_id,
        campaignId: rewardLog.campaign_id,
        month: fn.toStartOfMonth(fn.toDateTime(rewardLog.created_at)).as("month"),
        orderId: rewardLog.order_id,
        rewardPoints: rewardLog.reward_points,
      })
      .from(rewardLog)
      .where(ck.and(ck.eq(rewardLog._peerdb_is_deleted, 0), ck.eq(rewardLog.channel, 1)))
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
