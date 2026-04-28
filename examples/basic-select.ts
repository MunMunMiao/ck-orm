import { ck, ckAlias, clickhouseClient, fn } from "./ck-orm";
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

export const buildRewardLeaderboardExample = () => {
  const commerceDb = createCommerceDb();
  const rewardLog = ckAlias(orderRewardLog, "orl");
  const totalRewardPoints = fn.sum(rewardLog.rewardPoints).as("total_reward_points");

  const query = commerceDb
    .select({
      userId: rewardLog.userId,
      totalRewardPoints,
      rewardEventCount: fn.count().as("reward_event_count"),
      campaignCount: fn.uniqExact(rewardLog.campaignId).as("campaign_count"),
    })
    .from(rewardLog)
    .where(ck.and(ck.eq(rewardLog.peerdbIsDeleted, 0), ck.inArray(rewardLog.channel, [1, 2])))
    .groupBy(rewardLog.userId)
    .orderBy(ck.desc(totalRewardPoints))
    .limit(20)
    .final();

  return {
    query,
  };
};

export const runRewardLeaderboardExample = async () => {
  const { query } = buildRewardLeaderboardExample();
  return query.execute();
};
