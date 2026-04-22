import { alias, and, clickhouseClient, desc, eq, fn, inArray } from "./ck-orm";
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
  const rewardLog = alias(orderRewardLog, "orl");

  const query = commerceDb
    .select({
      userId: rewardLog.user_id,
      totalRewardPoints: fn.sum(rewardLog.reward_points).as("total_reward_points"),
      activeUsers: fn.uniqExact(rewardLog.user_id),
    })
    .from(rewardLog)
    .where(and(eq(rewardLog.user_id, "user_100"), inArray(rewardLog.channel, [1, 2])))
    .orderBy(desc(rewardLog.created_at))
    .limit(20)
    .offset(0)
    .final();

  return {
    query,
  };
};

export const runRewardLeaderboardExample = async () => {
  const { query } = buildRewardLeaderboardExample();
  return query;
};
