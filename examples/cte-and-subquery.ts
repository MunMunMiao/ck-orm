import { clickhouseClient, desc, eq, fn } from "./ck-orm";
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

export const buildRewardSummaryWithLatestEventExample = () => {
  const commerceDb = createCommerceDb();

  const rankedUsers = commerceDb.$with("ranked_users").as(
    commerceDb
      .select({
        userId: orderRewardLog.user_id,
        totalRewardPoints: fn.sum(orderRewardLog.reward_points).as("total_reward_points"),
      })
      .from(orderRewardLog)
      .groupBy(orderRewardLog.user_id),
  );

  const latestRewardEvent = commerceDb
    .select({
      userId: orderRewardLog.user_id,
      createdAt: orderRewardLog.created_at,
    })
    .from(orderRewardLog)
    .orderBy(desc(orderRewardLog.created_at))
    .limit(10)
    .as("latest_reward_event");

  const query = commerceDb
    .with(rankedUsers)
    .select({
      userId: rankedUsers.userId,
      totalRewardPoints: rankedUsers.totalRewardPoints,
      latestCreatedAt: latestRewardEvent.createdAt,
    })
    .from(rankedUsers)
    .leftJoin(latestRewardEvent, eq(rankedUsers.userId, latestRewardEvent.userId));

  return {
    query,
  };
};

export const runRewardSummaryWithLatestEventExample = async () => {
  const { query } = buildRewardSummaryWithLatestEventExample();
  return query;
};
