import { ck, clickhouseClient, fn } from "./ck-orm";
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

export const buildRawExpressionExample = () => {
  const commerceDb = createCommerceDb();

  const query = commerceDb
    .select({
      userId: orderRewardLog.user_id,
      month: fn.toStartOfMonth(orderRewardLog.created_at).as("month"),
      createdAtText: fn.toString(orderRewardLog.created_at).as("created_at_text"),
    })
    .from(orderRewardLog);

  return {
    query,
  };
};

export const runRawQueryExample = async () => {
  const commerceDb = createCommerceDb();
  const threshold = 10;

  return commerceDb.execute(ck.sql`
    select
      ${orderRewardLog.user_id},
      ${fn.sum(orderRewardLog.reward_points)} as total_reward_points
    from ${orderRewardLog}
    where ${orderRewardLog.id} > ${threshold}
    group by ${orderRewardLog.user_id}
  `);
};

export const runPlainStringRawQueryExample = async () => {
  const commerceDb = createCommerceDb();
  return commerceDb.execute(ck.sql("SELECT 1 AS one"));
};

export const buildTableFunctionExample = () => {
  const commerceDb = createCommerceDb();
  const numbers = fn.table.call("numbers", 10).as("n");

  const query = commerceDb
    .select({
      total: fn.count(),
    })
    .from(numbers);

  return {
    query,
  };
};
