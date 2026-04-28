import { clickhouseClient, csql, fn } from "./ck-orm";
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
      userId: orderRewardLog.userId,
      month: fn.toStartOfMonth(orderRewardLog.createdAt).as("month"),
      createdAtText: fn.toString(orderRewardLog.createdAt).as("created_at_text"),
    })
    .from(orderRewardLog);

  return {
    query,
  };
};

export const runRawQueryExample = async () => {
  const commerceDb = createCommerceDb();
  const threshold = 10;

  return commerceDb.execute(csql`
    select
      ${orderRewardLog.userId},
      ${fn.sum(orderRewardLog.rewardPoints)} as total_reward_points
    from ${orderRewardLog}
    where ${orderRewardLog.id} > ${threshold}
    group by ${orderRewardLog.userId}
  `);
};

export const runTaggedTemplateRawQueryExample = async () => {
  const commerceDb = createCommerceDb();
  return commerceDb.execute(csql`SELECT 1 AS one`);
};

export const runIdentifierQueryExample = async () => {
  const commerceDb = createCommerceDb();
  const selectedColumns = csql.join([csql.identifier("user_id"), csql.identifier("reward_points")], ", ");

  return commerceDb.execute(
    csql`
      SELECT ${selectedColumns}
      FROM ${csql.identifier("order_reward_log")}
      WHERE ${csql.identifier("region")} = ${"AU"}
      LIMIT ${10}
    `,
  );
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
