import { ck, ckSql, clickhouseClient } from "./ck-orm";
import { customerInvoice, orderRewardLog } from "./schema/commerce";

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

export const buildDirectValueQueryExample = () => {
  const commerceDb = createCommerceDb();

  const query = commerceDb
    .select({
      userId: orderRewardLog.userId,
      rewardPoints: orderRewardLog.rewardPoints,
    })
    .from(orderRewardLog)
    .where(ck.eq(orderRewardLog.userId, "user_100"))
    .limit(10);

  return {
    query,
  };
};

export const runDirectValueQueryExample = async () => {
  const { query } = buildDirectValueQueryExample();
  return query.execute();
};

export const runRawQueryParamsExample = async () => {
  const commerceDb = createCommerceDb();

  return commerceDb.execute(
    ckSql`select user_id, reward_points from order_reward_log where user_id = {user_id:String} limit {limit:Int64}`,
    {
      query_params: {
        user_id: "user_100",
        limit: 10,
      },
    },
  );
};

export const buildInsertExample = () => {
  const commerceDb = createCommerceDb();
  const insert = commerceDb.insert(customerInvoice).values({
    id: 1,
    invoiceNumber: "INV_EXAMPLE_001",
    userId: "user_100",
    channelId: 1,
    status: 1,
    subtotalAmount: "100.00000",
    feeAmount: "5.00000",
    totalAmount: "105.00000",
    createdAt: 1710000000,
    updatedAt: 1710000000,
    peerdbSyncedAt: new Date("2026-04-21T00:00:00.000Z"),
    peerdbIsDeleted: 0,
    peerdbVersion: "1",
  });

  return {
    insert,
  };
};
