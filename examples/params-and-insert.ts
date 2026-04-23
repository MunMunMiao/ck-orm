import { ck, clickhouseClient, csql } from "./ck-orm";
import { commerceSchema, customerInvoice, orderRewardLog } from "./schema/commerce";

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

export const buildDirectValueQueryExample = () => {
  const commerceDb = createCommerceDb();

  const query = commerceDb
    .select({
      userId: orderRewardLog.user_id,
      rewardPoints: orderRewardLog.reward_points,
    })
    .from(orderRewardLog)
    .where(ck.eq(orderRewardLog.user_id, "user_100"))
    .limit(10);

  return {
    query,
  };
};

export const runDirectValueQueryExample = async () => {
  const { query } = buildDirectValueQueryExample();
  return query;
};

export const runRawQueryParamsExample = async () => {
  const commerceDb = createCommerceDb();

  return commerceDb.execute(
    csql`select user_id, reward_points from order_reward_log where user_id = {user_id:String} limit {limit:Int64}`,
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
    invoice_number: "INV_EXAMPLE_001",
    user_id: "user_100",
    channel_id: 1,
    status: 1,
    subtotal_amount: "100.00000",
    fee_amount: "5.00000",
    total_amount: "105.00000",
    created_at: 1710000000,
    updated_at: 1710000000,
    _peerdb_synced_at: new Date("2026-04-21T00:00:00.000Z"),
    _peerdb_is_deleted: 0,
    _peerdb_version: 1n,
  });

  return {
    insert,
  };
};
