import { ck, clickhouseClient } from "./ck-orm";
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

export const buildDefaultLeftJoinExample = () => {
  const commerceDb = createCommerceDb();
  const query = commerceDb
    .select()
    .from(customerInvoice)
    .leftJoin(orderRewardLog, ck.eq(customerInvoice.user_id, orderRewardLog.user_id));

  return {
    query,
  };
};

export const runDefaultLeftJoinExample = async () => {
  const { query } = buildDefaultLeftJoinExample();
  return query;
};

export const runClickHouseDefaultJoinExample = async () => {
  const commerceDb = createCommerceDb();
  const rawDefaultDb = commerceDb.withSettings({
    join_use_nulls: 0,
  });

  return rawDefaultDb
    .select()
    .from(customerInvoice)
    .leftJoin(orderRewardLog, ck.eq(customerInvoice.user_id, orderRewardLog.user_id));
};

export const buildExplicitSelectJoinExample = () => {
  const commerceDb = createCommerceDb();
  const query = commerceDb
    .select({
      userId: customerInvoice.user_id,
      invoiceId: customerInvoice.id,
      rewardEventId: orderRewardLog.id,
      rewardPoints: orderRewardLog.reward_points,
    })
    .from(customerInvoice)
    .leftJoin(orderRewardLog, ck.eq(customerInvoice.user_id, orderRewardLog.user_id));

  return {
    query,
  };
};
