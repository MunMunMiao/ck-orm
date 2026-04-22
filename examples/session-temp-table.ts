import { clickhouseClient } from "./ck-orm";
import { commerceSchema } from "./schema/commerce";

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

export const runSessionTempTableExample = async () => {
  const commerceDb = createCommerceDb();

  return commerceDb.runInSession(async (sessionDb) => {
    await sessionDb.createTemporaryTable("tmp_scope", "(user_id String)");
    await sessionDb.insertJsonEachRow("tmp_scope", [{ user_id: "user_100" }, { user_id: "user_200" }]);

    return sessionDb.execute(
      `
        SELECT user_id
        FROM order_reward_log
        WHERE user_id IN (SELECT user_id FROM tmp_scope)
      `,
      {
        format: "JSON",
      },
    );
  });
};
