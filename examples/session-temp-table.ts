import { ckTable, ckType, clickhouseClient, csql } from "./ck-orm";
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
  const tmpScope = ckTable("tmp_scope", {
    user_id: ckType.string(),
  });

  return commerceDb.runInSession(async (sessionDb) => {
    // Temporary tables live only inside this Session and are cleaned up automatically.
    await sessionDb.createTemporaryTable(tmpScope);
    await sessionDb.insertJsonEachRow(tmpScope, [{ user_id: "user_100" }, { user_id: "user_200" }]);

    return sessionDb.execute(
      csql`
        SELECT user_id
        FROM order_reward_log
        WHERE user_id IN (SELECT user_id FROM tmp_scope)
      `,
    );
  });
};
