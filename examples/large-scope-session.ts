import { ckSql, ckTable, ckType, clickhouseClient } from "./ck-orm";
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

export const exportRewardSummaryForLargeUserScope = async (
  userIds: string[],
  onRow: (row: Record<string, unknown>) => Promise<void> | void,
) => {
  const commerceDb = createCommerceDb();
  const tmpUserScope = ckTable("tmp_user_scope", {
    user_id: ckType.string(),
  });

  return commerceDb.runInSession(async (sessionDb) => {
    // Temporary tables stay scoped to this Session and disappear after cleanup.
    await sessionDb.createTemporaryTable(tmpUserScope);
    await sessionDb.insertJsonEachRow(
      tmpUserScope,
      userIds.map((user_id) => ({ user_id })),
      {
        query_id: "reward_scope_seed",
      },
    );

    const scopeSummary = await sessionDb.execute(ckSql`
      select
        count() as scoped_user_count
      from tmp_user_scope
    `);

    for await (const row of sessionDb.stream(
      ckSql`
        SELECT
          user_id,
          sum(reward_points) AS total_reward_points,
          count() AS total_rows
        FROM order_reward_log
        WHERE user_id IN (SELECT user_id FROM tmp_user_scope)
        GROUP BY user_id
        ORDER BY total_reward_points DESC
      `,
      {
        format: "JSONEachRow",
        query_id: "reward_scope_export",
      },
    )) {
      await onRow(row);
    }

    return scopeSummary;
  });
};
