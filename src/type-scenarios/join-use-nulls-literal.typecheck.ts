import type { ClickHouseORMClient } from "../index";
import { ck, clickhouseClient } from "../index";
import { activityMetricLog, logicalMetrics } from "./fixtures";
import type { Equal, Expect, InferBuilderResult } from "./helpers";

type NullableJoin = { userId: string; groupId: number | null };
type NonNullJoin = { userId: string; groupId: number };

const leftJoinGroup = <TJoinUseNulls extends 0 | 1>(client: Pick<ClickHouseORMClient<TJoinUseNulls>, "select">) =>
  client
    .select({
      userId: logicalMetrics.userId,
      groupId: activityMetricLog.groupId,
    })
    .from(logicalMetrics)
    .leftJoin(activityMetricLog, ck.eq(logicalMetrics.userId, activityMetricLog.userId));

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/join_use_nulls_typecheck",
});

const literalZeroJoin = leftJoinGroup(db.withSettings({ join_use_nulls: 0 }));
type _LiteralZeroJoin = Expect<Equal<InferBuilderResult<typeof literalZeroJoin>, NonNullJoin>>;

const literalOneJoin = leftJoinGroup(db.withSettings({ join_use_nulls: 1 }));
type _LiteralOneJoin = Expect<Equal<InferBuilderResult<typeof literalOneJoin>, NullableJoin>>;

const omittedJoin = leftJoinGroup(db.withSettings({ max_threads: 2 }));
type _OmittedJoin = Expect<Equal<InferBuilderResult<typeof omittedJoin>, NullableJoin>>;

const joinUseNulls: number = 0;
const widenedJoin = leftJoinGroup(db.withSettings({ join_use_nulls: joinUseNulls }));
type _WidenedJoin = Expect<Equal<InferBuilderResult<typeof widenedJoin>, NullableJoin>>;

const configured = clickhouseClient({
  databaseUrl: "http://localhost:8123/join_use_nulls_typecheck",
  clickhouse_settings: { join_use_nulls: 0 },
});
const configuredJoin = leftJoinGroup(configured);
type _ConfiguredJoin = Expect<Equal<InferBuilderResult<typeof configuredJoin>, NonNullJoin>>;

const configuredDefault = clickhouseClient({
  databaseUrl: "http://localhost:8123/join_use_nulls_typecheck",
  clickhouse_settings: { max_threads: 2 },
});
const configuredDefaultJoin = leftJoinGroup(configuredDefault);
type _ConfiguredDefaultJoin = Expect<Equal<InferBuilderResult<typeof configuredDefaultJoin>, NullableJoin>>;

db.withSettings({ join_use_nulls: 0 }).runInSession(async (session) => {
  const sessionJoin = leftJoinGroup(session);
  type _SessionInheritedJoin = Expect<Equal<InferBuilderResult<typeof sessionJoin>, NonNullJoin>>;

  const sessionSettingsJoin = leftJoinGroup(session.withSettings({ join_use_nulls: 0 }));
  type _SessionSettingsJoin = Expect<Equal<InferBuilderResult<typeof sessionSettingsJoin>, NonNullJoin>>;
  void sessionJoin;
  void sessionSettingsJoin;
});
