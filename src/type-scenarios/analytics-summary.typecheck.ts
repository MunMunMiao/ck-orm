import { ck, ckAlias, ckSql, clickhouseClient, fn, type InferInsertModel, type InferSelectModel } from "../index";
import { activityMetricLog, logicalMetrics, typeScenarioSchema } from "./fixtures";
import type { Equal, Expect, InferBuilderResult } from "./helpers";

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/analytics_typecheck",
  schema: typeScenarioSchema,
});

type _LogicalSelectKeys = Expect<
  Equal<
    InferSelectModel<typeof logicalMetrics>,
    {
      userId: string;
      metricValue: string;
      createdAt: Date;
      tags: string[];
    }
  >
>;
type _LogicalInsertKeys = Expect<
  Equal<InferInsertModel<typeof logicalMetrics>, InferSelectModel<typeof logicalMetrics>>
>;

db.insert(logicalMetrics).values({
  userId: "user_1",
  metricValue: "10.50000",
  createdAt: new Date("2026-04-24T00:00:00.000Z"),
  tags: ["vip"],
});
db.insert(logicalMetrics).values([{ userId: "user_2" }]);
// @ts-expect-error insert values must use logical schema keys, not physical column names.
db.insert(logicalMetrics).values({ user_id: "user_1", metric_value: "10.50000" });

const metricLog = ckAlias(activityMetricLog, "aml");
const aliasedLogicalMetrics = ckAlias(logicalMetrics, "lr");
const aliasLogicalSelect = db.select({ userId: aliasedLogicalMetrics.userId }).from(aliasedLogicalMetrics);
type _AliasLogicalSelectType = Expect<Equal<InferBuilderResult<typeof aliasLogicalSelect>, { userId: string }>>;

const monthlyMetrics = db.$with("monthly_metrics").as(
  db
    .select({
      month: fn.toStartOfMonth(metricLog.createdAt).as("month"),
      userId: metricLog.userId,
      groupId: metricLog.groupId,
      itemId: metricLog.itemId,
      metricValue: metricLog.metricValue,
      activeFlag: ck.expr<number>(ckSql`1`, { decoder: (value) => Number(value), sqlType: "UInt8" }).as("active_flag"),
    })
    .from(metricLog)
    .where(
      ck.and(
        ck.eq(metricLog._peerdb_is_deleted, 0),
        ck.inArray(metricLog.status, [1, 2]),
        ck.containsIgnoreCase(metricLog.eventType, "metric"),
      ),
    )
    .final(),
);

const summaryQuery = db
  .with(monthlyMetrics)
  .select({
    month: monthlyMetrics.month,
    userId: monthlyMetrics.userId,
    groupId: monthlyMetrics.groupId,
    itemCount: fn.count(monthlyMetrics.itemId).toSafe().as("item_count"),
    uniqueItems: fn.uniqExact(monthlyMetrics.itemId).as("unique_items"),
    totalMetricValue: fn.sum(monthlyMetrics.metricValue).as("total_metric_value"),
    averageMetricValue: fn.avg(monthlyMetrics.metricValue).as("average_metric_value"),
    latestMetricValue: fn.max<string>(monthlyMetrics.metricValue).as("latest_metric_value"),
  })
  .from(monthlyMetrics)
  .groupBy(monthlyMetrics.month, monthlyMetrics.userId, monthlyMetrics.groupId)
  .having(ck.gt(fn.count(monthlyMetrics.itemId), 0))
  .orderBy(ck.desc(monthlyMetrics.month), ck.asc(monthlyMetrics.userId))
  .limit(100)
  .offset(20);

type _AnalyticsSummaryType = Expect<
  Equal<
    InferBuilderResult<typeof summaryQuery>,
    {
      month: Date;
      userId: string;
      groupId: number;
      itemCount: string;
      uniqueItems: number;
      totalMetricValue: number | string;
      averageMetricValue: number;
      latestMetricValue: string;
    }
  >
>;

const noNullsDb = db.withSettings({ join_use_nulls: 0 as const });
const defaultJoin = db
  .select({
    userId: logicalMetrics.userId,
    metricValue: monthlyMetrics.metricValue,
  })
  .from(logicalMetrics)
  .leftJoin(monthlyMetrics, ck.eq(logicalMetrics.userId, monthlyMetrics.userId));
const noNullsJoin = noNullsDb
  .with(monthlyMetrics)
  .select({
    userId: logicalMetrics.userId,
    metricValue: monthlyMetrics.metricValue,
  })
  .from(logicalMetrics)
  .leftJoin(monthlyMetrics, ck.eq(logicalMetrics.userId, monthlyMetrics.userId));

type _AnalyticsDefaultLeftJoinType = Expect<
  Equal<InferBuilderResult<typeof defaultJoin>, { userId: string; metricValue: string | null }>
>;
type _AnalyticsNoNullsLeftJoinType = Expect<
  Equal<InferBuilderResult<typeof noNullsJoin>, { userId: string; metricValue: string }>
>;
