import { clickhouseClient, csql, type Session } from "../index";
import { logicalMetrics, tempMetricScope, typeScenarioSchema } from "./fixtures";
import type { Equal, Expect, InferBuilderResult } from "./helpers";

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/session_typecheck",
  schema: typeScenarioSchema,
  session_max_concurrent_requests: 2,
});

const selectedMetrics = db
  .select({
    userId: logicalMetrics.userId,
    metricValue: logicalMetrics.metricValue,
  })
  .from(logicalMetrics);
const selectedMetricsIterator = selectedMetrics.iterator({
  query_id: "typed_select_iterator",
});
const selectedMetricsExecute = selectedMetrics.execute({
  query_id: "typed_select_execute",
});
const selectedMetricsCatch = selectedMetrics.catch((error: unknown) => {
  void error;
  return [] as Array<InferBuilderResult<typeof selectedMetrics>>;
});
const selectedMetricsFinally = selectedMetrics.finally(() => undefined);

type _SelectedMetricsType = Expect<
  Equal<InferBuilderResult<typeof selectedMetrics>, { userId: string; metricValue: string }>
>;
type _SelectedMetricsExecuteType = Expect<
  Equal<Awaited<typeof selectedMetricsExecute>, Array<{ userId: string; metricValue: string }>>
>;

const insertBuilder = db.insert(logicalMetrics).values({
  userId: "user_1",
  metricValue: "1.50000",
});
const insertExecute: Promise<undefined> = insertBuilder.execute({
  query_id: "typed_insert",
});
const insertCatch: Promise<undefined | "insert_failed"> = insertBuilder.catch(() => "insert_failed" as const);
const insertFinally: Promise<undefined> = insertBuilder.finally(() => undefined);

db.execute(csql`select 1`, { format: "JSON" });
db.stream(csql`select 1`, { format: "JSONEachRow" });
db.command(csql`optimize table logical_metrics`);
db.ping();
db.replicasStatus();
db.insertJsonEachRow(logicalMetrics, [
  {
    userId: "user_1",
    metricValue: "1.50000",
  },
]);
db.insertJsonEachRow("logical_metrics", [
  {
    metric_value: "1.50000",
    user_id: "user_1",
  },
]);

const noNullsDb = db.withSettings({ join_use_nulls: 0 as const, max_threads: 2 });
const noNullsSelect = noNullsDb.select({ userId: logicalMetrics.userId }).from(logicalMetrics);
type _NoNullsSelectType = Expect<Equal<InferBuilderResult<typeof noNullsSelect>, { userId: string }>>;

db.runInSession(
  async (session: Session<typeof typeScenarioSchema>) => {
    const sessionId: string = session.sessionId;
    await session.createTemporaryTable(tempMetricScope, { mode: "if_not_exists" });
    await session.createTemporaryTableRaw("tmp_metric_scope_raw", "(user_id String)");
    session.registerTempTable("tmp_manual_cleanup");
    await session.insertJsonEachRow(tempMetricScope, [{ userId: "user_1", groupId: 1 }]);
    const sessionRows = await session.select({ userId: tempMetricScope.userId }).from(tempMetricScope).execute();
    const sessionRawRows = await session.execute(csql`select 1`, { format: "JSON" });
    const sessionStream = session.stream(csql`select 1`, { format: "JSONEachRow" });
    await session.command(csql`select 1`);
    await session.ping();
    await session.replicasStatus();
    await session.withSettings({ join_use_nulls: 0 as const }).runInSession(async (childSession) => {
      const childId: string = childSession.sessionId;
      const childRows = await childSession.select({ userId: tempMetricScope.userId }).from(tempMetricScope);
      void childId;
      void childRows;
    });
    void sessionId;
    void sessionRows;
    void sessionRawRows;
    void sessionStream;
    return sessionRows;
  },
  {
    session_timeout: 30,
    session_check: 1,
    session_id: "session_typecheck_root",
    onCleanupError(errors, context) {
      const cleanupErrors: readonly unknown[] = errors;
      const cleanupSessionId: string = context.sessionId;
      void cleanupErrors;
      void cleanupSessionId;
    },
  },
);

// @ts-expect-error raw eager queries only support JSON output.
db.execute(csql`select 1`, { format: "JSONEachRow" });
// @ts-expect-error raw streaming queries only support JSONEachRow output.
db.stream(csql`select 1`, { format: "JSON" });
// @ts-expect-error raw query execution no longer accepts plain strings.
db.execute("select 1");
// @ts-expect-error raw command execution no longer accepts plain strings.
db.command("select 1");
// @ts-expect-error raw streaming no longer accepts plain strings.
db.stream("select 1");
// @ts-expect-error typed builder queries do not expose raw format overrides.
selectedMetrics.execute({ format: "JSON" });
// @ts-expect-error typed builder iterators do not expose raw format overrides.
selectedMetrics.iterator({ format: "JSONEachRow" });
// @ts-expect-error insert rows should reject unknown columns.
db.insert(logicalMetrics).values({ typo_name: "alice" });
clickhouseClient({
  databaseUrl: "http://localhost:8123/session_typecheck",
  schema: typeScenarioSchema,
  // @ts-expect-error client config no longer accepts session_timeout defaults.
  session_timeout: 30,
});
clickhouseClient({
  databaseUrl: "http://localhost:8123/session_typecheck",
  schema: typeScenarioSchema,
  // @ts-expect-error client config no longer accepts session_check defaults.
  session_check: 1,
});
clickhouseClient({
  databaseUrl: "http://localhost:8123/session_typecheck",
  schema: typeScenarioSchema,
  // @ts-expect-error session_max_concurrent_requests must be a number.
  session_max_concurrent_requests: "2",
});

void selectedMetricsIterator;
void selectedMetricsCatch;
void selectedMetricsFinally;
void insertExecute;
void insertCatch;
void insertFinally;
