import { trace } from "@opentelemetry/api";
import {
  type ClickHouseEndpointOptions,
  type ClickHouseORMInstrumentation,
  type ClickHouseORMLogger,
  clickhouseClient,
  csql,
} from "./ck-orm";
import { commerceSchema } from "./schema/commerce";

const logger: ClickHouseORMLogger = {
  trace(message, fields) {
    console.debug(message, fields);
  },
  debug(message, fields) {
    console.debug(message, fields);
  },
  info(message, fields) {
    console.info(message, fields);
  },
  warn(message, fields) {
    console.warn(message, fields);
  },
  error(message, fields) {
    console.error(message, fields);
  },
};

const instrumentation: ClickHouseORMInstrumentation = {
  onQueryStart(event) {
    console.log("clickhouse query started", event.operation, event.queryId);
  },
  onQuerySuccess(event) {
    console.log("clickhouse query finished", event.durationMs, event.rowCount);
  },
  onQueryError(event) {
    console.error("clickhouse query failed", event.operation, event.error);
  },
};

export const createInstrumentedCommerceDb = () => {
  return clickhouseClient({
    databaseUrl: "http://default:<password>@127.0.0.1:8123/demo_store",
    schema: commerceSchema,
    logger,
    logLevel: "info",
    instrumentation: [instrumentation],
    tracing: {
      tracer: trace.getTracer("ck-orm-examples"),
      includeStatement: false,
      includeRowCount: true,
    },
    application: "ck-orm-examples",
    clickhouse_settings: {
      max_execution_time: 10,
    },
  });
};

export const runEndpointAndRuntimeMethodsExample = async () => {
  const db = createInstrumentedCommerceDb();
  const reportDb = db.withSettings({
    max_execution_time: 30,
    join_use_nulls: 1,
  });

  await reportDb.ping();

  const rows = await reportDb.execute(csql`SELECT 1 AS one`, {
    query_id: "runtime_execute_example",
  });

  const streamedRows: Record<string, unknown>[] = [];
  for await (const row of reportDb.stream(csql`SELECT number FROM numbers(3)`, {
    format: "JSONEachRow",
    query_id: "runtime_stream_example",
  })) {
    streamedRows.push(row);
  }

  await reportDb.command(csql`SYSTEM FLUSH LOGS`, {
    query_id: "runtime_command_example",
  });

  return {
    rows,
    streamedRows,
  };
};

export const runReplicasStatusExample = async (options?: ClickHouseEndpointOptions) => {
  const db = createInstrumentedCommerceDb();
  await db.replicasStatus(options);
};
