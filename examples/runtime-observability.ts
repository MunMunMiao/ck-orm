import { trace } from "@opentelemetry/api";
import {
  type ClickHouseEndpointOptions,
  type ClickHouseORMInstrumentation,
  type ClickHouseORMLogger,
  ckSql,
  clickhouseClient,
} from "./ck-orm";

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

export const createInstrumentedProbeDb = () => {
  return clickhouseClient({
    databaseUrl: "http://default:<password>@127.0.0.1:8123/telemetry_lab",
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
  const db = createInstrumentedProbeDb();
  const telemetryDb = db.withSettings({
    max_execution_time: 30,
    join_use_nulls: 1,
  });

  await telemetryDb.ping();

  const rows = await telemetryDb.execute(ckSql`SELECT 1 AS one`, {
    query_id: "runtime_execute_example",
  });

  const streamedRows: Record<string, unknown>[] = [];
  for await (const row of telemetryDb.stream(ckSql`SELECT number FROM numbers(3)`, {
    format: "JSONEachRow",
    query_id: "runtime_stream_example",
  })) {
    streamedRows.push(row);
  }

  await telemetryDb.command(ckSql`SYSTEM FLUSH LOGS`, {
    query_id: "runtime_command_example",
  });

  return {
    rows,
    streamedRows,
  };
};

export const runReplicasStatusExample = async (options?: ClickHouseEndpointOptions) => {
  const db = createInstrumentedProbeDb();
  await db.replicasStatus(options);
};
