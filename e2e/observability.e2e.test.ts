import { expect, it } from "bun:test";
import type { Tracer } from "@opentelemetry/api";
import {
  type ClickHouseORMLogger,
  type ClickHouseORMQueryErrorEvent,
  type ClickHouseORMQueryEvent,
  type ClickHouseORMQueryResultEvent,
  ck,
  csql,
  fn,
} from "./ck-orm";
import { createE2EDb, getE2EConfig, users } from "./shared";
import { describeE2E } from "./test-helpers";

type CapturedSpan = {
  name: string;
  attributes: Record<string, string | number | boolean>;
  status?: unknown;
  exceptions: unknown[];
  ended: boolean;
};

const createTestTracer = (spans: CapturedSpan[]): Tracer => {
  const tracer = {
    startSpan(name: string, options?: { attributes?: Record<string, string | number | boolean> }) {
      const span: CapturedSpan = {
        name,
        attributes: { ...(options?.attributes ?? {}) },
        exceptions: [],
        ended: false,
      };
      spans.push(span);
      return {
        setAttribute(key: string, value: string | number | boolean) {
          span.attributes[key] = value;
        },
        recordException(error: unknown) {
          span.exceptions.push(error);
        },
        setStatus(status: unknown) {
          span.status = status;
        },
        end() {
          span.ended = true;
        },
      };
    },
    startActiveSpan(name: string, ...args: unknown[]) {
      const callback = args.find((arg) => typeof arg === "function");
      if (typeof callback !== "function") {
        throw new Error("Expected startActiveSpan callback");
      }
      const maybeOptions = args.find((arg) => typeof arg === "object" && arg !== null && !Array.isArray(arg)) as
        | { attributes?: Record<string, string | number | boolean> }
        | undefined;
      return callback(tracer.startSpan(name, maybeOptions));
    },
  };
  return tracer as unknown as Tracer;
};

describeE2E("ck-orm e2e observability", function describeObservability() {
  it("emits logger, tracing and instrumentation events against real clickhouse requests", async function testObservability() {
    const spans: CapturedSpan[] = [];
    const logs: Array<{
      level: string;
      message: string;
      fields?: Record<string, unknown>;
    }> = [];
    const lifecycle: string[] = [];
    const starts: ClickHouseORMQueryEvent[] = [];
    const successes: ClickHouseORMQueryResultEvent[] = [];
    const errors: ClickHouseORMQueryErrorEvent[] = [];
    const e2eConfig = getE2EConfig();
    const e2eUrl = new URL(e2eConfig.host);
    const expectedPort = e2eUrl.port ? Number(e2eUrl.port) : e2eUrl.protocol === "https:" ? 443 : 80;

    const logger: ClickHouseORMLogger = {
      trace(message, fields) {
        logs.push({ level: "trace", message, fields });
      },
      debug(message, fields) {
        logs.push({ level: "debug", message, fields });
      },
      info(message, fields) {
        logs.push({ level: "info", message, fields });
      },
      warn(message, fields) {
        logs.push({ level: "warn", message, fields });
      },
      error(message, fields) {
        logs.push({ level: "error", message, fields });
      },
    };

    const db = createE2EDb({
      logger,
      logLevel: "debug",
      tracing: {
        tracer: createTestTracer(spans),
        attributes: {
          "app.component": "ck-orm-e2e",
          "db.system": "user-supplied-should-be-ignored",
        },
      },
      instrumentation: [
        {
          onQueryStart(event: ClickHouseORMQueryEvent) {
            lifecycle.push("custom_start_1");
            starts.push(event);
          },
          onQuerySuccess(event: ClickHouseORMQueryResultEvent) {
            lifecycle.push("custom_success_1");
            successes.push(event);
          },
          onQueryError(event: ClickHouseORMQueryErrorEvent) {
            lifecycle.push("custom_error_1");
            errors.push(event);
          },
        },
        {
          onQueryStart(_event: ClickHouseORMQueryEvent) {
            lifecycle.push("custom_start_2");
          },
          onQuerySuccess(_event: ClickHouseORMQueryResultEvent) {
            lifecycle.push("custom_success_2");
          },
          onQueryError(_event: ClickHouseORMQueryErrorEvent) {
            lifecycle.push("custom_error_2");
          },
        },
      ],
    });

    expect(
      await db
        .select({
          id: users.id,
          hasMatch: fn.arrayExists(csql`x -> x = ${1}`, [1, 2]).as("has_match"),
        })
        .from(users)
        .where(ck.eq(users.id, 1)),
    ).toEqual([{ id: 1, hasMatch: true }]);

    await expect(db.execute(csql`SELECT * FROM missing_e2e_table`)).rejects.toThrow();

    expect(lifecycle).toEqual([
      "custom_start_1",
      "custom_start_2",
      "custom_success_2",
      "custom_success_1",
      "custom_start_1",
      "custom_start_2",
      "custom_error_2",
      "custom_error_1",
    ]);

    expect(logs.some((log) => log.level === "debug" && log.fields?.outcome === "start")).toBe(true);
    expect(logs.some((log) => log.level === "debug" && log.fields?.outcome === "success")).toBe(true);
    expect(logs.some((log) => log.level === "error" && log.fields?.outcome === "error")).toBe(true);
    const successLog = logs.find((log) => log.level === "debug" && log.fields?.outcome === "success");
    const errorLog = logs.find((log) => log.level === "error" && log.fields?.outcome === "error");
    expect(successLog?.fields).toMatchObject({
      databaseName: e2eConfig.database,
      serverAddress: e2eUrl.hostname,
      serverPort: expectedPort,
      querySummary: "SELECT users",
      tableName: "users",
    });
    expect(successLog?.fields?.statementHash).toBeDefined();
    expect(typeof successLog?.fields?.readRows).toBe("number");
    expect(typeof successLog?.fields?.readBytes).toBe("number");
    expect(typeof successLog?.fields?.serverElapsedMs).toBe("number");
    expect(errorLog?.fields).toMatchObject({
      databaseName: e2eConfig.database,
      querySummary: "SELECT",
      errorKind: "request_failed",
      executionState: "rejected",
    });

    expect(spans).toHaveLength(2);
    expect(spans[0]?.name).toBe("clickhouse QUERY");
    expect(spans[0]?.attributes["db.system"]).toBe("clickhouse");
    expect(spans[0]?.attributes["db.system.name"]).toBe("clickhouse");
    expect(spans[0]?.attributes["db.namespace"]).toBe(e2eConfig.database);
    expect(spans[0]?.attributes["db.operation.name"]).toBe("SELECT");
    expect(spans[0]?.attributes["db.query.summary"]).toBe("SELECT users");
    expect(spans[0]?.attributes["db.collection.name"]).toBe("users");
    expect(spans[0]?.attributes["server.address"]).toBe(e2eUrl.hostname);
    expect(spans[0]?.attributes["server.port"]).toBe(expectedPort);
    expect(spans[0]?.attributes["app.component"]).toBe("ck-orm-e2e");
    expect(spans[0]?.attributes["db.statement"]).toContain(
      "arrayExists(x -> x = {orm_param3:Int64}, {orm_param1:Array(Int64)}) as `has_match`",
    );
    expect(spans[0]?.attributes["db.query.text"]).toContain(
      "arrayExists(x -> x = {orm_param3:Int64}, {orm_param1:Array(Int64)}) as `has_match`",
    );
    expect(spans[0]?.attributes["db.statement"]).toContain("where `users`.`id` = {orm_param2:Int32}");
    expect(spans[0]?.attributes["db.statement.hash"]).toBeDefined();
    expect(spans[0]?.attributes["ck_orm.statement.hash"]).toBe(spans[0]?.attributes["db.statement.hash"]);
    expect(spans[0]?.attributes["db.response.returned_rows"]).toBe(1);
    expect(typeof spans[0]?.attributes["ck_orm.server.elapsed_ms"]).toBe("number");
    expect(typeof spans[0]?.attributes["ck_orm.read.rows"]).toBe("number");
    expect(typeof spans[0]?.attributes["ck_orm.read.bytes"]).toBe("number");
    expect(spans[0]?.ended).toBe(true);
    expect(spans[0]?.attributes).not.toHaveProperty("db.query_params");

    expect(spans[1]?.name).toBe("clickhouse QUERY");
    expect(spans[1]?.exceptions).toHaveLength(1);
    expect(spans[1]?.attributes["error.type"]).toBe("UNKNOWN_TABLE");
    expect(spans[1]?.attributes["ck_orm.error.kind"]).toBe("request_failed");
    expect(spans[1]?.attributes["ck_orm.execution_state"]).toBe("rejected");
    expect(spans[1]?.attributes["db.response.status_code"]).toBe("60");
    expect(spans[1]?.ended).toBe(true);
    expect(starts[0]).toMatchObject({
      databaseName: e2eConfig.database,
      serverAddress: e2eUrl.hostname,
      serverPort: expectedPort,
      querySummary: "SELECT users",
      tableName: "users",
    });
    expect(successes[0]).toMatchObject({
      databaseName: e2eConfig.database,
      querySummary: "SELECT users",
      tableName: "users",
      resultRows: 1,
    });
    expect(typeof successes[0]?.readRows).toBe("number");
    expect(typeof successes[0]?.readBytes).toBe("number");
    expect(typeof successes[0]?.serverElapsedMs).toBe("number");
    expect(errors).toHaveLength(1);
    expect(errors[0]?.operation).toBe("SELECT");
    expect(errors[0]?.databaseName).toBe(e2eConfig.database);
    expect(errors[0]?.querySummary).toBe("SELECT");
  });
});
