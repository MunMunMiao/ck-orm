import { expect, it } from "bun:test";
import type { Tracer } from "@opentelemetry/api";
import {
  type ClickHouseOrmLogger,
  type ClickHouseOrmQueryErrorEvent,
  type ClickHouseOrmQueryEvent,
  type ClickHouseOrmQueryResultEvent,
  ck,
  csql,
  fn,
} from "./ck-orm";
import { createE2EDb, users } from "./shared";
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
    const errors: ClickHouseOrmQueryErrorEvent[] = [];

    const logger: ClickHouseOrmLogger = {
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
          onQueryStart(_event: ClickHouseOrmQueryEvent) {
            lifecycle.push("custom_start_1");
          },
          onQuerySuccess(_event: ClickHouseOrmQueryResultEvent) {
            lifecycle.push("custom_success_1");
          },
          onQueryError(event: ClickHouseOrmQueryErrorEvent) {
            lifecycle.push("custom_error_1");
            errors.push(event);
          },
        },
        {
          onQueryStart(_event: ClickHouseOrmQueryEvent) {
            lifecycle.push("custom_start_2");
          },
          onQuerySuccess(_event: ClickHouseOrmQueryResultEvent) {
            lifecycle.push("custom_success_2");
          },
          onQueryError(_event: ClickHouseOrmQueryErrorEvent) {
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

    expect(spans).toHaveLength(2);
    expect(spans[0]?.name).toBe("clickhouse QUERY");
    expect(spans[0]?.attributes["db.system"]).toBe("clickhouse");
    expect(spans[0]?.attributes["app.component"]).toBe("ck-orm-e2e");
    expect(spans[0]?.attributes["db.statement"]).toContain(
      "arrayExists(x -> x = {orm_param3:Int64}, {orm_param1:Array(Int64)}) as `has_match`",
    );
    expect(spans[0]?.attributes["db.statement"]).toContain("where `users`.`id` = {orm_param2:Int32}");
    expect(spans[0]?.attributes["db.statement.hash"]).toBeDefined();
    expect(spans[0]?.ended).toBe(true);
    expect(spans[0]?.attributes).not.toHaveProperty("db.query_params");

    expect(spans[1]?.name).toBe("clickhouse QUERY");
    expect(spans[1]?.exceptions).toHaveLength(1);
    expect(spans[1]?.ended).toBe(true);
    expect(errors).toHaveLength(1);
    expect(errors[0]?.operation).toBe("SELECT");
  });
});
