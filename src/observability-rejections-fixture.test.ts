import assert from "node:assert/strict";
import { setImmediate as nextTurn } from "node:timers/promises";
import { ckTable, ckType, clickhouseClient, isClickHouseORMError, type StandardSchemaV1, ckSql as sql } from "./index";
import {
  createLoggerPlugin,
  createQueryErrorEvent,
  createQueryEvent,
  createQuerySuccessEvent,
  emitQueryError,
  emitQueryStart,
  emitQuerySuccess,
} from "./observability";

let id: string | undefined;
let layer: string | undefined;
let scenario: string | undefined;
let behavior: string | undefined;
let host: string | undefined;
const markerOf = () => `AUDIT_${id}_${scenario}_INDEPENDENT_REJECTION`;
const report = (event: string, data: Record<string, unknown> = {}) => {
  process.stdout.write(`${JSON.stringify({ event: `AUDIT_${event}`, id, layer, scenario, behavior, ...data })}\n`);
};

const checkError = (error: unknown, kind: "client_validation" | "decode" | "request_failed") => {
  assert.ok(isClickHouseORMError(error), "Expected an ORM error, not a fixture/loader error");
  assert.equal(error.kind, kind);
  if (id === "REL-004") {
    const diagnostic = scenario === "select" || scenario === "iterator" ? error.responseText : error.message;
    assert.match(
      diagnostic ?? "",
      behavior === "sync-invalid"
        ? /Standard Schema validation failed/
        : /Async Standard Schema validators are not supported/,
    );
  } else {
    assert.equal(error.httpStatus, 500);
    assert.equal(error.responseText, "audit operation error");
  }
  report("ORM_CAUGHT", { kind: error.kind, executionState: error.executionState });
};

const loggerCase = async () => {
  let targetCalls = 0;
  const write = (_message: string, fields?: Record<string, unknown>) => {
    if (fields?.outcome !== scenario) {
      return;
    }
    targetCalls += 1;
    report("TARGET_CALLED");
    if (behavior === "sync-throw") {
      throw new Error(markerOf());
    }
    return behavior === "reject" ? Promise.reject(new Error(markerOf())) : Promise.resolve();
  };
  const logger = { trace() {}, debug: write, info() {}, warn() {}, error: write };
  if (layer === "unit") {
    const plugin = createLoggerPlugin(logger, "debug");
    const event = createQueryEvent({
      mode: "query",
      queryKind: "raw",
      statement: "SELECT 1 AS one",
      operation: "SELECT",
      startedAt: Date.now(),
    });
    if (scenario === "start") {
      await emitQueryStart([plugin], event);
    } else if (scenario === "success") {
      await emitQuerySuccess([plugin], createQuerySuccessEvent(event, 1, 1));
    } else if (scenario === "error") {
      await emitQueryError([plugin], createQueryErrorEvent(event, new Error("original failure"), 1));
    } else {
      throw new Error("Unknown logger scenario");
    }
  } else {
    const db = clickhouseClient({ host, logger, logLevel: "debug", request_timeout: 2_000 });
    if (scenario === "error") {
      let caught: unknown;
      try {
        await db.execute(sql`SELECT 1 AS one`);
      } catch (error) {
        caught = error;
      }
      checkError(caught, "request_failed");
    } else {
      const rows = await db.execute(sql`SELECT 1 AS one`);
      assert.deepEqual(rows, [{ one: 7 }]);
    }
  }
  assert.equal(targetCalls, 1, "The intended logger hook must actually run");
  report("RESULT", { targetCalls });
};

const validatorCase = async () => {
  let targetCalls = 0;
  const schema: StandardSchemaV1<string, string> = {
    "~standard": {
      version: 1,
      vendor: "audit-synthetic-validator",
      validate(value) {
        targetCalls += 1;
        report("TARGET_CALLED");
        if (behavior === "reject") {
          return Promise.reject(new Error(markerOf()));
        }
        if (behavior === "resolve") {
          return Promise.resolve({ value: String(value) });
        }
        if (behavior === "sync-invalid") {
          return { issues: [{ message: "audit synthetic invalid value" }] };
        }
        return { value: String(value) };
      },
    },
  };
  const column = ckType.string().$validator(schema);
  const table = ckTable("audit_validator_fixture", { value: column });
  const db = layer === "e2e" ? clickhouseClient({ host, request_timeout: 2_000 }) : undefined;
  let caught: unknown;
  let result: unknown;
  try {
    switch (scenario) {
      case "encode":
        result = column.mapToDriverValue("audit value");
        break;
      case "decode":
        result = column.mapFromDriverValue("audit value");
        break;
      case "insert-values":
        assert.ok(db);
        result = await db.insert(table).values({ value: "audit value" });
        break;
      case "insert-json":
        assert.ok(db);
        result = await db.insertJsonEachRow(table, [{ value: "audit value" }]);
        break;
      case "select":
        assert.ok(db);
        result = await db.select().from(table).execute();
        break;
      case "iterator": {
        assert.ok(db);
        const rows = [];
        for await (const row of db.select().from(table).iterator()) {
          rows.push(row);
        }
        result = rows;
        break;
      }
      default:
        throw new Error("Unknown validator scenario");
    }
  } catch (error) {
    caught = error;
  }
  if (behavior === "sync-valid") {
    assert.equal(caught, undefined);
    if (scenario === "encode" || scenario === "decode") {
      assert.equal(result, "audit value");
    }
    if (scenario === "select" || scenario === "iterator") {
      assert.deepEqual(result, [{ value: "safe" }]);
    }
  } else {
    checkError(caught, scenario === "encode" || scenario?.startsWith("insert") ? "client_validation" : "decode");
  }
  assert.equal(targetCalls, 1, "The intended validator must actually run");
  report("RESULT", { targetCalls });
};

const main = async () => {
  [id, layer, scenario, behavior, host] = process.argv.slice(3);
  if (id !== "REL-003" && id !== "REL-004") {
    throw new Error(`ck-orm fixture: unknown or missing audit id ${id ?? "(missing)"}`);
  }
  assert.equal(process.listenerCount("unhandledRejection"), 0, "Default rejection behavior is required");
  assert.ok(layer === "unit" || layer === "e2e");
  if (layer === "e2e") {
    assert.ok(host);
    const url = new URL(host);
    assert.equal(url.hostname, "127.0.0.1");
    assert.equal(url.protocol, "http:");
  }
  report("READY", { runtime: process.versions.bun ? "bun" : "node", rejectionHandlers: 0 });
  if (id === "REL-003") {
    await loggerCase();
  } else if (id === "REL-004") {
    await validatorCase();
  } else {
    throw new Error("Unknown audit ID");
  }
  await nextTurn();
  await nextTurn();
  report("DRAINED");
};

if (import.meta.main && process.argv[2] === "--ck-orm-fixture") {
  main().catch((error: unknown) => {
    process.stderr.write(
      `AUDIT_HARNESS_FAILURE ${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
    );
    process.exitCode = 70;
  });
}
