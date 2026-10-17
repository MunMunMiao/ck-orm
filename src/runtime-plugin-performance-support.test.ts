import type { ClickHouseORMLogger, ClickHouseORMPlugin } from "./observability";
import { ckSql, ckTable, ckType, clickhouseClient } from "./public_api";

export const marker = "audit_perf006";
export const queryOptions = { query_id: "audit_perf006_explicit_query" };
export const table = ckTable("audit_perf006_rows", { n: ckType.int32() });
export const jsonResponse =
  '{"data":[{"n":1},{"n":2}],"rows":2,"rows_before_limit_at_least":4,"statistics":{"elapsed":0.002,"rows_read":4,"bytes_read":32}}';
export const streamResponse = '{"n":1}\n{"n":2}\n';

export const controlledSql = (text: string) =>
  ckSql(Object.assign([text], { raw: [text] }) as unknown as TemplateStringsArray);

export const statementOfSize = (bytes: number): string => {
  const prefix = `SELECT 1 AS n /* ${marker} `;
  const suffix = " */";
  if (bytes < prefix.length + suffix.length || bytes > 65_536) {
    throw new Error("SQL size outside audit budget");
  }
  return prefix + "x".repeat(bytes - prefix.length - suffix.length) + suffix;
};

export type ObservationCost = {
  sqlEncodeCalls: number;
  sqlEncodeCodeUnits: number;
  fnvSteps: number;
  randomUuids: number;
};

export const zeroCost: ObservationCost = {
  sqlEncodeCalls: 0,
  sqlEncodeCodeUnits: 0,
  fnvSteps: 0,
  randomUuids: 0,
};

export const observeCost = async <T>(operation: () => Promise<T>): Promise<{ value: T; cost: ObservationCost }> => {
  const cost = { ...zeroCost };
  const encode = TextEncoder.prototype.encode;
  const imul = Math.imul;
  const randomUUID = globalThis.crypto.randomUUID;
  TextEncoder.prototype.encode = function (input = "") {
    if (input.includes(marker) || input === "GET /ping" || input === "GET /replicas_status") {
      cost.sqlEncodeCalls += 1;
      cost.sqlEncodeCodeUnits += input.length;
    }
    return encode.call(this, input);
  };
  Math.imul = (left, right) => {
    if (right === 0x01000193) {
      cost.fnvSteps += 1;
    }
    return imul(left, right);
  };
  globalThis.crypto.randomUUID = () => {
    cost.randomUuids += 1;
    return randomUUID.call(globalThis.crypto);
  };
  try {
    return { value: await operation(), cost };
  } finally {
    TextEncoder.prototype.encode = encode;
    Math.imul = imul;
    globalThis.crypto.randomUUID = randomUUID;
  }
};

export const memoryClient = (
  options: {
    plugins?: readonly ClickHouseORMPlugin[];
    logger?: ClickHouseORMLogger | false;
    logLevel?: "debug" | "warn";
    fetch?: (request: Request) => Response | Promise<Response>;
  } = {},
) =>
  clickhouseClient({
    host: "http://127.0.0.1:1",
    ...options,
    fetch:
      options.fetch ??
      (async (request) => {
        if (request.method === "GET") {
          return new Response("Ok.\n");
        }
        const body = await request.text();
        if (body.includes("audit_perf006_fail")) {
          return new Response("synthetic query failure", { status: 500 });
        }
        if (body.endsWith("FORMAT JSONEachRow")) {
          return new Response(streamResponse);
        }
        if (body.endsWith("FORMAT JSON")) {
          return new Response(jsonResponse);
        }
        return new Response("");
      }),
  });

export const capturedLogger = () => {
  const entries: Array<{ level: string; fields?: Record<string, unknown> }> = [];
  const sink = (level: string) => (_message: string, fields?: Record<string, unknown>) => {
    entries.push({ level, fields });
  };
  return {
    entries,
    logger: {
      trace: sink("trace"),
      debug: sink("debug"),
      info: sink("info"),
      warn: sink("warn"),
      error: sink("error"),
    },
  };
};
