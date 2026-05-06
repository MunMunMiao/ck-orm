import { ck, isClickHouseORMError, isDecodeError } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeTelemetry } from "./schema/probe";

type PublicQueryError = {
  code: "clickhouse_query_failed";
  message: string;
};

const toPublicQueryError = (error: unknown): PublicQueryError => {
  if (isDecodeError(error)) {
    console.error("ClickHouse row decoding failed", {
      kind: error.kind,
      path: error.path,
      causeValue: error.causeValue,
    });

    return {
      code: "clickhouse_query_failed",
      message: "The query result could not be decoded.",
    };
  }

  if (isClickHouseORMError(error)) {
    console.error("ClickHouse request failed", {
      kind: error.kind,
      executionState: error.executionState,
      queryId: error.queryId,
      sessionId: error.sessionId,
      httpStatus: error.httpStatus,
      clickhouseCode: error.clickhouseCode,
      clickhouseName: error.clickhouseName,
      requestTimeoutMs: error.requestTimeoutMs,
    });

    if (error.kind === "timeout") {
      return {
        code: "clickhouse_query_failed",
        message: "The query timed out.",
      };
    }

    if (error.kind === "aborted") {
      return {
        code: "clickhouse_query_failed",
        message: "The query was cancelled.",
      };
    }
  }

  console.error("Unexpected query failure", error);

  return {
    code: "clickhouse_query_failed",
    message: "The query failed.",
  };
};

export const runCountModesExample = async () => {
  const probeDb = createProbeDb();
  const activeTelemetry = ck.isNull(probeTelemetry.deletedAt);

  const approximateNumber = await probeDb.count(probeTelemetry, activeTelemetry);
  const exactString = await probeDb.count(probeTelemetry, activeTelemetry).toSafe();
  const wireShape = await probeDb.count(probeTelemetry, activeTelemetry).toMixed();

  return {
    approximateNumber,
    exactString,
    wireShape,
  };
};

export const runErrorHandlingExample = async () => {
  const probeDb = createProbeDb();

  try {
    return await probeDb
      .select({
        probeId: probeTelemetry.probeId,
        signalStrength: probeTelemetry.signalStrength,
      })
      .from(probeTelemetry)
      .where(ck.isNull(probeTelemetry.deletedAt))
      .limit(20)
      .execute({
        query_id: "probe_error_handling_example",
      });
  } catch (error) {
    return toPublicQueryError(error);
  }
};
