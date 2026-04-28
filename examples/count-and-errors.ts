import { ck, clickhouseClient, isClickHouseORMError, isDecodeError } from "./ck-orm";
import { commerceSchema, orderRewardLog } from "./schema/commerce";

type PublicQueryError = {
  code: "clickhouse_query_failed";
  message: string;
};

const createCommerceDb = () => {
  return clickhouseClient({
    host: "http://127.0.0.1:8123",
    database: "demo_store",
    username: "default",
    password: "<password>",
    schema: commerceSchema,
    clickhouse_settings: {
      max_execution_time: 10,
    },
  });
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
  const commerceDb = createCommerceDb();
  const activeRewardEvents = ck.eq(orderRewardLog.peerdbIsDeleted, 0);

  const approximateNumber = await commerceDb.count(orderRewardLog, activeRewardEvents);
  const exactString = await commerceDb.count(orderRewardLog, activeRewardEvents).toSafe();
  const wireShape = await commerceDb.count(orderRewardLog, activeRewardEvents).toMixed();

  return {
    approximateNumber,
    exactString,
    wireShape,
  };
};

export const runErrorHandlingExample = async () => {
  const commerceDb = createCommerceDb();

  try {
    return await commerceDb
      .select({
        userId: orderRewardLog.userId,
        rewardPoints: orderRewardLog.rewardPoints,
      })
      .from(orderRewardLog)
      .where(ck.eq(orderRewardLog.peerdbIsDeleted, 0))
      .limit(20)
      .execute({
        query_id: "reward_error_handling_example",
      });
  } catch (error) {
    return toPublicQueryError(error);
  }
};
