import { describe, expect, it } from "bun:test";
import {
  ClickHouseOrmError,
  createAbortedError,
  createClientValidationError,
  createDecodeError,
  createRequestFailedError,
  createTimeoutError,
  DecodeError,
  isClickHouseOrmError,
  isDecodeError,
  normalizeTransportError,
  withClickHouseOrmErrorContext,
} from "./errors";

describe("ck-orm errors", function describeClickHouseOrmErrors() {
  it("creates guard-detectable error objects and keeps instanceof compatibility", function testErrorGuards() {
    const clientError = createClientValidationError("bad input");
    const requestError = createRequestFailedError({
      responseText: "Code: 62. DB::Exception: syntax error (SYNTAX_ERROR)",
      executionState: "rejected",
      httpStatus: 400,
    });
    const decodeError = createDecodeError("Failed to decode row", { id: "bad" }, { path: "row.id" });

    expect(clientError).toBeInstanceOf(Error);
    expect(clientError).toBeInstanceOf(ClickHouseOrmError);
    expect(clientError).not.toBeInstanceOf(DecodeError);
    expect(isClickHouseOrmError(clientError)).toBe(true);
    expect(isDecodeError(clientError)).toBe(false);

    expect(requestError).toBeInstanceOf(Error);
    expect(requestError).toBeInstanceOf(ClickHouseOrmError);
    expect(requestError).not.toBeInstanceOf(DecodeError);
    expect(isClickHouseOrmError(requestError)).toBe(true);
    expect(isDecodeError(requestError)).toBe(false);

    expect(decodeError).toBeInstanceOf(Error);
    expect(decodeError).toBeInstanceOf(ClickHouseOrmError);
    expect(decodeError).toBeInstanceOf(DecodeError);
    expect(isClickHouseOrmError(decodeError)).toBe(true);
    expect(isDecodeError(decodeError)).toBe(true);
  });

  it("withClickHouseOrmErrorContext clones only when it adds missing context", function testErrorContextCloning() {
    const baseError = createRequestFailedError({
      responseText: "network down",
      executionState: "unknown",
    });

    expect(withClickHouseOrmErrorContext(baseError, {})).toBe(baseError);

    const enriched = withClickHouseOrmErrorContext(baseError, {
      queryId: "query_1",
      sessionId: "session_1",
    });
    expect(enriched).not.toBe(baseError);
    expect(enriched.queryId).toBe("query_1");
    expect(enriched.sessionId).toBe("session_1");
    expect(baseError.queryId).toBeUndefined();
    expect(baseError.sessionId).toBeUndefined();

    const existingContext = createRequestFailedError({
      responseText: "already tagged",
      executionState: "rejected",
      queryId: "existing_query",
      sessionId: "existing_session",
    });
    expect(
      withClickHouseOrmErrorContext(existingContext, {
        queryId: "new_query",
        sessionId: "new_session",
      }),
    ).toBe(existingContext);
  });

  it("normalizeTransportError keeps unknown suffix stable and preserves guards", function testNormalizeTransportError() {
    const normalized = normalizeTransportError(new Error("socket hangup"), {
      queryId: "query_2",
      sessionId: "session_2",
    });
    expect(normalized).toBeInstanceOf(ClickHouseOrmError);
    expect(isClickHouseOrmError(normalized)).toBe(true);
    expect(normalized.kind).toBe("request_failed");
    expect(normalized.executionState).toBe("unknown");
    expect(normalized.queryId).toBe("query_2");
    expect(normalized.sessionId).toBe("session_2");
    expect(normalized.message.match(/execution state is unknown/g)).toHaveLength(1);

    const preMarked = normalizeTransportError(new Error("socket hangup; execution state is unknown"), {});
    expect(preMarked.message.match(/execution state is unknown/g)).toHaveLength(1);

    const original = createClientValidationError("unsafe query");
    const recontextualized = normalizeTransportError(original, { queryId: "query_3" });
    expect(recontextualized).not.toBe(original);
    expect(recontextualized).toBeInstanceOf(ClickHouseOrmError);
    expect(isClickHouseOrmError(recontextualized)).toBe(true);
    expect(recontextualized.queryId).toBe("query_3");
    expect(recontextualized.kind).toBe("client_validation");
  });

  it("covers primitive transport errors plus timeout and abort helpers", function testPrimitiveTransportErrorsAndHelpers() {
    const primitive = normalizeTransportError("socket closed", {
      queryId: "query_4",
      sessionId: "session_4",
    });
    expect(primitive).toBeInstanceOf(ClickHouseOrmError);
    expect(isClickHouseOrmError(primitive)).toBe(true);
    expect(primitive.message).toContain("socket closed; execution state is unknown");
    expect(primitive.queryId).toBe("query_4");
    expect(primitive.sessionId).toBe("session_4");

    const timeoutError = createTimeoutError(250, { queryId: "timeout_query" });
    expect(timeoutError.kind).toBe("timeout");
    expect(timeoutError.executionState).toBe("unknown");
    expect(timeoutError.message).toContain("250ms");

    const abortedError = createAbortedError(undefined, { sessionId: "aborted_session" });
    expect(abortedError.kind).toBe("aborted");
    expect(abortedError.executionState).toBe("unknown");
    expect(abortedError.sessionId).toBe("aborted_session");

    expect(ClickHouseOrmError()).toBeUndefined();
    expect(DecodeError()).toBeUndefined();
  });
});
