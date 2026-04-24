import { describe, expect, it } from "bun:test";
import {
  createAbortedError,
  createClientValidationError,
  createDecodeError,
  createInternalError,
  createRequestFailedError,
  createTimeoutError,
  isClickHouseORMError,
  isDecodeError,
  normalizeTransportError,
  withClickHouseORMErrorContext,
} from "./errors";

describe("ck-orm errors", function describeClickHouseORMErrors() {
  it("creates guard-detectable error objects without runtime compatibility classes", function testErrorGuards() {
    const clientError = createClientValidationError("bad input");
    const requestError = createRequestFailedError({
      responseText: "Code: 62. DB::Exception: syntax error (SYNTAX_ERROR)",
      executionState: "rejected",
      httpStatus: 400,
    });
    const decodeError = createDecodeError("Failed to decode row", { id: "bad" }, { path: "row.id" });
    const internalError = createInternalError("broken invariant");

    expect(clientError).toBeInstanceOf(Error);
    expect(isClickHouseORMError(clientError)).toBe(true);
    expect(isDecodeError(clientError)).toBe(false);

    expect(requestError).toBeInstanceOf(Error);
    expect(isClickHouseORMError(requestError)).toBe(true);
    expect(isDecodeError(requestError)).toBe(false);

    expect(decodeError).toBeInstanceOf(Error);
    expect(isClickHouseORMError(decodeError)).toBe(true);
    expect(isDecodeError(decodeError)).toBe(true);

    expect(internalError.kind).toBe("internal");
    expect(internalError.executionState).toBe("not_sent");
    expect(isClickHouseORMError(internalError)).toBe(true);
    expect(isDecodeError(internalError)).toBe(false);
  });

  it("withClickHouseORMErrorContext clones only when it adds missing context", function testErrorContextCloning() {
    const baseError = createRequestFailedError({
      responseText: "network down",
      executionState: "unknown",
    });

    expect(withClickHouseORMErrorContext(baseError, {})).toBe(baseError);

    const enriched = withClickHouseORMErrorContext(baseError, {
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
      withClickHouseORMErrorContext(existingContext, {
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
    expect(isClickHouseORMError(normalized)).toBe(true);
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
    expect(isClickHouseORMError(recontextualized)).toBe(true);
    expect(recontextualized.queryId).toBe("query_3");
    expect(recontextualized.kind).toBe("client_validation");
  });

  it("covers primitive transport errors plus timeout and abort helpers", function testPrimitiveTransportErrorsAndHelpers() {
    const primitive = normalizeTransportError("socket closed", {
      queryId: "query_4",
      sessionId: "session_4",
    });
    expect(isClickHouseORMError(primitive)).toBe(true);
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
  });
});
