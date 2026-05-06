import { isRecord } from "./internal/predicates";

export type ClickHouseORMExecutionState = "not_sent" | "rejected" | "unknown";

export type ClickHouseORMErrorKind =
  | "client_validation"
  | "request_failed"
  | "decode"
  | "timeout"
  | "aborted"
  | "session"
  | "internal";

type ClickHouseORMErrorFields = {
  kind: ClickHouseORMErrorKind;
  executionState: ClickHouseORMExecutionState;
  cause?: unknown;
  queryId?: string;
  sessionId?: string;
  httpStatus?: number;
  clickhouseCode?: number;
  clickhouseName?: string;
  responseText?: string;
  requestTimeoutMs?: number;
};

type DecodeErrorFields = ClickHouseORMErrorFields & {
  readonly kind: "decode";
  readonly executionState: "rejected";
  readonly causeValue: unknown;
  readonly path?: string;
};

type ClickHouseORMErrorOptions = Readonly<ClickHouseORMErrorFields>;

type RequestFailedErrorOptions = Omit<ClickHouseORMErrorOptions, "kind" | "executionState"> & {
  readonly executionState?: ClickHouseORMExecutionState;
};

export type ClickHouseORMError = Error & ClickHouseORMErrorFields;

export type DecodeError = ClickHouseORMError & DecodeErrorFields;

const CK_ORM_PREFIX = "[ck-orm] ";
const UNKNOWN_EXECUTION_STATE_SUFFIX = "; execution state is unknown";

const withCKORMPrefix = (message: string) =>
  message.startsWith(CK_ORM_PREFIX) ? message : `${CK_ORM_PREFIX}${message}`;

const createBaseError = <TError extends ClickHouseORMError | DecodeError>(
  name: "ClickHouseORMError" | "DecodeError",
  message: string,
  fields: Record<string, unknown>,
): TError => {
  const error = new Error(message) as TError;
  error.name = name;
  Object.assign(error, fields);
  return error;
};

const cloneError = <TError extends ClickHouseORMError>(error: TError): TError => {
  // `Error.prototype.stack` is not enumerable-own, so `Object.assign` skips
  // it; copy it explicitly after merging the rest of the fields.
  const cloned = Object.assign(new Error(error.message), error) as TError;
  cloned.name = error.name;
  cloned.stack ??= error.stack;
  return cloned;
};

const make = (
  kind: ClickHouseORMErrorKind,
  executionState: ClickHouseORMExecutionState,
  message: string,
  options?: Omit<ClickHouseORMErrorOptions, "kind" | "executionState"> & {
    readonly markUnknownExecutionState?: boolean;
  },
) => {
  const { markUnknownExecutionState, ...rest } = options ?? {};
  const finalMessage = withCKORMPrefix(
    markUnknownExecutionState && !message.endsWith(UNKNOWN_EXECUTION_STATE_SUFFIX)
      ? `${message}${UNKNOWN_EXECUTION_STATE_SUFFIX}`
      : message,
  );

  return createBaseError<ClickHouseORMError>("ClickHouseORMError", finalMessage, {
    kind,
    executionState,
    ...rest,
  });
};

const extractClickHouseName = (text: string) => {
  const matches = [...text.matchAll(/\(([A-Z][A-Z0-9_]+)\)/g)];
  return matches.at(-1)?.[1];
};

export const isClickHouseORMError = (error: unknown): error is ClickHouseORMError => {
  return (
    error instanceof Error &&
    isRecord(error) &&
    typeof error.kind === "string" &&
    typeof error.executionState === "string"
  );
};

export const isDecodeError = (error: unknown): error is DecodeError => {
  return isClickHouseORMError(error) && error.kind === "decode" && "causeValue" in error;
};

export const withClickHouseORMErrorContext = <TError extends ClickHouseORMError>(
  error: TError,
  context: {
    readonly queryId?: string;
    readonly sessionId?: string;
  },
) => {
  const nextQueryId = error.queryId ?? context.queryId;
  const nextSessionId = error.sessionId ?? context.sessionId;
  if (nextQueryId === error.queryId && nextSessionId === error.sessionId) {
    return error;
  }

  const cloned = cloneError(error);
  cloned.queryId = nextQueryId;
  cloned.sessionId = nextSessionId;
  return cloned;
};

export const createClientValidationError = (
  message: string,
  options?: Omit<ClickHouseORMErrorOptions, "kind" | "executionState">,
) => make("client_validation", "not_sent", message, options);

export const createSessionError = (
  message: string,
  options?: Omit<ClickHouseORMErrorOptions, "kind" | "executionState">,
) => make("session", "not_sent", message, options);

export const createInternalError = (
  message: string,
  options?: Omit<ClickHouseORMErrorOptions, "kind" | "executionState">,
) => make("internal", "not_sent", message, options);

/**
 * Constructs a `DecodeError` (a `ClickHouseORMError` of kind `"decode"`)
 * for when a value coming back from ClickHouse cannot be coerced into its
 * TypeScript representation. Pass `path` for container columns so the message
 * pinpoints the failing field, e.g. `items[2].user.email`.
 */
export const createDecodeError = (
  message: string,
  causeValue: unknown,
  options?: Omit<ClickHouseORMErrorOptions, "kind" | "executionState" | "cause"> & {
    readonly path?: string;
  },
) => {
  const finalMessage = withCKORMPrefix(options?.path ? `${message} (at ${options.path})` : message);
  return createBaseError<DecodeError>("DecodeError", finalMessage, {
    kind: "decode",
    executionState: "rejected",
    cause: causeValue,
    causeValue,
    path: options?.path,
    queryId: options?.queryId,
    sessionId: options?.sessionId,
    httpStatus: options?.httpStatus,
    clickhouseCode: options?.clickhouseCode,
    clickhouseName: options?.clickhouseName,
    responseText: options?.responseText,
    requestTimeoutMs: options?.requestTimeoutMs,
  });
};

export const createRequestFailedError = (options: RequestFailedErrorOptions) => {
  const responseText = options.responseText?.trim() ?? "";
  const statusPrefix =
    typeof options.httpStatus === "number"
      ? `ClickHouse request failed with status ${options.httpStatus}`
      : "ClickHouse request failed";
  const message = responseText === "" ? statusPrefix : `${statusPrefix}: ${responseText}`;

  return make("request_failed", options.executionState ?? "rejected", message, {
    ...options,
    responseText,
  });
};

export const createTimeoutError = (
  requestTimeoutMs: number,
  options?: Omit<ClickHouseORMErrorOptions, "kind" | "executionState" | "requestTimeoutMs">,
) =>
  make("timeout", "unknown", `ClickHouse request timed out after ${requestTimeoutMs}ms`, {
    ...options,
    requestTimeoutMs,
    markUnknownExecutionState: true,
  });

export const createAbortedError = (
  message = "ClickHouse request was aborted",
  options?: Omit<ClickHouseORMErrorOptions, "kind" | "executionState">,
) =>
  make("aborted", "unknown", message, {
    ...options,
    markUnknownExecutionState: true,
  });

export const extractClickHouseException = (text: string) => {
  const responseText = text.trim();
  if (
    responseText === "" ||
    (!/(?:^|\n)\s*Code:\s*\d+\.\s*DB::[A-Za-z]/m.test(responseText) && !responseText.includes("__exception__"))
  ) {
    return undefined;
  }

  const codeMatch = responseText.match(/\bCode:\s*(\d+)\b/);
  const clickhouseCode = codeMatch ? Number(codeMatch[1]) : undefined;
  const clickhouseName = extractClickHouseName(responseText);

  return {
    clickhouseCode,
    clickhouseName,
    responseText,
  };
};

export const normalizeTransportError = (
  error: unknown,
  context: {
    readonly queryId?: string;
    readonly sessionId?: string;
  },
) => {
  if (isClickHouseORMError(error)) {
    return withClickHouseORMErrorContext(error, context);
  }

  if (error instanceof Error) {
    return createRequestFailedError({
      responseText: error.message.endsWith(UNKNOWN_EXECUTION_STATE_SUFFIX)
        ? error.message
        : `${error.message}${UNKNOWN_EXECUTION_STATE_SUFFIX}`,
      cause: error,
      executionState: "unknown",
      queryId: context.queryId,
      sessionId: context.sessionId,
    });
  }

  const stringified = String(error);
  return createRequestFailedError({
    responseText: stringified.endsWith(UNKNOWN_EXECUTION_STATE_SUFFIX)
      ? stringified
      : `${stringified}${UNKNOWN_EXECUTION_STATE_SUFFIX}`,
    cause: error,
    executionState: "unknown",
    queryId: context.queryId,
    sessionId: context.sessionId,
  });
};
