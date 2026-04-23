export type ClickHouseOrmExecutionState = "not_sent" | "rejected" | "unknown";

export type ClickHouseOrmErrorKind =
  | "client_validation"
  | "request_failed"
  | "decode"
  | "timeout"
  | "aborted"
  | "session";

type ClickHouseOrmErrorFields = {
  kind: ClickHouseOrmErrorKind;
  executionState: ClickHouseOrmExecutionState;
  cause?: unknown;
  queryId?: string;
  sessionId?: string;
  httpStatus?: number;
  clickhouseCode?: number;
  clickhouseName?: string;
  responseText?: string;
  requestTimeoutMs?: number;
};

type DecodeErrorFields = ClickHouseOrmErrorFields & {
  readonly kind: "decode";
  readonly executionState: "rejected";
  readonly causeValue: unknown;
  readonly path?: string;
};

type ClickHouseOrmErrorOptions = Readonly<ClickHouseOrmErrorFields>;

type RequestFailedErrorOptions = Omit<ClickHouseOrmErrorOptions, "kind" | "executionState"> & {
  readonly executionState?: ClickHouseOrmExecutionState;
};

export type ClickHouseOrmError = Error & ClickHouseOrmErrorFields;

export type DecodeError = ClickHouseOrmError & DecodeErrorFields;

const CK_ORM_PREFIX = "[ck-orm] ";
const UNKNOWN_EXECUTION_STATE_SUFFIX = "; execution state is unknown";

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === "object" && value !== null;
};

const withCkOrmPrefix = (message: string) =>
  message.startsWith(CK_ORM_PREFIX) ? message : `${CK_ORM_PREFIX}${message}`;

const createBaseError = <TError extends ClickHouseOrmError | DecodeError>(
  name: "ClickHouseOrmError" | "DecodeError",
  message: string,
  fields: Record<string, unknown>,
): TError => {
  const error = new Error(message) as TError;
  error.name = name;
  Object.assign(error, fields);
  return error;
};

const cloneError = <TError extends ClickHouseOrmError>(error: TError): TError => {
  const cloned = new Error(error.message) as TError;
  cloned.name = error.name;
  if (error.stack) {
    cloned.stack = error.stack;
  }
  Object.assign(cloned, error);
  return cloned;
};

const make = (
  kind: ClickHouseOrmErrorKind,
  executionState: ClickHouseOrmExecutionState,
  message: string,
  options?: Omit<ClickHouseOrmErrorOptions, "kind" | "executionState"> & {
    readonly markUnknownExecutionState?: boolean;
  },
) => {
  const { markUnknownExecutionState, ...rest } = options ?? {};
  const finalMessage = withCkOrmPrefix(
    markUnknownExecutionState && !message.endsWith(UNKNOWN_EXECUTION_STATE_SUFFIX)
      ? `${message}${UNKNOWN_EXECUTION_STATE_SUFFIX}`
      : message,
  );

  return createBaseError<ClickHouseOrmError>("ClickHouseOrmError", finalMessage, {
    kind,
    executionState,
    ...rest,
  });
};

const extractClickHouseName = (text: string) => {
  const matches = [...text.matchAll(/\(([A-Z][A-Z0-9_]+)\)/g)];
  return matches.at(-1)?.[1];
};

export const isClickHouseOrmError = (error: unknown): error is ClickHouseOrmError => {
  return (
    error instanceof Error &&
    isRecord(error) &&
    typeof error.kind === "string" &&
    typeof error.executionState === "string"
  );
};

export const isDecodeError = (error: unknown): error is DecodeError => {
  return isClickHouseOrmError(error) && error.kind === "decode" && "causeValue" in error;
};

export function ClickHouseOrmError() {
  /* compatibility guard */
}
Object.defineProperty(ClickHouseOrmError, Symbol.hasInstance, {
  value(value: unknown) {
    return isClickHouseOrmError(value);
  },
});

export function DecodeError() {
  /* compatibility guard */
}
Object.defineProperty(DecodeError, Symbol.hasInstance, {
  value(value: unknown) {
    return isDecodeError(value);
  },
});

export const withClickHouseOrmErrorContext = <TError extends ClickHouseOrmError>(
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
  options?: Omit<ClickHouseOrmErrorOptions, "kind" | "executionState">,
) => make("client_validation", "not_sent", message, options);

export const createSessionError = (
  message: string,
  options?: Omit<ClickHouseOrmErrorOptions, "kind" | "executionState">,
) => make("session", "not_sent", message, options);

/**
 * Constructs a `DecodeError` (a `ClickHouseOrmError` of kind `"decode"`)
 * for when a value coming back from ClickHouse cannot be coerced into its
 * TypeScript representation. Pass `path` for container columns so the message
 * pinpoints the failing field, e.g. `items[2].user.email`.
 */
export const createDecodeError = (
  message: string,
  causeValue: unknown,
  options?: Omit<ClickHouseOrmErrorOptions, "kind" | "executionState" | "cause"> & {
    readonly path?: string;
  },
) => {
  const finalMessage = withCkOrmPrefix(options?.path ? `${message} (at ${options.path})` : message);
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
  options?: Omit<ClickHouseOrmErrorOptions, "kind" | "executionState" | "requestTimeoutMs">,
) =>
  make("timeout", "unknown", `ClickHouse request timed out after ${requestTimeoutMs}ms`, {
    ...options,
    requestTimeoutMs,
    markUnknownExecutionState: true,
  });

export const createAbortedError = (
  message = "ClickHouse request was aborted",
  options?: Omit<ClickHouseOrmErrorOptions, "kind" | "executionState">,
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
  if (isClickHouseOrmError(error)) {
    return withClickHouseOrmErrorContext(error, context);
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
