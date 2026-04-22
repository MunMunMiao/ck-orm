export type ClickHouseOrmExecutionState = "not_sent" | "rejected" | "unknown";

export type ClickHouseOrmErrorKind =
  | "client_validation"
  | "request_failed"
  | "decode"
  | "timeout"
  | "aborted"
  | "session";

type ClickHouseOrmErrorOptions = {
  readonly kind: ClickHouseOrmErrorKind;
  readonly executionState: ClickHouseOrmExecutionState;
  readonly cause?: unknown;
  readonly queryId?: string;
  readonly sessionId?: string;
  readonly httpStatus?: number;
  readonly clickhouseCode?: number;
  readonly clickhouseName?: string;
  readonly responseText?: string;
  readonly requestTimeoutMs?: number;
};

type RequestFailedErrorOptions = Omit<ClickHouseOrmErrorOptions, "kind" | "executionState"> & {
  readonly executionState?: ClickHouseOrmExecutionState;
};

const CK_ORM_PREFIX = "[ck-orm] ";
const withCkOrmPrefix = (message: string) =>
  message.startsWith(CK_ORM_PREFIX) ? message : `${CK_ORM_PREFIX}${message}`;

const UNKNOWN_EXECUTION_STATE_SUFFIX = "; execution state is unknown";

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
  return new ClickHouseOrmError(finalMessage, {
    kind,
    executionState,
    ...rest,
  });
};

const extractClickHouseName = (text: string) => {
  const matches = [...text.matchAll(/\(([A-Z][A-Z0-9_]+)\)/g)];
  return matches.at(-1)?.[1];
};

export class ClickHouseOrmError extends Error {
  kind: ClickHouseOrmErrorKind;
  executionState: ClickHouseOrmExecutionState;
  override cause?: unknown;
  queryId?: string;
  sessionId?: string;
  httpStatus?: number;
  clickhouseCode?: number;
  clickhouseName?: string;
  responseText?: string;
  requestTimeoutMs?: number;

  constructor(message: string, options: ClickHouseOrmErrorOptions) {
    super(message);
    this.name = "ClickHouseOrmError";
    this.kind = options.kind;
    this.executionState = options.executionState;
    this.cause = options.cause;
    this.queryId = options.queryId;
    this.sessionId = options.sessionId;
    this.httpStatus = options.httpStatus;
    this.clickhouseCode = options.clickhouseCode;
    this.clickhouseName = options.clickhouseName;
    this.responseText = options.responseText;
    this.requestTimeoutMs = options.requestTimeoutMs;
  }
}

export class DecodeError extends ClickHouseOrmError {
  readonly causeValue: unknown;
  /** Dot/bracket-style path describing where decode failed inside a row, e.g. `items[2].user.email`. */
  readonly path?: string;

  constructor(
    message: string,
    causeValue: unknown,
    options?: Omit<ClickHouseOrmErrorOptions, "kind" | "executionState" | "cause"> & {
      readonly path?: string;
    },
  ) {
    super(withCkOrmPrefix(options?.path ? `${message} (at ${options.path})` : message), {
      kind: "decode",
      executionState: "rejected",
      cause: causeValue,
      ...options,
    });
    this.name = "DecodeError";
    this.causeValue = causeValue;
    this.path = options?.path;
  }
}

export const withClickHouseOrmErrorContext = <TError extends ClickHouseOrmError>(
  error: TError,
  context: {
    readonly queryId?: string;
    readonly sessionId?: string;
  },
) => {
  if (!error.queryId) {
    error.queryId = context.queryId;
  }
  if (!error.sessionId) {
    error.sessionId = context.sessionId;
  }
  return error;
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
 * Constructs a {@link DecodeError} (a `ClickHouseOrmError` of kind `"decode"`)
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
  return new DecodeError(message, causeValue, options);
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
  if (error instanceof ClickHouseOrmError) {
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
