import {
  createClientValidationError,
  createRequestFailedError,
  extractClickHouseException,
  normalizeTransportError,
} from "../errors";
import type { ClickHouseORMQueryStatistics } from "../observability";
import { resolveStreamRequestBodyMode } from "../platform";

export type JsonHandling = {
  readonly parse: (text: string) => unknown;
  readonly stringify: (value: unknown) => string;
};

export type JsonEachRowRequestBody = {
  body: string | ReadableStream<Uint8Array>;
  duplex?: "half";
};

type ClickHouseJsonStatistics = {
  readonly elapsed?: unknown;
  readonly rows_read?: unknown;
  readonly bytes_read?: unknown;
};

export type ClickHouseJsonResponse<T> = {
  readonly data?: T[];
  readonly rows?: unknown;
  readonly rows_before_limit_at_least?: unknown;
  readonly statistics?: ClickHouseJsonStatistics;
};

const toFiniteNumber = (value: unknown): number | undefined => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : undefined;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
};

export const extractClickHouseJsonStatistics = <T>(
  response: ClickHouseJsonResponse<T>,
): ClickHouseORMQueryStatistics | undefined => {
  const elapsedSeconds = toFiniteNumber(response.statistics?.elapsed);
  const readRows = toFiniteNumber(response.statistics?.rows_read);
  const readBytes = toFiniteNumber(response.statistics?.bytes_read);
  const resultRows = toFiniteNumber(response.rows);
  const rowsBeforeLimitAtLeast = toFiniteNumber(response.rows_before_limit_at_least);
  const statistics: ClickHouseORMQueryStatistics = {
    ...(elapsedSeconds === undefined ? {} : { serverElapsedMs: elapsedSeconds * 1000 }),
    ...(readRows === undefined ? {} : { readRows }),
    ...(readBytes === undefined ? {} : { readBytes }),
    ...(resultRows === undefined ? {} : { resultRows }),
    ...(rowsBeforeLimitAtLeast === undefined ? {} : { rowsBeforeLimitAtLeast }),
  };

  return Object.keys(statistics).length === 0 ? undefined : statistics;
};

const readResponseText = async (response: Response) => {
  return await response.text();
};

export const readValidatedResponseText = async (input: {
  readonly response: Response;
  readonly queryId: string;
  readonly sessionId?: string;
  readonly ignoreErrorResponse: boolean;
}) => {
  let text: string;
  try {
    text = await readResponseText(input.response);
  } catch (error) {
    throw normalizeTransportError(error, {
      queryId: input.queryId,
      sessionId: input.sessionId,
    });
  }
  if (input.ignoreErrorResponse) {
    return text;
  }

  const embeddedException = extractClickHouseException(text);
  if (!input.response.ok) {
    throw createRequestFailedError({
      httpStatus: input.response.status,
      queryId: input.queryId,
      sessionId: input.sessionId,
      responseText: embeddedException?.responseText ?? text,
      clickhouseCode: embeddedException?.clickhouseCode,
      clickhouseName: embeddedException?.clickhouseName,
    });
  }

  if (embeddedException) {
    throw createRequestFailedError({
      httpStatus: input.response.status,
      queryId: input.queryId,
      sessionId: input.sessionId,
      responseText: embeddedException.responseText,
      clickhouseCode: embeddedException.clickhouseCode,
      clickhouseName: embeddedException.clickhouseName,
    });
  }

  return text;
};

export const parseValidatedResponseJson = async <T>(input: {
  readonly response: Response;
  readonly queryId: string;
  readonly sessionId?: string;
  readonly json: JsonHandling;
  readonly ignoreErrorResponse: boolean;
}): Promise<T> => {
  const text = await readValidatedResponseText(input);
  try {
    return input.json.parse(text) as T;
  } catch (error) {
    throw createRequestFailedError({
      httpStatus: input.response.status,
      queryId: input.queryId,
      sessionId: input.sessionId,
      responseText: text,
      cause: error,
    });
  }
};

export const parseJsonEachRowLine = (input: {
  readonly line: string;
  readonly response: Response;
  readonly queryId: string;
  readonly sessionId?: string;
  readonly json: JsonHandling;
}) => {
  const embeddedException = extractClickHouseException(input.line);
  if (embeddedException) {
    throw createRequestFailedError({
      httpStatus: input.response.status,
      queryId: input.queryId,
      sessionId: input.sessionId,
      responseText: embeddedException.responseText,
      clickhouseCode: embeddedException.clickhouseCode,
      clickhouseName: embeddedException.clickhouseName,
    });
  }

  try {
    return input.json.parse(input.line) as Record<string, unknown>;
  } catch (error) {
    throw createRequestFailedError({
      httpStatus: input.response.status,
      queryId: input.queryId,
      sessionId: input.sessionId,
      responseText: input.line,
      cause: error,
    });
  }
};

export const createLineStream = async function* (response: Response) {
  if (!response.body) {
    return;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      let newlineIndex = buffer.indexOf("\n");
      while (newlineIndex >= 0) {
        const line = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);
        if (line) {
          yield line;
        }
        newlineIndex = buffer.indexOf("\n");
      }
    }

    const finalLine = buffer.trim();
    if (finalLine) {
      yield finalLine;
    }
  } finally {
    try {
      await reader.cancel();
    } catch {
      // ignore reader cancellation failures
    }
  }
};

export const createJsonEachRowBody = (
  rows: readonly Record<string, unknown>[] | AsyncIterable<Record<string, unknown>>,
  json: JsonHandling,
): JsonEachRowRequestBody | Promise<JsonEachRowRequestBody> => {
  if (Array.isArray(rows)) {
    return {
      body: rows.map((row) => json.stringify(row)).join("\n"),
    };
  }

  const bodyMode = resolveStreamRequestBodyMode();
  if (bodyMode === "buffered") {
    throw createClientValidationError(
      "insertJsonEachRow() AsyncIterable rows require a runtime with streaming request body support; pass a bounded array when stream uploads are unavailable",
    );
  }

  const encoder = new TextEncoder();
  const iterator = (rows as AsyncIterable<Record<string, unknown>>)[Symbol.asyncIterator]();
  let closing: Promise<unknown> | undefined;

  const closeIterator = () => {
    closing ??= Promise.resolve(iterator.return?.());
    return closing;
  };

  return {
    body: new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const result = await iterator.next();
          if (result.done) {
            await closeIterator();
            controller.close();
            return;
          }
          controller.enqueue(encoder.encode(`${json.stringify(result.value)}\n`));
        } catch (error) {
          try {
            await closeIterator();
          } catch {
            // ignore iterator cleanup failures
          }
          controller.error(error);
          throw error;
        }
      },
      async cancel() {
        try {
          await closeIterator();
        } catch {
          // ignore iterator cleanup failures
        }
      },
    }),
    ...(bodyMode === "duplex-half" ? { duplex: "half" as const } : {}),
  };
};
