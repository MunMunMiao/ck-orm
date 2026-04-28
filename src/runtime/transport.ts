import { createClientValidationError, normalizeTransportError } from "../errors";
import { createUuid } from "../platform";
import { compileSql, sql } from "../sql";
import { createAbortController } from "./abort";
import {
  assertValidQueryId,
  assertValidQueryParamKey,
  assertValidSessionId,
  buildEndpointUrl,
  buildRequestUrl,
  buildSearchParams,
  type ClickHouseBaseQueryOptions,
  type ClickHouseEndpointOptions,
  type ClickHouseQueryOptions,
  type ClickHouseStreamOptions,
  createHeaders,
  formatQueryParamValue,
  mergeClickHouseSettings,
  type NormalizedClientConfig,
  normalizeQuery,
  normalizeSingleStatementSql,
  normalizeTransportSettings,
  type RequestBody,
  type RequestHandle,
  type ResponseParseMode,
  type TransportQueryInput,
  validateFormat,
} from "./config";
import {
  createJsonEachRowBody,
  createLineStream,
  parseJsonEachRowLine,
  parseValidatedResponseJson,
  readValidatedResponseText,
} from "./json-stream";

export interface FetchClickHouseTransport {
  queryJSON<T = Record<string, unknown>>(input: TransportQueryInput): Promise<T[]>;
  queryStream<T = Record<string, unknown>>(input: TransportQueryInput): AsyncGenerator<T, void, unknown>;
  command(
    statement: string,
    options?: ClickHouseBaseQueryOptions,
    query_params?: Record<string, unknown>,
  ): Promise<void>;
  insertJsonEachRow(
    tableName: string,
    rows: readonly Record<string, unknown>[] | AsyncIterable<Record<string, unknown>>,
    options?: ClickHouseBaseQueryOptions,
  ): Promise<void>;
  endpoint(path: string, options?: ClickHouseEndpointOptions, method?: "GET" | "POST"): Promise<void>;
}

export const createFetchClickHouseTransport = (config: NormalizedClientConfig): FetchClickHouseTransport => {
  const mergeOptions = <TOptions extends ClickHouseBaseQueryOptions>(
    options?: TOptions,
  ): ClickHouseBaseQueryOptions & TOptions => {
    return {
      ...options,
      clickhouse_settings: mergeClickHouseSettings(config.clickhouse_settings, options?.clickhouse_settings),
      http_headers: {
        ...(options?.http_headers ?? {}),
      },
      session_id: options?.session_id ?? config.session_id,
      role: options?.role ?? config.role,
    } as ClickHouseBaseQueryOptions & TOptions;
  };

  const send = async (input: {
    readonly statement: string;
    readonly options?: ClickHouseBaseQueryOptions;
    readonly query_params?: Record<string, unknown>;
    readonly format?: ClickHouseQueryOptions["format"] | ClickHouseStreamOptions["format"];
    readonly parseMode: ResponseParseMode;
    readonly body?: RequestBody;
    readonly duplex?: "half";
    readonly sendQueryInBody: boolean;
    readonly ignoreErrorResponse?: boolean;
  }): Promise<RequestHandle> => {
    const mergedOptions = mergeOptions(input.options);
    const queryId = mergedOptions.query_id ?? createUuid();
    assertValidQueryId(queryId);
    if (mergedOptions.session_id) {
      assertValidSessionId(mergedOptions.session_id);
    }
    if (config.authSource === "databaseUrl" && mergedOptions.auth) {
      throw createClientValidationError("Per-request auth cannot override credentials embedded in databaseUrl");
    }
    const auth = config.authSource === "databaseUrl" ? config.auth : (mergedOptions.auth ?? config.auth);
    const clickhouseSettings = normalizeTransportSettings({
      settings: mergeClickHouseSettings(
        mergedOptions.clickhouse_settings,
        config.compression.response ? { enable_http_compression: 1 } : undefined,
      ),
      parseMode: input.parseMode,
    });

    const normalizedStatement = input.format
      ? normalizeQuery(input.statement, input.format)
      : normalizeSingleStatementSql(
          input.statement,
          "Query contains multiple statements; only a single statement is allowed per request",
        );
    const useMultipart =
      input.query_params !== undefined && Object.keys(input.query_params).length > 0 && input.sendQueryInBody;

    const searchParams = buildSearchParams({
      database: config.database,
      clickhouse_settings: clickhouseSettings,
      query: input.sendQueryInBody || useMultipart ? undefined : normalizedStatement,
      query_id: queryId,
      session_id: mergedOptions.session_id,
      session_timeout: mergedOptions.session_timeout,
      session_check: mergedOptions.session_check,
      role: mergedOptions.role,
    });

    let body: RequestBody | undefined = input.body;
    if (body === undefined) {
      if (useMultipart) {
        const form = new FormData();
        form.append("query", normalizedStatement);
        for (const [key, value] of Object.entries(input.query_params ?? {})) {
          assertValidQueryParamKey(key);
          form.append(`param_${key}`, formatQueryParamValue(value));
        }
        body = form;
      } else if (input.sendQueryInBody) {
        body = normalizedStatement;
      }
    }

    const headers = createHeaders({
      config,
      options: mergedOptions,
      auth,
    });

    const { signal, cleanup } = createAbortController(config.request_timeout, mergedOptions.abort_signal);
    const finalize = () => {
      cleanup();
    };

    try {
      const init: RequestInit & { duplex?: "half" } = {
        method: "POST",
        headers,
        body: body as BodyInit | null | undefined,
        signal,
      };
      if (body instanceof ReadableStream && input.duplex === "half") {
        init.duplex = "half";
      }

      const response = await fetch(buildRequestUrl(config.url, searchParams), init);
      const ignoreErrorResponse = mergedOptions.ignore_error_response ?? false;
      return {
        response,
        queryId,
        options: mergedOptions,
        finalize,
        readValidatedText: () =>
          readValidatedResponseText({
            response,
            queryId,
            sessionId: mergedOptions.session_id,
            ignoreErrorResponse,
          }),
      };
    } catch (error) {
      finalize();
      throw normalizeTransportError(error, {
        queryId,
        sessionId: mergedOptions.session_id,
      });
    }
  };

  const transport: FetchClickHouseTransport = {
    async queryJSON<T = Record<string, unknown>>(input: TransportQueryInput): Promise<T[]> {
      const format = input.format ?? "JSON";
      validateFormat("query", format);
      const request = await send({
        statement: input.statement,
        query_params: input.query_params,
        options: input.options,
        format,
        parseMode: "json",
        sendQueryInBody: true,
      });
      try {
        const result = await parseValidatedResponseJson<{ data?: T[] }>({
          response: request.response,
          queryId: request.queryId,
          sessionId: request.options.session_id,
          json: config.json,
          ignoreErrorResponse: request.options.ignore_error_response ?? false,
        });
        return result.data ?? [];
      } finally {
        request.finalize();
      }
    },

    async *queryStream<T = Record<string, unknown>>(input: TransportQueryInput): AsyncGenerator<T, void, unknown> {
      const format = input.format ?? "JSONEachRow";
      validateFormat("stream", format);
      const request = await send({
        statement: input.statement,
        query_params: input.query_params,
        options: input.options,
        format,
        parseMode: "json_each_row",
        sendQueryInBody: true,
      });
      try {
        if (!request.response.ok) {
          await request.readValidatedText();
          return;
        }
        for await (const line of createLineStream(request.response)) {
          yield parseJsonEachRowLine({
            line,
            response: request.response,
            queryId: request.queryId,
            sessionId: request.options.session_id,
            json: config.json,
          }) as T;
        }
      } catch (error) {
        throw normalizeTransportError(error, {
          queryId: request.queryId,
          sessionId: request.options.session_id,
        });
      } finally {
        request.finalize();
      }
    },

    async command(statement: string, options?: ClickHouseBaseQueryOptions, query_params?: Record<string, unknown>) {
      const request = await send({
        statement,
        query_params,
        options,
        parseMode: "text",
        sendQueryInBody: true,
        ignoreErrorResponse: options?.ignore_error_response,
      });
      try {
        await request.readValidatedText();
      } finally {
        request.finalize();
      }
    },

    async insertJsonEachRow(
      tableName: string,
      rows: readonly Record<string, unknown>[] | AsyncIterable<Record<string, unknown>>,
      options?: ClickHouseBaseQueryOptions,
    ) {
      const requestBody = await createJsonEachRowBody(rows, config.json);
      const tableIdentifier = compileSql(sql.identifier({ table: tableName })).query;
      const statement = `INSERT INTO ${tableIdentifier} FORMAT JSONEachRow`;
      const request = await send({
        statement,
        options,
        body: requestBody.body,
        duplex: requestBody.duplex,
        parseMode: "text",
        sendQueryInBody: false,
      });
      try {
        await request.readValidatedText();
      } finally {
        request.finalize();
      }
    },

    async endpoint(path: string, options?: ClickHouseEndpointOptions, method: "GET" | "POST" = "GET") {
      const mergedOptions = mergeOptions(options);
      if (config.authSource === "databaseUrl" && mergedOptions.auth) {
        throw createClientValidationError("Per-request auth cannot override credentials embedded in databaseUrl");
      }
      const auth = config.authSource === "databaseUrl" ? config.auth : (mergedOptions.auth ?? config.auth);
      const queryId = mergedOptions.query_id ?? createUuid();
      assertValidQueryId(queryId);
      const headers = createHeaders({
        config,
        options: mergedOptions,
        auth,
      });
      const { signal, cleanup } = createAbortController(config.request_timeout, mergedOptions.abort_signal);

      try {
        const response = await fetch(buildEndpointUrl(config.url, path), {
          method,
          headers,
          signal,
        });
        await readValidatedResponseText({
          response,
          queryId,
          sessionId: mergedOptions.session_id,
          ignoreErrorResponse: false,
        });
      } catch (error) {
        throw normalizeTransportError(error, {
          queryId,
          sessionId: mergedOptions.session_id,
        });
      } finally {
        cleanup();
      }
    },
  };

  return transport;
};
