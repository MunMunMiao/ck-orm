import { createClientValidationError, createSessionError } from "../errors";
import {
  type ClickHouseOrmInstrumentation,
  type ClickHouseOrmQueryKind,
  type ClickHouseOrmQueryMode,
  createQueryErrorEvent,
  createQueryEvent,
  createQuerySuccessEvent,
  emitQueryError,
  emitQueryStart,
  emitQuerySuccess,
  resolveSqlOperation,
} from "../observability";
import type { CompiledQuery, QueryClient } from "../query";
import { createQueryClient, createSessionId, decodeRow } from "../query";
import type { QueryParams } from "../query-shared";
import type { AnyTable } from "../schema";
import { buildCreateTemporaryTableStatement } from "../schema-ddl";
import { compileSql, sql } from "../sql";
import {
  buildQueryParams,
  type ClickHouseBaseQueryOptions,
  type ClickHouseEndpointOptions,
  type ClickHouseQueryOptions,
  type ClickHouseStreamOptions,
  type CreateTemporaryTableOptions,
  mergeClickHouseSettings,
  mergeQueryParams,
  normalizeRawQueryInput,
  normalizeSingleStatementSql,
  type RawQueryInput,
  type ResolveJoinUseNulls,
  type Session,
  type SessionRunInSessionOptions,
  toClickHouseTableName,
} from "./config";
import type { SessionConcurrencyController } from "./session-concurrency";
import type { FetchClickHouseTransport } from "./transport";

export interface ClickHouseOrmClient<TSchema, TJoinUseNulls extends 0 | 1 = 1>
  extends QueryClient<TSchema, TJoinUseNulls>,
    Session<TSchema, TJoinUseNulls> {
  readonly $client: FetchClickHouseTransport;
  executeCompiled<TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    options?: ClickHouseBaseQueryOptions,
  ): Promise<TResult[]>;
  iteratorCompiled<TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    options?: ClickHouseBaseQueryOptions,
  ): AsyncGenerator<TResult, void, unknown>;
}

type ClickHouseOrmClientConfig<TSchema, TJoinUseNulls extends 0 | 1> = {
  schema: TSchema;
  client: FetchClickHouseTransport;
  defaultOptions?: ClickHouseBaseQueryOptions;
  instrumentations?: readonly ClickHouseOrmInstrumentation[];
  sessionController?: SessionController;
  sessionConcurrencyController?: SessionConcurrencyController;
  joinUseNulls?: TJoinUseNulls;
};

type SessionController = {
  readonly sessionId: string;
  readonly ancestorSessionIds: readonly string[];
  registerTempTable(name: string): void;
  listTempTablesForCleanup(): readonly string[];
  createChildSessionController(sessionId: string): SessionController;
};

const createSessionController = (sessionId: string, ancestorSessionIds: readonly string[]): SessionController => {
  const tempTables: string[] = [];

  return {
    sessionId,
    ancestorSessionIds,
    registerTempTable(name: string) {
      if (!tempTables.includes(name)) {
        tempTables.push(name);
      }
    },
    listTempTablesForCleanup() {
      return [...tempTables].reverse();
    },
    createChildSessionController(childSessionId: string): SessionController {
      if ([sessionId, ...ancestorSessionIds].includes(childSessionId)) {
        throw createSessionError("Nested runInSession() cannot reuse an existing session_id");
      }
      return createSessionController(childSessionId, [sessionId, ...ancestorSessionIds]);
    },
  };
};

export const createClickHouseOrmClient = <TSchema, TJoinUseNulls extends 0 | 1 = 1>(
  config: ClickHouseOrmClientConfig<TSchema, TJoinUseNulls>,
): ClickHouseOrmClient<TSchema, TJoinUseNulls> => {
  const defaultOptions = config.defaultOptions ?? {};
  const instrumentations = config.instrumentations ?? [];
  const sessionController = config.sessionController;
  const sessionConcurrencyController = config.sessionConcurrencyController;
  const joinUseNulls = (config.joinUseNulls ?? 1) as TJoinUseNulls;

  let client!: ClickHouseOrmClient<TSchema, TJoinUseNulls>;
  const runner = createOrmRunner(() => client);
  const queryClient = createQueryClient<TSchema, TJoinUseNulls>({
    schema: config.schema,
    runner,
    joinUseNulls,
  });

  const mergeOptions = <TOptions extends ClickHouseBaseQueryOptions>(
    options?: TOptions,
  ): ClickHouseBaseQueryOptions & TOptions => {
    const queryParams = {
      ...(defaultOptions.query_params ?? {}),
      ...(options?.query_params ?? {}),
    };
    mergeQueryParams(undefined, queryParams);

    return {
      ...defaultOptions,
      ...options,
      clickhouse_settings: mergeClickHouseSettings(defaultOptions.clickhouse_settings, options?.clickhouse_settings),
      http_headers: {
        ...(defaultOptions.http_headers ?? {}),
        ...(options?.http_headers ?? {}),
      },
      query_params: queryParams,
    } as ClickHouseBaseQueryOptions & TOptions;
  };

  const runSerial = async <TValue>(
    options: ClickHouseBaseQueryOptions,
    operation: () => Promise<TValue>,
  ): Promise<TValue> => {
    if (!sessionConcurrencyController) {
      return await operation();
    }
    return await sessionConcurrencyController.run(options.session_id, operation);
  };

  const buildQueryEvent = (input: {
    mode: ClickHouseOrmQueryMode;
    queryKind: ClickHouseOrmQueryKind;
    statement: string;
    operation?: string;
    options: ClickHouseBaseQueryOptions;
    format?: ClickHouseQueryOptions["format"] | ClickHouseStreamOptions["format"];
    tableName?: string;
  }) => {
    return createQueryEvent({
      mode: input.mode,
      queryKind: input.queryKind,
      statement: input.statement,
      operation: input.operation ?? resolveSqlOperation(input.statement),
      queryId: input.options.query_id,
      sessionId: input.options.session_id,
      format: input.format,
      settings: input.options.clickhouse_settings,
      startedAt: Date.now(),
      tableName: input.tableName,
    });
  };

  const executeWithInstrumentation = async <TValue>(
    input: {
      mode: ClickHouseOrmQueryMode;
      queryKind: ClickHouseOrmQueryKind;
      statement: string;
      options: ClickHouseBaseQueryOptions;
      format?: ClickHouseQueryOptions["format"] | ClickHouseStreamOptions["format"];
      tableName?: string;
      operation?: string;
    },
    operation: () => Promise<{ value: TValue; rowCount?: number }>,
  ): Promise<TValue> => {
    const event = buildQueryEvent(input);
    await emitQueryStart(instrumentations, event);

    try {
      const result = await operation();
      await emitQuerySuccess(
        instrumentations,
        createQuerySuccessEvent(event, Date.now() - event.startedAt, result.rowCount),
      );
      return result.value;
    } catch (error) {
      await emitQueryError(instrumentations, createQueryErrorEvent(event, error, Date.now() - event.startedAt));
      throw error;
    }
  };

  const streamWithInstrumentation = async function* <TValue>(
    input: {
      mode: ClickHouseOrmQueryMode;
      queryKind: ClickHouseOrmQueryKind;
      statement: string;
      options: ClickHouseBaseQueryOptions;
      format?: ClickHouseQueryOptions["format"] | ClickHouseStreamOptions["format"];
      tableName?: string;
      operation?: string;
    },
    operation: () => AsyncGenerator<TValue, void, unknown>,
  ): AsyncGenerator<TValue, void, unknown> {
    const event = buildQueryEvent(input);
    await emitQueryStart(instrumentations, event);

    let rowCount = 0;
    let caughtError: unknown;

    try {
      for await (const row of operation()) {
        rowCount += 1;
        yield row;
      }
    } catch (error) {
      caughtError = error;
      throw error;
    } finally {
      const durationMs = Date.now() - event.startedAt;
      if (caughtError === undefined) {
        await emitQuerySuccess(instrumentations, createQuerySuccessEvent(event, durationMs, rowCount));
      } else {
        await emitQueryError(instrumentations, createQueryErrorEvent(event, caughtError, durationMs, rowCount));
      }
    }
  };

  const runLockedStream = async function* <TValue>(
    options: ClickHouseBaseQueryOptions,
    operation: () => AsyncGenerator<TValue, void, unknown>,
  ): AsyncGenerator<TValue, void, unknown> {
    if (!sessionConcurrencyController) {
      yield* operation();
      return;
    }
    yield* sessionConcurrencyController.runStream(options.session_id, operation);
  };

  const executeCompiled = async <TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    options?: ClickHouseBaseQueryOptions,
  ): Promise<TResult[]> => {
    const baseOptions = mergeOptions(options);
    const mergedOptions = {
      ...baseOptions,
      clickhouse_settings: mergeClickHouseSettings(baseOptions.clickhouse_settings, compiled.forcedSettings),
    };
    const queryConfig = buildQueryParams(compiled, mergedOptions);

    return runSerial(mergedOptions, async () => {
      const operation = resolveSqlOperation(queryConfig.query);
      const mode = compiled.mode === "command" ? (operation === "INSERT" ? "insert" : "command") : "query";

      return executeWithInstrumentation(
        {
          mode,
          queryKind: "typed",
          statement: queryConfig.query,
          options: mergedOptions,
          format: compiled.mode === "command" ? undefined : "JSON",
          operation,
        },
        async () => {
          if (compiled.mode === "command") {
            await config.client.command(queryConfig.query, mergedOptions, queryConfig.query_params);
            return { value: [] as TResult[] };
          }

          const rows = await config.client.queryJSON<Record<string, unknown>>({
            statement: queryConfig.query,
            query_params: queryConfig.query_params,
            options: mergedOptions,
            format: "JSON",
          });

          if (compiled.selection.length === 0) {
            return {
              value: rows as TResult[],
              rowCount: rows.length,
            };
          }

          return {
            value: rows.map((row) => decodeRow<TResult>(row, compiled.selection)),
            rowCount: rows.length,
          };
        },
      );
    });
  };

  const createStreamGenerator = async function* <TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    query: string,
    queryParams: QueryParams,
    options: ClickHouseBaseQueryOptions,
  ): AsyncGenerator<TResult, void, unknown> {
    for await (const row of config.client.queryStream<Record<string, unknown>>({
      statement: query,
      query_params: queryParams,
      options,
      format: "JSONEachRow",
    })) {
      if (compiled.selection.length === 0) {
        yield row as TResult;
        continue;
      }
      yield decodeRow<TResult>(row, compiled.selection);
    }
  };

  const iteratorCompiled = async function* <TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    options?: ClickHouseBaseQueryOptions,
  ): AsyncGenerator<TResult, void, unknown> {
    const baseOptions = mergeOptions(options);
    const mergedOptions = {
      ...baseOptions,
      clickhouse_settings: mergeClickHouseSettings(baseOptions.clickhouse_settings, compiled.forcedSettings),
    };
    const queryConfig = buildQueryParams(compiled, mergedOptions);
    const operation = resolveSqlOperation(queryConfig.query);

    yield* runLockedStream(mergedOptions, () =>
      streamWithInstrumentation(
        {
          mode: "stream",
          queryKind: "typed",
          statement: queryConfig.query,
          options: mergedOptions,
          format: "JSONEachRow",
          operation,
        },
        () => createStreamGenerator(compiled, queryConfig.query, queryConfig.query_params, mergedOptions),
      ),
    );
  };

  const createChild = <TNextJoinUseNulls extends 0 | 1 = TJoinUseNulls>(
    nextDefaultOptions: ClickHouseBaseQueryOptions,
    nextSessionController?: SessionController,
    nextJoinUseNulls?: TNextJoinUseNulls,
  ) => {
    return createClickHouseOrmClient<TSchema, TNextJoinUseNulls>({
      client: config.client,
      defaultOptions: nextDefaultOptions,
      instrumentations,
      schema: config.schema,
      sessionController: nextSessionController,
      sessionConcurrencyController,
      joinUseNulls: nextJoinUseNulls ?? (joinUseNulls as unknown as TNextJoinUseNulls),
    });
  };

  const renderTempTableIdentifier = (name: string) => {
    return compileSql(sql.identifier({ table: name })).query;
  };

  const registerValidatedTempTable = (name: string) => {
    sessionController?.registerTempTable(name);
  };

  const mapLogicalJsonEachRowKeys = (
    table: AnyTable | string,
    rows: readonly Record<string, unknown>[] | AsyncIterable<Record<string, unknown>>,
  ): readonly Record<string, unknown>[] | AsyncIterable<Record<string, unknown>> => {
    if (typeof table === "string") {
      return rows;
    }

    const mappings = Object.entries(table.columns)
      .map(([schemaKey, column]) => ({
        logicalKey: column.key ?? schemaKey,
        physicalName: column.name ?? schemaKey,
      }))
      .filter((mapping) => mapping.logicalKey !== mapping.physicalName);

    if (mappings.length === 0) {
      return rows;
    }

    const mapRow = (row: Record<string, unknown>): Record<string, unknown> => {
      let mapped: Record<string, unknown> | undefined;
      for (const { logicalKey, physicalName } of mappings) {
        if (!Object.hasOwn(row, logicalKey)) {
          continue;
        }
        if (Object.hasOwn(row, physicalName)) {
          throw createClientValidationError(
            `insertJsonEachRow() row contains both logical key "${logicalKey}" and database column "${physicalName}"`,
          );
        }
        mapped ??= { ...row };
        mapped[physicalName] = mapped[logicalKey];
        delete mapped[logicalKey];
      }
      return mapped ?? row;
    };

    if (Array.isArray(rows)) {
      return rows.map(mapRow);
    }

    return (async function* mappedRows() {
      for await (const row of rows) {
        yield mapRow(row);
      }
    })();
  };

  client = {
    ...queryClient,
    $client: config.client,
    sessionId: sessionController?.sessionId ?? defaultOptions.session_id ?? "",

    async execute(query: RawQueryInput, options?: ClickHouseQueryOptions) {
      const mergedOptions = mergeOptions(options);
      const normalized = normalizeRawQueryInput(query);
      return runSerial(mergedOptions, async () => {
        return executeWithInstrumentation(
          {
            mode: "query",
            queryKind: "raw",
            statement: normalized.query,
            options: mergedOptions,
            format: "JSON",
          },
          async () => {
            const rows = await config.client.queryJSON<Record<string, unknown>>({
              statement: normalized.query,
              query_params: mergeQueryParams(normalized.params, mergedOptions.query_params),
              options: mergedOptions,
              format: mergedOptions.format ?? "JSON",
            });
            return {
              value: rows,
              rowCount: rows.length,
            };
          },
        );
      });
    },

    async *stream(
      query: RawQueryInput,
      options?: ClickHouseStreamOptions,
    ): AsyncGenerator<Record<string, unknown>, void, unknown> {
      const mergedOptions = mergeOptions(options);
      const normalized = normalizeRawQueryInput(query);

      const createGenerator = async function* () {
        yield* config.client.queryStream<Record<string, unknown>>({
          statement: normalized.query,
          query_params: mergeQueryParams(normalized.params, mergedOptions.query_params),
          options: mergedOptions,
          format: mergedOptions.format ?? "JSONEachRow",
        });
      };

      yield* runLockedStream(mergedOptions, () =>
        streamWithInstrumentation(
          {
            mode: "stream",
            queryKind: "raw",
            statement: normalized.query,
            options: mergedOptions,
            format: mergedOptions.format ?? "JSONEachRow",
          },
          createGenerator,
        ),
      );
    },

    async command(query: RawQueryInput, options?: ClickHouseBaseQueryOptions): Promise<void> {
      const mergedOptions = mergeOptions(options);
      const normalized = normalizeRawQueryInput(query);
      await runSerial(mergedOptions, async () => {
        await executeWithInstrumentation(
          {
            mode: "command",
            queryKind: "raw",
            statement: normalized.query,
            options: mergedOptions,
          },
          async () => {
            await config.client.command(
              normalized.query,
              mergedOptions,
              mergeQueryParams(normalized.params, mergedOptions.query_params),
            );
            return { value: undefined };
          },
        );
      });
    },

    async ping(options?: ClickHouseEndpointOptions): Promise<void> {
      const mergedOptions = mergeOptions(options);
      await runSerial(mergedOptions, async () => {
        await executeWithInstrumentation(
          {
            mode: "command",
            queryKind: "raw",
            statement: "GET /ping",
            options: mergedOptions,
            operation: "PING",
          },
          async () => {
            await config.client.endpoint("/ping", mergedOptions, "GET");
            return { value: undefined };
          },
        );
      });
    },

    async replicasStatus(options?: ClickHouseEndpointOptions): Promise<void> {
      const mergedOptions = mergeOptions(options);
      await runSerial(mergedOptions, async () => {
        await executeWithInstrumentation(
          {
            mode: "command",
            queryKind: "raw",
            statement: "GET /replicas_status",
            options: mergedOptions,
            operation: "REPLICAS_STATUS",
          },
          async () => {
            await config.client.endpoint("/replicas_status", mergedOptions, "GET");
            return { value: undefined };
          },
        );
      });
    },

    withSettings<TSettings extends Record<string, string | number | boolean>>(
      settings: TSettings,
    ): ClickHouseOrmClient<TSchema, ResolveJoinUseNulls<TSettings, TJoinUseNulls>> {
      const nextJoinUseNulls = (
        settings.join_use_nulls === 0 || settings.join_use_nulls === 1 ? settings.join_use_nulls : joinUseNulls
      ) as ResolveJoinUseNulls<TSettings, TJoinUseNulls>;

      return createChild(
        {
          ...defaultOptions,
          clickhouse_settings: {
            ...(defaultOptions.clickhouse_settings ?? {}),
            ...settings,
          },
        },
        sessionController,
        nextJoinUseNulls,
      );
    },

    async insertJsonEachRow(
      table: AnyTable | string,
      rows: readonly Record<string, unknown>[] | AsyncIterable<Record<string, unknown>>,
      options?: ClickHouseBaseQueryOptions,
    ) {
      const mergedOptions = mergeOptions(options);
      const tableName = toClickHouseTableName(table);
      const tableIdentifier = compileSql(sql.identifier({ table: tableName })).query;
      const mappedRows = mapLogicalJsonEachRowKeys(table, rows);
      const arrayRowCount = Array.isArray(rows) ? rows.length : undefined;
      return runSerial(mergedOptions, async () => {
        await executeWithInstrumentation(
          {
            mode: "insert",
            queryKind: "raw",
            statement: `INSERT INTO ${tableIdentifier} FORMAT JSONEachRow`,
            options: mergedOptions,
            format: "JSONEachRow",
            operation: "INSERT",
            tableName,
          },
          async () => {
            if (arrayRowCount !== 0) {
              await config.client.insertJsonEachRow(tableName, mappedRows, mergedOptions);
            }
            return {
              value: undefined,
              rowCount: arrayRowCount,
            };
          },
        );
      });
    },

    registerTempTable(name: string) {
      if (!sessionController) {
        throw createSessionError("registerTempTable() requires runInSession()");
      }
      renderTempTableIdentifier(name);
      registerValidatedTempTable(name);
    },

    async createTemporaryTable(table: AnyTable, options?: CreateTemporaryTableOptions) {
      if (!sessionController) {
        throw createSessionError("createTemporaryTable() requires runInSession()");
      }
      const statement = buildCreateTemporaryTableStatement(table, options?.mode);
      registerValidatedTempTable(table.originalName);
      await client.command(sql(statement));
    },

    async createTemporaryTableRaw(name: string, definition: string) {
      if (!sessionController) {
        throw createSessionError("createTemporaryTableRaw() requires runInSession()");
      }
      const normalizedDefinition = normalizeSingleStatementSql(
        definition,
        "createTemporaryTableRaw() definition must not contain multiple statements; use developer-controlled SQL only",
      );
      const identifier = renderTempTableIdentifier(name);
      registerValidatedTempTable(name);
      await client.command(sql(`CREATE TEMPORARY TABLE ${identifier} ${normalizedDefinition}`));
    },

    async runInSession<TResult>(
      callback: (session: Session<TSchema, TJoinUseNulls>) => Promise<TResult>,
      options?: SessionRunInSessionOptions,
    ): Promise<TResult> {
      if (sessionController && options?.session_check === 1) {
        throw createClientValidationError(
          "Nested runInSession() cannot use session_check=1 because child sessions are created by ck-orm",
        );
      }

      if (!sessionController && options?.session_check === 1 && !options.session_id) {
        throw createClientValidationError(
          "runInSession() requires an explicit session_id when session_check=1 because ClickHouse only validates existing sessions",
        );
      }

      const sessionId = options?.session_id ?? createSessionId();
      const childSessionController = sessionController
        ? sessionController.createChildSessionController(sessionId)
        : createSessionController(sessionId, []);
      const sessionClient = createChild(
        {
          ...defaultOptions,
          ...options,
          session_id: sessionId,
        },
        childSessionController,
      );

      let userError: unknown;
      let userErrored = false;
      let result: TResult | undefined;
      try {
        result = await callback(sessionClient);
      } catch (error) {
        userError = error;
        userErrored = true;
      }

      const cleanupErrors: unknown[] = [];
      for (const tableName of childSessionController.listTempTablesForCleanup()) {
        try {
          const identifier = renderTempTableIdentifier(tableName);
          await sessionClient.command(sql(`DROP TABLE IF EXISTS ${identifier}`), {
            ignore_error_response: true,
          });
        } catch (error) {
          cleanupErrors.push(error);
        }
      }

      if (cleanupErrors.length > 0) {
        if (options?.onCleanupError) {
          try {
            options.onCleanupError(cleanupErrors, { sessionId });
          } catch {
            // The user-provided hook must not be allowed to mask either the
            // user's error or the act of returning the user's result.
          }
        } else if (!userErrored) {
          const cause =
            cleanupErrors.length === 1
              ? cleanupErrors[0]
              : new AggregateError(cleanupErrors, "Multiple temporary-table cleanups failed");
          throw createSessionError(
            `Failed to drop ${cleanupErrors.length} temporary table${cleanupErrors.length === 1 ? "" : "s"} for session ${sessionId}`,
            { cause },
          );
        }
      }

      if (userErrored) {
        throw userError;
      }
      return result as TResult;
    },

    executeCompiled,
    iteratorCompiled,
  } as ClickHouseOrmClient<TSchema, TJoinUseNulls>;

  return client;
};

export const createOrmRunner = <TSchema, TJoinUseNulls extends 0 | 1>(
  resolveClient: () => Pick<ClickHouseOrmClient<TSchema, TJoinUseNulls>, "executeCompiled" | "iteratorCompiled">,
) => ({
  execute<TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    options?: ClickHouseBaseQueryOptions,
  ) {
    return resolveClient().executeCompiled(compiled, options);
  },
  iterator<TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    options?: ClickHouseBaseQueryOptions,
  ) {
    return resolveClient().iteratorCompiled(compiled, options);
  },
  async command(compiled: CompiledQuery<Record<string, unknown>>, options?: ClickHouseBaseQueryOptions) {
    await resolveClient().executeCompiled(
      {
        ...compiled,
        mode: "command",
      },
      options,
    );
  },
});
