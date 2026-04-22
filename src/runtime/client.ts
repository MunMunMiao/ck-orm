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
import type { CompiledQuery } from "../query";
import { createSessionId, decodeRow, QueryClient } from "../query";
import type { QueryParams } from "../query-shared";
import type { AnyTable } from "../schema";
import { compileSql, sql } from "../sql";
import {
  buildQueryParams,
  type ClickHouseBaseQueryOptions,
  type ClickHouseEndpointOptions,
  type ClickHouseQueryOptions,
  type ClickHouseStreamOptions,
  mergeClickHouseSettings,
  mergeQueryParams,
  normalizeRawQueryInput,
  normalizeSingleStatementSql,
  type RawQueryInput,
  type ResolveJoinUseNulls,
  type SessionApi,
  type SessionContext,
  toClickHouseTableName,
} from "./config";
import type { FetchClickHouseTransport } from "./transport";

export class ClickHouseOrmClient<TSchema, TJoinUseNulls extends 0 | 1 = 1>
  extends QueryClient<TSchema, TJoinUseNulls>
  implements SessionApi
{
  readonly $client: FetchClickHouseTransport;
  readonly sessionId: string;
  private readonly defaultOptions: ClickHouseBaseQueryOptions;
  private readonly instrumentations: readonly ClickHouseOrmInstrumentation[];
  private readonly sessionContext?: SessionContext;

  constructor(config: {
    schema: TSchema;
    client: FetchClickHouseTransport;
    defaultOptions?: ClickHouseBaseQueryOptions;
    instrumentations?: readonly ClickHouseOrmInstrumentation[];
    sessionContext?: SessionContext;
    joinUseNulls?: TJoinUseNulls;
    runner: {
      execute<TResult extends Record<string, unknown>>(
        compiled: CompiledQuery<TResult>,
        options?: ClickHouseBaseQueryOptions,
      ): Promise<TResult[]>;
      iterator<TResult extends Record<string, unknown>>(
        compiled: CompiledQuery<TResult>,
        options?: ClickHouseBaseQueryOptions,
      ): AsyncGenerator<TResult, void, unknown>;
      command(compiled: CompiledQuery<Record<string, unknown>>, options?: ClickHouseBaseQueryOptions): Promise<void>;
    };
  }) {
    super({
      schema: config.schema,
      runner: config.runner,
      joinUseNulls: config.joinUseNulls,
    });
    this.$client = config.client;
    this.defaultOptions = config.defaultOptions ?? {};
    this.instrumentations = config.instrumentations ?? [];
    this.sessionContext = config.sessionContext;
    this.sessionId = config.defaultOptions?.session_id ?? "";
  }

  private mergeOptions<TOptions extends ClickHouseBaseQueryOptions>(
    options?: TOptions,
  ): ClickHouseBaseQueryOptions & TOptions {
    const queryParams = {
      ...(this.defaultOptions.query_params ?? {}),
      ...(options?.query_params ?? {}),
    };
    mergeQueryParams(undefined, queryParams);

    return {
      ...this.defaultOptions,
      ...options,
      clickhouse_settings: mergeClickHouseSettings(
        this.defaultOptions.clickhouse_settings,
        options?.clickhouse_settings,
      ),
      http_headers: {
        ...(this.defaultOptions.http_headers ?? {}),
        ...(options?.http_headers ?? {}),
      },
      query_params: queryParams,
    } as ClickHouseBaseQueryOptions & TOptions;
  }

  private async runSerial<TValue>(operation: () => Promise<TValue>): Promise<TValue> {
    if (!this.sessionContext) {
      return operation();
    }

    const next = this.sessionContext.queue.then(operation, operation);
    const clearQueue = () => undefined;
    this.sessionContext.queue = next.then(clearQueue, clearQueue);
    return next;
  }

  private buildQueryEvent(input: {
    mode: ClickHouseOrmQueryMode;
    queryKind: ClickHouseOrmQueryKind;
    statement: string;
    operation?: string;
    options: ClickHouseBaseQueryOptions;
    format?: ClickHouseQueryOptions["format"] | ClickHouseStreamOptions["format"];
    tableName?: string;
  }) {
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
  }

  private async executeWithInstrumentation<TValue>(
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
  ): Promise<TValue> {
    const event = this.buildQueryEvent(input);
    await emitQueryStart(this.instrumentations, event);

    try {
      const result = await operation();
      await emitQuerySuccess(
        this.instrumentations,
        createQuerySuccessEvent(event, Date.now() - event.startedAt, result.rowCount),
      );
      return result.value;
    } catch (error) {
      await emitQueryError(this.instrumentations, createQueryErrorEvent(event, error, Date.now() - event.startedAt));
      throw error;
    }
  }

  private async *streamWithInstrumentation<TValue>(
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
    const event = this.buildQueryEvent(input);
    await emitQueryStart(this.instrumentations, event);

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
        await emitQuerySuccess(this.instrumentations, createQuerySuccessEvent(event, durationMs, rowCount));
      } else {
        await emitQueryError(this.instrumentations, createQueryErrorEvent(event, caughtError, durationMs, rowCount));
      }
    }
  }

  private async *runLockedStream<TValue>(
    operation: () => AsyncGenerator<TValue, void, unknown>,
  ): AsyncGenerator<TValue, void, unknown> {
    if (!this.sessionContext) {
      yield* operation();
      return;
    }

    const previous = this.sessionContext.queue;
    let release!: () => void;
    const next = new Promise<void>((resolve) => {
      release = resolve;
    });
    const continueWithNext = () => next;
    this.sessionContext.queue = previous.then(continueWithNext, continueWithNext);
    await previous;

    try {
      yield* operation();
    } finally {
      release();
    }
  }

  async executeCompiled<TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    options?: ClickHouseBaseQueryOptions,
  ): Promise<TResult[]> {
    const baseOptions = this.mergeOptions(options);
    const mergedOptions = {
      ...baseOptions,
      clickhouse_settings: mergeClickHouseSettings(baseOptions.clickhouse_settings, compiled.forcedSettings),
    };
    const queryConfig = buildQueryParams(compiled, mergedOptions);

    return this.runSerial(async () => {
      const operation = resolveSqlOperation(queryConfig.query);
      const mode = compiled.mode === "command" ? (operation === "INSERT" ? "insert" : "command") : "query";

      return this.executeWithInstrumentation(
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
            await this.$client.command(queryConfig.query, mergedOptions, queryConfig.query_params);
            return { value: [] as TResult[] };
          }

          const rows = await this.$client.queryJSON<Record<string, unknown>>({
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
  }

  async *iteratorCompiled<TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    options?: ClickHouseBaseQueryOptions,
  ): AsyncGenerator<TResult, void, unknown> {
    const baseOptions = this.mergeOptions(options);
    const mergedOptions = {
      ...baseOptions,
      clickhouse_settings: mergeClickHouseSettings(baseOptions.clickhouse_settings, compiled.forcedSettings),
    };
    const queryConfig = buildQueryParams(compiled, mergedOptions);
    const operation = resolveSqlOperation(queryConfig.query);

    yield* this.runLockedStream(() =>
      this.streamWithInstrumentation(
        {
          mode: "stream",
          queryKind: "typed",
          statement: queryConfig.query,
          options: mergedOptions,
          format: "JSONEachRow",
          operation,
        },
        () => this.createStreamGenerator(compiled, queryConfig.query, queryConfig.query_params, mergedOptions),
      ),
    );
  }

  private async *createStreamGenerator<TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    query: string,
    queryParams: QueryParams,
    options: ClickHouseBaseQueryOptions,
  ): AsyncGenerator<TResult, void, unknown> {
    for await (const row of this.$client.queryStream<Record<string, unknown>>({
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
  }

  async execute(query: RawQueryInput, options?: ClickHouseQueryOptions) {
    const mergedOptions = this.mergeOptions(options);
    const normalized = normalizeRawQueryInput(query);
    return this.runSerial(async () => {
      return this.executeWithInstrumentation(
        {
          mode: "query",
          queryKind: "raw",
          statement: normalized.query,
          options: mergedOptions,
          format: "JSON",
        },
        async () => {
          const rows = await this.$client.queryJSON<Record<string, unknown>>({
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
  }

  async *stream(
    query: RawQueryInput,
    options?: ClickHouseStreamOptions,
  ): AsyncGenerator<Record<string, unknown>, void, unknown> {
    const mergedOptions = this.mergeOptions(options);
    const normalized = normalizeRawQueryInput(query);

    const createGenerator = async function* (client: FetchClickHouseTransport) {
      yield* client.queryStream<Record<string, unknown>>({
        statement: normalized.query,
        query_params: mergeQueryParams(normalized.params, mergedOptions.query_params),
        options: mergedOptions,
        format: mergedOptions.format ?? "JSONEachRow",
      });
    };

    yield* this.runLockedStream(() =>
      this.streamWithInstrumentation(
        {
          mode: "stream",
          queryKind: "raw",
          statement: normalized.query,
          options: mergedOptions,
          format: mergedOptions.format ?? "JSONEachRow",
        },
        () => createGenerator(this.$client),
      ),
    );
  }

  async command(query: RawQueryInput, options?: ClickHouseBaseQueryOptions): Promise<void> {
    const mergedOptions = this.mergeOptions(options);
    const normalized = normalizeRawQueryInput(query);
    await this.runSerial(async () => {
      await this.executeWithInstrumentation(
        {
          mode: "command",
          queryKind: "raw",
          statement: normalized.query,
          options: mergedOptions,
        },
        async () => {
          await this.$client.command(
            normalized.query,
            mergedOptions,
            mergeQueryParams(normalized.params, mergedOptions.query_params),
          );
          return { value: undefined };
        },
      );
    });
  }

  async ping(options?: ClickHouseEndpointOptions): Promise<void> {
    const mergedOptions = this.mergeOptions(options);
    await this.runSerial(async () => {
      await this.executeWithInstrumentation(
        {
          mode: "command",
          queryKind: "raw",
          statement: "GET /ping",
          options: mergedOptions,
          operation: "PING",
        },
        async () => {
          await this.$client.endpoint("/ping", mergedOptions, "GET");
          return { value: undefined };
        },
      );
    });
  }

  async replicasStatus(options?: ClickHouseEndpointOptions): Promise<void> {
    const mergedOptions = this.mergeOptions(options);
    await this.runSerial(async () => {
      await this.executeWithInstrumentation(
        {
          mode: "command",
          queryKind: "raw",
          statement: "GET /replicas_status",
          options: mergedOptions,
          operation: "REPLICAS_STATUS",
        },
        async () => {
          await this.$client.endpoint("/replicas_status", mergedOptions, "GET");
          return { value: undefined };
        },
      );
    });
  }

  private createChild<TNextJoinUseNulls extends 0 | 1 = TJoinUseNulls>(
    defaultOptions: ClickHouseBaseQueryOptions,
    sessionContext?: SessionContext,
    joinUseNulls?: TNextJoinUseNulls,
  ) {
    let child: ClickHouseOrmClient<TSchema, TNextJoinUseNulls>;
    const runner = createOrmRunner(() => child);
    child = new ClickHouseOrmClient<TSchema, TNextJoinUseNulls>({
      client: this.$client,
      defaultOptions,
      instrumentations: this.instrumentations,
      schema: this.schema,
      sessionContext,
      joinUseNulls: joinUseNulls ?? (this.joinUseNulls as unknown as TNextJoinUseNulls),
      runner,
    });
    return child;
  }

  withSettings<TSettings extends Record<string, string | number | boolean>>(
    settings: TSettings,
  ): ClickHouseOrmClient<TSchema, ResolveJoinUseNulls<TSettings, TJoinUseNulls>> {
    const nextJoinUseNulls = (
      settings.join_use_nulls === 0 || settings.join_use_nulls === 1 ? settings.join_use_nulls : this.joinUseNulls
    ) as ResolveJoinUseNulls<TSettings, TJoinUseNulls>;

    return this.createChild(
      {
        ...this.defaultOptions,
        clickhouse_settings: {
          ...(this.defaultOptions.clickhouse_settings ?? {}),
          ...settings,
        },
      },
      this.sessionContext,
      nextJoinUseNulls,
    );
  }

  async insertJsonEachRow(
    table: AnyTable | string,
    rows: readonly Record<string, unknown>[] | AsyncIterable<Record<string, unknown>>,
    options?: ClickHouseBaseQueryOptions,
  ) {
    const mergedOptions = this.mergeOptions(options);
    const tableName = toClickHouseTableName(table);
    const tableIdentifier = compileSql(sql.identifier({ table: tableName })).query;
    return this.runSerial(async () => {
      await this.executeWithInstrumentation(
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
          await this.$client.insertJsonEachRow(tableName, rows, mergedOptions);
          return {
            value: undefined,
            rowCount: Array.isArray(rows) ? rows.length : undefined,
          };
        },
      );
    });
  }

  registerTempTable(name: string) {
    if (!this.sessionContext) {
      throw createSessionError("registerTempTable() requires runInSession()");
    }
    // Validate before mutating cleanup state so invalid names cannot mask the original error later.
    this.renderTempTableIdentifier(name);
    this.registerValidatedTempTable(name);
  }

  private renderTempTableIdentifier(name: string) {
    return compileSql(sql.identifier({ table: name })).query;
  }

  private registerValidatedTempTable(name: string) {
    if (!this.sessionContext) {
      throw createSessionError("registerTempTable() requires runInSession()");
    }
    if (!this.sessionContext.tempTables.includes(name)) {
      this.sessionContext.tempTables.push(name);
    }
  }

  /**
   * Creates a CLICKHOUSE TEMPORARY TABLE inside the current session.
   *
   * @param name Table name (validated via {@link sql.identifier}).
   * @param definition Raw column/engine definition appended after the table identifier
   *   (e.g. `"(id UInt64) ENGINE = Memory"`). This value is **embedded into the SQL
   *   statement verbatim** — same trust model as {@link sql.raw}. **Never pass
   *   user-controlled input here.** Only a single top-level statement is allowed;
   *   trailing top-level `;` are stripped, but multiple statements are rejected.
   *   This is not a substitute for treating the argument as developer-controlled.
   */
  async createTemporaryTable(name: string, definition: string) {
    if (!this.sessionContext) {
      throw createSessionError("createTemporaryTable() requires runInSession()");
    }
    const normalizedDefinition = normalizeSingleStatementSql(
      definition,
      "createTemporaryTable() definition must not contain multiple statements; use developer-controlled SQL only",
    );
    const identifier = this.renderTempTableIdentifier(name);
    this.registerValidatedTempTable(name);
    await this.command(`CREATE TEMPORARY TABLE ${identifier} ${normalizedDefinition}`);
  }

  async runInSession<TResult>(
    callback: (db: ClickHouseOrmClient<TSchema, TJoinUseNulls>) => Promise<TResult>,
    options?: Omit<ClickHouseBaseQueryOptions, "session_id"> & {
      session_id?: string;
      /**
       * Optional callback invoked when one or more temporary-table cleanup
       * statements (`DROP TABLE IF EXISTS`) fail at the end of a session.
       *
       * Cleanup errors never override the user callback's error — they are
       * collected after the user callback resolves/rejects and surfaced here.
       *
       * If this hook is **not** supplied:
       * - When the user callback succeeded but cleanup failed, a
       *   {@link ClickHouseOrmError} of `kind: "session"` is thrown with the
       *   underlying cleanup errors attached via `cause` (an `AggregateError`
       *   when there is more than one).
       * - When the user callback already threw, cleanup errors are silently
       *   discarded to preserve the original error (matches prior behaviour).
       */
      onCleanupError?: (errors: readonly unknown[], context: { sessionId: string }) => void;
    },
  ): Promise<TResult> {
    if (this.sessionContext) {
      if (options?.session_id && options.session_id !== this.defaultOptions.session_id) {
        throw createSessionError("Nested runInSession() cannot create a different session");
      }
      return callback(this);
    }

    if (options?.session_check === 1 && !options.session_id) {
      throw createClientValidationError(
        "runInSession() requires an explicit session_id when session_check=1 because ClickHouse only validates existing sessions",
      );
    }

    const sessionId = options?.session_id ?? createSessionId();
    const sessionContext: SessionContext = {
      sessionId,
      tempTables: [],
      queue: Promise.resolve(),
    };
    const sessionClient = this.createChild(
      {
        ...this.defaultOptions,
        ...options,
        session_id: sessionId,
      },
      sessionContext,
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
    for (const tableName of [...sessionContext.tempTables].reverse()) {
      try {
        const identifier = this.renderTempTableIdentifier(tableName);
        await sessionClient.command(`DROP TABLE IF EXISTS ${identifier}`, {
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
  }
}

export const createOrmRunner = <TSchema, TJoinUseNulls extends 0 | 1>(
  resolveClient: () => ClickHouseOrmClient<TSchema, TJoinUseNulls>,
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
