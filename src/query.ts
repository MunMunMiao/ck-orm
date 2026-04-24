import type { AnyColumn } from "./columns";
import { createClientValidationError, createInternalError } from "./errors";
import { createUuid } from "./platform";
import type { InferSelectionResult, NoJoinedSources, SelectionRecord } from "./query/types";
import {
  type BuildContext,
  compileValue,
  createExpression,
  type Decoder,
  decodeValue,
  ensureExpression,
  getExpressionSourceKey,
  joinSqlParts,
  type Order,
  type Predicate,
  type QueryParams,
  type Selection,
  type SelectionMeta,
  type SqlExpression,
  wrapSql,
} from "./query-shared";
import type { ClickHouseBaseQueryOptions } from "./runtime";
import type { ClickHouseSettings, ClickHouseSettingValue } from "./runtime/settings";
import type { AnyTable, Table } from "./schema";
import { renderTableIdentifier } from "./schema";
import { compileSql, type SQLFragment, sql } from "./sql";

type QuerySource = AnyTable | AnySubquery | AnyCte | TableFunctionSource;
type KnownQuerySource = AnyTable | AnySubquery | AnyCte;
type ForcedSettings = Readonly<ClickHouseSettings>;
type MutableForcedSettings = Record<string, ClickHouseSettingValue>;

type SqlSelection<TData = unknown, TSourceKey extends string | undefined = string | undefined> = SqlExpression<
  TData,
  TSourceKey
>;
type SqlPredicate<TSourceKey extends string | undefined = string | undefined> = SqlExpression<boolean, TSourceKey>;
type SqlOrder = {
  readonly expression: SqlSelection<unknown>;
  readonly direction: "asc" | "desc";
};

type SourceColumns = Record<string, Selection<unknown>>;

type QueryMode = "query" | "command";
type JoinUseNulls = 0 | 1;
type PredicateInput = Predicate | undefined;
type CompileState = {
  forcedSettings?: MutableForcedSettings;
};
const compileStateStackStore = new WeakMap<BuildContext, CompileState[]>();

export const compileWithContextSymbol = Symbol("clickhouseOrmCompileWithContext");
export const compileQuerySymbol = Symbol("clickhouseOrmCompileQuery");
const selectBuilderResultSymbol = Symbol("clickhouseOrmSelectBuilderResult");

type PrimitiveValue = string | number | bigint | boolean | null | Date;
type CountSource = AnyTable | AnySubquery | AnyCte;
type InsertRowInput<TTable extends AnyTable> = Partial<TTable["$inferInsert"]>;

type SourceKey<TSource extends KnownQuerySource> =
  TSource extends Table<Record<string, AnyColumn>, infer TName, infer TAlias, string>
    ? TAlias extends string
      ? TAlias
      : TName
    : TSource extends Subquery<infer _TResult, infer TAlias>
      ? TAlias
      : TSource extends Cte<infer _TResult, infer TName>
        ? TName
        : never;

type SourceResult<TSource extends KnownQuerySource> =
  TSource extends Table<Record<string, AnyColumn>, string, string | undefined, string>
    ? TSource["$inferSelect"]
    : TSource extends Subquery<infer TResult, infer _TAlias>
      ? TResult
      : TSource extends Cte<infer TResult, infer _TName>
        ? TResult
        : never;

type JoinedSourceState = {
  readonly row: Record<string, unknown>;
  readonly nullable: boolean;
};

type JoinedSources = Record<string, JoinedSourceState>;
export type AnySelectBuilder<TResult extends Record<string, unknown> = Record<string, unknown>> = SelectBuilder<
  TResult,
  SelectionRecord | undefined,
  KnownQuerySource | undefined,
  JoinedSources,
  JoinUseNulls
>;

type AddJoinedSource<
  TSources extends JoinedSources,
  TSource extends KnownQuerySource,
  TNullable extends boolean,
> = TSources & {
  [K in SourceKey<TSource>]: {
    readonly row: SourceResult<TSource>;
    readonly nullable: TNullable;
  };
};

type NullableSourceMap<
  TRootSource extends KnownQuerySource | undefined,
  TJoinedSources extends JoinedSources,
> = (TRootSource extends KnownQuerySource ? { [K in SourceKey<TRootSource>]: false } : NoJoinedSources) & {
  [K in keyof TJoinedSources]: TJoinedSources[K]["nullable"];
};

type DefaultJoinedResult<
  TRootSource extends KnownQuerySource,
  TJoinedSources extends JoinedSources,
> = keyof TJoinedSources extends never
  ? SourceResult<TRootSource>
  : {
      [K in SourceKey<TRootSource>]: SourceResult<TRootSource>;
    } & {
      [K in keyof TJoinedSources]: TJoinedSources[K]["nullable"] extends true
        ? TJoinedSources[K]["row"] | null
        : TJoinedSources[K]["row"];
    };

/**
 * Resolves the row shape of a SELECT after a join is added.
 *
 * When the user supplied an explicit `select(...)` projection
 * (`TSelection extends SelectionRecord`), each picked expression is
 * widened with the new join's nullability map.
 *
 * Otherwise, when the root source is known, fall back to a
 * `{[rootKey]: rootRow, [joinedKey]: joinedRow|null}` shape.
 *
 * Otherwise keep the existing inferred `TResult` (e.g. dynamic
 * subqueries).
 */
type InferJoinResult<
  TSelection,
  TResult,
  TRootSource extends KnownQuerySource | undefined,
  TJoinedSourcesAfter extends JoinedSources,
> = TSelection extends SelectionRecord
  ? InferSelectionResult<TSelection, NullableSourceMap<TRootSource, TJoinedSourcesAfter>>
  : TRootSource extends KnownQuerySource
    ? DefaultJoinedResult<TRootSource, TJoinedSourcesAfter>
    : TResult;

export interface TableFunctionSource {
  readonly kind: "table-function";
  readonly alias?: string;
  compileSource(ctx: BuildContext): SQLFragment;
  as<TAlias extends string>(alias: TAlias): TableFunctionSource;
}

interface SelectionItem {
  readonly key: string;
  readonly sqlAlias: string;
  readonly expression: SqlSelection<unknown>;
  readonly path: readonly [string] | readonly [string, string];
  readonly nullable?: boolean;
  readonly groupNullable?: boolean;
}

interface JoinClause {
  readonly type: "inner" | "left";
  readonly source: QuerySource;
  readonly on: SqlPredicate;
}

export interface CompiledQueryMetadata {
  readonly rootSourceName?: string;
  readonly joinCount?: number;
  readonly tags?: ReadonlyArray<string>;
}

export interface CompiledQuery<_TResult = Record<string, unknown>> {
  readonly kind: "compiled-query";
  readonly mode: QueryMode;
  readonly statement: string;
  readonly params: QueryParams;
  readonly selection: readonly SelectionMeta[];
  readonly forcedSettings?: ForcedSettings;
  readonly metadata?: CompiledQueryMetadata;
}

interface PreparedRunner {
  execute<TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    options?: ClickHouseBaseQueryOptions,
  ): Promise<TResult[]>;
  iterator<TResult extends Record<string, unknown>>(
    compiled: CompiledQuery<TResult>,
    options?: ClickHouseBaseQueryOptions,
  ): AsyncGenerator<TResult, void, unknown>;
  command(compiled: CompiledQuery<Record<string, unknown>>, options?: ClickHouseBaseQueryOptions): Promise<void>;
}

const ensureRunner = (runner: PreparedRunner | undefined, operation: string): PreparedRunner => {
  if (!runner) {
    throw createClientValidationError(
      `${operation}() requires a clickhouseClient-backed query runner. Attach one with clickhouseClient(...).select(...) or clickhouseClient(...).from(table).`,
    );
  }
  return runner;
};

type ThenHandler<TValue, TResult> = ((value: TValue) => TResult | PromiseLike<TResult>) | null | undefined;
type CatchHandler<TResult> = ((reason: unknown) => TResult | PromiseLike<TResult>) | null | undefined;

type ReferenceColumns<TRow extends SelectionRecord, TSourceKey extends string> = {
  [K in keyof TRow]: Selection<TRow[K], TSourceKey>;
};

const isSubquery = (value: unknown): value is AnySubquery => {
  return typeof value === "object" && value !== null && (value as AnySubquery).kind === "subquery";
};

const isCte = (value: unknown): value is AnyCte => {
  return typeof value === "object" && value !== null && (value as AnyCte).kind === "cte";
};

const createCompiledQuery = <TResult>(
  statement: string,
  selection: readonly SelectionMeta[],
  mode: QueryMode,
  params: QueryParams,
  forcedSettings?: ForcedSettings,
  metadata?: CompiledQueryMetadata,
): CompiledQuery<TResult> => {
  return {
    kind: "compiled-query",
    mode,
    statement,
    params,
    selection,
    forcedSettings,
    metadata,
  };
};

const mergeForcedSettings = (
  current: MutableForcedSettings | undefined,
  next: ForcedSettings | undefined,
): MutableForcedSettings | undefined => {
  if (!next) {
    return current;
  }

  const merged = current ? { ...current } : {};
  for (const [key, value] of Object.entries(next)) {
    if (key in merged && merged[key] !== value) {
      throw createClientValidationError(
        `Conflicting forced setting "${key}" detected while composing nested queries: ${String(merged[key])} !== ${String(value)}`,
      );
    }
    merged[key] = value;
  }
  return merged;
};

const pushCompileState = (ctx: BuildContext, state: CompileState): void => {
  const stack = compileStateStackStore.get(ctx) ?? [];
  stack.push(state);
  compileStateStackStore.set(ctx, stack);
};

const popCompileState = (ctx: BuildContext): void => {
  const stack = compileStateStackStore.get(ctx);
  if (!stack || stack.length === 0) throw createInternalError("Query compile-state stack underflow");

  stack.pop();
  if (stack.length === 0) {
    compileStateStackStore.delete(ctx);
  }
};

const getActiveCompileState = (ctx: BuildContext): CompileState | undefined => {
  return compileStateStackStore.get(ctx)?.at(-1);
};

const collectForcedSettings = (ctx: BuildContext, settings: ForcedSettings | undefined): void => {
  if (!settings) {
    return;
  }

  const state = getActiveCompileState(ctx);
  if (!state) {
    throw createInternalError("Missing active compile state while collecting forced settings");
  }
  state.forcedSettings = mergeForcedSettings(state.forcedSettings, settings);
};

const compileNestedQuery = <TResult extends Record<string, unknown>>(
  query: AnySelectBuilder<TResult>,
  ctx: BuildContext,
): CompiledQuery<TResult> => {
  const compiled = query[compileWithContextSymbol](ctx);
  collectForcedSettings(ctx, compiled.forcedSettings);
  return compiled;
};

const withCompileState = <TResult>(
  ctx: BuildContext,
  operation: () => TResult,
): {
  result: TResult;
  forcedSettings?: ForcedSettings;
} => {
  const state: CompileState = {};
  pushCompileState(ctx, state);

  try {
    return {
      result: operation(),
      forcedSettings: state.forcedSettings,
    };
  } finally {
    popCompileState(ctx);
  }
};

const isInsertRowRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === "object" && value !== null && !Array.isArray(value);
};

const normalizeInsertRows = <TTable extends AnyTable>(
  table: TTable,
  value: InsertRowInput<TTable> | readonly InsertRowInput<TTable>[],
): InsertRowInput<TTable>[] => {
  const rows = Array.isArray(value) ? [...value] : [value];
  if (rows.length === 0) {
    throw createClientValidationError(
      "insert().values() requires at least one row. Pass a single object or a non-empty array of objects.",
    );
  }

  const knownColumns = new Set(Object.keys(table.columns));
  for (const [index, row] of rows.entries()) {
    if (!isInsertRowRecord(row)) {
      throw createClientValidationError(`insert().values() row ${index + 1} must be an object`);
    }

    const unknownColumns = Object.keys(row).filter((columnName) => !knownColumns.has(columnName));
    if (unknownColumns.length > 0) {
      throw createClientValidationError(
        `insert().values() row ${index + 1} contains unknown columns: ${unknownColumns.join(", ")}`,
      );
    }
  }

  return rows;
};

const createReferenceExpression = <TData, TSourceKey extends string>(
  sourceAlias: TSourceKey,
  columnName: string,
  decoder: Decoder<TData>,
  sqlType?: string,
): SqlSelection<TData, TSourceKey> => {
  return createExpression({
    compile: () =>
      sql.identifier({
        table: sourceAlias,
        column: columnName,
      }),
    decoder,
    sqlType,
    sourceKey: sourceAlias,
  });
};

const buildReferenceColumns = <TRow extends SelectionRecord, TSourceKey extends string>(
  sourceAlias: TSourceKey,
  selectionItems: readonly SelectionItem[],
): ReferenceColumns<TRow, TSourceKey> => {
  const columns = {} as ReferenceColumns<TRow, TSourceKey>;

  for (const item of selectionItems) {
    columns[item.key as keyof TRow] = createReferenceExpression(
      sourceAlias,
      item.sqlAlias,
      item.expression.decoder,
      item.expression.sqlType,
    ) as ReferenceColumns<TRow, TSourceKey>[keyof TRow];
  }

  return columns;
};

const renderSource = (source: QuerySource, ctx: BuildContext): SQLFragment => {
  switch (source.kind) {
    case "table":
      return renderTableIdentifier(source);
    case "subquery":
      return sql`${sql.raw("(")}${sql.raw(compileNestedQuery(source.query, ctx).statement)}${sql.raw(") as ")}${sql.identifier(source.alias)}`;
    case "cte":
      return sql.identifier(source.name);
    case "table-function":
      return source.compileSource(ctx);
  }
};

const getSourceColumns = (source: QuerySource): SourceColumns | undefined => {
  switch (source.kind) {
    case "table":
    case "subquery":
    case "cte":
      return source.columns;
    case "table-function":
      return undefined;
  }
};

const getSourceKey = (source: QuerySource): string | undefined => {
  switch (source.kind) {
    case "table":
      return source.alias ?? source.originalName;
    case "subquery":
      return source.alias;
    case "cte":
      return source.name;
    case "table-function":
      return source.alias;
  }
};

const renderSelection = (selectionItems: readonly SelectionItem[], ctx: BuildContext) => {
  const selectionParts = selectionItems.map((item) => {
    return sql`${item.expression.compile(ctx)}${sql.raw(" as ")}${sql.identifier(item.sqlAlias)}`;
  });

  return joinSqlParts(selectionParts, ", ");
};

const normalizeSelectionRecord = (
  selection: SelectionRecord,
  nullableSources: ReadonlySet<string>,
): SelectionItem[] => {
  const selectionItems: SelectionItem[] = [];

  for (const [key, rawValue] of Object.entries(selection)) {
    const expression = ensureExpression(rawValue);
    const sourceKey = getExpressionSourceKey(expression);
    selectionItems.push({
      key,
      sqlAlias: expression.outputAlias ?? key,
      expression,
      path: [key],
      nullable: sourceKey ? nullableSources.has(sourceKey) : false,
    });
  }

  return selectionItems;
};

const normalizeLimitValue = (value: PrimitiveValue | Selection<unknown>, ctx: BuildContext) => {
  return compileValue(value, ctx, "Int64");
};

type CountMode = "unsafe" | "safe" | "mixed";
type CountModeResult<TMode extends CountMode> = TMode extends "safe"
  ? string
  : TMode extends "mixed"
    ? number | string
    : number;

const COUNT_DECIMAL_PATTERN = /^(0|[1-9]\d*)$/;

const createInvalidCountValueError = (value: unknown) =>
  createClientValidationError(
    `Failed to decode count() result: ${String(value)}. Expected a non-negative integer count value from ClickHouse.`,
    { cause: value },
  );

const isNonNegativeIntegerNumber = (value: number): boolean => {
  return Number.isFinite(value) && Number.isInteger(value) && value >= 0;
};

const countUnsafeDecoder: Decoder<number> = (value) => {
  if (typeof value === "number" && isNonNegativeIntegerNumber(value)) {
    return value;
  }

  if (typeof value === "bigint" && value >= 0n) {
    const nextValue = Number(value);
    if (isNonNegativeIntegerNumber(nextValue)) {
      return nextValue;
    }
  }

  if (typeof value === "string" && value.length > 0 && value.trim() === value) {
    const nextValue = Number(value);
    if (isNonNegativeIntegerNumber(nextValue)) {
      return nextValue;
    }
  }

  throw createInvalidCountValueError(value);
};

const countSafeDecoder: Decoder<string> = (value) => {
  if (typeof value === "string" && COUNT_DECIMAL_PATTERN.test(value)) {
    return value;
  }

  if (typeof value === "bigint" && value >= 0n) {
    return value.toString();
  }

  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) {
    return String(value);
  }

  throw createInvalidCountValueError(value);
};

const countMixedDecoder: Decoder<number | string> = (value) => {
  if (typeof value === "string" && COUNT_DECIMAL_PATTERN.test(value)) {
    return value;
  }

  if (typeof value === "number" && isNonNegativeIntegerNumber(value)) {
    return value;
  }

  if (typeof value === "bigint" && value >= 0n) {
    return value.toString();
  }

  throw createInvalidCountValueError(value);
};

const getCountSqlType = (mode: CountMode): string => {
  switch (mode) {
    case "safe":
      return "String";
    case "mixed":
      return "UInt64";
    case "unsafe":
      return "Float64";
  }
};

const getCountDecoder = <TMode extends CountMode>(mode: TMode): Decoder<CountModeResult<TMode>> => {
  switch (mode) {
    case "safe":
      return countSafeDecoder as Decoder<CountModeResult<TMode>>;
    case "mixed":
      return countMixedDecoder as Decoder<CountModeResult<TMode>>;
    case "unsafe":
      return countUnsafeDecoder as Decoder<CountModeResult<TMode>>;
  }
};

const renderCountExpression = (mode: CountMode): SQLFragment => {
  switch (mode) {
    case "safe":
      return sql.raw("toString(count())");
    case "mixed":
      return sql.raw("toUInt64(count())");
    case "unsafe":
      return sql.raw("toFloat64(count())");
  }
};

const buildLogicalPredicate = (operator: "and" | "or", predicates: readonly SqlPredicate[]): SqlPredicate => {
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${sql.raw("(")}${joinSqlParts(
        predicates.map((predicate) => predicate.compile(ctx)),
        ` ${operator} `,
      )}${sql.raw(")")}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

const normalizePredicateGroup = (
  operator: "and" | "or",
  predicates: readonly PredicateInput[],
): SqlPredicate | undefined => {
  const filteredPredicates = predicates.filter((predicate): predicate is Predicate => predicate !== undefined) as
    | SqlPredicate[]
    | [];

  if (filteredPredicates.length === 0) {
    return undefined;
  }

  if (filteredPredicates.length === 1) {
    return filteredPredicates[0];
  }

  return buildLogicalPredicate(operator, filteredPredicates);
};

type CountQuery<TData = number> = Selection<TData> &
  PromiseLike<TData> & {
    execute(options?: ClickHouseBaseQueryOptions): Promise<TData>;
    toSafe(): CountQuery<string>;
    toUnsafe(): CountQuery<number>;
    toMixed(): CountQuery<number | string>;
    catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<TData | TResult2>;
    finally(onfinally?: (() => void) | null): Promise<TData>;
  };

const renderCtes = (ctes: readonly AnyCte[], ctx: BuildContext): SQLFragment | undefined => {
  if (ctes.length === 0) {
    return undefined;
  }

  const cteParts = ctes.map((cte) => {
    return sql`${sql.identifier(cte.name)}${sql.raw(" as (")}${sql.raw(compileNestedQuery(cte.query, ctx).statement)}${sql.raw(")")}`;
  });

  return sql`${sql.raw("with ")}${joinSqlParts(cteParts, ", ")}`;
};

const buildCountStatement = (
  ctx: BuildContext,
  config: {
    ctes?: readonly AnyCte[];
    source: CountSource;
    condition?: SqlPredicate;
    mode: CountMode;
    outputAlias?: string;
  },
): SQLFragment => {
  const queryParts: SQLFragment[] = [];
  const cteFragment = renderCtes(config.ctes ?? [], ctx);

  if (cteFragment) {
    queryParts.push(cteFragment);
  }

  queryParts.push(
    config.outputAlias
      ? sql`${sql.raw("select ")}${renderCountExpression(config.mode)}${sql.raw(" as ")}${sql.identifier(config.outputAlias)}`
      : sql`${sql.raw("select ")}${renderCountExpression(config.mode)}`,
  );
  queryParts.push(sql`${sql.raw("from ")}${renderSource(config.source, ctx)}`);

  if (config.condition) {
    queryParts.push(sql`${sql.raw("where ")}${config.condition.compile(ctx)}`);
  }

  return sql`${joinSqlParts(queryParts, " ")}`;
};

const createCountQuery = <TMode extends CountMode = "unsafe">(config: {
  ctes: readonly AnyCte[];
  mode?: TMode;
  runner?: PreparedRunner;
  source: CountSource;
  predicates?: PredicateInput[];
}): CountQuery<CountModeResult<TMode>> => {
  type TResult = CountModeResult<TMode>;
  const mode = (config.mode ?? "unsafe") as TMode;
  const decoder = getCountDecoder(mode);
  const condition = normalizePredicateGroup("and", config.predicates ?? []);

  const expression = createExpression<TResult>({
    compile: (ctx) =>
      sql`${sql.raw("(")}${buildCountStatement(ctx, {
        ctes: config.ctes,
        source: config.source,
        condition,
        mode,
      })}${sql.raw(")")}`,
    decoder,
    sqlType: getCountSqlType(mode),
  });

  const createWithMode = <TNextMode extends CountMode>(nextMode: TNextMode): CountQuery<CountModeResult<TNextMode>> =>
    createCountQuery({
      ctes: config.ctes,
      mode: nextMode,
      runner: config.runner,
      source: config.source,
      predicates: config.predicates,
    });

  const execute = (options?: ClickHouseBaseQueryOptions): Promise<TResult> => {
    const runner = ensureRunner(config.runner, "count");
    const ctx: BuildContext = {
      params: {},
      nextParamIndex: 0,
    };
    const { result: compiledResult, forcedSettings } = withCompileState(ctx, () => {
      const statement = buildCountStatement(ctx, {
        ctes: config.ctes,
        source: config.source,
        condition,
        mode,
        outputAlias: "__orm_count",
      });
      const compiled = compileSql(statement, ctx);

      return {
        query: compiled.query,
        params: { ...compiled.params },
      };
    });

    return runner
      .execute(
        createCompiledQuery<{ value: TResult }>(
          compiledResult.query,
          [
            {
              key: "value",
              sqlAlias: "__orm_count",
              decoder,
              path: ["value"],
            },
          ],
          "query",
          compiledResult.params,
          forcedSettings,
        ),
        options,
      )
      .then((rows) => {
        const [row] = rows;

        if (!row) {
          throw createClientValidationError("count() query did not return a result row");
        }

        return row.value;
      });
  };

  return Object.assign(expression, {
    execute,
    toSafe() {
      return createWithMode("safe");
    },
    toUnsafe() {
      return createWithMode("unsafe");
    },
    toMixed() {
      return createWithMode("mixed");
    },
    /**
     * Builders are intentionally re-entrant: each `await db.count(...)` triggers a fresh
     * `execute()` and a new ClickHouse request. To memoize the result, capture the promise
     * once: `const pending = builder.execute()`. This matches Drizzle/Kysely semantics.
     */
    // biome-ignore lint/suspicious/noThenProperty: count queries are intentionally thenable so await db.count(...) matches Drizzle-style usage.
    then<TResult1 = TResult, TResult2 = never>(
      onfulfilled?: ThenHandler<TResult, TResult1>,
      onrejected?: CatchHandler<TResult2>,
    ): PromiseLike<TResult1 | TResult2> {
      return execute().then(onfulfilled, onrejected);
    },
    catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<TResult | TResult2> {
      return execute().catch(onrejected);
    },
    finally(onfinally?: (() => void) | null): Promise<TResult> {
      return execute().finally(onfinally ?? undefined);
    },
  }) as CountQuery<TResult>;
};

interface SelectBuilderConfig<_TResult extends Record<string, unknown>> {
  ctes?: AnyCte[];
  runner?: PreparedRunner;
  selection?: SelectionRecord;
  fromSource?: QuerySource;
  joins?: JoinClause[];
  whereClause?: SqlPredicate;
  groupByItems?: SqlSelection[];
  havingClause?: SqlPredicate;
  orderByItems?: SqlOrder[];
  limitValue?: PrimitiveValue | Selection<unknown>;
  offsetValue?: PrimitiveValue | Selection<unknown>;
  limitByValue?: {
    readonly columns: SqlSelection[];
    readonly limit: PrimitiveValue | Selection<unknown>;
  };
  useFinal?: boolean;
  joinUseNulls?: JoinUseNulls;
}

type SelectBuilderState<
  _TResult extends Record<string, unknown> = Record<string, unknown>,
  TSelection extends SelectionRecord | undefined = SelectionRecord | undefined,
  _TRootSource extends KnownQuerySource | undefined = KnownQuerySource | undefined,
  _TJoinedSources extends JoinedSources = NoJoinedSources,
  TJoinUseNulls extends JoinUseNulls = 1,
> = {
  readonly ctes: AnyCte[];
  readonly runner?: PreparedRunner;
  readonly selection?: TSelection;
  readonly fromSource?: QuerySource;
  readonly joins: JoinClause[];
  readonly whereClause?: SqlPredicate;
  readonly groupByItems: SqlSelection[];
  readonly havingClause?: SqlPredicate;
  readonly orderByItems: SqlOrder[];
  readonly limitValue?: PrimitiveValue | Selection<unknown>;
  readonly offsetValue?: PrimitiveValue | Selection<unknown>;
  readonly limitByValue?: {
    readonly columns: SqlSelection[];
    readonly limit: PrimitiveValue | Selection<unknown>;
  };
  readonly useFinal: boolean;
  readonly joinUseNulls: TJoinUseNulls;
};

const normalizeSelectBuilderState = <
  TResult extends Record<string, unknown>,
  TSelection extends SelectionRecord | undefined,
  TJoinUseNulls extends JoinUseNulls,
>(
  config?: SelectBuilderConfig<TResult> & { selection?: TSelection },
): SelectBuilderState<TResult, TSelection, KnownQuerySource | undefined, JoinedSources, TJoinUseNulls> => {
  return {
    ctes: config?.ctes ?? [],
    runner: config?.runner,
    selection: config?.selection,
    fromSource: config?.fromSource,
    joins: config?.joins ?? [],
    whereClause: config?.whereClause,
    groupByItems: config?.groupByItems ?? [],
    havingClause: config?.havingClause,
    orderByItems: config?.orderByItems ?? [],
    limitValue: config?.limitValue,
    offsetValue: config?.offsetValue,
    limitByValue: config?.limitByValue,
    useFinal: config?.useFinal ?? false,
    joinUseNulls: (config?.joinUseNulls ?? 1) as TJoinUseNulls,
  };
};

export interface SelectBuilder<
  TResult extends Record<string, unknown> = Record<string, unknown>,
  TSelection extends SelectionRecord | undefined = SelectionRecord | undefined,
  TRootSource extends KnownQuerySource | undefined = KnownQuerySource | undefined,
  TJoinedSources extends JoinedSources = NoJoinedSources,
  TJoinUseNulls extends JoinUseNulls = 1,
> extends PromiseLike<TResult[]> {
  readonly [selectBuilderResultSymbol]?: TResult;
  execute(options?: ClickHouseBaseQueryOptions): Promise<TResult[]>;
  iterator(options?: ClickHouseBaseQueryOptions): AsyncGenerator<TResult, void, unknown>;
  catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<TResult[] | TResult2>;
  finally(onfinally?: (() => void) | null): Promise<TResult[]>;
  buildSelectionItems(): SelectionItem[];
  from<TSource extends QuerySource>(
    source: TSource,
  ): SelectBuilder<
    TSelection extends SelectionRecord
      ? InferSelectionResult<
          TSelection,
          NullableSourceMap<TSource extends KnownQuerySource ? TSource : undefined, NoJoinedSources>
        >
      : TSource extends KnownQuerySource
        ? DefaultJoinedResult<TSource, NoJoinedSources>
        : Record<string, unknown>,
    TSelection,
    TSource extends KnownQuerySource ? TSource : undefined,
    NoJoinedSources,
    TJoinUseNulls
  >;
  innerJoin<TSource extends KnownQuerySource>(
    source: TSource,
    on: Predicate,
  ): SelectBuilder<
    InferJoinResult<TSelection, TResult, TRootSource, AddJoinedSource<TJoinedSources, TSource, false>>,
    TSelection,
    TRootSource,
    AddJoinedSource<TJoinedSources, TSource, false>,
    TJoinUseNulls
  >;
  leftJoin<TSource extends KnownQuerySource>(
    source: TSource,
    on: Predicate,
  ): SelectBuilder<
    InferJoinResult<
      TSelection,
      TResult,
      TRootSource,
      AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
    >,
    TSelection,
    TRootSource,
    AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>,
    TJoinUseNulls
  >;
  where(
    ...predicates: PredicateInput[]
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  groupBy(
    ...expressions: Selection<unknown>[]
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  having(condition?: Predicate): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  orderBy(
    ...expressions: Array<Order | Selection<unknown>>
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  limit(
    value: PrimitiveValue | Selection<unknown>,
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  offset(
    value: PrimitiveValue | Selection<unknown>,
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  final(): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  limitBy(
    columns: Selection<unknown>[],
    limit: PrimitiveValue | Selection<unknown>,
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  [compileWithContextSymbol](ctx: BuildContext): CompiledQuery<TResult>;
  [compileQuerySymbol](): CompiledQuery<TResult>;
  as<TAlias extends string>(alias: TAlias): Subquery<TResult, TAlias>;
}

export const createSelectBuilder = <
  TResult extends Record<string, unknown> = Record<string, unknown>,
  TSelection extends SelectionRecord | undefined = SelectionRecord | undefined,
  TRootSource extends KnownQuerySource | undefined = KnownQuerySource | undefined,
  TJoinedSources extends JoinedSources = NoJoinedSources,
  TJoinUseNulls extends JoinUseNulls = 1,
>(
  config?: SelectBuilderConfig<TResult> & { selection?: TSelection },
): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> => {
  const state = normalizeSelectBuilderState<TResult, TSelection, TJoinUseNulls>(config) as SelectBuilderState<
    TResult,
    TSelection,
    TRootSource,
    TJoinedSources,
    TJoinUseNulls
  >;

  const clone = <
    TNextResult extends Record<string, unknown> = TResult,
    TNextRoot extends KnownQuerySource | undefined = TRootSource,
    TNextJoined extends JoinedSources = TJoinedSources,
  >(
    overrides: Partial<SelectBuilderConfig<TNextResult>> & {
      selection?: TSelection;
    },
  ): SelectBuilder<TNextResult, TSelection, TNextRoot, TNextJoined, TJoinUseNulls> => {
    return createSelectBuilder<TNextResult, TSelection, TNextRoot, TNextJoined, TJoinUseNulls>({
      ...(state as SelectBuilderConfig<TNextResult> & { selection?: TSelection }),
      ...overrides,
    });
  };

  const isNullableJoinEnabled = (): boolean => {
    return state.joinUseNulls === 1;
  };

  const getNullableSources = (): Set<string> => {
    const result = new Set<string>();
    if (!isNullableJoinEnabled()) {
      return result;
    }
    for (const join of state.joins) {
      if (join.type !== "left") {
        continue;
      }
      const sourceKey = getSourceKey(join.source);
      if (sourceKey) {
        result.add(sourceKey);
      }
    }
    return result;
  };

  const buildSelectionItems = (): SelectionItem[] => {
    if (state.selection) {
      return normalizeSelectionRecord(state.selection, getNullableSources());
    }

    if (!state.fromSource) {
      throw createClientValidationError(
        "select() without explicit selection requires from() first. Call .from(table) before .select(), or pass an explicit selection object to select({...}).",
      );
    }

    const rootSourceKey = getSourceKey(state.fromSource);
    const rootSourceColumns = getSourceColumns(state.fromSource);
    if (!rootSourceColumns || !rootSourceKey) {
      throw createClientValidationError(
        "select() without explicit selection requires a source with known columns. Use a defined table()/subquery()/cte() source, or pass an explicit selection object to select({...}).",
      );
    }

    const selectionItems: SelectionItem[] = [];
    const hasJoins = state.joins.some((join) => getSourceColumns(join.source));
    const nullableJoinEnabled = isNullableJoinEnabled();
    let nextIndex = 0;

    const appendSourceColumns = (sourceKey: string, sourceColumns: SourceColumns, groupNullable: boolean) => {
      for (const [fieldKey, expression] of Object.entries(sourceColumns)) {
        nextIndex += 1;
        selectionItems.push({
          key: fieldKey,
          sqlAlias: hasJoins ? `__orm_${nextIndex}` : fieldKey,
          expression: expression as SqlSelection<unknown>,
          path: hasJoins ? [sourceKey, fieldKey] : [fieldKey],
          nullable: groupNullable,
          groupNullable: hasJoins ? groupNullable : false,
        });
      }
    };

    appendSourceColumns(rootSourceKey, rootSourceColumns, false);

    if (!hasJoins) {
      return selectionItems;
    }

    for (const join of state.joins) {
      const joinSourceKey = getSourceKey(join.source);
      const joinSourceColumns = getSourceColumns(join.source);
      if (!joinSourceKey || !joinSourceColumns) {
        continue;
      }
      appendSourceColumns(joinSourceKey, joinSourceColumns, join.type === "left" && nullableJoinEnabled);
    }

    return selectionItems;
  };

  const builder = {
    execute(options?: ClickHouseBaseQueryOptions): Promise<TResult[]> {
      const runner = ensureRunner(state.runner, "execute");
      return runner.execute(builder[compileQuerySymbol](), options);
    },

    iterator(options?: ClickHouseBaseQueryOptions): AsyncGenerator<TResult, void, unknown> {
      const runner = ensureRunner(state.runner, "iterator");
      return runner.iterator(builder[compileQuerySymbol](), options);
    },

    // biome-ignore lint/suspicious/noThenProperty: builders are intentionally thenable so await builder matches Drizzle-style usage.
    then<TResult1 = TResult[], TResult2 = never>(
      onfulfilled?: ThenHandler<TResult[], TResult1>,
      onrejected?: CatchHandler<TResult2>,
    ): PromiseLike<TResult1 | TResult2> {
      return builder.execute().then(onfulfilled, onrejected);
    },

    catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<TResult[] | TResult2> {
      return builder.execute().catch(onrejected);
    },

    finally(onfinally?: (() => void) | null): Promise<TResult[]> {
      return builder.execute().finally(onfinally ?? undefined);
    },

    buildSelectionItems(): SelectionItem[] {
      return buildSelectionItems();
    },

    from<TSource extends QuerySource>(
      source: TSource,
    ): SelectBuilder<
      TSelection extends SelectionRecord
        ? InferSelectionResult<
            TSelection,
            NullableSourceMap<TSource extends KnownQuerySource ? TSource : undefined, NoJoinedSources>
          >
        : TSource extends KnownQuerySource
          ? DefaultJoinedResult<TSource, NoJoinedSources>
          : Record<string, unknown>,
      TSelection,
      TSource extends KnownQuerySource ? TSource : undefined,
      NoJoinedSources,
      TJoinUseNulls
    > {
      return clone<
        TSelection extends SelectionRecord
          ? InferSelectionResult<
              TSelection,
              NullableSourceMap<TSource extends KnownQuerySource ? TSource : undefined, NoJoinedSources>
            >
          : TSource extends KnownQuerySource
            ? DefaultJoinedResult<TSource, NoJoinedSources>
            : Record<string, unknown>,
        TSource extends KnownQuerySource ? TSource : undefined,
        NoJoinedSources
      >({
        fromSource: source,
      });
    },

    innerJoin<TSource extends KnownQuerySource>(
      source: TSource,
      on: Predicate,
    ): SelectBuilder<
      InferJoinResult<TSelection, TResult, TRootSource, AddJoinedSource<TJoinedSources, TSource, false>>,
      TSelection,
      TRootSource,
      AddJoinedSource<TJoinedSources, TSource, false>,
      TJoinUseNulls
    > {
      return clone<
        InferJoinResult<TSelection, TResult, TRootSource, AddJoinedSource<TJoinedSources, TSource, false>>,
        TRootSource,
        AddJoinedSource<TJoinedSources, TSource, false>
      >({
        joins: [...state.joins, { type: "inner", source, on: on as SqlPredicate }],
      });
    },

    leftJoin<TSource extends KnownQuerySource>(
      source: TSource,
      on: Predicate,
    ): SelectBuilder<
      InferJoinResult<
        TSelection,
        TResult,
        TRootSource,
        AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
      >,
      TSelection,
      TRootSource,
      AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>,
      TJoinUseNulls
    > {
      return clone<
        InferJoinResult<
          TSelection,
          TResult,
          TRootSource,
          AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
        >,
        TRootSource,
        AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
      >({
        joins: [...state.joins, { type: "left", source, on: on as SqlPredicate }],
      });
    },

    where(
      ...predicates: PredicateInput[]
    ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      return clone({
        whereClause: normalizePredicateGroup("and", predicates),
      });
    },

    groupBy(
      ...expressions: Selection<unknown>[]
    ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      return clone({
        groupByItems: [...state.groupByItems, ...expressions.map((expression) => ensureExpression(expression))],
      });
    },

    having(condition?: Predicate): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      return clone({
        havingClause: condition as SqlPredicate | undefined,
      });
    },

    orderBy(
      ...expressions: Array<Order | Selection<unknown>>
    ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      const nextOrderItems = expressions.map((expression): SqlOrder => {
        if ("direction" in expression && "expression" in expression) {
          return {
            direction: expression.direction,
            expression: ensureExpression(expression.expression),
          };
        }
        return {
          direction: "asc",
          expression: ensureExpression(expression),
        };
      });
      return clone({
        orderByItems: [...state.orderByItems, ...nextOrderItems],
      });
    },

    limit(
      value: PrimitiveValue | Selection<unknown>,
    ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      return clone({
        limitValue: value,
      });
    },

    offset(
      value: PrimitiveValue | Selection<unknown>,
    ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      return clone({
        offsetValue: value,
      });
    },

    final(): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      return clone({
        useFinal: true,
      });
    },

    limitBy(
      columns: Selection<unknown>[],
      limit: PrimitiveValue | Selection<unknown>,
    ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      return clone({
        limitByValue: {
          columns: columns.map((column) => ensureExpression(column)),
          limit,
        },
      });
    },

    [compileWithContextSymbol](ctx: BuildContext): CompiledQuery<TResult> {
      const { result, forcedSettings: nestedForcedSettings } = withCompileState(ctx, () => {
        const selectionItems = buildSelectionItems();
        const queryParts: SQLFragment[] = [];
        const cteFragment = renderCtes(state.ctes, ctx);
        if (cteFragment) {
          queryParts.push(cteFragment);
        }

        queryParts.push(sql`${sql.raw("select ")}${renderSelection(selectionItems, ctx)}`);

        if (state.fromSource) {
          const fromSource = renderSource(state.fromSource, ctx);
          const finalSuffix = state.useFinal ? sql.raw(" final") : sql.raw("");
          queryParts.push(sql`${sql.raw("from ")}${fromSource}${finalSuffix}`);
        }

        if (state.joins.length > 0) {
          for (const join of state.joins) {
            const joinKeyword = join.type === "inner" ? "inner join" : "left join";
            queryParts.push(
              sql`${sql.raw(`${joinKeyword} `)}${renderSource(join.source, ctx)}${sql.raw(" on ")}${join.on.compile(ctx)}`,
            );
          }
        }

        if (state.whereClause) {
          queryParts.push(sql`${sql.raw("where ")}${state.whereClause.compile(ctx)}`);
        }

        if (state.groupByItems.length > 0) {
          queryParts.push(
            sql`${sql.raw("group by ")}${joinSqlParts(
              state.groupByItems.map((item) => item.compile(ctx)),
              ", ",
            )}`,
          );
        }

        if (state.havingClause) {
          queryParts.push(sql`${sql.raw("having ")}${state.havingClause.compile(ctx)}`);
        }

        if (state.orderByItems.length > 0) {
          const orderByParts = state.orderByItems.map((item) => {
            return sql`${item.expression.compile(ctx)}${sql.raw(` ${item.direction.toUpperCase()}`)}`;
          });
          queryParts.push(sql`${sql.raw("order by ")}${joinSqlParts(orderByParts, ", ")}`);
        }

        if (state.limitByValue) {
          queryParts.push(
            sql`${sql.raw("limit ")}${normalizeLimitValue(state.limitByValue.limit, ctx)}${sql.raw(" by ")}${joinSqlParts(
              state.limitByValue.columns.map((column) => column.compile(ctx)),
              ", ",
            )}`,
          );
        }

        if (state.limitValue !== undefined) {
          queryParts.push(sql`${sql.raw("limit ")}${normalizeLimitValue(state.limitValue, ctx)}`);
        }

        if (state.offsetValue !== undefined) {
          queryParts.push(sql`${sql.raw("offset ")}${normalizeLimitValue(state.offsetValue, ctx)}`);
        }

        const statement = sql`${joinSqlParts(queryParts, " ")}`;
        const compiled = compileSql(statement, ctx);
        const selection = selectionItems.map((item) => ({
          key: item.key,
          sqlAlias: item.sqlAlias,
          decoder: item.expression.decoder,
          path: item.path,
          nullable: item.nullable,
          groupNullable: item.groupNullable,
        }));

        const localForcedSettings =
          isNullableJoinEnabled() && state.joins.some((join) => join.type === "left")
            ? { join_use_nulls: 1 }
            : undefined;

        const metadata: CompiledQueryMetadata | undefined = state.fromSource
          ? {
              rootSourceName: getSourceKey(state.fromSource),
              joinCount: state.joins.length,
            }
          : state.joins.length > 0
            ? { joinCount: state.joins.length }
            : undefined;

        return {
          compiled,
          selection,
          localForcedSettings,
          metadata,
        };
      });

      const forcedSettings = mergeForcedSettings(
        mergeForcedSettings(undefined, nestedForcedSettings),
        result.localForcedSettings,
      );

      return createCompiledQuery<TResult>(
        result.compiled.query,
        result.selection,
        "query",
        { ...result.compiled.params },
        forcedSettings,
        result.metadata,
      );
    },

    [compileQuerySymbol](): CompiledQuery<TResult> {
      return builder[compileWithContextSymbol]({
        params: {},
        nextParamIndex: 0,
      });
    },

    as<TAlias extends string>(alias: TAlias): Subquery<TResult, TAlias> {
      const selectionItems = buildSelectionItems();
      const columns = buildReferenceColumns<TResult, TAlias>(alias, selectionItems);
      const subquery = {
        kind: "subquery" as const,
        alias,
        query: builder as AnySelectBuilder<TResult>,
        columns,
      };

      return Object.assign(subquery, columns) as Subquery<TResult, TAlias>;
    },
  } as SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;

  return builder;
};

export interface InsertBuilder<TTable extends AnyTable> extends PromiseLike<undefined> {
  values(values: InsertRowInput<TTable> | readonly InsertRowInput<TTable>[]): InsertBuilder<TTable>;
  execute(options?: ClickHouseBaseQueryOptions): Promise<undefined>;
  catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<undefined | TResult2>;
  finally(onfinally?: (() => void) | null): Promise<undefined>;
  [compileQuerySymbol](): CompiledQuery<never>;
}

export const createInsertBuilder = <TTable extends AnyTable>(
  table: TTable,
  runner?: PreparedRunner,
  rows: InsertRowInput<TTable>[] = [],
): InsertBuilder<TTable> => {
  const builder = {
    values(values: InsertRowInput<TTable> | readonly InsertRowInput<TTable>[]): InsertBuilder<TTable> {
      return createInsertBuilder(table, runner, normalizeInsertRows(table, values));
    },

    execute(options?: ClickHouseBaseQueryOptions): Promise<undefined> {
      const preparedRunner = ensureRunner(runner, "execute");
      return preparedRunner
        .command(builder[compileQuerySymbol]() as unknown as CompiledQuery<Record<string, unknown>>, options)
        .then(() => undefined);
    },

    // biome-ignore lint/suspicious/noThenProperty: insert builders are intentionally thenable so await builder matches Drizzle-style usage.
    then<TResult1 = undefined, TResult2 = never>(
      onfulfilled?: ThenHandler<undefined, TResult1>,
      onrejected?: CatchHandler<TResult2>,
    ): PromiseLike<TResult1 | TResult2> {
      return builder.execute().then(onfulfilled, onrejected);
    },

    catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<undefined | TResult2> {
      return builder.execute().catch(onrejected);
    },

    finally(onfinally?: (() => void) | null): Promise<undefined> {
      return builder.execute().finally(onfinally ?? undefined);
    },

    [compileQuerySymbol](): CompiledQuery<never> {
      const columnEntries = Object.entries(table.columns).map(([schemaKey, column]) => ({
        key: column.key ?? schemaKey,
        name: column.name ?? schemaKey,
        column,
      }));
      const columnTypes = columnEntries.map(({ column }) => column.sqlType);
      const ctx: BuildContext = {
        params: {},
        nextParamIndex: 0,
      };
      const valueRows = rows.map(
        (row) =>
          sql`(${joinSqlParts(
            columnEntries.map(({ key, column }, index) => {
              const value = (row as Record<string, unknown>)[key];
              if (value === undefined) {
                return sql.raw("DEFAULT");
              }
              return compileValue(column.mapToDriverValue(value as never), ctx, columnTypes[index]);
            }),
            ", ",
          )})`,
      );
      const statement = sql`insert into ${renderTableIdentifier(table)} (${joinSqlParts(
        columnEntries.map(({ name }) => sql.identifier(name)),
        ", ",
      )}) values ${joinSqlParts(valueRows, ", ")}`;
      const compiled = compileSql(statement, ctx);

      return createCompiledQuery(compiled.query, [], "command", {
        ...compiled.params,
      });
    },
  } as InsertBuilder<TTable>;

  return builder;
};

export type Subquery<
  TResult extends Record<string, unknown> = Record<string, unknown>,
  TAlias extends string = string,
> = {
  readonly kind: "subquery";
  readonly alias: TAlias;
  readonly query: AnySelectBuilder<TResult>;
  readonly columns: ReferenceColumns<TResult, TAlias>;
} & ReferenceColumns<TResult, TAlias>;

export type AnySubquery = {
  readonly kind: "subquery";
  readonly alias: string;
  readonly query: AnySelectBuilder<Record<string, unknown>>;
  readonly columns: SourceColumns;
};

export type Cte<TResult extends Record<string, unknown> = Record<string, unknown>, TName extends string = string> = {
  readonly kind: "cte";
  readonly name: TName;
  readonly query: AnySelectBuilder<TResult>;
  readonly columns: ReferenceColumns<TResult, TName>;
} & ReferenceColumns<TResult, TName>;

type SelectBuilderRow<TQuery> = TQuery extends {
  execute(options?: ClickHouseBaseQueryOptions): Promise<Array<infer TResult>>;
}
  ? TResult extends Record<string, unknown>
    ? TResult
    : never
  : never;

type CteFromQuery<TQuery, TName extends string> = {
  readonly kind: "cte";
  readonly name: TName;
  readonly query: AnySelectBuilder<SelectBuilderRow<TQuery>>;
  readonly columns: ReferenceColumns<SelectBuilderRow<TQuery>, TName>;
} & ReferenceColumns<SelectBuilderRow<TQuery>, TName>;

export type AnyCte = {
  readonly kind: "cte";
  readonly name: string;
  readonly query: AnySelectBuilder<Record<string, unknown>>;
  readonly columns: SourceColumns;
};

export interface QueryClient<TSchema = unknown, TJoinUseNulls extends JoinUseNulls = 1> {
  readonly schema: TSchema;
  readonly ctes: AnyCte[];
  select<TSelection extends SelectionRecord | undefined = undefined>(
    selection?: TSelection,
  ): SelectBuilder<
    TSelection extends SelectionRecord ? InferSelectionResult<TSelection> : Record<string, unknown>,
    TSelection,
    undefined,
    NoJoinedSources,
    TJoinUseNulls
  >;
  count(source: CountSource, ...predicates: PredicateInput[]): CountQuery<number>;
  insert<TTable extends AnyTable>(table: TTable): InsertBuilder<TTable>;
  $with<TName extends string>(
    name: TName,
  ): {
    as: <TQuery>(
      query: TQuery & (SelectBuilderRow<TQuery> extends never ? never : unknown),
    ) => CteFromQuery<TQuery, TName>;
  };
  with(...ctes: AnyCte[]): QueryClient<TSchema, TJoinUseNulls>;
}

export const createQueryClient = <TSchema, TJoinUseNulls extends JoinUseNulls = 1>(config: {
  schema: TSchema;
  ctes?: AnyCte[];
  runner?: PreparedRunner;
  joinUseNulls?: TJoinUseNulls;
}): QueryClient<TSchema, TJoinUseNulls> => {
  const state = {
    schema: config.schema,
    ctes: config.ctes ?? [],
    runner: config.runner,
    joinUseNulls: (config.joinUseNulls ?? 1) as TJoinUseNulls,
  };

  const client = {
    schema: state.schema,
    ctes: state.ctes,
    select<TSelection extends SelectionRecord | undefined = undefined>(
      selection?: TSelection,
    ): SelectBuilder<
      TSelection extends SelectionRecord ? InferSelectionResult<TSelection> : Record<string, unknown>,
      TSelection,
      undefined,
      NoJoinedSources,
      TJoinUseNulls
    > {
      return createSelectBuilder<
        TSelection extends SelectionRecord ? InferSelectionResult<TSelection> : Record<string, unknown>,
        TSelection,
        undefined,
        NoJoinedSources,
        TJoinUseNulls
      >({
        ctes: state.ctes,
        runner: state.runner,
        selection,
        joinUseNulls: state.joinUseNulls,
      });
    },

    count(source: CountSource, ...predicates: PredicateInput[]): CountQuery<number> {
      return createCountQuery({
        ctes: state.ctes,
        runner: state.runner,
        source,
        predicates,
      });
    },

    insert<TTable extends AnyTable>(table: TTable): InsertBuilder<TTable> {
      return createInsertBuilder(table, state.runner);
    },

    $with<TName extends string>(name: TName) {
      return {
        as: <TQuery>(
          query: TQuery & (SelectBuilderRow<TQuery> extends never ? never : unknown),
        ): CteFromQuery<TQuery, TName> => {
          const selectQuery = query as unknown as AnySelectBuilder<SelectBuilderRow<TQuery>>;
          const selectionItems = selectQuery.buildSelectionItems();
          const columns = buildReferenceColumns<SelectBuilderRow<TQuery>, TName>(name, selectionItems);
          const cte = {
            kind: "cte" as const,
            name,
            query: selectQuery,
            columns,
          };
          return Object.assign(cte, columns) as CteFromQuery<TQuery, TName>;
        },
      };
    },

    with(...ctes: AnyCte[]): QueryClient<TSchema, TJoinUseNulls> {
      return createQueryClient<TSchema, TJoinUseNulls>({
        schema: state.schema,
        ctes: [...state.ctes, ...ctes],
        runner: state.runner,
        joinUseNulls: state.joinUseNulls,
      });
    },
  } as QueryClient<TSchema, TJoinUseNulls>;

  return client;
};

const ensureComparableExpression = (value: unknown): SqlSelection<unknown> => {
  return ensureExpression(value);
};

const createBinaryExpression = (operator: string, left: unknown, right: unknown): Predicate => {
  const leftExpression = ensureComparableExpression(left);
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${leftExpression.compile(ctx)}${sql.raw(` ${operator} `)}${compileValue(right, ctx, leftExpression.sqlType)}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export function and(): undefined;
export function and(...conditions: [Predicate, ...PredicateInput[]]): Predicate;
export function and(...conditions: PredicateInput[]): Predicate | undefined;
export function and(...conditions: PredicateInput[]): Predicate | undefined {
  return normalizePredicateGroup("and", conditions);
}

export function or(): undefined;
export function or(...conditions: [Predicate, ...PredicateInput[]]): Predicate;
export function or(...conditions: PredicateInput[]): Predicate | undefined;
export function or(...conditions: PredicateInput[]): Predicate | undefined {
  return normalizePredicateGroup("or", conditions);
}

export const not = (condition: Predicate): Predicate => {
  const wrapped = condition as SqlPredicate;
  return createExpression<boolean>({
    compile: (ctx) => sql`${sql.raw("not (")}${wrapped.compile(ctx)}${sql.raw(")")}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

const makeBinary =
  (operator: string) =>
  (left: unknown, right: unknown): Predicate =>
    createBinaryExpression(operator, left, right);

export const eq = makeBinary("=");
export const ne = makeBinary("!=");
export const gt = makeBinary(">");
export const gte = makeBinary(">=");
export const lt = makeBinary("<");
export const lte = makeBinary("<=");

const LIKE_ESCAPE_CHAR = "\\";
type LikeOperator = "like" | "not like" | "ilike" | "not ilike";
type LikeLiteralMode = "contains" | "startsWith" | "endsWith";

const escapeLikePattern = (value: string): string => {
  return value
    .replaceAll(LIKE_ESCAPE_CHAR, LIKE_ESCAPE_CHAR + LIKE_ESCAPE_CHAR)
    .replaceAll("%", `${LIKE_ESCAPE_CHAR}%`)
    .replaceAll("_", `${LIKE_ESCAPE_CHAR}_`);
};

const toLiteralLikePattern = (value: string, mode: LikeLiteralMode): string => {
  const escaped = escapeLikePattern(value);
  if (mode === "startsWith") {
    return `${escaped}%`;
  }
  if (mode === "endsWith") {
    return `%${escaped}`;
  }
  return `%${escaped}%`;
};

const createLikePredicate = (left: unknown, right: string, operator: LikeOperator): Predicate => {
  const leftExpression = ensureComparableExpression(left);
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${leftExpression.compile(ctx)}${sql.raw(` ${operator} `)}${compileValue(right, ctx, "String")}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const like = (left: unknown, right: string): Predicate => createLikePredicate(left, right, "like");

export const notLike = (left: unknown, right: string): Predicate => {
  return createLikePredicate(left, right, "not like");
};

export const ilike = (left: unknown, right: string): Predicate => createLikePredicate(left, right, "ilike");

export const notIlike = (left: unknown, right: string): Predicate => {
  return createLikePredicate(left, right, "not ilike");
};

export const contains = (left: unknown, right: string): Predicate => {
  return like(left, toLiteralLikePattern(right, "contains"));
};

export const startsWith = (left: unknown, right: string): Predicate => {
  return like(left, toLiteralLikePattern(right, "startsWith"));
};

export const endsWith = (left: unknown, right: string): Predicate => {
  return like(left, toLiteralLikePattern(right, "endsWith"));
};

export const containsIgnoreCase = (left: unknown, right: string): Predicate => {
  return ilike(left, toLiteralLikePattern(right, "contains"));
};

export const startsWithIgnoreCase = (left: unknown, right: string): Predicate => {
  return ilike(left, toLiteralLikePattern(right, "startsWith"));
};

export const endsWithIgnoreCase = (left: unknown, right: string): Predicate => {
  return ilike(left, toLiteralLikePattern(right, "endsWith"));
};

export const between = (expression: unknown, start: unknown, end: unknown): Predicate => {
  const wrapped = ensureComparableExpression(expression);
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${wrapped.compile(ctx)}${sql.raw(" between ")}${compileValue(start, ctx, wrapped.sqlType)}${sql.raw(" and ")}${compileValue(end, ctx, wrapped.sqlType)}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

const isArraySqlType = (sqlType?: string): sqlType is `Array(${string})` => {
  return typeof sqlType === "string" && sqlType.startsWith("Array(") && sqlType.endsWith(")");
};

const compilePredicateFunction = (name: string, args: SQLFragment[]): SQLFragment => {
  return sql`${sql.raw(name)}(${joinSqlParts(args, ", ")})`;
};

const compileArrayFunctionArg = (value: unknown, ctx: BuildContext, leftExpression: SqlSelection<unknown>) => {
  if (Array.isArray(value) && isArraySqlType(leftExpression.sqlType)) {
    return compileValue(value, ctx, leftExpression.sqlType);
  }
  return compileValue(value, ctx);
};

export const has = (haystack: unknown, needle: unknown): Predicate => {
  const haystackExpression = ensureComparableExpression(haystack);
  return createExpression<boolean>({
    compile: (ctx) => compilePredicateFunction("has", [haystackExpression.compile(ctx), compileValue(needle, ctx)]),
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const hasAll = (set: unknown, subset: unknown): Predicate => {
  const setExpression = ensureComparableExpression(set);
  return createExpression<boolean>({
    compile: (ctx) =>
      compilePredicateFunction("hasAll", [
        setExpression.compile(ctx),
        compileArrayFunctionArg(subset, ctx, setExpression),
      ]),
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const hasAny = (arrX: unknown, arrY: unknown): Predicate => {
  const arrXExpression = ensureComparableExpression(arrX);
  return createExpression<boolean>({
    compile: (ctx) =>
      compilePredicateFunction("hasAny", [
        arrXExpression.compile(ctx),
        compileArrayFunctionArg(arrY, ctx, arrXExpression),
      ]),
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const hasSubstr = (array: unknown, needle: unknown): Predicate => {
  const arrayExpression = ensureComparableExpression(array);
  return createExpression<boolean>({
    compile: (ctx) =>
      compilePredicateFunction("hasSubstr", [
        arrayExpression.compile(ctx),
        compileArrayFunctionArg(needle, ctx, arrayExpression),
      ]),
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

const createInExpression = (
  negate: boolean,
  left: unknown,
  right: readonly unknown[] | AnySubquery | AnyCte,
): Predicate => {
  const leftExpression = ensureComparableExpression(left);
  const operator = negate ? " not in (" : " in (";
  const emptyArrayLiteral = negate ? "1" : "0";
  return createExpression<boolean>({
    compile: (ctx) => {
      if (Array.isArray(right)) {
        if (right.length === 0) {
          return sql.raw(emptyArrayLiteral);
        }
        const parts = right.map((value) => compileValue(value, ctx, leftExpression.sqlType));
        return sql`${leftExpression.compile(ctx)}${sql.raw(operator)}${joinSqlParts(parts, ", ")}${sql.raw(")")}`;
      }

      const querySource = (right as AnySubquery | AnyCte).query;
      return sql`${leftExpression.compile(ctx)}${sql.raw(operator)}${sql.raw(compileNestedQuery(querySource, ctx).statement)}${sql.raw(")")}`;
    },
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const inArray = (left: unknown, right: readonly unknown[] | AnySubquery | AnyCte): Predicate =>
  createInExpression(false, left, right);

export const notInArray = (left: unknown, right: readonly unknown[] | AnySubquery | AnyCte): Predicate =>
  createInExpression(true, left, right);

export const exists = (query: AnySubquery | AnyCte | SelectBuilder<Record<string, unknown>>): Predicate => {
  const selectQuery = isSubquery(query) || isCte(query) ? query.query : query;
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${sql.raw("exists (")}${sql.raw(compileNestedQuery(selectQuery, ctx).statement)}${sql.raw(")")}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const notExists = (query: AnySubquery | AnyCte | SelectBuilder<Record<string, unknown>>): Predicate => {
  return not(exists(query));
};

export const asc = (expression: Selection<unknown>): Order => ({
  expression,
  direction: "asc",
});

export const desc = (expression: Selection<unknown>): Order => ({
  expression,
  direction: "desc",
});

export const createTableFunctionSource = (
  compileSource: (ctx: BuildContext) => SQLFragment,
  aliasName?: string,
): TableFunctionSource => {
  return {
    kind: "table-function",
    alias: aliasName,
    compileSource(ctx) {
      const source = compileSource(ctx);
      if (!aliasName) {
        return source;
      }
      return sql`${source}${sql.raw(" as ")}${sql.identifier(aliasName)}`;
    },
    as<TAlias extends string>(nextAlias: TAlias) {
      return createTableFunctionSource(compileSource, nextAlias);
    },
  };
};

type NestedGroupAccumulator = {
  fields: Record<string, unknown>;
  nullable: boolean;
  allNull: boolean;
};

const isNullish = (value: unknown): value is null | undefined => value === null || value === undefined;

const decodeFlatField = (item: SelectionMeta, rawValue: unknown): unknown => {
  if (item.nullable && isNullish(rawValue)) {
    return null;
  }
  return decodeValue(item.decoder, rawValue, item.sqlAlias);
};

const applyNestedField = (
  nestedGroups: Map<string, NestedGroupAccumulator>,
  item: SelectionMeta,
  rawValue: unknown,
): void => {
  if (item.path.length !== 2) {
    return;
  }
  const [groupKey, fieldKey] = item.path;
  const existing =
    nestedGroups.get(groupKey) ??
    ({
      fields: {},
      nullable: Boolean(item.groupNullable),
      allNull: true,
    } satisfies NestedGroupAccumulator);
  existing.allNull = existing.allNull && isNullish(rawValue);
  existing.fields[fieldKey] = decodeFlatField(item, rawValue);
  nestedGroups.set(groupKey, existing);
};

const finalizeNestedGroup = (group: NestedGroupAccumulator): Record<string, unknown> | null => {
  if (group.nullable && group.allNull) {
    return null;
  }
  return group.fields;
};

/**
 * Decode a single ClickHouse row into the shape declared by a builder's selection.
 *
 * `selection` is the compiled metadata array attached to a `CompiledQuery`; each entry
 * carries a logical `path` (1 segment for flat fields, 2 segments for nested struct fields),
 * the source `sqlAlias` to look up in the raw row, a `decoder`, and `nullable` / `groupNullable`
 * flags.
 *
 * Nested-group all-null collapse rule:
 * when every field of a nested group is null/undefined AND the group itself is declared
 * nullable (`groupNullable === true`), the whole group collapses to `null` rather than
 * `{ field: null, ... }`. This matches ClickHouse's left-join-on-Nested semantics under
 * `join_use_nulls = 1`.
 *
 * Exposed via `public_api` for users post-processing raw rows from the streaming API.
 */
export const decodeRow = <TRow extends Record<string, unknown>>(
  row: Record<string, unknown>,
  selection: readonly SelectionMeta[],
): TRow => {
  const decodedRow = {} as TRow;
  const nestedGroups = new Map<string, NestedGroupAccumulator>();

  for (const item of selection) {
    const rawValue = row[item.sqlAlias];

    if (item.path.length === 1) {
      decodedRow[item.path[0] as keyof TRow] = decodeFlatField(item, rawValue) as TRow[keyof TRow];
      continue;
    }

    applyNestedField(nestedGroups, item, rawValue);
  }

  for (const [groupKey, group] of nestedGroups) {
    decodedRow[groupKey as keyof TRow] = finalizeNestedGroup(group) as TRow[keyof TRow];
  }

  return decodedRow;
};

export const createSessionId = () => {
  return `ck_orm_${createUuid().replaceAll("-", "_")}`;
};

export const expr = <TData = unknown>(
  value: SQLFragment,
  config?: { decoder?: Decoder<TData>; sqlType?: string },
): Selection<TData> =>
  wrapSql(value, {
    decoder: (config?.decoder ?? value.decoder) as Decoder<TData>,
    sqlType: config?.sqlType,
  });
