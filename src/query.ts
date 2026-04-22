import type { AnyColumn, Column } from "./columns";
import { createClientValidationError } from "./errors";
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
  type OrderByExpression,
  type Predicate,
  type QueryParams,
  type SelectionMeta,
  type SqlExpression,
  wrapSql,
} from "./query-shared";
import type { ClickHouseBaseQueryOptions } from "./runtime";
import type { AnyTable, Table } from "./schema";
import { renderTableIdentifier } from "./schema";
import { compileSql, type SQLFragment, sql } from "./sql";

type QuerySource = AnyTable | AnySubquery | AnyCte | TableFunctionSource;
type KnownQuerySource = AnyTable | AnySubquery | AnyCte;
type ForcedSettings = Readonly<Record<string, string | number | boolean>>;
type MutableForcedSettings = Record<string, string | number | boolean>;

type SourceColumns = Record<string, SqlExpression<unknown>>;

type QueryMode = "query" | "command";
type JoinUseNulls = 0 | 1;
type PredicateInput = Predicate | undefined;
type CompileState = {
  forcedSettings?: MutableForcedSettings;
};
const compileStateStackStore = new WeakMap<BuildContext, CompileState[]>();

export const compileWithContextSymbol = Symbol("clickhouseOrmCompileWithContext");
export const compileQuerySymbol = Symbol("clickhouseOrmCompileQuery");

type PrimitiveValue = string | number | bigint | boolean | null | Date;
type CountSource = AnyTable | AnySubquery | AnyCte;
type InsertRowInput<TTable extends AnyTable> = Partial<TTable["$inferInsert"]>;

type SourceKey<TSource extends KnownQuerySource> =
  TSource extends Table<Record<string, AnyColumn>, infer TName, infer TAlias, string>
    ? TAlias extends string
      ? TAlias
      : TName
    : TSource extends Subquery<Record<string, unknown>, infer TAlias>
      ? TAlias
      : TSource extends Cte<Record<string, unknown>, infer TName>
        ? TName
        : never;

type SourceResult<TSource extends KnownQuerySource> =
  TSource extends Table<Record<string, AnyColumn>, string, string | undefined, string>
    ? TSource["$inferSelect"]
    : TSource extends Subquery<infer TResult, string>
      ? TResult
      : TSource extends Cte<infer TResult, string>
        ? TResult
        : never;

type JoinedSourceState = {
  readonly row: Record<string, unknown>;
  readonly nullable: boolean;
};

type JoinedSources = Record<string, JoinedSourceState>;
type AnySelectBuilder<TResult extends Record<string, unknown> = Record<string, unknown>> = SelectBuilder<
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
  readonly expression: SqlExpression<unknown>;
  readonly path: readonly [string] | readonly [string, string];
  readonly nullable?: boolean;
  readonly groupNullable?: boolean;
}

interface JoinClause {
  readonly type: "inner" | "left";
  readonly source: QuerySource;
  readonly on: Predicate;
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
  readonly forcedSettings?: Readonly<Record<string, string | number | boolean>>;
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

type ReferenceColumns<TSelection extends SelectionRecord> = {
  [K in keyof TSelection]: SqlExpression<
    TSelection[K] extends SqlExpression<infer TData>
      ? TData
      : TSelection[K] extends Column<infer TData, string>
        ? TData
        : unknown
  >;
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
  if (!stack) {
    return;
  }

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
    return;
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

const createReferenceExpression = <TData>(
  sourceAlias: string,
  columnName: string,
  decoder: Decoder<TData>,
  sqlType?: string,
): SqlExpression<TData> => {
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

const buildReferenceColumns = <TSelection extends SelectionRecord>(
  sourceAlias: string,
  selectionItems: readonly SelectionItem[],
): ReferenceColumns<TSelection> => {
  const columns = {} as ReferenceColumns<TSelection>;

  for (const item of selectionItems) {
    columns[item.key as keyof TSelection] = createReferenceExpression(
      sourceAlias,
      item.sqlAlias,
      item.expression.decoder,
      item.expression.sqlType,
    ) as ReferenceColumns<TSelection>[keyof TSelection];
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

const normalizeLimitValue = (value: PrimitiveValue | SqlExpression<unknown>, ctx: BuildContext) => {
  return compileValue(value, ctx, "Int64");
};

const countDecoder: Decoder<number> = (value) => {
  const nextValue =
    typeof value === "number" ? value : typeof value === "bigint" ? Number(value) : Number(String(value));

  if (Number.isNaN(nextValue)) {
    throw createClientValidationError(
      `Failed to decode count() result: ${String(value)}. Expected a numeric value or numeric string from ClickHouse; verify the count() query was not aliased to a non-numeric expression.`,
      { cause: value },
    );
  }

  return nextValue;
};

const buildLogicalPredicate = (operator: "and" | "or", predicates: readonly Predicate[]): Predicate => {
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
): Predicate | undefined => {
  const filteredPredicates = predicates.filter((predicate): predicate is Predicate => predicate !== undefined);

  if (filteredPredicates.length === 0) {
    return undefined;
  }

  if (filteredPredicates.length === 1) {
    return filteredPredicates[0];
  }

  return buildLogicalPredicate(operator, filteredPredicates);
};

type CountQuery<TData = number> = SqlExpression<TData> &
  PromiseLike<TData> & {
    execute(options?: ClickHouseBaseQueryOptions): Promise<TData>;
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
    condition?: Predicate;
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
      ? sql`${sql.raw("select count() as ")}${sql.identifier(config.outputAlias)}`
      : sql`select count()`,
  );
  queryParts.push(sql`${sql.raw("from ")}${renderSource(config.source, ctx)}`);

  if (config.condition) {
    queryParts.push(sql`${sql.raw("where ")}${config.condition.compile(ctx)}`);
  }

  return sql`${joinSqlParts(queryParts, " ")}`;
};

const createCountQuery = (config: {
  ctes: readonly AnyCte[];
  runner?: PreparedRunner;
  source: CountSource;
  predicates?: PredicateInput[];
}): CountQuery<number> => {
  const condition = normalizePredicateGroup("and", config.predicates ?? []);

  const expression = createExpression<number>({
    compile: (ctx) =>
      sql`${sql.raw("(")}${buildCountStatement(ctx, {
        ctes: config.ctes,
        source: config.source,
        condition,
      })}${sql.raw(")")}`,
    decoder: countDecoder,
    sqlType: "UInt64",
  });

  const execute = (options?: ClickHouseBaseQueryOptions): Promise<number> => {
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
        createCompiledQuery<{ value: number }>(
          compiledResult.query,
          [
            {
              key: "value",
              sqlAlias: "__orm_count",
              decoder: countDecoder,
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
    /**
     * Builders are intentionally re-entrant: each `await db.count(...)` triggers a fresh
     * `execute()` and a new ClickHouse request. To memoize the result, capture the promise
     * once: `const pending = builder.execute()`. This matches Drizzle/Kysely semantics.
     */
    // biome-ignore lint/suspicious/noThenProperty: count queries are intentionally thenable so await db.count(...) matches Drizzle-style usage.
    then<TResult1 = number, TResult2 = never>(
      onfulfilled?: ThenHandler<number, TResult1>,
      onrejected?: CatchHandler<TResult2>,
    ): PromiseLike<TResult1 | TResult2> {
      return execute().then(onfulfilled, onrejected);
    },
    catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<number | TResult2> {
      return execute().catch(onrejected);
    },
    finally(onfinally?: (() => void) | null): Promise<number> {
      return execute().finally(onfinally ?? undefined);
    },
  }) as CountQuery<number>;
};

interface SelectBuilderConfig<_TResult extends Record<string, unknown>> {
  ctes?: AnyCte[];
  runner?: PreparedRunner;
  selection?: SelectionRecord;
  fromSource?: QuerySource;
  joins?: JoinClause[];
  whereClause?: Predicate;
  groupByItems?: SqlExpression<unknown>[];
  havingClause?: Predicate;
  orderByItems?: OrderByExpression[];
  limitValue?: PrimitiveValue | SqlExpression<unknown>;
  offsetValue?: PrimitiveValue | SqlExpression<unknown>;
  limitByValue?: {
    readonly columns: SqlExpression<unknown>[];
    readonly limit: PrimitiveValue | SqlExpression<unknown>;
  };
  useFinal?: boolean;
  joinUseNulls?: JoinUseNulls;
}

export class SelectBuilder<
  TResult extends Record<string, unknown> = Record<string, unknown>,
  TSelection extends SelectionRecord | undefined = SelectionRecord | undefined,
  TRootSource extends KnownQuerySource | undefined = KnownQuerySource | undefined,
  TJoinedSources extends JoinedSources = NoJoinedSources,
  TJoinUseNulls extends JoinUseNulls = 1,
> implements PromiseLike<TResult[]>
{
  private readonly ctes: AnyCte[];
  private readonly runner?: PreparedRunner;
  private readonly selection?: TSelection;
  private fromSource?: QuerySource;
  private readonly joins: JoinClause[];
  private whereClause?: Predicate;
  private readonly groupByItems: SqlExpression<unknown>[];
  private havingClause?: Predicate;
  private readonly orderByItems: OrderByExpression[];
  private limitValue?: PrimitiveValue | SqlExpression<unknown>;
  private offsetValue?: PrimitiveValue | SqlExpression<unknown>;
  private limitByValue?: {
    readonly columns: SqlExpression<unknown>[];
    readonly limit: PrimitiveValue | SqlExpression<unknown>;
  };
  private useFinal: boolean;
  private readonly joinUseNulls: TJoinUseNulls;

  constructor(config?: SelectBuilderConfig<TResult> & { selection?: TSelection }) {
    this.ctes = config?.ctes ?? [];
    this.runner = config?.runner;
    this.selection = config?.selection;
    this.fromSource = config?.fromSource;
    this.joins = config?.joins ?? [];
    this.whereClause = config?.whereClause;
    this.groupByItems = config?.groupByItems ?? [];
    this.havingClause = config?.havingClause;
    this.orderByItems = config?.orderByItems ?? [];
    this.limitValue = config?.limitValue;
    this.offsetValue = config?.offsetValue;
    this.limitByValue = config?.limitByValue;
    this.useFinal = config?.useFinal ?? false;
    this.joinUseNulls = (config?.joinUseNulls ?? 1) as TJoinUseNulls;
  }

  private snapshotConfig(): SelectBuilderConfig<TResult> & { selection?: TSelection } {
    return {
      ctes: this.ctes,
      runner: this.runner,
      selection: this.selection,
      fromSource: this.fromSource,
      joins: this.joins,
      whereClause: this.whereClause,
      groupByItems: this.groupByItems,
      havingClause: this.havingClause,
      orderByItems: this.orderByItems,
      limitValue: this.limitValue,
      offsetValue: this.offsetValue,
      limitByValue: this.limitByValue,
      useFinal: this.useFinal,
      joinUseNulls: this.joinUseNulls,
    };
  }

  private clone<
    TNextResult extends Record<string, unknown> = TResult,
    TNextRoot extends KnownQuerySource | undefined = TRootSource,
    TNextJoined extends JoinedSources = TJoinedSources,
  >(
    overrides: Partial<SelectBuilderConfig<TNextResult>> & {
      selection?: TSelection;
    },
  ): SelectBuilder<TNextResult, TSelection, TNextRoot, TNextJoined, TJoinUseNulls> {
    return new SelectBuilder<TNextResult, TSelection, TNextRoot, TNextJoined, TJoinUseNulls>({
      ...(this.snapshotConfig() as SelectBuilderConfig<TNextResult> & { selection?: TSelection }),
      ...overrides,
    });
  }

  execute(options?: ClickHouseBaseQueryOptions): Promise<TResult[]> {
    const runner = ensureRunner(this.runner, "execute");
    return runner.execute(this[compileQuerySymbol](), options);
  }

  iterator(options?: ClickHouseBaseQueryOptions): AsyncGenerator<TResult, void, unknown> {
    const runner = ensureRunner(this.runner, "iterator");
    return runner.iterator(this[compileQuerySymbol](), options);
  }

  /**
   * Builders are intentionally re-entrant: each `await builder` triggers a fresh
   * `execute()` and a new ClickHouse request. To memoize the result, capture the promise
   * once: `const pending = builder.execute()`. This matches Drizzle/Kysely semantics and
   * lets callers refresh data simply by awaiting the builder again.
   */
  // biome-ignore lint/suspicious/noThenProperty: builders are intentionally thenable so await builder matches Drizzle-style usage.
  then<TResult1 = TResult[], TResult2 = never>(
    onfulfilled?: ThenHandler<TResult[], TResult1>,
    onrejected?: CatchHandler<TResult2>,
  ): PromiseLike<TResult1 | TResult2> {
    return this.execute().then(onfulfilled, onrejected);
  }

  catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<TResult[] | TResult2> {
    return this.execute().catch(onrejected);
  }

  finally(onfinally?: (() => void) | null): Promise<TResult[]> {
    return this.execute().finally(onfinally ?? undefined);
  }

  private isNullableJoinEnabled(): boolean {
    return this.joinUseNulls === 1;
  }

  private getNullableSources(): Set<string> {
    const result = new Set<string>();
    if (!this.isNullableJoinEnabled()) {
      return result;
    }
    for (const join of this.joins) {
      if (join.type !== "left") {
        continue;
      }
      const sourceKey = getSourceKey(join.source);
      if (sourceKey) {
        result.add(sourceKey);
      }
    }
    return result;
  }

  buildSelectionItems(): SelectionItem[] {
    if (this.selection) {
      return normalizeSelectionRecord(this.selection, this.getNullableSources());
    }

    if (!this.fromSource) {
      throw createClientValidationError(
        "select() without explicit selection requires from() first. Call .from(table) before .select(), or pass an explicit selection object to select({...}).",
      );
    }

    const rootSourceKey = getSourceKey(this.fromSource);
    const rootSourceColumns = getSourceColumns(this.fromSource);
    if (!rootSourceColumns || !rootSourceKey) {
      throw createClientValidationError(
        "select() without explicit selection requires a source with known columns. Use a defined table()/subquery()/cte() source, or pass an explicit selection object to select({...}).",
      );
    }

    const selectionItems: SelectionItem[] = [];
    const hasJoins = this.joins.some((join) => getSourceColumns(join.source));
    const nullableJoinEnabled = this.isNullableJoinEnabled();
    let nextIndex = 0;

    const appendSourceColumns = (sourceKey: string, sourceColumns: SourceColumns, groupNullable: boolean) => {
      for (const [fieldKey, expression] of Object.entries(sourceColumns)) {
        nextIndex += 1;
        selectionItems.push({
          key: fieldKey,
          sqlAlias: hasJoins ? `__orm_${nextIndex}` : fieldKey,
          expression,
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

    for (const join of this.joins) {
      const joinSourceKey = getSourceKey(join.source);
      const joinSourceColumns = getSourceColumns(join.source);
      if (!joinSourceKey || !joinSourceColumns) {
        continue;
      }
      appendSourceColumns(joinSourceKey, joinSourceColumns, join.type === "left" && nullableJoinEnabled);
    }

    return selectionItems;
  }

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
    return this.clone<
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
  }

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
    return this.clone<
      InferJoinResult<TSelection, TResult, TRootSource, AddJoinedSource<TJoinedSources, TSource, false>>,
      TRootSource,
      AddJoinedSource<TJoinedSources, TSource, false>
    >({
      joins: [...this.joins, { type: "inner", source, on }],
    });
  }

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
    return this.clone<
      InferJoinResult<
        TSelection,
        TResult,
        TRootSource,
        AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
      >,
      TRootSource,
      AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
    >({
      joins: [...this.joins, { type: "left", source, on }],
    });
  }

  where(
    ...predicates: PredicateInput[]
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
    return this.clone({
      whereClause: normalizePredicateGroup("and", predicates),
    });
  }

  groupBy(
    ...expressions: SqlExpression<unknown>[]
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
    return this.clone({
      groupByItems: [...this.groupByItems, ...expressions],
    });
  }

  having(condition?: Predicate): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
    return this.clone({
      havingClause: condition,
    });
  }

  orderBy(
    ...expressions: Array<OrderByExpression | SqlExpression<unknown> | AnyColumn>
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
    const nextOrderItems = expressions.map((expression) => {
      if ("direction" in expression && "expression" in expression) {
        return expression;
      }
      return asc(ensureExpression(expression));
    });
    return this.clone({
      orderByItems: [...this.orderByItems, ...nextOrderItems],
    });
  }

  limit(
    value: PrimitiveValue | SqlExpression<unknown>,
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
    return this.clone({
      limitValue: value,
    });
  }

  offset(
    value: PrimitiveValue | SqlExpression<unknown>,
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
    return this.clone({
      offsetValue: value,
    });
  }

  final(): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
    return this.clone({
      useFinal: true,
    });
  }

  limitBy(
    columns: SqlExpression<unknown>[],
    limit: PrimitiveValue | SqlExpression<unknown>,
  ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
    return this.clone({
      limitByValue: {
        columns,
        limit,
      },
    });
  }

  [compileWithContextSymbol](ctx: BuildContext): CompiledQuery<TResult> {
    const { result, forcedSettings: nestedForcedSettings } = withCompileState(ctx, () => {
      const selectionItems = this.buildSelectionItems();
      const queryParts: SQLFragment[] = [];
      const cteFragment = renderCtes(this.ctes, ctx);
      if (cteFragment) {
        queryParts.push(cteFragment);
      }

      queryParts.push(sql`${sql.raw("select ")}${renderSelection(selectionItems, ctx)}`);

      if (this.fromSource) {
        const fromSource = renderSource(this.fromSource, ctx);
        const finalSuffix = this.useFinal ? sql.raw(" final") : sql.raw("");
        queryParts.push(sql`${sql.raw("from ")}${fromSource}${finalSuffix}`);
      }

      if (this.joins.length > 0) {
        for (const join of this.joins) {
          const joinKeyword = join.type === "inner" ? "inner join" : "left join";
          queryParts.push(
            sql`${sql.raw(`${joinKeyword} `)}${renderSource(join.source, ctx)}${sql.raw(" on ")}${join.on.compile(ctx)}`,
          );
        }
      }

      if (this.whereClause) {
        queryParts.push(sql`${sql.raw("where ")}${this.whereClause.compile(ctx)}`);
      }

      if (this.groupByItems.length > 0) {
        queryParts.push(
          sql`${sql.raw("group by ")}${joinSqlParts(
            this.groupByItems.map((item) => item.compile(ctx)),
            ", ",
          )}`,
        );
      }

      if (this.havingClause) {
        queryParts.push(sql`${sql.raw("having ")}${this.havingClause.compile(ctx)}`);
      }

      if (this.orderByItems.length > 0) {
        const orderByParts = this.orderByItems.map((item) => {
          return sql`${item.expression.compile(ctx)}${sql.raw(` ${item.direction.toUpperCase()}`)}`;
        });
        queryParts.push(sql`${sql.raw("order by ")}${joinSqlParts(orderByParts, ", ")}`);
      }

      if (this.limitByValue) {
        queryParts.push(
          sql`${sql.raw("limit ")}${normalizeLimitValue(this.limitByValue.limit, ctx)}${sql.raw(" by ")}${joinSqlParts(
            this.limitByValue.columns.map((column) => column.compile(ctx)),
            ", ",
          )}`,
        );
      }

      if (this.limitValue !== undefined) {
        queryParts.push(sql`${sql.raw("limit ")}${normalizeLimitValue(this.limitValue, ctx)}`);
      }

      if (this.offsetValue !== undefined) {
        queryParts.push(sql`${sql.raw("offset ")}${normalizeLimitValue(this.offsetValue, ctx)}`);
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
        this.isNullableJoinEnabled() && this.joins.some((join) => join.type === "left")
          ? { join_use_nulls: 1 }
          : undefined;

      const metadata: CompiledQueryMetadata | undefined = this.fromSource
        ? {
            rootSourceName: getSourceKey(this.fromSource),
            joinCount: this.joins.length,
          }
        : this.joins.length > 0
          ? { joinCount: this.joins.length }
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
  }

  [compileQuerySymbol](): CompiledQuery<TResult> {
    return this[compileWithContextSymbol]({
      params: {},
      nextParamIndex: 0,
    });
  }

  as<TAlias extends string>(alias: TAlias): Subquery<TResult, TAlias> {
    const selectionItems = this.buildSelectionItems();
    const columns = buildReferenceColumns<TResult>(alias, selectionItems);
    const subquery = {
      kind: "subquery" as const,
      alias,
      query: this as AnySelectBuilder<TResult>,
      columns,
    };

    return Object.assign(subquery, columns) as Subquery<TResult, TAlias>;
  }
}

export class InsertBuilder<TTable extends AnyTable> implements PromiseLike<undefined> {
  private readonly table: TTable;
  private readonly runner?: PreparedRunner;
  private rows: InsertRowInput<TTable>[] = [];

  constructor(table: TTable, runner?: PreparedRunner) {
    this.table = table;
    this.runner = runner;
  }

  values(values: InsertRowInput<TTable> | readonly InsertRowInput<TTable>[]): InsertBuilder<TTable> {
    const next = new InsertBuilder(this.table, this.runner);
    next.rows = normalizeInsertRows(this.table, values);
    return next;
  }

  execute(options?: ClickHouseBaseQueryOptions): Promise<undefined> {
    const runner = ensureRunner(this.runner, "execute");
    return runner
      .command(this[compileQuerySymbol]() as unknown as CompiledQuery<Record<string, unknown>>, options)
      .then(() => undefined);
  }

  /**
   * Insert builders are intentionally re-entrant: each `await builder` triggers a fresh
   * `execute()` and a new INSERT request. To memoize the result, capture the promise
   * once: `const pending = builder.execute()`. This matches Drizzle/Kysely semantics.
   */
  // biome-ignore lint/suspicious/noThenProperty: insert builders are intentionally thenable so await builder matches Drizzle-style usage.
  then<TResult1 = undefined, TResult2 = never>(
    onfulfilled?: ThenHandler<undefined, TResult1>,
    onrejected?: CatchHandler<TResult2>,
  ): PromiseLike<TResult1 | TResult2> {
    return this.execute().then(onfulfilled, onrejected);
  }

  catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<undefined | TResult2> {
    return this.execute().catch(onrejected);
  }

  finally(onfinally?: (() => void) | null): Promise<undefined> {
    return this.execute().finally(onfinally ?? undefined);
  }

  [compileQuerySymbol](): CompiledQuery<never> {
    const columnEntries = Object.entries(this.table.columns);
    const columnNames = columnEntries.map(([name]) => name);
    const columnTypes = columnEntries.map(([, column]) => column.sqlType);
    const ctx: BuildContext = {
      params: {},
      nextParamIndex: 0,
    };
    const valueRows = this.rows.map(
      (row) =>
        sql`(${joinSqlParts(
          columnEntries.map(([columnName, column], index) => {
            const value = (row as Record<string, unknown>)[columnName];
            if (value === undefined) {
              return sql.raw("DEFAULT");
            }
            return compileValue(column.mapToDriverValue(value as never), ctx, columnTypes[index]);
          }),
          ", ",
        )})`,
    );
    const statement = sql`insert into ${renderTableIdentifier(this.table)} (${joinSqlParts(
      columnNames.map((columnName) => sql.identifier(columnName)),
      ", ",
    )}) values ${joinSqlParts(valueRows, ", ")}`;
    const compiled = compileSql(statement, ctx);

    return createCompiledQuery(compiled.query, [], "command", {
      ...compiled.params,
    });
  }
}

export type Subquery<
  TResult extends Record<string, unknown> = Record<string, unknown>,
  TAlias extends string = string,
> = {
  readonly kind: "subquery";
  readonly alias: TAlias;
  readonly query: AnySelectBuilder<TResult>;
  readonly columns: ReferenceColumns<TResult>;
} & ReferenceColumns<TResult>;

type AnySubquery = {
  readonly kind: "subquery";
  readonly alias: string;
  readonly query: AnySelectBuilder<Record<string, unknown>>;
  readonly columns: SourceColumns;
};

export type Cte<TResult extends Record<string, unknown> = Record<string, unknown>, TName extends string = string> = {
  readonly kind: "cte";
  readonly name: TName;
  readonly query: AnySelectBuilder<TResult>;
  readonly columns: ReferenceColumns<TResult>;
} & ReferenceColumns<TResult>;

type AnyCte = {
  readonly kind: "cte";
  readonly name: string;
  readonly query: AnySelectBuilder<Record<string, unknown>>;
  readonly columns: SourceColumns;
};

export class QueryClient<TSchema = unknown, TJoinUseNulls extends JoinUseNulls = 1> {
  readonly schema: TSchema;
  readonly ctes: AnyCte[];
  private readonly runner?: PreparedRunner;
  protected readonly joinUseNulls: TJoinUseNulls;

  constructor(config: {
    schema: TSchema;
    ctes?: AnyCte[];
    runner?: PreparedRunner;
    joinUseNulls?: TJoinUseNulls;
  }) {
    this.schema = config.schema;
    this.ctes = config.ctes ?? [];
    this.runner = config.runner;
    this.joinUseNulls = (config.joinUseNulls ?? 1) as TJoinUseNulls;
  }

  select<TSelection extends SelectionRecord | undefined = undefined>(
    selection?: TSelection,
  ): SelectBuilder<
    TSelection extends SelectionRecord ? InferSelectionResult<TSelection> : Record<string, unknown>,
    TSelection,
    undefined,
    NoJoinedSources,
    TJoinUseNulls
  > {
    return new SelectBuilder<
      TSelection extends SelectionRecord ? InferSelectionResult<TSelection> : Record<string, unknown>,
      TSelection,
      undefined,
      NoJoinedSources,
      TJoinUseNulls
    >({
      ctes: this.ctes,
      runner: this.runner,
      selection,
      joinUseNulls: this.joinUseNulls,
    });
  }

  count(source: CountSource, ...predicates: PredicateInput[]): CountQuery<number> {
    return createCountQuery({
      ctes: this.ctes,
      runner: this.runner,
      source,
      predicates,
    });
  }

  insert<TTable extends AnyTable>(table: TTable): InsertBuilder<TTable> {
    return new InsertBuilder(table, this.runner);
  }

  $with<TName extends string>(name: TName) {
    return {
      as: <TResult extends Record<string, unknown>>(query: AnySelectBuilder<TResult>): Cte<TResult, TName> => {
        const selectionItems = query.buildSelectionItems();
        const columns = buildReferenceColumns<TResult>(name, selectionItems);
        const cte = {
          kind: "cte" as const,
          name,
          query,
          columns,
        };
        return Object.assign(cte, columns) as Cte<TResult, TName>;
      },
    };
  }

  with(...ctes: AnyCte[]): QueryClient<TSchema, TJoinUseNulls> {
    return new QueryClient<TSchema, TJoinUseNulls>({
      schema: this.schema,
      ctes: [...this.ctes, ...ctes],
      runner: this.runner,
      joinUseNulls: this.joinUseNulls,
    });
  }
}

const ensureComparableExpression = (value: unknown): SqlExpression<unknown> => {
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
  return createExpression<boolean>({
    compile: (ctx) => sql`${sql.raw("not (")}${condition.compile(ctx)}${sql.raw(")")}`,
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

/**
 * Escapes `%` and `_` wildcard characters in a LIKE pattern so they are
 * treated as literal characters.
 *
 * ClickHouse's `LIKE` operator uses `\` as the default escape character.
 * ClickHouse does **not** support the standard SQL `ESCAPE` keyword, so the
 * backslash is the only available escape character.
 *
 * Use this when matching user input that may contain these characters:
 * ```ts
 * db.select().from(users).where(like(users.name, escapeLike("50%")))
 * ```
 */
export const escapeLike = (value: string): string => {
  return value
    .replaceAll(LIKE_ESCAPE_CHAR, LIKE_ESCAPE_CHAR + LIKE_ESCAPE_CHAR)
    .replaceAll("%", `${LIKE_ESCAPE_CHAR}%`)
    .replaceAll("_", `${LIKE_ESCAPE_CHAR}_`);
};

export const like = (left: unknown, right: string): Predicate => {
  const leftExpression = ensureComparableExpression(left);
  return createExpression<boolean>({
    compile: (ctx) => sql`${leftExpression.compile(ctx)}${sql.raw(" like ")}${compileValue(right, ctx, "String")}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const notLike = (left: unknown, right: string): Predicate => {
  const leftExpression = ensureComparableExpression(left);
  return createExpression<boolean>({
    compile: (ctx) => sql`${leftExpression.compile(ctx)}${sql.raw(" not like ")}${compileValue(right, ctx, "String")}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const ilike = (left: unknown, right: string): Predicate => {
  const leftExpression = ensureComparableExpression(left);
  return createExpression<boolean>({
    compile: (ctx) => sql`${leftExpression.compile(ctx)}${sql.raw(" ilike ")}${compileValue(right, ctx, "String")}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const notIlike = (left: unknown, right: string): Predicate => {
  const leftExpression = ensureComparableExpression(left);
  return createExpression<boolean>({
    compile: (ctx) => sql`${leftExpression.compile(ctx)}${sql.raw(" not ilike ")}${compileValue(right, ctx, "String")}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
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

const compileArrayFunctionArg = (value: unknown, ctx: BuildContext, leftExpression: SqlExpression<unknown>) => {
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

export const asc = (expression: SqlExpression<unknown>): OrderByExpression => ({
  expression,
  direction: "asc",
});

export const desc = (expression: SqlExpression<unknown>): OrderByExpression => ({
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

export const expr = wrapSql;
