import type { AnyColumn } from "./columns";
import { createClientValidationError, createInternalError } from "./errors";
import { getArrayElementType } from "./internal/clickhouse-type";
import { type CountMode, type CountModeResult, getCountDecoder, getCountSqlType, wrapCountSql } from "./internal/count";
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
  isExpression,
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
import {
  compileSql,
  isSqlFragment,
  type QueryParamTypes,
  quoteIdentifier,
  type SQLFragment,
  sql,
  trustSqlSourceObject,
} from "./sql";

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
type PredicateSqlValue = SQLFragment<unknown> | Selection<unknown>;
type CompileState = {
  forcedSettings?: MutableForcedSettings;
};
const compileStateStackStore = new WeakMap<BuildContext, CompileState[]>();

export const compileWithContextSymbol = Symbol("clickhouseORMCompileWithContext");
export const compileQuerySymbol = Symbol("clickhouseORMCompileQuery");
const selectBuilderResultSymbol = Symbol("clickhouseORMSelectBuilderResult");

type LimitValue = number | bigint | SQLFragment<unknown>;
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

// SelectionItem **is** a SelectionMeta with the live expression attached for
// the compile pass — once compiled, it is structurally compatible with the
// SelectionMeta array embedded in CompiledQuery, so no remapping is needed.
interface SelectionItem extends SelectionMeta {
  readonly expression: SqlSelection<unknown>;
}

interface JoinClause {
  readonly type: "inner" | "left";
  readonly source: QuerySource;
  readonly on: SqlPredicate;
}

export interface CompiledQueryMetadata {
  readonly rootSourceName?: string;
  readonly tableName?: string;
  readonly joinCount?: number;
  readonly tags?: ReadonlyArray<string>;
}

export interface CompiledQuery<_TResult = Record<string, unknown>> {
  readonly kind: "compiled-query";
  readonly mode: QueryMode;
  readonly statement: string;
  readonly params: QueryParams;
  readonly paramTypes?: QueryParamTypes;
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
  paramTypes?: QueryParamTypes,
  forcedSettings?: ForcedSettings,
  metadata?: CompiledQueryMetadata,
): CompiledQuery<TResult> => {
  return {
    kind: "compiled-query",
    mode,
    statement,
    params,
    paramTypes,
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

type InsertColumnEntry =
  | {
      readonly kind: "column";
      readonly key: string;
      readonly name: string;
      readonly column: AnyColumn;
      readonly sqlType: string;
    }
  | {
      readonly kind: "nested-field";
      readonly key: string;
      readonly name: string;
      readonly fieldKey: string;
      readonly fieldColumn: AnyColumn;
      readonly sqlType: string;
    };

const renderInsertColumnIdentifier = (entry: InsertColumnEntry): SQLFragment => {
  if (entry.kind === "nested-field") {
    return sql.raw(`${quoteIdentifier(entry.name)}.${quoteIdentifier(entry.fieldKey)}`);
  }
  return sql.identifier(entry.name);
};

const createInsertColumnEntries = (table: AnyTable): InsertColumnEntry[] => {
  const entries: InsertColumnEntry[] = [];
  for (const [schemaKey, column] of Object.entries(table.columns)) {
    if (column.ddl?.materialized !== undefined || column.ddl?.aliasExpr !== undefined) {
      continue;
    }
    const key = column.key ?? schemaKey;
    const name = column.name ?? schemaKey;
    if (column.nestedShape) {
      for (const [fieldKey, fieldColumn] of Object.entries(column.nestedShape)) {
        entries.push({
          kind: "nested-field",
          key,
          name,
          fieldKey,
          fieldColumn,
          sqlType: `Array(${fieldColumn.sqlType})`,
        });
      }
      continue;
    }
    entries.push({
      kind: "column",
      key,
      name,
      column,
      sqlType: column.sqlType,
    });
  }
  return entries;
};

const compileNestedInsertFieldValue = (
  row: Record<string, unknown>,
  entry: Extract<InsertColumnEntry, { kind: "nested-field" }>,
  ctx: BuildContext,
): SQLFragment => {
  const value = row[entry.key];
  if (value === undefined) {
    return sql.raw("DEFAULT");
  }
  if (!Array.isArray(value)) {
    throw createClientValidationError(`Nested column "${entry.key}" expects an array of objects`);
  }
  const encodedValues = value.map((item, index) => {
    if (!isInsertRowRecord(item)) {
      throw createClientValidationError(`Nested column "${entry.key}" item ${index + 1} must be an object`);
    }
    if (!Object.hasOwn(item, entry.fieldKey) || item[entry.fieldKey] === undefined) {
      throw createClientValidationError(
        `Nested column "${entry.key}" item ${index + 1} is missing required field "${entry.fieldKey}"`,
      );
    }
    return entry.fieldColumn.mapToDriverValue(item[entry.fieldKey] as never);
  });
  return compileValue(encodedValues, ctx, entry.sqlType);
};

const compileInsertColumnValue = (
  row: Record<string, unknown>,
  entry: InsertColumnEntry,
  ctx: BuildContext,
): SQLFragment => {
  if (entry.kind === "nested-field") {
    return compileNestedInsertFieldValue(row, entry, ctx);
  }

  const value = row[entry.key];
  if (value === undefined) {
    return sql.raw("DEFAULT");
  }
  if (value === null) {
    return sql.raw("NULL");
  }
  return compileValue(entry.column.mapToDriverValue(value as never), ctx, entry.sqlType);
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
  const generatedColumns = Object.entries(table.columns)
    .filter(([, column]) => column.ddl?.materialized !== undefined || column.ddl?.aliasExpr !== undefined)
    .map(([schemaKey, column]) => column.key ?? schemaKey);
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
    const explicitGeneratedColumns = generatedColumns.filter((columnName) =>
      Object.hasOwn(row as Record<string, unknown>, columnName),
    );
    if (explicitGeneratedColumns.length > 0) {
      throw createClientValidationError(
        `insert().values() row ${index + 1} cannot provide generated columns: ${explicitGeneratedColumns.join(", ")}`,
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

const renderTableFinalSubquery = (table: AnyTable): SQLFragment => {
  const sourceAlias = table.alias ?? table.originalName;
  const selectionParts = Object.entries(table.columns).map(([schemaKey, column]) => {
    const physicalName = column.name ?? schemaKey;
    return sql`${sql.identifier({
      table: table.originalName,
      column: physicalName,
    })}${sql.raw(" as ")}${sql.identifier(physicalName)}`;
  });

  return sql`${sql.raw("(")}${sql.raw("select ")}${joinSqlParts(selectionParts, ", ")}${sql.raw(" from ")}${sql.identifier(
    {
      table: table.originalName,
    },
  )}${sql.raw(" final) as ")}${sql.identifier(sourceAlias)}`;
};

const renderRootSource = (
  source: QuerySource,
  ctx: BuildContext,
  useFinal: boolean,
  hasJoins: boolean,
): SQLFragment => {
  if (!useFinal) {
    return renderSource(source, ctx);
  }

  if (source.kind !== "table") {
    throw createClientValidationError(
      "final() only supports table sources. Move final() into the table-backed subquery before using it as a source.",
    );
  }

  if (!source.alias && !hasJoins) {
    return sql`${renderTableIdentifier(source)}${sql.raw(" final")}`;
  }

  return renderTableFinalSubquery(source);
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

const getSingleTableName = (source: QuerySource | undefined, joins: readonly JoinClause[] = []): string | undefined => {
  if (!source || joins.length > 0 || source.kind !== "table") {
    return undefined;
  }
  return source.originalName;
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
  const usedSqlAliases = new Set<string>();

  for (const [key, rawValue] of Object.entries(selection)) {
    const expression = ensureExpression(rawValue);
    const sourceKey = getExpressionSourceKey(expression);
    const sqlAlias = expression.outputAlias ?? key;
    if (usedSqlAliases.has(sqlAlias)) {
      throw createClientValidationError(`Duplicate SQL selection alias "${sqlAlias}"`);
    }
    usedSqlAliases.add(sqlAlias);
    selectionItems.push({
      key,
      sqlAlias,
      expression,
      decoder: expression.decoder,
      path: [key],
      nullable: sourceKey ? nullableSources.has(sourceKey) : false,
    });
  }

  return selectionItems;
};

const MAX_SAFE_INTEGER_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);

const assertValidLimitValue = (value: unknown): void => {
  if (isSqlFragment(value)) {
    return;
  }

  const isValidNumber = typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
  const isValidBigInt = typeof value === "bigint" && value >= 0n && value <= MAX_SAFE_INTEGER_BIGINT;
  if (!isValidNumber && !isValidBigInt) {
    throw createClientValidationError(
      `limit()/offset()/limitBy() expects a non-negative safe integer or SQL fragment, got ${String(value)}`,
    );
  }
};

const normalizeLimitValue = (value: LimitValue, ctx: BuildContext) => {
  assertValidLimitValue(value);
  if (isSqlFragment(value)) {
    return compileValue(value, ctx, "Int64");
  }

  return sql.raw(String(value));
};

const renderCountExpression = (mode: CountMode): SQLFragment => {
  return wrapCountSql(sql.raw("count()"), mode);
};

const buildLogicalPredicate = (operator: "and" | "or", predicates: readonly SqlPredicate[]): SqlPredicate => {
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${sql.raw("(")}${sql.join(
        predicates.map((predicate) => predicate.compile(ctx)),
        sql.raw(` ${operator} `),
      )}${sql.raw(")")}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

const normalizePredicateInput = (helperName: string, predicate: PredicateInput): SqlPredicate | undefined => {
  if (predicate === undefined) {
    return undefined;
  }
  if (isExpression(predicate)) {
    return predicate as SqlPredicate;
  }
  if (isSqlFragment(predicate)) {
    return wrapSql<boolean>(predicate, {
      decoder: (value) => Boolean(value),
      sqlType: "Bool",
    }) as SqlPredicate;
  }
  if (typeof predicate === "boolean") {
    throw createClientValidationError(
      `${helperName}() expects a SQL predicate or undefined; use ck.eq(column, ${String(predicate)}) to compare boolean columns`,
    );
  }
  throw createClientValidationError(
    `${helperName}() expects a SQL predicate or undefined; received ${String(predicate)}`,
  );
};

const normalizePredicateGroup = (
  helperName: string,
  operator: "and" | "or",
  predicates: readonly PredicateInput[],
): SqlPredicate | undefined => {
  const filteredPredicates: SqlPredicate[] = [];
  for (const predicate of predicates) {
    const normalized = normalizePredicateInput(helperName, predicate);
    if (normalized) {
      filteredPredicates.push(normalized);
    }
  }

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
  const condition = normalizePredicateGroup("count", "and", config.predicates ?? []);

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
      paramTypes: {},
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
        paramTypes: { ...compiled.paramTypes },
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
          compiledResult.paramTypes,
          forcedSettings,
          config.source.kind === "table"
            ? { rootSourceName: getSourceKey(config.source), tableName: config.source.originalName }
            : undefined,
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
  limitValue?: LimitValue;
  offsetValue?: LimitValue;
  limitByValue?: {
    readonly columns: SqlSelection[];
    readonly limit: LimitValue;
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
  readonly limitValue?: LimitValue;
  readonly offsetValue?: LimitValue;
  readonly limitByValue?: {
    readonly columns: SqlSelection[];
    readonly limit: LimitValue;
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
  limit(value: LimitValue): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  offset(value: LimitValue): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  final(): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  limitBy(
    columns: Selection<unknown>[],
    limit: LimitValue,
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
        const expressionAsSql = expression as SqlSelection<unknown>;
        selectionItems.push({
          key: fieldKey,
          sqlAlias: hasJoins ? `__orm_${nextIndex}` : fieldKey,
          expression: expressionAsSql,
          decoder: expressionAsSql.decoder,
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
        whereClause: normalizePredicateGroup("where", "and", predicates),
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
        havingClause: normalizePredicateGroup("having", "and", [condition]),
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

    limit(value: LimitValue): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      assertValidLimitValue(value);
      return clone({
        limitValue: value,
      });
    },

    offset(value: LimitValue): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      assertValidLimitValue(value);
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
      limit: LimitValue,
    ): SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      assertValidLimitValue(limit);
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
          const fromSource = renderRootSource(state.fromSource, ctx, state.useFinal, state.joins.length > 0);
          queryParts.push(sql`${sql.raw("from ")}${fromSource}`);
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
        // SelectionItem extends SelectionMeta; the embedded `expression` field
        // is harmless extra data on the wire-serialised metadata.
        const selection: readonly SelectionMeta[] = selectionItems;

        const localForcedSettings =
          isNullableJoinEnabled() && state.joins.some((join) => join.type === "left")
            ? { join_use_nulls: 1 }
            : undefined;

        const metadata: CompiledQueryMetadata | undefined = state.fromSource
          ? {
              rootSourceName: getSourceKey(state.fromSource),
              tableName: getSingleTableName(state.fromSource, state.joins),
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
        { ...result.compiled.paramTypes },
        forcedSettings,
        result.metadata,
      );
    },

    [compileQuerySymbol](): CompiledQuery<TResult> {
      return builder[compileWithContextSymbol]({
        params: {},
        paramTypes: {},
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
  if (table.alias) {
    throw createClientValidationError("insert() requires a base table and does not accept aliased table targets");
  }

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
      if (rows.length === 0) {
        throw createClientValidationError("insert().values() must be called with at least one row before execute()");
      }
      const columnEntries = createInsertColumnEntries(table);
      const ctx: BuildContext = {
        params: {},
        paramTypes: {},
        nextParamIndex: 0,
      };
      const valueRows = rows.map(
        (row) =>
          sql`(${joinSqlParts(
            columnEntries.map((entry) => compileInsertColumnValue(row as Record<string, unknown>, entry, ctx)),
            ", ",
          )})`,
      );
      const statement = sql`insert into ${renderTableIdentifier(table)} (${joinSqlParts(
        columnEntries.map((entry) => renderInsertColumnIdentifier(entry)),
        ", ",
      )}) values ${joinSqlParts(valueRows, ", ")}`;
      const compiled = compileSql(statement, ctx);

      return createCompiledQuery(
        compiled.query,
        [],
        "command",
        {
          ...compiled.params,
        },
        {
          ...compiled.paramTypes,
        },
        undefined,
        {
          rootSourceName: table.originalName,
          tableName: table.originalName,
        },
      );
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

export interface QueryClient<TJoinUseNulls extends JoinUseNulls = 1> {
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
  with(...ctes: AnyCte[]): QueryClient<TJoinUseNulls>;
}

export const createQueryClient = <TJoinUseNulls extends JoinUseNulls = 1>(
  config: { ctes?: AnyCte[]; runner?: PreparedRunner; joinUseNulls?: TJoinUseNulls } = {},
): QueryClient<TJoinUseNulls> => {
  const state = {
    ctes: config.ctes ?? [],
    runner: config.runner,
    joinUseNulls: (config.joinUseNulls ?? 1) as TJoinUseNulls,
  };

  const client = {
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

    with(...ctes: AnyCte[]): QueryClient<TJoinUseNulls> {
      return createQueryClient<TJoinUseNulls>({
        ctes: [...state.ctes, ...ctes],
        runner: state.runner,
        joinUseNulls: state.joinUseNulls,
      });
    },
  } as QueryClient<TJoinUseNulls>;

  return client;
};

const ensureComparableExpression = (value: unknown): SqlSelection<unknown> => {
  return ensureExpression(value);
};

const isBareNullish = (value: unknown): value is null | undefined => value === null || value === undefined;

const assertPredicateExpressionInput = (value: unknown, helperName: string): void => {
  if (isBareNullish(value)) {
    throw createClientValidationError(`${helperName}() expects a SQL expression; received ${String(value)}`);
  }
  if (!isExpression(value) && !isSqlFragment(value)) {
    throw createClientValidationError(`${helperName}() expects a SQL expression; received a literal value`);
  }
};

const assertPredicateValue = (value: unknown, helperName: string): void => {
  if (isExpression(value) || isSqlFragment(value)) {
    return;
  }
  if (isBareNullish(value)) {
    throw createClientValidationError(
      `${helperName}() does not accept bare ${String(value)} as a predicate value; ` +
        `use isNull()/isNotNull() for NULL checks or omit the predicate at where()/and()/or() level for dynamic filters`,
    );
  }
};

function assertStringPredicateValue(value: unknown, helperName: string): asserts value is string {
  assertPredicateValue(value, helperName);
  if (typeof value !== "string") {
    throw createClientValidationError(`${helperName}() expects a string predicate value`);
  }
}

const assertStringOrSqlPredicateValue = (value: unknown, helperName: string): void => {
  assertPredicateValue(value, helperName);
  if (isExpression(value) || isSqlFragment(value)) {
    return;
  }
  if (typeof value !== "string") {
    throw createClientValidationError(`${helperName}() expects a string predicate value or SQL expression`);
  }
};

const assertPredicateValueArray = (
  values: readonly unknown[],
  helperName: string,
  options: { allowSqlFragments?: boolean } = {},
): void => {
  for (const [index, value] of values.entries()) {
    if (options.allowSqlFragments && (isExpression(value) || isSqlFragment(value))) {
      continue;
    }
    if (isBareNullish(value)) {
      throw createClientValidationError(
        `${helperName}() does not accept bare ${String(value)} at array index ${index}; ` +
          `use isNull()/isNotNull() or compose an explicit OR predicate for NULL checks`,
      );
    }
  }
};

const isColumnExpression = (value: unknown): value is AnyColumn => {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { kind?: unknown }).kind === "column" &&
    typeof (value as { mapToDriverValue?: unknown }).mapToDriverValue === "function"
  );
};

const encodePredicateValue = (left: unknown, value: unknown): unknown => {
  if (isExpression(value) || isSqlFragment(value)) {
    return value;
  }
  return isColumnExpression(left) ? left.mapToDriverValue(value as never) : value;
};

const compilePredicateValue = (
  left: unknown,
  value: unknown,
  ctx: BuildContext,
  sqlType: string | undefined,
): SQLFragment => {
  return compileValue(encodePredicateValue(left, value), ctx, sqlType);
};

const createNullPredicateExpression = (operator: "is null" | "is not null", left: unknown): Predicate => {
  assertPredicateExpressionInput(left, operator === "is null" ? "isNull" : "isNotNull");
  const leftExpression = ensureComparableExpression(left);
  return createExpression<boolean>({
    compile: (ctx) => sql`${leftExpression.compile(ctx)}${sql.raw(` ${operator}`)}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const isNull = (expression: unknown): Predicate => createNullPredicateExpression("is null", expression);

export const isNotNull = (expression: unknown): Predicate => createNullPredicateExpression("is not null", expression);

const HELPER_NAME_BY_OPERATOR: Readonly<Record<string, string>> = {
  "=": "eq",
  "!=": "ne",
  ">": "gt",
  ">=": "gte",
  "<": "lt",
  "<=": "lte",
};

const createBinaryExpression = (operator: string, left: unknown, right: unknown): Predicate => {
  assertPredicateValue(right, HELPER_NAME_BY_OPERATOR[operator] ?? operator);
  const leftExpression = ensureComparableExpression(left);
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${leftExpression.compile(ctx)}${sql.raw(` ${operator} `)}${compilePredicateValue(
        left,
        right,
        ctx,
        leftExpression.sqlType,
      )}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export function and(): undefined;
export function and(...conditions: [Predicate, ...PredicateInput[]]): Predicate;
export function and(...conditions: PredicateInput[]): Predicate | undefined;
export function and(...conditions: PredicateInput[]): Predicate | undefined {
  return normalizePredicateGroup("and", "and", conditions);
}

export function or(): undefined;
export function or(...conditions: [Predicate, ...PredicateInput[]]): Predicate;
export function or(...conditions: PredicateInput[]): Predicate | undefined;
export function or(...conditions: PredicateInput[]): Predicate | undefined {
  return normalizePredicateGroup("or", "or", conditions);
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

const createLikePredicate = (left: unknown, right: unknown, operator: LikeOperator): Predicate => {
  assertStringOrSqlPredicateValue(right, operator);
  const leftExpression = ensureComparableExpression(left);
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${leftExpression.compile(ctx)}${sql.raw(` ${operator} `)}${compileValue(right, ctx, "String")}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const like = (left: unknown, right: string | PredicateSqlValue): Predicate =>
  createLikePredicate(left, right, "like");

export const notLike = (left: unknown, right: string | PredicateSqlValue): Predicate => {
  return createLikePredicate(left, right, "not like");
};

export const ilike = (left: unknown, right: string | PredicateSqlValue): Predicate =>
  createLikePredicate(left, right, "ilike");

export const notIlike = (left: unknown, right: string | PredicateSqlValue): Predicate => {
  return createLikePredicate(left, right, "not ilike");
};

export const contains = (left: unknown, right: string): Predicate => {
  assertStringPredicateValue(right, "contains");
  return like(left, toLiteralLikePattern(right, "contains"));
};

export const startsWith = (left: unknown, right: string): Predicate => {
  assertStringPredicateValue(right, "startsWith");
  return like(left, toLiteralLikePattern(right, "startsWith"));
};

export const endsWith = (left: unknown, right: string): Predicate => {
  assertStringPredicateValue(right, "endsWith");
  return like(left, toLiteralLikePattern(right, "endsWith"));
};

export const containsIgnoreCase = (left: unknown, right: string): Predicate => {
  assertStringPredicateValue(right, "containsIgnoreCase");
  return ilike(left, toLiteralLikePattern(right, "contains"));
};

export const startsWithIgnoreCase = (left: unknown, right: string): Predicate => {
  assertStringPredicateValue(right, "startsWithIgnoreCase");
  return ilike(left, toLiteralLikePattern(right, "startsWith"));
};

export const endsWithIgnoreCase = (left: unknown, right: string): Predicate => {
  assertStringPredicateValue(right, "endsWithIgnoreCase");
  return ilike(left, toLiteralLikePattern(right, "endsWith"));
};

export const between = (expression: unknown, start: unknown, end: unknown): Predicate => {
  const wrapped = ensureComparableExpression(expression);
  assertPredicateValue(start, "between");
  assertPredicateValue(end, "between");
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${wrapped.compile(ctx)}${sql.raw(" between ")}${compilePredicateValue(
        expression,
        start,
        ctx,
        wrapped.sqlType,
      )}${sql.raw(" and ")}${compilePredicateValue(expression, end, ctx, wrapped.sqlType)}`,
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

const compilePredicateFunction = (name: string, args: SQLFragment[]): SQLFragment => {
  return sql`${sql.raw(name)}(${joinSqlParts(args, ", ")})`;
};

const encodeArrayColumnValues = (left: unknown, value: readonly unknown[]): readonly unknown[] | undefined => {
  if (!isColumnExpression(left) || !getArrayElementType(left.sqlType)) {
    return undefined;
  }
  return left.mapToDriverValue(value as never) as readonly unknown[];
};

const compileHasNeedle = (
  haystack: unknown,
  needle: unknown,
  ctx: BuildContext,
  haystackExpression: SqlSelection<unknown>,
): SQLFragment => {
  assertPredicateValue(needle, "has");
  if (Array.isArray(needle)) {
    assertPredicateValueArray(needle, "has");
  }

  const elementType = getArrayElementType(haystackExpression.sqlType);
  const shouldUseElementEncoder =
    elementType !== undefined &&
    isColumnExpression(haystack) &&
    (!Array.isArray(needle) || getArrayElementType(elementType));
  if (shouldUseElementEncoder) {
    const encoded = encodeArrayColumnValues(haystack, [needle]);
    return compileValue(encoded?.[0] ?? needle, ctx, elementType);
  }

  return compileValue(needle, ctx, Array.isArray(needle) ? haystackExpression.sqlType : elementType);
};

const compileArrayFunctionArg = (
  left: unknown,
  value: unknown,
  ctx: BuildContext,
  leftExpression: SqlSelection<unknown>,
  helperName: string,
) => {
  assertPredicateValue(value, helperName);
  if (Array.isArray(value)) {
    assertPredicateValueArray(value, helperName);
  }
  if (Array.isArray(value) && getArrayElementType(leftExpression.sqlType)) {
    const encoded = encodeArrayColumnValues(left, value);
    return compileValue(encoded ?? value, ctx, leftExpression.sqlType);
  }
  return compileValue(value, ctx);
};

export const has = (haystack: unknown, needle: unknown): Predicate => {
  assertPredicateValue(needle, "has");
  if (Array.isArray(needle)) {
    assertPredicateValueArray(needle, "has");
  }
  const haystackExpression = ensureComparableExpression(haystack);
  return createExpression<boolean>({
    compile: (ctx) =>
      compilePredicateFunction("has", [
        haystackExpression.compile(ctx),
        compileHasNeedle(haystack, needle, ctx, haystackExpression),
      ]),
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const hasAll = (set: unknown, subset: unknown): Predicate => {
  assertPredicateValue(subset, "hasAll");
  if (Array.isArray(subset)) {
    assertPredicateValueArray(subset, "hasAll");
  }
  const setExpression = ensureComparableExpression(set);
  return createExpression<boolean>({
    compile: (ctx) =>
      compilePredicateFunction("hasAll", [
        setExpression.compile(ctx),
        compileArrayFunctionArg(set, subset, ctx, setExpression, "hasAll"),
      ]),
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const hasAny = (arrX: unknown, arrY: unknown): Predicate => {
  assertPredicateValue(arrY, "hasAny");
  if (Array.isArray(arrY)) {
    assertPredicateValueArray(arrY, "hasAny");
  }
  const arrXExpression = ensureComparableExpression(arrX);
  return createExpression<boolean>({
    compile: (ctx) =>
      compilePredicateFunction("hasAny", [
        arrXExpression.compile(ctx),
        compileArrayFunctionArg(arrX, arrY, ctx, arrXExpression, "hasAny"),
      ]),
    decoder: (value) => Boolean(value),
    sqlType: "Bool",
  });
};

export const hasSubstr = (array: unknown, needle: unknown): Predicate => {
  assertPredicateValue(needle, "hasSubstr");
  if (Array.isArray(needle)) {
    assertPredicateValueArray(needle, "hasSubstr");
  }
  const arrayExpression = ensureComparableExpression(array);
  return createExpression<boolean>({
    compile: (ctx) =>
      compilePredicateFunction("hasSubstr", [
        arrayExpression.compile(ctx),
        compileArrayFunctionArg(array, needle, ctx, arrayExpression, "hasSubstr"),
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
  const helperName = negate ? "notInArray" : "inArray";
  assertPredicateValue(right, helperName);
  if (Array.isArray(right)) {
    assertPredicateValueArray(right, helperName, { allowSqlFragments: true });
  }
  const leftExpression = ensureComparableExpression(left);
  const operator = negate ? " not in (" : " in (";
  const emptyArrayLiteral = negate ? "1" : "0";
  return createExpression<boolean>({
    compile: (ctx) => {
      if (Array.isArray(right)) {
        if (right.length === 0) {
          return sql.raw(emptyArrayLiteral);
        }
        const parts = right.map((value) => compilePredicateValue(left, value, ctx, leftExpression.sqlType));
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
  const source: TableFunctionSource = {
    kind: "table-function",
    alias: aliasName,
    compileSource(ctx: BuildContext) {
      const compiledSource = compileSource(ctx);
      if (!aliasName) {
        return compiledSource;
      }
      return sql`${compiledSource}${sql.raw(" as ")}${sql.identifier(aliasName)}`;
    },
    as<TAlias extends string>(nextAlias: TAlias) {
      return createTableFunctionSource(compileSource, nextAlias);
    },
  };
  return trustSqlSourceObject(source);
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
  // Most selections are flat — defer the Map allocation until the first
  // nested-path entry. For a million-row, no-nested-column result this saves
  // a million Map constructions.
  let nestedGroups: Map<string, NestedGroupAccumulator> | undefined;

  for (const item of selection) {
    const rawValue = row[item.sqlAlias];

    if (item.path.length === 1) {
      decodedRow[item.path[0] as keyof TRow] = decodeFlatField(item, rawValue) as TRow[keyof TRow];
      continue;
    }

    nestedGroups ??= new Map();
    applyNestedField(nestedGroups, item, rawValue);
  }

  if (nestedGroups) {
    for (const [groupKey, group] of nestedGroups) {
      decodedRow[groupKey as keyof TRow] = finalizeNestedGroup(group) as TRow[keyof TRow];
    }
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
