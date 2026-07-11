import type { AnyColumn } from "./columns";
import { createClientValidationError, createInternalError } from "./errors";
import { getArrayElementType, unwrapNullableLowCardinalityType } from "./internal/clickhouse-type";
import { isColumnLike } from "./internal/column";
import { type CountMode, type CountModeResult, getCountDecoder, getCountSqlType, wrapCountSql } from "./internal/count";
import { isPlainObject } from "./internal/predicates";
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
import type { AllInsertableKeys, AnyTable, InsertDataOf, RequiredInsertKeys, Table } from "./schema";
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

// SQL keyword/punctuation fragments. These are byte-identical and used by
// every query compile, so building them once at module load saves a
// `SQLFragment` allocation per occurrence per query.
const SQL_OPEN_PAREN = sql.raw("(");
const SQL_CLOSE_PAREN = sql.raw(")");
const SQL_PAREN_AS = sql.raw(") as ");
const SQL_AS = sql.raw(" as ");
const SQL_AS_OPEN = sql.raw(" as (");
const SQL_SELECT = sql.raw("select ");
const SQL_FROM = sql.raw("from ");
const SQL_WHERE = sql.raw("where ");
const SQL_GROUP_BY = sql.raw("group by ");
const SQL_HAVING = sql.raw("having ");
const SQL_ORDER_BY = sql.raw("order by ");
const SQL_LIMIT = sql.raw("limit ");
const SQL_OFFSET = sql.raw("offset ");
const SQL_LIMIT_BY = sql.raw(" by ");
const SQL_ON = sql.raw(" on ");
const SQL_FINAL = sql.raw(" final");
const SQL_FINAL_PAREN_AS = sql.raw(" final) as ");
const SQL_INNER_JOIN = sql.raw("inner join ");
const SQL_LEFT_JOIN = sql.raw("left join ");
const SQL_DEFAULT = sql.raw("DEFAULT");
const SQL_NULL = sql.raw("NULL");
const SQL_COUNT = sql.raw("count()");
const SQL_WITH = sql.raw("with ");
const SQL_EXISTS_OPEN = sql.raw("exists (");
const SQL_NOT_OPEN = sql.raw("not (");
const SQL_ASC = sql.raw(" ASC");
const SQL_DESC = sql.raw(" DESC");
const SQL_LEADING_FROM = sql.raw(" from ");
const SQL_BETWEEN = sql.raw(" between ");
const SQL_AND = sql.raw(" and ");
const SQL_IN_OPEN = sql.raw(" in (");
const SQL_NOT_IN_OPEN = sql.raw(" not in (");
const SQL_FALSE_LITERAL = sql.raw("0");
const SQL_TRUE_LITERAL = sql.raw("1");
const SQL_IS_NULL = sql.raw(" is null");
const SQL_IS_NOT_NULL = sql.raw(" is not null");
const SQL_OR_SEPARATOR = sql.raw(" or ");
// `LIKE` / `ILIKE` operator fragments — keyed by the same `LikeOperator`
// union string the predicate factories take, so the lookup is exhaustive.
const SQL_LIKE = sql.raw(" like ");
const SQL_NOT_LIKE = sql.raw(" not like ");
const SQL_ILIKE = sql.raw(" ilike ");
const SQL_NOT_ILIKE = sql.raw(" not ilike ");

type QuerySource = AnyTable | AnySubquery | AnyCte | TableFunctionSource;
type KnownQuerySource = AnyTable | AnySubquery | AnyCte;
// `QuerySource` plus bare SelectBuilders that user code passed without calling
// `.as(name)`. Used wherever a source is stored or rendered. Code that needs
// the tagged `source.kind` switch must `isSelectBuilder()` first.
type AnySource = QuerySource | AnySelectBuilder;
// Loose SelectBuilder shape for places that accept "any narrow builder". The
// `any` is intentional: SelectBuilder is structurally invariant in TResult
// after PR-B's intersection-based column refs, so narrow
// `SelectBuilder<{a:1}>` no longer auto-conforms to
// `SelectBuilder<Record<string, unknown>>`. This alias keeps the API
// accepting any shape while sidestepping the structural variance check.
// biome-ignore lint/suspicious/noExplicitAny: see the comment above — this is the single source of truth for the deliberate variance escape.
type AnySelectBuilderLike = AnySelectBuilder<any>;
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

// Per-compile state for bare-SelectBuilder subqueries and anonymous CTEs.
// Each top-level compile gets its own numbering, snapshot tests stay
// stable, and entries GC with the context.
//
// The state is attached to the BuildContext as an enumerable own symbol
// property (not a WeakMap<ctx, state>): when `compileSql` spreads the
// caller's ctx into a fresh inner ctx, the symbol property goes along for
// the ride, so deferred lazy refs (e.g. `${cte.col}` embedded in a sql
// template via `wrapSql`) resolve against the same state during the inner
// compile phase that prebuild used.
type BareBuilderCtxState = {
  counter: number;
  aliases: WeakMap<AnySelectBuilder, string>;
  usedAsSource: WeakSet<AnySelectBuilder>;
  cteCounter: number;
  cteAliases: WeakMap<AnyCte, string>;
};
const bareBuilderStateSymbol = Symbol("clickhouseORMBareBuilderState");

export const compileWithContextSymbol = Symbol("clickhouseORMCompileWithContext");
export const compileQuerySymbol = Symbol("clickhouseORMCompileQuery");
const selectBuilderResultSymbol = Symbol("clickhouseORMSelectBuilderResult");
const selectBuilderKindSymbol = Symbol("clickhouseORMSelectBuilderKind");

type LimitValue = number | bigint | SQLFragment<unknown>;
type CountSource = AnyTable | AnySubquery | AnyCte;
// `$inferInsert` now models per-column optionality (DEFAULT columns optional,
// MATERIALIZED/ALIAS columns removed). The outer `Partial<>` would re-allow
// dropping every required column at the TS level, which masks real mistakes
// — drop it now that the inner shape carries the right requiredness.
type InsertRowInput<TTable extends AnyTable> = TTable["$inferInsert"];

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
  readonly source: AnySource;
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

export const isSelectBuilder = (value: unknown): value is AnySelectBuilder =>
  typeof value === "object" &&
  value !== null &&
  selectBuilderKindSymbol in value &&
  value[selectBuilderKindSymbol] === true;

// `Object.defineProperty` shortcut for non-enumerable / non-configurable
// own-data properties. Used for the SelectBuilder brand symbol and the
// auto-attached column refs — both want to stay invisible to
// `Object.keys` / `JSON.stringify` while remaining directly accessible.
const defineHidden = <T extends object>(target: T, key: PropertyKey, value: unknown): void => {
  Object.defineProperty(target, key, {
    value,
    enumerable: false,
    configurable: false,
    writable: false,
  });
};

const getBareBuilderState = (ctx: BuildContext): BareBuilderCtxState => {
  const ctxWithState = ctx as BuildContext & { [bareBuilderStateSymbol]?: BareBuilderCtxState };
  const existing = ctxWithState[bareBuilderStateSymbol];
  if (existing) return existing;
  const state: BareBuilderCtxState = {
    counter: 0,
    aliases: new WeakMap(),
    usedAsSource: new WeakSet(),
    cteCounter: 0,
    cteAliases: new WeakMap(),
  };
  // Assign via direct property write so the field is enumerable: own
  // symbol-keyed enumerable properties get carried across `compileSql`'s
  // `{ ...initialContext }` spread, keeping prebuild and inner-compile
  // resolutions on the same state.
  ctxWithState[bareBuilderStateSymbol] = state;
  return state;
};

const resolveAutoSubqueryAlias = (ctx: BuildContext, builder: AnySelectBuilder): string => {
  const state = getBareBuilderState(ctx);
  const cached = state.aliases.get(builder);
  if (cached) return cached;
  const alias = `__sub_${++state.counter}`;
  state.aliases.set(builder, alias);
  return alias;
};

// CTE object's own fields. Column refs sharing these keys would otherwise
// overwrite the CTE's `name`/`kind`/etc via the auto-attach loop and break
// SQL rendering (e.g. `db.$with("t").as(db.select({ name: ... }))` —
// without this guard, `cte.name` becomes the column expression and
// `sql.identifier(cte.name)` fails). The collision keys keep their CTE
// meaning; the column ref stays available via `cte.columns[key]`.
const FORBIDDEN_CTE_COLUMN_KEYS = {
  kind: true,
  name: true,
  query: true,
  columns: true,
} as const satisfies Record<string, true>;

// Subquery's own fields. Selection keys colliding with these would otherwise
// overwrite `subquery.kind` / `alias` / `query` / `columns` via the auto-attach
// loop and break SQL rendering (e.g. `db.select({ kind: src.x }).from(src).as("s")`
// — without this guard, `subquery.kind` becomes the column expression and
// `renderSource`'s `switch (source.kind)` falls through every case). The
// collision keys keep their subquery meaning; the column ref stays available
// via `subquery.columns[key]`.
const FORBIDDEN_SUBQUERY_COLUMN_KEYS = {
  kind: true,
  alias: true,
  query: true,
  columns: true,
} as const satisfies Record<string, true>;

// Attaches each column ref as a non-enumerable own property of `target`,
// skipping keys that collide with the target's own metadata fields. Shared by
// CTE / Subquery / bare-SelectBuilder auto-attach paths — each supplies its
// own forbidden-key table (see `FORBIDDEN_CTE_COLUMN_KEYS` etc.). Centralised
// here so a future addition (a new source kind, a new metadata field) can't
// accidentally skip the guard the way `.as()` did before this rewrite.
const attachSafeColumnRefs = (target: object, columns: SourceColumns, forbiddenKeys: Record<string, true>): void => {
  for (const key of Object.keys(columns)) {
    if (key in forbiddenKeys) continue;
    defineHidden(target, key, columns[key]);
  }
};

// Anonymous CTEs (no user-supplied name) — alias resolved at compile time
// like bare subqueries. WeakMap-cached so the same CTE renders as the
// same `__cte_N` across `renderCtes` (WITH definition) and `renderSource`
// (FROM/JOIN reference) within one compile. CTEs intentionally lack the
// `usedAsSource` first-use check: SQL allows referencing a CTE multiple
// times.
const resolveAnonymousCteAlias = (ctx: BuildContext, cte: AnyCte): string => {
  const state = getBareBuilderState(ctx);
  const cached = state.cteAliases.get(cte);
  if (cached) return cached;
  const alias = `__cte_${++state.cteCounter}`;
  state.cteAliases.set(cte, alias);
  return alias;
};

// renderSource hot-path helper: one state lookup yields both the resolved
// alias and whether this is the first time the builder appears in the
// source list (FROM + JOINs) of the outer compile. Duplicate appearances
// would emit colliding aliases and produce invalid SQL — caller throws.
const claimSourceBuilder = (ctx: BuildContext, builder: AnySelectBuilder): { alias: string; isFirstUse: boolean } => {
  const state = getBareBuilderState(ctx);
  const isFirstUse = !state.usedAsSource.has(builder);
  if (isFirstUse) state.usedAsSource.add(builder);
  let alias = state.aliases.get(builder);
  if (!alias) {
    alias = `__sub_${++state.counter}`;
    state.aliases.set(builder, alias);
  }
  return { alias, isFirstUse };
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
  // Index access avoids `Array.prototype.at`'s negative-index wrap check on a
  // hot path — every nested forced-setting collection runs through here.
  const stack = compileStateStackStore.get(ctx);
  return stack && stack.length > 0 ? stack[stack.length - 1] : undefined;
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

type InsertColumnEntry =
  | {
      readonly kind: "column";
      readonly key: string;
      readonly name: string;
      readonly column: AnyColumn;
      readonly sqlType: string;
      // Pre-rendered `name` identifier — built once when the table metadata
      // is cached so each insert compile just hands back the cached fragment.
      readonly identifierFragment: SQLFragment;
    }
  | {
      readonly kind: "nested-field";
      readonly key: string;
      readonly name: string;
      readonly fieldKey: string;
      readonly fieldColumn: AnyColumn;
      readonly sqlType: string;
      // Pre-rendered `name.fieldKey` dotted identifier.
      readonly identifierFragment: SQLFragment;
    };

const renderInsertColumnIdentifier = (entry: InsertColumnEntry): SQLFragment => entry.identifierFragment;

type InsertTableMetadata = {
  readonly entries: readonly InsertColumnEntry[];
  readonly knownColumnKeys: ReadonlySet<string>;
  readonly generatedColumnKeys: readonly string[];
  // Logical keys of nested columns that the user marked with
  // `.requiredOnInsert()`. The runtime path in `compileInsertFromSelect`
  // uses this set to surface a missing-required-column error when a
  // bypassed-by-`as never` projection omits one of these keys.
  readonly requiredNestedColumnKeys: ReadonlySet<string>;
};

// Tables are immutable, so the schema-derived insert metadata
// (column entries + known schema keys + generated-column keys) only needs to
// be computed once per table object. WeakMap-keyed cache means dropped tables
// do not leak.
const insertTableMetadataCache = new WeakMap<AnyTable, InsertTableMetadata>();

const getInsertTableMetadata = (table: AnyTable): InsertTableMetadata => {
  const cached = insertTableMetadataCache.get(table);
  if (cached) return cached;

  const entries: InsertColumnEntry[] = [];
  const knownColumnKeys = new Set<string>();
  const generatedColumnKeys: string[] = [];
  const requiredNestedColumnKeys = new Set<string>();

  for (const [schemaKey, column] of Object.entries(table.columns)) {
    knownColumnKeys.add(schemaKey);
    const key = column.key ?? schemaKey;
    if (column.ddl?.materialized !== undefined || column.ddl?.aliasExpr !== undefined) {
      generatedColumnKeys.push(key);
      continue;
    }
    const name = column.name ?? schemaKey;
    if (column.nestedShape) {
      const nestedNameQuoted = quoteIdentifier(name);
      if (column.nestedRequiredOnInsert) {
        requiredNestedColumnKeys.add(key);
      }
      for (const [fieldKey, fieldColumn] of Object.entries(column.nestedShape)) {
        entries.push({
          kind: "nested-field",
          key,
          name,
          fieldKey,
          fieldColumn,
          sqlType: `Array(${fieldColumn.sqlType})`,
          identifierFragment: sql.raw(`${nestedNameQuoted}.${quoteIdentifier(fieldKey)}`),
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
      identifierFragment: sql.identifier(name),
    });
  }

  const metadata: InsertTableMetadata = {
    entries,
    knownColumnKeys,
    generatedColumnKeys,
    requiredNestedColumnKeys,
  };
  insertTableMetadataCache.set(table, metadata);
  return metadata;
};

const createInsertColumnEntries = (table: AnyTable): readonly InsertColumnEntry[] =>
  getInsertTableMetadata(table).entries;

const compileNestedInsertFieldValue = (
  row: Record<string, unknown>,
  entry: Extract<InsertColumnEntry, { kind: "nested-field" }>,
  ctx: BuildContext,
): SQLFragment => {
  const value = row[entry.key];
  if (value === undefined) {
    return SQL_DEFAULT;
  }
  if (!Array.isArray(value)) {
    throw createClientValidationError(`Nested column "${entry.key}" expects an array of objects`);
  }
  const encodedValues = value.map((item, index) => {
    if (!isPlainObject(item)) {
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

// ClickHouse parameterized VALUES inserts encode plain-object parameters as a
// `Map(String,...)` literal (`{'k':'v',...}` with single quotes). That format
// is not parseable as a `JSON` column value — CH rejects it with INCORRECT_DATA.
// For NewJSON columns we route the encoded object through `JSON.stringify`
// and downgrade the parameter type to `String`; ClickHouse implicitly casts
// the resulting double-quoted JSON literal back to `JSON(...)` on the server
// side. The read path is unaffected.
//
// Note: this stringify-then-CAST workaround applies only to `.values()` (the
// parameterised VALUES path). `.fromSelect()` performs no `mapToDriverValue`
// — JSON columns travel server-side as native JSON between SELECT output and
// INSERT input, so there is no analogous wire-format step. Both paths reach
// the same logical result, but if you ever need to introspect what ck-orm
// sent over the wire, expect the two paths to look different.
const isJsonInsertSqlType = (sqlType: string): boolean => sqlType === "JSON" || sqlType.startsWith("JSON(");

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
    return SQL_DEFAULT;
  }
  if (value === null) {
    return SQL_NULL;
  }
  const encoded = entry.column.mapToDriverValue(value as never);
  // Unwrap Nullable / LowCardinality before checking for JSON — keeps this
  // defensive path future-proof if ClickHouse ever permits `LowCardinality(JSON)`
  // or a similar wrapper. The wire-format step (stringify + cast) is identical
  // regardless of the outer wrapper.
  if (
    isJsonInsertSqlType(unwrapNullableLowCardinalityType(entry.sqlType)) &&
    encoded !== null &&
    encoded !== undefined
  ) {
    // Stringify the encoded plain object so the server-side String→JSON
    // implicit cast can handle it. `mapToDriverValue` already validated the
    // shape, so the stringify call is purely a wire-format step.
    return compileValue(JSON.stringify(encoded), ctx, "String");
  }
  return compileValue(encoded, ctx, entry.sqlType);
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

  // Both validations consult the same per-table metadata that
  // `createInsertColumnEntries` already builds — share the cached copy
  // instead of rebuilding two parallel sets/arrays per `.values()` call.
  const { knownColumnKeys, generatedColumnKeys } = getInsertTableMetadata(table);
  for (const [index, row] of rows.entries()) {
    if (!isPlainObject(row)) {
      throw createClientValidationError(`insert().values() row ${index + 1} must be an object`);
    }

    const unknownColumns = Object.keys(row).filter((columnName) => !knownColumnKeys.has(columnName));
    if (unknownColumns.length > 0) {
      throw createClientValidationError(
        `insert().values() row ${index + 1} contains unknown columns: ${unknownColumns.join(", ")}`,
      );
    }
    const explicitGeneratedColumns = generatedColumnKeys.filter((columnName) =>
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
  nestedShape?: Record<string, AnyColumn>,
): SqlSelection<TData, TSourceKey> => {
  // Subquery / CTE references are immutable — pre-build the
  // `sourceAlias.columnName` fragment once instead of paying for
  // `sql.identifier({ ... })` on every compile of the same reference.
  const identifierFragment = sql.identifier({ table: sourceAlias, column: columnName });
  const expression = createExpression({
    compile: () => identifierFragment,
    decoder,
    sqlType,
    sourceKey: sourceAlias,
  });
  // Propagate `nestedShape` metadata when the underlying selection item is a
  // direct nested column reference. `compileInsertFromSelect` reads this to
  // recognise CTE/subquery-wrapped nested column refs (otherwise it can only
  // see `kind: "expression"` and would reject them as computed expressions).
  if (nestedShape) {
    defineHidden(expression, "nestedShape", nestedShape);
  }
  return expression;
};

// Column refs whose SQL alias isn't known until compile time (bare
// SelectBuilder subqueries, anonymous CTEs). The resolver closure looks up
// the per-BuildContext alias so `subq.x` / `cte.x` and the source's WITH
// definition / FROM rendering all share the same `__sub_N` / `__cte_N`.
// sourceKey is deliberately omitted — without a stable string key, callers
// that rely on it (nullable-join tracking) skip these refs.
const createLazyAliasReferenceExpression = <TData>(
  resolveAlias: (ctx: BuildContext) => string,
  columnName: string,
  decoder: Decoder<TData>,
  sqlType?: string,
): SqlSelection<TData, string> =>
  createExpression<TData, string>({
    compile: (ctx) => sql.identifier({ table: resolveAlias(ctx), column: columnName }),
    decoder,
    sqlType,
  });

const buildReferenceColumns = <TRow extends SelectionRecord, TSourceKey extends string>(
  sourceAlias: TSourceKey,
  selectionItems: readonly SelectionItem[],
): ReferenceColumns<TRow, TSourceKey> => {
  const columns = {} as ReferenceColumns<TRow, TSourceKey>;

  for (const item of selectionItems) {
    // If the source selection item is a direct nested column reference (e.g.
    // `select({ events: src.events })`), propagate the column's nestedShape
    // onto the produced reference so downstream consumers (CTE/subquery →
    // INSERT fromSelect) can identify nested-column refs across one or more
    // layers of subquery wrapping.
    const sourceExpression = item.expression as SqlSelection<unknown> & {
      readonly kind?: string;
      readonly nestedShape?: Record<string, AnyColumn>;
    };
    const propagatedNestedShape =
      sourceExpression.kind === "column" && sourceExpression.nestedShape ? sourceExpression.nestedShape : undefined;
    columns[item.key as keyof TRow] = createReferenceExpression(
      sourceAlias,
      item.sqlAlias,
      item.expression.decoder,
      item.expression.sqlType,
      propagatedNestedShape,
    ) as ReferenceColumns<TRow, TSourceKey>[keyof TRow];
  }

  return columns;
};

const renderSource = (source: AnySource, ctx: BuildContext): SQLFragment => {
  if (isSelectBuilder(source)) {
    const { alias, isFirstUse } = claimSourceBuilder(ctx, source);
    if (!isFirstUse) {
      throw createClientValidationError(
        `SelectBuilder instance used twice in the same query's source list (auto-alias "${alias}"). Each occurrence needs a distinct alias. Two fixes:
  1. Call .as("name1") and .as("name2") to give each usage a distinct alias.
  2. Build the subquery twice — each .select(...) call yields its own instance.`,
      );
    }
    return sql`${SQL_OPEN_PAREN}${sql.raw(compileNestedQuery(source, ctx).statement)}${SQL_PAREN_AS}${sql.identifier(alias)}`;
  }
  switch (source.kind) {
    case "table":
      return renderTableIdentifier(source);
    case "subquery":
      return sql`${SQL_OPEN_PAREN}${sql.raw(compileNestedQuery(source.query, ctx).statement)}${SQL_PAREN_AS}${sql.identifier(source.alias)}`;
    case "cte":
      return sql.identifier(source.name ?? resolveAnonymousCteAlias(ctx, source));
    case "table-function":
      return source.compileSource(ctx);
  }
};

// `final()` against a table that's also reused as a join source produces
// the same `(select … from t final) as alias` fragment every time. Cache it
// so wide tables don't re-walk their column shape per query.
const tableFinalSubqueryCache = new WeakMap<AnyTable, SQLFragment>();

const renderTableFinalSubquery = (table: AnyTable): SQLFragment => {
  const cached = tableFinalSubqueryCache.get(table);
  if (cached) return cached;

  const sourceAlias = table.alias ?? table.originalName;
  const selectionParts = Object.entries(table.columns).map(([schemaKey, column]) => {
    const physicalName = column.name ?? schemaKey;
    return sql`${sql.identifier({
      table: table.originalName,
      column: physicalName,
    })}${SQL_AS}${sql.identifier(physicalName)}`;
  });

  const fragment = sql`${SQL_OPEN_PAREN}${SQL_SELECT}${joinSqlParts(selectionParts, ", ")}${SQL_LEADING_FROM}${sql.identifier(
    { table: table.originalName },
  )}${SQL_FINAL_PAREN_AS}${sql.identifier(sourceAlias)}`;
  tableFinalSubqueryCache.set(table, fragment);
  return fragment;
};

const renderRootSource = (source: AnySource, ctx: BuildContext, useFinal: boolean, hasJoins: boolean): SQLFragment => {
  if (!useFinal) {
    return renderSource(source, ctx);
  }

  if (isSelectBuilder(source) || source.kind !== "table") {
    throw createClientValidationError(
      "final() only supports table sources. Move final() into the table-backed subquery before using it as a source.",
    );
  }

  if (!source.alias && !hasJoins) {
    return sql`${renderTableIdentifier(source)}${SQL_FINAL}`;
  }

  return renderTableFinalSubquery(source);
};

const getSourceColumns = (source: AnySource): SourceColumns | undefined => {
  if (isSelectBuilder(source)) {
    // PR-B will expose column refs on bare builders. For now, bare builders
    // expose no joinable columns from outside.
    return undefined;
  }
  switch (source.kind) {
    case "table":
    case "subquery":
    case "cte":
      return source.columns;
    case "table-function":
      return undefined;
  }
};

const getSourceKey = (source: AnySource): string | undefined => {
  if (isSelectBuilder(source)) {
    // Auto-aliases resolve at compile time inside a BuildContext. Without
    // ctx access here we have no stable key — callers that need a stable
    // string source key (e.g. nullable-join tracking) skip bare builders.
    return undefined;
  }
  switch (source.kind) {
    case "table":
      return source.alias ?? source.originalName;
    case "subquery":
      return source.alias;
    case "cte":
      // Anonymous CTEs resolve their alias at compile time inside a
      // BuildContext (same constraint as bare builders above). Callers
      // that need a stable string source key skip anonymous CTEs.
      return source.name;
    case "table-function":
      return source.alias;
  }
};

const getSingleTableName = (source: AnySource | undefined, joins: readonly JoinClause[] = []): string | undefined => {
  if (!source || joins.length > 0 || isSelectBuilder(source) || source.kind !== "table") {
    return undefined;
  }
  return source.originalName;
};

const renderSelection = (selectionItems: readonly SelectionItem[], ctx: BuildContext) => {
  const selectionParts = selectionItems.map((item) => {
    return sql`${item.expression.compile(ctx)}${SQL_AS}${sql.identifier(item.sqlAlias)}`;
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
  return wrapCountSql(SQL_COUNT, mode);
};

// Cast-to-boolean decoder shared by every predicate-building helper. Hoisting
// it to module scope means each `eq/ne/and/or/...` call doesn't allocate its
// own closure on a hot expression-construction path.
const booleanCastDecoder: Decoder<boolean> = (value) => Boolean(value);

const buildLogicalPredicate = (operator: "and" | "or", predicates: readonly SqlPredicate[]): SqlPredicate => {
  // `SQL_AND` is the same ` and ` literal that BETWEEN uses, so we reuse it
  // as the AND-separator instead of allocating a parallel constant.
  const separatorFragment = operator === "and" ? SQL_AND : SQL_OR_SEPARATOR;
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${SQL_OPEN_PAREN}${sql.join(
        predicates.map((predicate) => predicate.compile(ctx)),
        separatorFragment,
      )}${SQL_CLOSE_PAREN}`,
    decoder: booleanCastDecoder,
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
      decoder: booleanCastDecoder,
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
    const effectiveName = cte.name ?? resolveAnonymousCteAlias(ctx, cte);
    return sql`${sql.identifier(effectiveName)}${SQL_AS_OPEN}${sql.raw(compileNestedQuery(cte.query, ctx).statement)}${SQL_CLOSE_PAREN}`;
  });

  return sql`${SQL_WITH}${joinSqlParts(cteParts, ", ")}`;
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
      ? sql`${SQL_SELECT}${renderCountExpression(config.mode)}${SQL_AS}${sql.identifier(config.outputAlias)}`
      : sql`${SQL_SELECT}${renderCountExpression(config.mode)}`,
  );
  queryParts.push(sql`${SQL_FROM}${renderSource(config.source, ctx)}`);

  if (config.condition) {
    queryParts.push(sql`${SQL_WHERE}${config.condition.compile(ctx)}`);
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
      sql`${SQL_OPEN_PAREN}${buildCountStatement(ctx, {
        ctes: config.ctes,
        source: config.source,
        condition,
        mode,
      })}${SQL_CLOSE_PAREN}`,
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
  fromSource?: AnySource;
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
  readonly fromSource?: AnySource;
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

// Frozen sentinels reused across every builder that doesn't supply a value
// for the corresponding state field. The chain methods (`innerJoin`,
// `leftJoin`, `groupBy`, `orderBy`, ...) all rebuild these arrays with
// `[...state.x, newItem]`, so the underlying sentinels are never mutated.
// Sharing one frozen array per slot avoids allocating four empty arrays per
// chain step (which on a 5-method chain is 20 dead allocations).
const EMPTY_CTES: AnyCte[] = Object.freeze([]) as unknown as AnyCte[];
const EMPTY_JOINS: JoinClause[] = Object.freeze([]) as unknown as JoinClause[];
const EMPTY_GROUP_BY: SqlSelection[] = Object.freeze([]) as unknown as SqlSelection[];
const EMPTY_ORDER_BY: SqlOrder[] = Object.freeze([]) as unknown as SqlOrder[];

const normalizeSelectBuilderState = <
  TResult extends Record<string, unknown>,
  TSelection extends SelectionRecord | undefined,
  TJoinUseNulls extends JoinUseNulls,
>(
  config?: SelectBuilderConfig<TResult> & { selection?: TSelection },
): SelectBuilderState<TResult, TSelection, KnownQuerySource | undefined, JoinedSources, TJoinUseNulls> => {
  return {
    ctes: config?.ctes ?? EMPTY_CTES,
    runner: config?.runner,
    selection: config?.selection,
    fromSource: config?.fromSource,
    joins: config?.joins ?? EMPTY_JOINS,
    whereClause: config?.whereClause,
    groupByItems: config?.groupByItems ?? EMPTY_GROUP_BY,
    havingClause: config?.havingClause,
    orderByItems: config?.orderByItems ?? EMPTY_ORDER_BY,
    limitValue: config?.limitValue,
    offsetValue: config?.offsetValue,
    limitByValue: config?.limitByValue,
    useFinal: config?.useFinal ?? false,
    joinUseNulls: (config?.joinUseNulls ?? 1) as TJoinUseNulls,
  };
};

// Selection keys that conflict with SelectBuilder's own methods/PromiseLike API.
// Column refs for these keys are NOT spread onto the builder — accessing
// `sub.from` / `sub.where` etc. keeps returning the builder method (the
// `Omit<ReferenceColumns<...>, ForbiddenAutoColumnKeys>` mask in
// `SelectBuilderColumnRefs` hides the conflicting ref at the type layer; the
// runtime attach loop in `createSelectBuilder` skips the key likewise).
//
// Single source of truth: the object literal drives both the type union
// (via `keyof typeof`) and the runtime check (via `in`). Adding/removing a
// reserved key here is the only edit needed.
const FORBIDDEN_AUTO_COLUMN_KEYS = {
  execute: true,
  iterator: true,
  // biome-ignore lint/suspicious/noThenProperty: this is a reserved-keys lookup table, not a thenable — `then` must be listed because SelectBuilder is intentionally a Promise-like.
  then: true,
  catch: true,
  finally: true,
  buildSelectionItems: true,
  from: true,
  innerJoin: true,
  leftJoin: true,
  where: true,
  groupBy: true,
  having: true,
  orderBy: true,
  limit: true,
  offset: true,
  final: true,
  limitBy: true,
  as: true,
} as const satisfies Record<string, true>;

type ForbiddenAutoColumnKeys = keyof typeof FORBIDDEN_AUTO_COLUMN_KEYS;

// Type-level masked column refs for SelectBuilder. When the user supplied a
// `select({...})` projection, we expose its keys as references — but mask out
// any names that collide with builder methods (those keep their original
// method type so chaining still works).
//
// The `string extends keyof TResult` guard rejects the wide
// `Record<string, unknown>` (no specific keys known): generating a string
// index signature there would clash with method signatures like
// `as(alias): Subquery<...>` (whose `kind: "subquery"` doesn't match the
// generic `Selection<unknown, string>`).
//
// Note: the outer `TResult extends Record<string, unknown>` conditional looks
// redundant given the SelectBuilderWithRefs constraint, but experiments show
// inlining the constraint as a parameter bound breaks deep TS inference at
// CteFromQuery / Subquery boundaries (cascading variance failures on `.as()`
// return types). Keep the three-layer conditional — TS evaluates it lazily
// and that laziness is load-bearing.
type SelectBuilderColumnRefs<TResult, TSelection> = TSelection extends SelectionRecord
  ? TResult extends Record<string, unknown>
    ? string extends keyof TResult
      ? unknown
      : Omit<ReferenceColumns<TResult, string>, ForbiddenAutoColumnKeys>
    : unknown
  : unknown;

// Notes on "selection key conflicts with builder method": when a user writes
// `db.select({ from: ... })`, `Omit<..., ForbiddenAutoColumnKeys>` masks
// the auto column ref so `sub.from` keeps its method type. The runtime also
// skips attaching that key. We attempted a `ValidateSelectionKeys` mapped
// type at the call site but inference for mapped-type parameters reliably
// dodges the check — kept the Omit-on-output approach instead.

export interface SelectBuilder<
  TResult extends Record<string, unknown> = Record<string, unknown>,
  TSelection extends SelectionRecord | undefined = SelectionRecord | undefined,
  TRootSource extends KnownQuerySource | undefined = KnownQuerySource | undefined,
  TJoinedSources extends JoinedSources = NoJoinedSources,
  TJoinUseNulls extends JoinUseNulls = 1,
> extends PromiseLike<TResult[]> {
  readonly [selectBuilderResultSymbol]?: TResult;
  readonly [selectBuilderKindSymbol]: true;
  execute(options?: ClickHouseBaseQueryOptions): Promise<TResult[]>;
  iterator(options?: ClickHouseBaseQueryOptions): AsyncGenerator<TResult, void, unknown>;
  catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<TResult[] | TResult2>;
  finally(onfinally?: (() => void) | null): Promise<TResult[]>;
  buildSelectionItems(): SelectionItem[];
  from<TSource extends QuerySource | AnySelectBuilderLike>(
    source: TSource,
  ): SelectBuilderWithRefs<
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
  innerJoin<TSource extends KnownQuerySource | AnySelectBuilderLike>(
    source: TSource,
    on: Predicate,
  ): SelectBuilderWithRefs<
    TSource extends KnownQuerySource
      ? InferJoinResult<TSelection, TResult, TRootSource, AddJoinedSource<TJoinedSources, TSource, false>>
      : TResult,
    TSelection,
    TRootSource,
    TSource extends KnownQuerySource ? AddJoinedSource<TJoinedSources, TSource, false> : TJoinedSources,
    TJoinUseNulls
  >;
  innerJoin<TSource extends KnownQuerySource | AnySelectBuilderLike>(
    source: TSource,
    on: (joined: TSource) => Predicate,
  ): SelectBuilderWithRefs<
    TSource extends KnownQuerySource
      ? InferJoinResult<TSelection, TResult, TRootSource, AddJoinedSource<TJoinedSources, TSource, false>>
      : TResult,
    TSelection,
    TRootSource,
    TSource extends KnownQuerySource ? AddJoinedSource<TJoinedSources, TSource, false> : TJoinedSources,
    TJoinUseNulls
  >;
  leftJoin<TSource extends KnownQuerySource | AnySelectBuilderLike>(
    source: TSource,
    on: Predicate,
  ): SelectBuilderWithRefs<
    TSource extends KnownQuerySource
      ? InferJoinResult<
          TSelection,
          TResult,
          TRootSource,
          AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
        >
      : TResult,
    TSelection,
    TRootSource,
    TSource extends KnownQuerySource
      ? AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
      : TJoinedSources,
    TJoinUseNulls
  >;
  leftJoin<TSource extends KnownQuerySource | AnySelectBuilderLike>(
    source: TSource,
    on: (joined: TSource) => Predicate,
  ): SelectBuilderWithRefs<
    TSource extends KnownQuerySource
      ? InferJoinResult<
          TSelection,
          TResult,
          TRootSource,
          AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
        >
      : TResult,
    TSelection,
    TRootSource,
    TSource extends KnownQuerySource
      ? AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
      : TJoinedSources,
    TJoinUseNulls
  >;
  where(
    ...predicates: PredicateInput[]
  ): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  groupBy(
    ...expressions: Selection<unknown>[]
  ): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  having(condition?: Predicate): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  orderBy(
    ...expressions: Array<Order | Selection<unknown>>
  ): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  limit(value: LimitValue): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  offset(value: LimitValue): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  final(): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  limitBy(
    columns: Selection<unknown>[],
    limit: LimitValue,
  ): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;
  [compileWithContextSymbol](ctx: BuildContext): CompiledQuery<TResult>;
  [compileQuerySymbol](): CompiledQuery<TResult>;
  as<TAlias extends string>(alias: TAlias): Subquery<TResult, TAlias>;
}

// SelectBuilder enriched with auto-attached column references. Methods of the
// builder always return this enriched type so chained outputs like
// `db.select({...}).from(t).where(p).x` work both at the TS and runtime
// levels. Non-`select(...)` paths get `unknown` from `SelectBuilderColumnRefs`,
// which intersects away cleanly.
export type SelectBuilderWithRefs<
  TResult extends Record<string, unknown> = Record<string, unknown>,
  TSelection extends SelectionRecord | undefined = SelectionRecord | undefined,
  TRootSource extends KnownQuerySource | undefined = KnownQuerySource | undefined,
  TJoinedSources extends JoinedSources = NoJoinedSources,
  TJoinUseNulls extends JoinUseNulls = 1,
> = SelectBuilder<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> &
  SelectBuilderColumnRefs<TResult, TSelection>;

export const createSelectBuilder = <
  TResult extends Record<string, unknown> = Record<string, unknown>,
  TSelection extends SelectionRecord | undefined = SelectionRecord | undefined,
  TRootSource extends KnownQuerySource | undefined = KnownQuerySource | undefined,
  TJoinedSources extends JoinedSources = NoJoinedSources,
  TJoinUseNulls extends JoinUseNulls = 1,
>(
  config?: SelectBuilderConfig<TResult> & { selection?: TSelection },
): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> => {
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
  ): SelectBuilderWithRefs<TNextResult, TSelection, TNextRoot, TNextJoined, TJoinUseNulls> => {
    return createSelectBuilder<TNextResult, TSelection, TNextRoot, TNextJoined, TJoinUseNulls>({
      ...(state as SelectBuilderConfig<TNextResult> & { selection?: TSelection }),
      ...overrides,
    });
  };

  const isNullableJoinEnabled = (): boolean => {
    return state.joinUseNulls === 1;
  };

  // Builder state is immutable for the instance's lifetime (every chain method
  // returns a fresh builder via `clone`). That means both the nullable-source
  // map and the resolved selection items can be computed exactly once per
  // instance — both are called multiple times in a single use (auto-attach,
  // `.as()`, `compileWithContextSymbol`, `db.$with().as()`,
  // `insert.fromSelect()` etc.). Cache them in closure-private slots.
  let memoNullableSources: ReadonlySet<string> | undefined;
  let memoSelectionItems: SelectionItem[] | undefined;

  const computeNullableSources = (): ReadonlySet<string> => {
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

  const getNullableSources = (): ReadonlySet<string> => {
    if (memoNullableSources) return memoNullableSources;
    memoNullableSources = computeNullableSources();
    return memoNullableSources;
  };

  const computeSelectionItems = (): SelectionItem[] => {
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

  const buildSelectionItems = (): SelectionItem[] => {
    if (memoSelectionItems) return memoSelectionItems;
    memoSelectionItems = computeSelectionItems();
    return memoSelectionItems;
  };

  const builder = {
    // Runtime brand for `isSelectBuilder`. Symbol keys are invisible to
    // `Object.keys` / `JSON.stringify` / `for..in` by ES spec, so no
    // explicit `defineProperty` dance is needed to "hide" it.
    [selectBuilderKindSymbol]: true as const,
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

    from<TSource extends QuerySource | AnySelectBuilderLike>(
      source: TSource,
    ): SelectBuilderWithRefs<
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

    innerJoin<TSource extends KnownQuerySource | AnySelectBuilderLike>(
      source: TSource,
      on: Predicate | ((joined: TSource) => Predicate),
    ): SelectBuilderWithRefs<
      TSource extends KnownQuerySource
        ? InferJoinResult<TSelection, TResult, TRootSource, AddJoinedSource<TJoinedSources, TSource, false>>
        : TResult,
      TSelection,
      TRootSource,
      TSource extends KnownQuerySource ? AddJoinedSource<TJoinedSources, TSource, false> : TJoinedSources,
      TJoinUseNulls
    > {
      const predicate = typeof on === "function" ? on(source) : on;
      return clone<
        TSource extends KnownQuerySource
          ? InferJoinResult<TSelection, TResult, TRootSource, AddJoinedSource<TJoinedSources, TSource, false>>
          : TResult,
        TRootSource,
        TSource extends KnownQuerySource ? AddJoinedSource<TJoinedSources, TSource, false> : TJoinedSources
      >({
        joins: [...state.joins, { type: "inner", source, on: predicate as SqlPredicate }],
      });
    },

    leftJoin<TSource extends KnownQuerySource | AnySelectBuilderLike>(
      source: TSource,
      on: Predicate | ((joined: TSource) => Predicate),
    ): SelectBuilderWithRefs<
      TSource extends KnownQuerySource
        ? InferJoinResult<
            TSelection,
            TResult,
            TRootSource,
            AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
          >
        : TResult,
      TSelection,
      TRootSource,
      TSource extends KnownQuerySource
        ? AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
        : TJoinedSources,
      TJoinUseNulls
    > {
      const predicate = typeof on === "function" ? on(source) : on;
      return clone<
        TSource extends KnownQuerySource
          ? InferJoinResult<
              TSelection,
              TResult,
              TRootSource,
              AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
            >
          : TResult,
        TRootSource,
        TSource extends KnownQuerySource
          ? AddJoinedSource<TJoinedSources, TSource, TJoinUseNulls extends 1 ? true : false>
          : TJoinedSources
      >({
        joins: [...state.joins, { type: "left", source, on: predicate as SqlPredicate }],
      });
    },

    where(
      ...predicates: PredicateInput[]
    ): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      return clone({
        whereClause: normalizePredicateGroup("where", "and", predicates),
      });
    },

    groupBy(
      ...expressions: Selection<unknown>[]
    ): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      return clone({
        groupByItems: [...state.groupByItems, ...expressions.map((expression) => ensureExpression(expression))],
      });
    },

    having(
      condition?: Predicate,
    ): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      return clone({
        havingClause: normalizePredicateGroup("having", "and", [condition]),
      });
    },

    orderBy(
      ...expressions: Array<Order | Selection<unknown>>
    ): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
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

    limit(value: LimitValue): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      assertValidLimitValue(value);
      return clone({
        limitValue: value,
      });
    },

    offset(value: LimitValue): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      assertValidLimitValue(value);
      return clone({
        offsetValue: value,
      });
    },

    final(): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
      return clone({
        useFinal: true,
      });
    },

    limitBy(
      columns: Selection<unknown>[],
      limit: LimitValue,
    ): SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls> {
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

        queryParts.push(sql`${SQL_SELECT}${renderSelection(selectionItems, ctx)}`);

        if (state.fromSource) {
          const fromSource = renderRootSource(state.fromSource, ctx, state.useFinal, state.joins.length > 0);
          queryParts.push(sql`${SQL_FROM}${fromSource}`);
        }

        if (state.joins.length > 0) {
          for (const join of state.joins) {
            const joinKeyword = join.type === "inner" ? SQL_INNER_JOIN : SQL_LEFT_JOIN;
            queryParts.push(sql`${joinKeyword}${renderSource(join.source, ctx)}${SQL_ON}${join.on.compile(ctx)}`);
          }
        }

        if (state.whereClause) {
          queryParts.push(sql`${SQL_WHERE}${state.whereClause.compile(ctx)}`);
        }

        if (state.groupByItems.length > 0) {
          queryParts.push(
            sql`${SQL_GROUP_BY}${joinSqlParts(
              state.groupByItems.map((item) => item.compile(ctx)),
              ", ",
            )}`,
          );
        }

        if (state.havingClause) {
          queryParts.push(sql`${SQL_HAVING}${state.havingClause.compile(ctx)}`);
        }

        if (state.orderByItems.length > 0) {
          const orderByParts = state.orderByItems.map((item) => {
            const directionFragment = item.direction === "asc" ? SQL_ASC : SQL_DESC;
            return sql`${item.expression.compile(ctx)}${directionFragment}`;
          });
          queryParts.push(sql`${SQL_ORDER_BY}${joinSqlParts(orderByParts, ", ")}`);
        }

        if (state.limitByValue) {
          queryParts.push(
            sql`${SQL_LIMIT}${normalizeLimitValue(state.limitByValue.limit, ctx)}${SQL_LIMIT_BY}${joinSqlParts(
              state.limitByValue.columns.map((column) => column.compile(ctx)),
              ", ",
            )}`,
          );
        }

        if (state.limitValue !== undefined) {
          queryParts.push(sql`${SQL_LIMIT}${normalizeLimitValue(state.limitValue, ctx)}`);
        }

        if (state.offsetValue !== undefined) {
          queryParts.push(sql`${SQL_OFFSET}${normalizeLimitValue(state.offsetValue, ctx)}`);
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
      const subquery: AnySubquery = {
        kind: "subquery",
        alias,
        query: builder as AnySelectBuilder<Record<string, unknown>>,
        columns,
      };
      // Hide column refs as non-enumerable own properties, skipping any keys
      // that collide with the subquery's own metadata fields (see
      // FORBIDDEN_SUBQUERY_COLUMN_KEYS). Previously this path used
      // `Object.assign(subquery, columns)`, which would happily overwrite
      // `subquery.kind` / `alias` etc. when the user picked a colliding name.
      attachSafeColumnRefs(subquery, columns, FORBIDDEN_SUBQUERY_COLUMN_KEYS);
      return subquery as unknown as Subquery<TResult, TAlias>;
    },
  } as SelectBuilderWithRefs<TResult, TSelection, TRootSource, TJoinedSources, TJoinUseNulls>;

  // Auto-attach column refs when the user supplied an explicit selection.
  // Each ref's `compile(ctx)` lazy-resolves the bare-builder's per-compile
  // auto alias (so `subq.x` and the SQL rendering of the source both use
  // the same `__sub_N`). `buildSelectionItems()` is safe here:
  // `state.selection != null` guarantees the early-return branch, so it
  // cannot hit the "missing source/columns" throw path.
  if (state.selection != null) {
    const autoRefs: SourceColumns = {};
    for (const item of buildSelectionItems()) {
      autoRefs[item.key] = createLazyAliasReferenceExpression(
        (ctx) => resolveAutoSubqueryAlias(ctx, builder),
        item.sqlAlias,
        item.expression.decoder,
        item.expression.sqlType,
      );
    }
    attachSafeColumnRefs(builder, autoRefs, FORBIDDEN_AUTO_COLUMN_KEYS);
  }

  return builder;
};

// Compile-time guard for `InsertBuilder.fromSelect(query)`. The returned type
// must `extend` the passed `TQuery`, so we intersect the user-supplied builder
// with a per-column compatibility check. When everything lines up, every
// branch resolves to `unknown` and the intersection is a no-op; on mismatch
// the offending key gets a phantom `{__error,column,...}` object that turns
// the whole intersection unassignable, which renders as a precise TS error.
/** @internal */
export type FromSelectShapeConstraint<TTable extends AnyTable, TResultShape> = [
  Exclude<RequiredInsertKeys<TTable["columns"]>, keyof TResultShape>,
] extends [never]
  ? [Exclude<keyof TResultShape, AllInsertableKeys<TTable["columns"]>>] extends [never]
    ? {
        [K in keyof TResultShape]: K extends AllInsertableKeys<TTable["columns"]>
          ? TResultShape[K] extends InsertDataOf<TTable["columns"][K]>
            ? unknown
            : {
                readonly __error: "fromSelect column type mismatch";
                readonly column: K;
                readonly expected: InsertDataOf<TTable["columns"][K]>;
                readonly got: TResultShape[K];
              }
          : {
              readonly __error: "fromSelect unknown column";
              readonly column: K;
            };
      }
    : {
        readonly __error: "fromSelect select has columns the target table does not";
        readonly extra: Exclude<keyof TResultShape, AllInsertableKeys<TTable["columns"]>>;
      }
  : {
      readonly __error: "fromSelect select is missing required columns";
      readonly missing: Exclude<RequiredInsertKeys<TTable["columns"]>, keyof TResultShape>;
    };

// Base builder returned by `client.insert(table)` — both `.values()` and
// `.fromSelect()` are valid initial calls. Each transitions to a narrowed
// builder type that no longer offers the other side of the union, mirroring
// `SelectBuilder` / `SelectBuilderWithRefs` phantom narrowing.
export interface InsertBuilder<TTable extends AnyTable> extends PromiseLike<undefined> {
  values(values: InsertRowInput<TTable> | readonly InsertRowInput<TTable>[]): InsertValuesBuilder<TTable>;
  fromSelect<TQuery extends AnySelectBuilderLike>(
    query: TQuery & FromSelectShapeConstraint<TTable, SelectBuilderRow<TQuery>>,
  ): InsertFromSelectBuilder<TTable>;
  execute(options?: ClickHouseBaseQueryOptions): Promise<undefined>;
  catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<undefined | TResult2>;
  finally(onfinally?: (() => void) | null): Promise<undefined>;
  [compileQuerySymbol](): CompiledQuery<never>;
}

// After `.values(...)` only `.values(...)` (append more rows) remains; calling
// `.fromSelect(...)` is a type error, not just a runtime one.
export interface InsertValuesBuilder<TTable extends AnyTable> extends PromiseLike<undefined> {
  values(values: InsertRowInput<TTable> | readonly InsertRowInput<TTable>[]): InsertValuesBuilder<TTable>;
  execute(options?: ClickHouseBaseQueryOptions): Promise<undefined>;
  catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<undefined | TResult2>;
  finally(onfinally?: (() => void) | null): Promise<undefined>;
  [compileQuerySymbol](): CompiledQuery<never>;
}

// After `.fromSelect(...)` neither `.values()` nor a second `.fromSelect()`
// is allowed. The compiled SQL is `INSERT INTO t (cols) <selectStatement>`
// with no second body to merge.
//
// `TTable` is exposed as a phantom field so users can still annotate values
// as `InsertFromSelectBuilder<MyTable>`; the symbol key is invisible at
// runtime and excluded from `Object.keys` / autocomplete.
export interface InsertFromSelectBuilder<TTable extends AnyTable> extends PromiseLike<undefined> {
  readonly [insertFromSelectBuilderTableSymbol]?: TTable;
  execute(options?: ClickHouseBaseQueryOptions): Promise<undefined>;
  catch<TResult2 = never>(onrejected?: CatchHandler<TResult2>): Promise<undefined | TResult2>;
  finally(onfinally?: (() => void) | null): Promise<undefined>;
  [compileQuerySymbol](): CompiledQuery<never>;
}

const insertFromSelectBuilderTableSymbol = Symbol("clickhouseORMInsertFromSelectBuilderTable");

type InsertBuilderState<TTable extends AnyTable> =
  | { readonly kind: "empty" }
  | { readonly kind: "values"; readonly rows: readonly InsertRowInput<TTable>[] }
  | { readonly kind: "from_select"; readonly query: AnySelectBuilder };

const compileInsertValues = <TTable extends AnyTable>(
  table: TTable,
  rows: readonly InsertRowInput<TTable>[],
): CompiledQuery<never> => {
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
    { ...compiled.params },
    { ...compiled.paramTypes },
    undefined,
    {
      rootSourceName: table.originalName,
      tableName: table.originalName,
    },
  );
};

// INSERT-from-SELECT path: zero data on the wire — the SELECT runs entirely
// on the server. Column list is aligned by SELECT-projection key (the JS
// object key the user wrote in `select({...})`), not by SQL alias and not by
// position. ClickHouse aligns INSERT (cols) ↔ SELECT projection by position;
// we keep both sides in projection-key order so the visible semantics match
// "by name".
const compileInsertFromSelect = <TTable extends AnyTable>(
  table: TTable,
  query: AnySelectBuilder,
): CompiledQuery<never> => {
  const selectionItems = query.buildSelectionItems();
  if (selectionItems.length === 0) {
    throw createClientValidationError(
      "insert().fromSelect() requires the select query to project at least one column. Pass an explicit selection to select({...}).",
    );
  }

  const {
    entries: tableEntries,
    knownColumnKeys,
    generatedColumnKeys,
    requiredNestedColumnKeys,
  } = getInsertTableMetadata(table);
  // Index by logical schema key. Plain `kind: "column"` entries can be
  // populated by a single SELECT projection; `kind: "nested-field"` entries
  // are aggregated under the nested column's logical key so that a single
  // selection like `select({ events: src.events })` can fan out into all
  // physical `events.name` / `events.score` / … fields.
  const entriesByKey = new Map<string, InsertColumnEntry>();
  type NestedFieldGroup = {
    readonly key: string;
    readonly fieldEntries: readonly Extract<InsertColumnEntry, { kind: "nested-field" }>[];
    readonly fieldKeySet: ReadonlySet<string>;
  };
  const nestedGroups = new Map<string, NestedFieldGroup>();
  for (const entry of tableEntries) {
    if (entry.kind === "nested-field") {
      const existing = nestedGroups.get(entry.key);
      if (existing) {
        (existing.fieldEntries as Extract<InsertColumnEntry, { kind: "nested-field" }>[]).push(entry);
        (existing.fieldKeySet as Set<string>).add(entry.fieldKey);
      } else {
        nestedGroups.set(entry.key, {
          key: entry.key,
          fieldEntries: [entry],
          fieldKeySet: new Set([entry.fieldKey]),
        });
      }
      continue;
    }
    entriesByKey.set(entry.key, entry);
  }
  const generatedKeySet = new Set(generatedColumnKeys);

  // Per-selection-item plan: how the INSERT column list and the outer SELECT
  // projection should look. `nested-fan-out` produces N entries in the
  // INSERT list + N outer-SELECT dot-paths from one source projection.
  type ProjectionPlan =
    | {
        readonly mode: "plain";
        readonly entry: InsertColumnEntry;
        readonly sqlAlias: string;
      }
    | {
        readonly mode: "nested-fan-out";
        readonly fieldEntries: readonly Extract<InsertColumnEntry, { kind: "nested-field" }>[];
        readonly sqlAlias: string;
      };
  const projectionPlans: ProjectionPlan[] = [];
  // `seenKeys` doubles as projection-key tracker and the input for the
  // required-column check below. SelectionItem keys come from
  // `Object.entries(selection)` (JS guarantees unique keys), so we never need
  // to detect duplicates here — the set is purely a "what got projected" log.
  const seenKeys = new Set<string>();
  for (const item of selectionItems) {
    const projectionKey = item.key;
    seenKeys.add(projectionKey);

    if (generatedKeySet.has(projectionKey)) {
      throw createClientValidationError(
        `insert().fromSelect() cannot target generated column "${projectionKey}" (MATERIALIZED/ALIAS columns are computed by ClickHouse)`,
      );
    }

    const nestedGroup = nestedGroups.get(projectionKey);
    if (nestedGroup) {
      // Projection targets a nested column. The expression must carry
      // nestedShape metadata, which only direct nested column references
      // (and CTE/subquery references propagated through
      // `buildReferenceColumns`) do — a computed expression like
      // `fn.multiIf(...)` cannot fan out into the multiple parallel array
      // fields a nested column expands to physically. Computed values must
      // flow through `.values(...)` / `.insertJsonEachRow(...)` instead.
      const expressionLike = item.expression as SqlSelection<unknown> & {
        readonly nestedShape?: Record<string, AnyColumn>;
      };
      if (expressionLike.nestedShape == null) {
        throw createClientValidationError(
          `insert().fromSelect() projection for nested column "${projectionKey}" must be a direct nested column reference (e.g. \`source.${projectionKey}\`). Computed expressions cannot fill nested columns because they expand to multiple parallel array fields; use .values(...) or .insertJsonEachRow(...) for row-wise insertion of computed nested data.`,
        );
      }
      const sourceFieldKeys = new Set(Object.keys(expressionLike.nestedShape));
      for (const targetFieldKey of nestedGroup.fieldKeySet) {
        if (!sourceFieldKeys.has(targetFieldKey)) {
          const sourceKeys = [...sourceFieldKeys].join(", ");
          throw createClientValidationError(
            `insert().fromSelect() nested column "${projectionKey}" shape mismatch: target requires field "${targetFieldKey}", source nested shape provides {${sourceKeys}}`,
          );
        }
      }
      projectionPlans.push({
        mode: "nested-fan-out",
        fieldEntries: nestedGroup.fieldEntries,
        sqlAlias: item.sqlAlias,
      });
      continue;
    }

    if (!knownColumnKeys.has(projectionKey)) {
      throw createClientValidationError(
        `insert().fromSelect() projects unknown column "${projectionKey}" — not a column of "${table.originalName}"`,
      );
    }
    // At this point `projectionKey` is in `knownColumnKeys`, is *not* in
    // `nestedGroups`, and is *not* in `generatedKeySet`. The remaining
    // schema kinds all land in `entriesByKey` during the metadata walk
    // above, so the lookup is guaranteed to return a value.
    const entry = entriesByKey.get(projectionKey) as InsertColumnEntry;
    projectionPlans.push({ mode: "plain", entry, sqlAlias: item.sqlAlias });
  }

  // Required columns omitted from the projection would have ClickHouse silently
  // fill defaults (or zero values), which is almost never what the user wants
  // — surface this client-side to match the typecheck rule for
  // `FromSelectShapeConstraint`. Columns with an explicit DEFAULT and nested
  // columns are optional by default (nested ones expand to empty arrays
  // server-side); nested columns the user explicitly marked
  // `.requiredOnInsert()` are tracked separately and added back into the
  // missing-column scan so a `as never`-bypass cannot silently drop them.
  const requiredKeysMissing: string[] = [];
  const seenForRequired = new Set<string>();
  for (const entry of tableEntries) {
    if (entry.kind === "nested-field") continue;
    if (entry.column.ddl?.default !== undefined) continue;
    if (seenForRequired.has(entry.key)) continue;
    seenForRequired.add(entry.key);
    if (!seenKeys.has(entry.key)) {
      requiredKeysMissing.push(entry.key);
    }
  }
  for (const requiredNestedKey of requiredNestedColumnKeys) {
    if (!seenKeys.has(requiredNestedKey)) {
      requiredKeysMissing.push(requiredNestedKey);
    }
  }
  if (requiredKeysMissing.length > 0) {
    throw createClientValidationError(
      `insert().fromSelect() select is missing required columns: ${requiredKeysMissing.join(", ")}`,
    );
  }

  // Build the INSERT column-list fragments and the outer-SELECT projection
  // (only used in the nested-fan-out branch). When there are no nested
  // projections we keep the simple single-SELECT form to avoid pointless
  // subquery wrapping.
  const insertIdentifiers: SQLFragment[] = [];
  const outerSelectParts: SQLFragment[] = [];
  let hasNestedFanOut = false;
  const innerAlias = sql.identifier("__ck_inner");
  for (const plan of projectionPlans) {
    if (plan.mode === "plain") {
      insertIdentifiers.push(plan.entry.identifierFragment);
      outerSelectParts.push(sql`${innerAlias}.${sql.identifier(plan.sqlAlias)}`);
      continue;
    }
    hasNestedFanOut = true;
    for (const fieldEntry of plan.fieldEntries) {
      insertIdentifiers.push(fieldEntry.identifierFragment);
      // Outer-SELECT dot-path access — `__ck_inner.events.name` reads the
      // `name` sub-field of the inner `events` projection (ClickHouse
      // supports dot-path access on named-tuple / nested columns returned
      // from a subquery; verified on CH 26.3).
      outerSelectParts.push(sql`${innerAlias}.${sql.identifier(plan.sqlAlias)}.${sql.identifier(fieldEntry.fieldKey)}`);
    }
  }

  const ctx: BuildContext = {
    params: {},
    paramTypes: {},
    nextParamIndex: 0,
  };
  // Wrap the nested compile in a CompileState so the inner SELECT's
  // forcedSettings (e.g. join_use_nulls = 1 from a leftJoin) bubble up
  // through `collectForcedSettings`. Mirrors `SelectBuilder` at line ~1708.
  const { result: nestedCompiled, forcedSettings } = withCompileState(ctx, () => compileNestedQuery(query, ctx));

  const insertHeader = sql`insert into ${renderTableIdentifier(table)} (${joinSqlParts(insertIdentifiers, ", ")})`;
  const statement = hasNestedFanOut
    ? sql`${insertHeader} select ${joinSqlParts(outerSelectParts, ", ")} from (${sql.raw(nestedCompiled.statement)}) as ${innerAlias}`
    : sql`${insertHeader} ${sql.raw(nestedCompiled.statement)}`;
  const compiled = compileSql(statement, ctx);

  return createCompiledQuery(
    compiled.query,
    [],
    "command",
    { ...compiled.params },
    { ...compiled.paramTypes },
    forcedSettings,
    {
      rootSourceName: table.originalName,
      tableName: table.originalName,
    },
  );
};

const isAnySelectBuilder = (value: unknown): value is AnySelectBuilder => isSelectBuilder(value);

export const createInsertBuilder = <TTable extends AnyTable>(
  table: TTable,
  runner?: PreparedRunner,
  initialState: InsertBuilderState<TTable> = { kind: "empty" },
): InsertBuilder<TTable> => {
  if (table.alias) {
    throw createClientValidationError("insert() requires a base table and does not accept aliased table targets");
  }

  const builder = {
    values(values: InsertRowInput<TTable> | readonly InsertRowInput<TTable>[]): InsertValuesBuilder<TTable> {
      if (initialState.kind === "from_select") {
        throw createClientValidationError(
          "insert().values() cannot follow insert().fromSelect() — pick one of the two on a single insert chain",
        );
      }
      const normalized = normalizeInsertRows(table, values);
      const mergedRows = initialState.kind === "values" ? [...initialState.rows, ...normalized] : normalized;
      return createInsertBuilder(table, runner, {
        kind: "values",
        rows: mergedRows,
      }) as unknown as InsertValuesBuilder<TTable>;
    },

    fromSelect<TQuery extends AnySelectBuilderLike>(
      query: TQuery & FromSelectShapeConstraint<TTable, SelectBuilderRow<TQuery>>,
    ): InsertFromSelectBuilder<TTable> {
      if (initialState.kind === "values") {
        throw createClientValidationError(
          "insert().fromSelect() cannot follow insert().values() — pick one of the two on a single insert chain",
        );
      }
      if (initialState.kind === "from_select") {
        throw createClientValidationError("insert().fromSelect() cannot be called twice on the same insert chain");
      }
      if (!isAnySelectBuilder(query)) {
        throw createClientValidationError(
          "insert().fromSelect() expects a SelectBuilder (e.g. session.select({...}).from(table)); subqueries created via .as(alias) are not accepted directly",
        );
      }
      return createInsertBuilder(table, runner, {
        kind: "from_select",
        query: query as AnySelectBuilder,
      }) as unknown as InsertFromSelectBuilder<TTable>;
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
      if (initialState.kind === "empty") {
        throw createClientValidationError(
          "insert() requires .values(rows) or .fromSelect(selectBuilder) before execute()",
        );
      }
      if (initialState.kind === "values") {
        if (initialState.rows.length === 0) {
          throw createClientValidationError("insert().values() must be called with at least one row before execute()");
        }
        return compileInsertValues(table, initialState.rows);
      }
      return compileInsertFromSelect(table, initialState.query);
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

// Anonymous CTE — name is assigned per-compile (`__cte_N`) instead of by
// the user. Column refs use the same lazy-alias mechanism as bare
// SelectBuilder subqueries; their sourceKey is widened to `string`
// because the actual alias isn't known at type-creation time.
type CteFromAnonymousQuery<TQuery> = {
  readonly kind: "cte";
  readonly name: undefined;
  readonly query: AnySelectBuilder<SelectBuilderRow<TQuery>>;
  readonly columns: ReferenceColumns<SelectBuilderRow<TQuery>, string>;
} & ReferenceColumns<SelectBuilderRow<TQuery>, string>;

export type AnyCte = {
  readonly kind: "cte";
  readonly name: string | undefined;
  readonly query: AnySelectBuilder<Record<string, unknown>>;
  readonly columns: SourceColumns;
};

export interface QueryClient<TJoinUseNulls extends JoinUseNulls = 1> {
  readonly ctes: AnyCte[];
  select<TSelection extends SelectionRecord | undefined = undefined>(
    selection?: TSelection,
  ): SelectBuilderWithRefs<
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
  $with(): {
    as: <TQuery>(
      query: TQuery & (SelectBuilderRow<TQuery> extends never ? never : unknown),
    ) => CteFromAnonymousQuery<TQuery>;
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
    ): SelectBuilderWithRefs<
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

    $with(name?: string) {
      return {
        as: <TQuery>(query: TQuery & (SelectBuilderRow<TQuery> extends never ? never : unknown)) => {
          const selectQuery = query as unknown as AnySelectBuilder<SelectBuilderRow<TQuery>>;
          const selectionItems = selectQuery.buildSelectionItems();

          if (name === undefined) {
            // Anonymous CTE — alias resolved per-compile via the cte object's
            // identity. Column refs close over `cte` so all references render
            // with the same `__cte_N` as the WITH definition.
            const columns: SourceColumns = {};
            const cte: AnyCte = {
              kind: "cte",
              name: undefined,
              query: selectQuery as AnySelectBuilder<Record<string, unknown>>,
              columns,
            };
            for (const item of selectionItems) {
              columns[item.key] = createLazyAliasReferenceExpression(
                (ctx) => resolveAnonymousCteAlias(ctx, cte),
                item.sqlAlias,
                item.expression.decoder,
                item.expression.sqlType,
              );
            }
            attachSafeColumnRefs(cte, columns, FORBIDDEN_CTE_COLUMN_KEYS);
            return cte as unknown as CteFromAnonymousQuery<TQuery>;
          }

          const columns = buildReferenceColumns<SelectBuilderRow<TQuery>, string>(name, selectionItems);
          const cte: AnyCte = {
            kind: "cte",
            name,
            query: selectQuery as AnySelectBuilder<Record<string, unknown>>,
            columns,
          };
          attachSafeColumnRefs(cte, columns, FORBIDDEN_CTE_COLUMN_KEYS);
          return cte as unknown as CteFromQuery<TQuery, string>;
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

// Predicate-builder paths add a `mapToDriverValue` callable check on top of
// the shared `isColumnLike` so the operand encoders aren't fooled by stray
// objects that happen to carry `kind: "column"` (e.g. test doubles).
const isColumnExpression = (value: unknown): value is AnyColumn =>
  isColumnLike(value) && typeof (value as { mapToDriverValue?: unknown }).mapToDriverValue === "function";

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
  const operatorFragment = operator === "is null" ? SQL_IS_NULL : SQL_IS_NOT_NULL;
  return createExpression<boolean>({
    compile: (ctx) => sql`${leftExpression.compile(ctx)}${operatorFragment}`,
    decoder: booleanCastDecoder,
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
  // The operator string is closed over and never changes — build the SQL
  // fragment once at builder time and reuse it every compile.
  const operatorFragment = sql.raw(` ${operator} `);
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${leftExpression.compile(ctx)}${operatorFragment}${compilePredicateValue(
        left,
        right,
        ctx,
        leftExpression.sqlType,
      )}`,
    decoder: booleanCastDecoder,
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
    compile: (ctx) => sql`${SQL_NOT_OPEN}${wrapped.compile(ctx)}${SQL_CLOSE_PAREN}`,
    decoder: booleanCastDecoder,
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

// Lookup for the LIKE-family operator fragments — exhaustive over
// `LikeOperator`, so adding a new variant requires updating this table.
const LIKE_OPERATOR_FRAGMENTS: Record<LikeOperator, SQLFragment> = {
  like: SQL_LIKE,
  "not like": SQL_NOT_LIKE,
  ilike: SQL_ILIKE,
  "not ilike": SQL_NOT_ILIKE,
};
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
  const operatorFragment = LIKE_OPERATOR_FRAGMENTS[operator];
  return createExpression<boolean>({
    compile: (ctx) => sql`${leftExpression.compile(ctx)}${operatorFragment}${compileValue(right, ctx, "String")}`,
    decoder: booleanCastDecoder,
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

// Six string-shape predicates fan out from one factory: each helper escapes
// its right-hand value into a LIKE/ILIKE literal pattern and forwards to the
// matching base operator. Adding a new shape (e.g. `equalsIgnoreCase`) is a
// one-line export now.
const makeStringShapePredicate =
  (helperName: string, mode: LikeLiteralMode, op: typeof like) =>
  (left: unknown, right: string): Predicate => {
    assertStringPredicateValue(right, helperName);
    return op(left, toLiteralLikePattern(right, mode));
  };

export const contains = makeStringShapePredicate("contains", "contains", like);
export const startsWith = makeStringShapePredicate("startsWith", "startsWith", like);
export const endsWith = makeStringShapePredicate("endsWith", "endsWith", like);
export const containsIgnoreCase = makeStringShapePredicate("containsIgnoreCase", "contains", ilike);
export const startsWithIgnoreCase = makeStringShapePredicate("startsWithIgnoreCase", "startsWith", ilike);
export const endsWithIgnoreCase = makeStringShapePredicate("endsWithIgnoreCase", "endsWith", ilike);

export const between = (expression: unknown, start: unknown, end: unknown): Predicate => {
  const wrapped = ensureComparableExpression(expression);
  assertPredicateValue(start, "between");
  assertPredicateValue(end, "between");
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${wrapped.compile(ctx)}${SQL_BETWEEN}${compilePredicateValue(
        expression,
        start,
        ctx,
        wrapped.sqlType,
      )}${SQL_AND}${compilePredicateValue(expression, end, ctx, wrapped.sqlType)}`,
    decoder: booleanCastDecoder,
    sqlType: "Bool",
  });
};

// Builders that emit the same ClickHouse function name on every compile
// pre-render the name fragment once and feed it here. The string literal
// matches `sql.raw(name)` byte-for-byte but skips the per-compile allocation.
const compilePredicateFunction = (nameFragment: SQLFragment, args: SQLFragment[]): SQLFragment => {
  return sql`${nameFragment}(${joinSqlParts(args, ", ")})`;
};

// Pre-rendered name fragments for the array-containment predicates. Hoisted
// so each `has` / `hasAll` / `hasAny` / `hasSubstr` builder uses the same
// fragment regardless of how many times it's compiled.
const HAS_FN_FRAGMENT = sql.raw("has");
const HAS_ALL_FN_FRAGMENT = sql.raw("hasAll");
const HAS_ANY_FN_FRAGMENT = sql.raw("hasAny");
const HAS_SUBSTR_FN_FRAGMENT = sql.raw("hasSubstr");
const ARRAY_CONTAINMENT_FN_FRAGMENTS: Record<string, SQLFragment> = {
  hasAll: HAS_ALL_FN_FRAGMENT,
  hasAny: HAS_ANY_FN_FRAGMENT,
  hasSubstr: HAS_SUBSTR_FN_FRAGMENT,
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
      compilePredicateFunction(HAS_FN_FRAGMENT, [
        haystackExpression.compile(ctx),
        compileHasNeedle(haystack, needle, ctx, haystackExpression),
      ]),
    decoder: booleanCastDecoder,
    sqlType: "Bool",
  });
};

// `hasAll` / `hasAny` / `hasSubstr` differ only in the ClickHouse function
// name they emit — same validation, same encoding path. One factory keeps
// future array-containment helpers (e.g. `hasAnyExcept`) cheap to add.
const makeArrayContainmentPredicate = (helperName: "hasAll" | "hasAny" | "hasSubstr") => {
  const nameFragment = ARRAY_CONTAINMENT_FN_FRAGMENTS[helperName];
  return (left: unknown, right: unknown): Predicate => {
    assertPredicateValue(right, helperName);
    if (Array.isArray(right)) {
      assertPredicateValueArray(right, helperName);
    }
    const leftExpression = ensureComparableExpression(left);
    return createExpression<boolean>({
      compile: (ctx) =>
        compilePredicateFunction(nameFragment, [
          leftExpression.compile(ctx),
          compileArrayFunctionArg(left, right, ctx, leftExpression, helperName),
        ]),
      decoder: booleanCastDecoder,
      sqlType: "Bool",
    });
  };
};

export const hasAll = makeArrayContainmentPredicate("hasAll");
export const hasAny = makeArrayContainmentPredicate("hasAny");
export const hasSubstr = makeArrayContainmentPredicate("hasSubstr");

const createInExpression = (
  negate: boolean,
  left: unknown,
  right: readonly unknown[] | AnySubquery | AnyCte | AnySelectBuilderLike,
): Predicate => {
  const helperName = negate ? "notInArray" : "inArray";
  assertPredicateValue(right, helperName);
  if (Array.isArray(right)) {
    assertPredicateValueArray(right, helperName, { allowSqlFragments: true });
  }
  const leftExpression = ensureComparableExpression(left);
  // Both branches are decided once at builder time — pick the matching
  // pre-built keyword fragment and `0`/`1` literal instead of building them
  // anew on every compile.
  const operatorFragment = negate ? SQL_NOT_IN_OPEN : SQL_IN_OPEN;
  const emptyArrayFragment = negate ? SQL_TRUE_LITERAL : SQL_FALSE_LITERAL;
  return createExpression<boolean>({
    compile: (ctx) => {
      if (Array.isArray(right)) {
        if (right.length === 0) {
          return emptyArrayFragment;
        }
        const parts = right.map((value) => compilePredicateValue(left, value, ctx, leftExpression.sqlType));
        return sql`${leftExpression.compile(ctx)}${operatorFragment}${joinSqlParts(parts, ", ")}${SQL_CLOSE_PAREN}`;
      }

      const querySource = isSelectBuilder(right) ? right : (right as AnySubquery | AnyCte).query;
      return sql`${leftExpression.compile(ctx)}${operatorFragment}${sql.raw(compileNestedQuery(querySource, ctx).statement)}${SQL_CLOSE_PAREN}`;
    },
    decoder: booleanCastDecoder,
    sqlType: "Bool",
  });
};

export const inArray = (
  left: unknown,
  right: readonly unknown[] | AnySubquery | AnyCte | AnySelectBuilderLike,
): Predicate => createInExpression(false, left, right);

export const notInArray = (
  left: unknown,
  right: readonly unknown[] | AnySubquery | AnyCte | AnySelectBuilderLike,
): Predicate => createInExpression(true, left, right);

export const exists = (query: AnySubquery | AnyCte | AnySelectBuilderLike): Predicate => {
  const selectQuery = isSubquery(query) || isCte(query) ? query.query : query;
  return createExpression<boolean>({
    compile: (ctx) =>
      sql`${SQL_EXISTS_OPEN}${sql.raw(compileNestedQuery(selectQuery, ctx).statement)}${SQL_CLOSE_PAREN}`,
    decoder: booleanCastDecoder,
    sqlType: "Bool",
  });
};

export const notExists = (query: AnySubquery | AnyCte | AnySelectBuilderLike): Predicate => {
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
      return sql`${compiledSource}${SQL_AS}${sql.identifier(aliasName)}`;
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

export function expr<TData>(value: SQLFragment<TData>): Selection<TData>;
export function expr<TData = unknown>(
  value: SQLFragment,
  config?: { decoder?: Decoder<TData>; sqlType?: string },
): Selection<TData>;
export function expr<TData = unknown>(
  value: SQLFragment,
  config?: { decoder?: Decoder<TData>; sqlType?: string },
): Selection<TData> {
  return wrapSql(value, {
    decoder: (config?.decoder ?? value.decoder) as Decoder<TData>,
    sqlType: config?.sqlType,
  });
}
