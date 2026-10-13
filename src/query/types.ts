import type { Column } from "../columns";
import type { Selection } from "../query-shared";
import type { SQLFragment } from "../sql";

/**
 * Record shape supplied to `select({ ... })` — maps field names to
 * {@link Selection}, {@link Column} or `SQLFragment` values.
 */
export type SelectionRecord = Record<string, unknown>;

/**
 * Sentinel type for a query that has not joined any additional sources.
 * Used as the default for nullable-source maps so that every base column
 * retains its original non-null type.
 */
export type NoJoinedSources = Record<never, never>;

/**
 * Joined-source key for sources without a literal name: bare select builders
 * and anonymous CTEs. Their column refs carry the wide `string` source key.
 */
export type AnonymousSourceKey = "__ck_orm_anonymous_source";

type IsWideString<T> = [T] extends [string] ? (string extends T ? true : false) : false;

/**
 * Widens a column or expression type with `| null` when its source comes from
 * a nullable join.
 */
export type ApplyNullability<
  TData,
  TSourceKey extends string | undefined,
  TNullableSources extends Record<string, boolean>,
> =
  IsWideString<TSourceKey> extends true
    ? TNullableSources extends Record<AnonymousSourceKey, true>
      ? TData | null
      : string extends keyof TNullableSources
        ? TNullableSources[string] extends true
          ? TData | null
          : TData
        : TData
    : TSourceKey extends keyof TNullableSources
      ? TNullableSources[TSourceKey] extends true
        ? TData | null
        : TData
      : TData;

/**
 * Infers the result shape from an explicit `select({ ... })` projection,
 * applying nullability based on the source map.
 */
export type InferSelectionResult<
  TSelection extends SelectionRecord,
  TNullableSources extends Record<string, boolean> = NoJoinedSources,
> = {
  // Raw fragments (ckSql.decimal, JSON paths, column casts) come first: they also match the Selection shape,
  // but carry their data type in the decoder rather than the Selection brand, and have no source.
  [K in keyof TSelection]: TSelection[K] extends SQLFragment<infer TData>
    ? TData
    : TSelection[K] extends Selection<infer TData, infer TSourceKey>
      ? ApplyNullability<TData, TSourceKey, TNullableSources>
      : TSelection[K] extends Column<infer TData, string, infer TTableName, infer TTableAlias>
        ? ApplyNullability<TData, TTableAlias extends string ? TTableAlias : TTableName, TNullableSources>
        : unknown;
};
