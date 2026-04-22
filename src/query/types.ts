import type { Column } from "../columns";
import type { SqlExpression } from "../query-shared";

/**
 * Record shape supplied to `select({ ... })` — maps field names to
 * {@link SqlExpression} or {@link Column} values.
 */
export type SelectionRecord = Record<string, unknown>;

/**
 * Sentinel type for a query that has not joined any additional sources.
 * Used as the default for nullable-source maps so that every base column
 * retains its original non-null type.
 */
export type NoJoinedSources = Record<never, never>;

/**
 * Widens a column or expression type with `| null` when its source comes from
 * a nullable join.
 */
export type ApplyNullability<
  TData,
  TSourceKey extends string | undefined,
  TNullableSources extends Record<string, boolean>,
> = TSourceKey extends keyof TNullableSources
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
  [K in keyof TSelection]: TSelection[K] extends SqlExpression<infer TData, infer TSourceKey>
    ? ApplyNullability<TData, TSourceKey, TNullableSources>
    : TSelection[K] extends Column<infer TData, string, infer TTableName, infer TTableAlias>
      ? ApplyNullability<TData, TTableAlias extends string ? TTableAlias : TTableName, TNullableSources>
      : unknown;
};
