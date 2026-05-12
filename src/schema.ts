import type { AnyColumn, Column, DdlFragmentInput, JsonColumn, JsonShape } from "./columns";
import { createClientValidationError } from "./errors";
import { isColumnLike } from "./internal/column";
import { assertValidSqlIdentifier } from "./internal/identifier";
import type {
  ColumnHasDefault,
  ColumnIoMarker,
  IsColumnGenerated,
  IsNestedColumn,
  IsNestedRequiredOnInsert,
  NestedColumnBrand,
} from "./query-shared";
import { type SQLFragment, sql } from "./sql";

type InferSelect<TColumns extends Record<string, AnyColumn>> = {
  [K in keyof TColumns]: TColumns[K] extends Column<infer TData, string> ? TData : never;
};

// Pull the insert-side TData out of a column. Defaults to the column's select
// TData when the user hasn't called `.$type<{select, insert}>()` to diverge
// the two — every `Column<X, ...>` already extends `ColumnIoMarker<X, false,
// false>` so the brand is always populated. Nested columns carry a separate
// `NestedColumnBrand<TInsert, ...>` because their insert shape (`Array<{...}>`)
// must survive `.$type` / `.$validator` chains that otherwise reset
// `ColumnIoMarker`.
/** @internal */
export type InsertDataOf<TColumn> =
  TColumn extends NestedColumnBrand<infer TNestedInsert, boolean>
    ? TNestedInsert
    : TColumn extends ColumnIoMarker<infer TInsert, boolean, boolean>
      ? TInsert
      : TColumn extends Column<infer TData, string>
        ? TData
        : never;

// `RequiredInsertKeys` order matters: nested columns are checked *before*
// `ColumnHasDefault` so that `.$type`/`.$validator` chains (which reset
// `ColumnIoMarker` back to `<…, false, false>`) cannot flip a nested column
// back to required. Only `.requiredOnInsert()` (Part C) deliberately opts
// nested back into required via `IsNestedRequiredOnInsert`.
/** @internal */
export type RequiredInsertKeys<TColumns extends Record<string, AnyColumn>> = {
  [K in keyof TColumns]: IsColumnGenerated<TColumns[K]> extends true
    ? never
    : IsNestedColumn<TColumns[K]> extends true
      ? IsNestedRequiredOnInsert<TColumns[K]> extends true
        ? K
        : never
      : ColumnHasDefault<TColumns[K]> extends true
        ? never
        : K;
}[keyof TColumns];

/** @internal */
export type OptionalInsertKeys<TColumns extends Record<string, AnyColumn>> = {
  [K in keyof TColumns]: IsColumnGenerated<TColumns[K]> extends true
    ? never
    : IsNestedColumn<TColumns[K]> extends true
      ? IsNestedRequiredOnInsert<TColumns[K]> extends true
        ? never
        : K
      : ColumnHasDefault<TColumns[K]> extends true
        ? K
        : never;
}[keyof TColumns];

/** @internal */
export type AllInsertableKeys<TColumns extends Record<string, AnyColumn>> =
  | RequiredInsertKeys<TColumns>
  | OptionalInsertKeys<TColumns>;

// Flatten intersection types into a single object type so structural equality
// helpers (`Equal<A, B>`) compare cleanly against the analogous select model.
type Flatten<T> = T extends infer U ? { [K in keyof U]: U[K] } : never;

// Insert model: drop MATERIALIZED / ALIAS columns entirely, make DEFAULT and
// `nested(...)` columns optional, and use the per-column insert TData for the
// rest. Nested columns are optional because ck-orm encodes a missing value as
// SQL `DEFAULT`, which ClickHouse expands into an empty parallel array — there
// is no explicit DEFAULT clause on the schema side. Use `.requiredOnInsert()`
// to opt back into required at the type layer.
type InferInsert<TColumns extends Record<string, AnyColumn>> = Flatten<
  {
    [K in RequiredInsertKeys<TColumns>]: InsertDataOf<TColumns[K]>;
  } & {
    [K in OptionalInsertKeys<TColumns>]?: InsertDataOf<TColumns[K]>;
  }
>;

type BoundColumns<
  TColumns extends Record<string, AnyColumn>,
  TTableName extends string,
  TTableAlias extends string | undefined = undefined,
> = {
  // `JsonColumn<T>` carries extra `path` / `castPath` / `subobject` / `merged`
  // / `arrayPath` methods that must survive binding — intersect the bound
  // column shape with `Pick<JsonColumn<T>, ...>` so the methods reappear
  // alongside the new `TTableName` / `TTableAlias` sourceKey.
  //
  // `NestedColumnBrand` (set by `ckType.nested(...)`) must also survive
  // binding, otherwise `RequiredInsertKeys` would lose track of nested
  // columns after `bindColumns` rebinds them to a specific table. The brand
  // is intersected back in via a dedicated `NestedColumnBrand` extends-branch
  // below.
  [K in keyof TColumns]: TColumns[K] extends JsonColumn<infer TJsonData>
    ? TColumns[K] extends ColumnIoMarker<infer TInsert, infer TGen, infer THas>
      ? Column<TJsonData, string, TTableName, TTableAlias> &
          Pick<JsonColumn<TJsonData>, "path" | "castPath" | "subobject" | "merged" | "arrayPath"> &
          ColumnIoMarker<TInsert, TGen, THas>
      : Column<TJsonData, string, TTableName, TTableAlias> &
          Pick<JsonColumn<TJsonData>, "path" | "castPath" | "subobject" | "merged" | "arrayPath">
    : TColumns[K] extends NestedColumnBrand<infer TNestedInsert, infer TNestedRequired>
      ? TColumns[K] extends Column<infer TData, infer TSqlType, string | undefined, string | undefined>
        ? TColumns[K] extends ColumnIoMarker<infer TInsert, infer TGen, infer THas>
          ? Column<TData, TSqlType, TTableName, TTableAlias> &
              NestedColumnBrand<TNestedInsert, TNestedRequired> &
              ColumnIoMarker<TInsert, TGen, THas>
          : Column<TData, TSqlType, TTableName, TTableAlias> & NestedColumnBrand<TNestedInsert, TNestedRequired>
        : never
      : TColumns[K] extends Column<infer TData, infer TSqlType, string | undefined, string | undefined>
        ? TColumns[K] extends ColumnIoMarker<infer TInsert, infer TGen, infer THas>
          ? Column<TData, TSqlType, TTableName, TTableAlias> & ColumnIoMarker<TInsert, TGen, THas>
          : Column<TData, TSqlType, TTableName, TTableAlias>
        : never;
};

// Internal compile-time guard: `JsonShape` re-exported here only to keep the
// constraint generic in the conditional above stable across module boundaries.
type _UseJsonShape = JsonShape;

export const mergeTreeTableEngines = [
  "MergeTree",
  "ReplacingMergeTree",
  "SummingMergeTree",
  "AggregatingMergeTree",
  "CollapsingMergeTree",
  "VersionedCollapsingMergeTree",
  "GraphiteMergeTree",
  "CoalescingMergeTree",
] as const;

export const logTableEngines = ["TinyLog", "StripeLog", "Log"] as const;

export const integrationTableEngines = [
  "ODBC",
  "JDBC",
  "MySQL",
  "MongoDB",
  "Redis",
  "HDFS",
  "S3",
  "Kafka",
  "EmbeddedRocksDB",
  "RabbitMQ",
  "PostgreSQL",
  "S3Queue",
  "TimeSeries",
] as const;

export const specialTableEngines = [
  "Distributed",
  "Dictionary",
  "Merge",
  "Executable",
  "ExecutablePool",
  "File",
  "Null",
  "Set",
  "Join",
  "URL",
  "View",
  "Memory",
  "Buffer",
  "GenerateRandom",
  "KeeperMap",
  "FileLog",
] as const;

export type MergeTreeTableEngine = (typeof mergeTreeTableEngines)[number];
export type ReplicatedMergeTreeTableEngine = `Replicated${MergeTreeTableEngine}`;
export type LogTableEngine = (typeof logTableEngines)[number];
export type IntegrationTableEngine = (typeof integrationTableEngines)[number];
export type SpecialTableEngine = (typeof specialTableEngines)[number];
export type ClickHouseTableEngine =
  | MergeTreeTableEngine
  | ReplicatedMergeTreeTableEngine
  | LogTableEngine
  | IntegrationTableEngine
  | SpecialTableEngine;

type TableColumnRef<TColumns extends Record<string, AnyColumn>> = TColumns[keyof TColumns];

type TableExpressionInput<TColumns extends Record<string, AnyColumn>> = TableColumnRef<TColumns> | DdlFragmentInput;
type TableExpressionListInput<TColumns extends Record<string, AnyColumn>> =
  | TableExpressionInput<TColumns>
  | readonly TableExpressionInput<TColumns>[];
type TableSettingValue = string | number | boolean | SQLFragment<unknown>;

type TableOptionsConfig<TColumns extends Record<string, AnyColumn>> = {
  readonly engine?: ClickHouseTableEngine | SQLFragment<unknown>;
  readonly partitionBy?: TableExpressionListInput<TColumns>;
  readonly primaryKey?: TableExpressionListInput<TColumns>;
  readonly orderBy?: readonly TableExpressionInput<TColumns>[];
  readonly sampleBy?: TableExpressionInput<TColumns>;
  readonly ttl?: DdlFragmentInput | readonly DdlFragmentInput[];
  readonly settings?: Record<string, TableSettingValue>;
  readonly comment?: string;
  readonly versionColumn?: TableColumnRef<TColumns>;
};

export interface TableOptions {
  readonly engine?: ClickHouseTableEngine | SQLFragment<unknown>;
  readonly partitionBy?: TableExpressionListInput<Record<string, AnyColumn>>;
  readonly primaryKey?: TableExpressionListInput<Record<string, AnyColumn>>;
  readonly orderBy?: readonly (AnyColumn | DdlFragmentInput)[];
  readonly sampleBy?: AnyColumn | DdlFragmentInput;
  readonly ttl?: DdlFragmentInput | readonly DdlFragmentInput[];
  readonly settings?: Record<string, TableSettingValue>;
  readonly comment?: string;
  readonly versionColumn?: AnyColumn;
}

export type InferSelectModel<TTable extends { readonly $inferSelect: unknown }> = TTable["$inferSelect"];
export type InferInsertModel<TTable extends { readonly $inferInsert: unknown }> = TTable["$inferInsert"];
export type InferSelectSchema<TSchema extends Record<string, { readonly $inferSelect: unknown }>> = {
  [K in keyof TSchema]: InferSelectModel<TSchema[K]>;
};
export type InferInsertSchema<TSchema extends Record<string, { readonly $inferInsert: unknown }>> = {
  [K in keyof TSchema]: InferInsertModel<TSchema[K]>;
};

export interface Table<
  TColumns extends Record<string, AnyColumn> = Record<string, AnyColumn>,
  TName extends string = string,
  TAlias extends string | undefined = undefined,
  TOriginalName extends string = TName,
> {
  readonly kind: "table";
  readonly tableName: TName;
  readonly originalName: TOriginalName;
  readonly alias?: TAlias;
  readonly columns: TColumns;
  readonly options: TableOptions;
  readonly $inferSelect: InferSelect<TColumns>;
  readonly $inferInsert: InferInsert<TColumns>;
}

type TableWithColumns<
  TColumns extends Record<string, AnyColumn>,
  TName extends string = string,
  TAlias extends string | undefined = undefined,
  TOriginalName extends string = TName,
> = Table<TColumns, TName, TAlias, TOriginalName> & TColumns;

export type AnyTable = Table<Record<string, AnyColumn>, string, string | undefined, string>;

type TableOptionsFactory<
  TColumns extends Record<string, AnyColumn>,
  TName extends string,
  TAlias extends string | undefined = undefined,
> = (
  table: TableWithColumns<BoundColumns<TColumns, TName, TAlias>, TName, TAlias>,
) => TableOptionsConfig<BoundColumns<TColumns, TName, TAlias>>;

type TableOptionsInput<
  TColumns extends Record<string, AnyColumn>,
  TName extends string,
  TAlias extends string | undefined = undefined,
> = TableOptionsConfig<BoundColumns<TColumns, TName, TAlias>> | TableOptionsFactory<TColumns, TName, TAlias>;

const bindColumns = <
  TColumns extends Record<string, AnyColumn>,
  TTableName extends string,
  TTableAlias extends string | undefined = undefined,
>(
  tableName: TTableName,
  columns: TColumns,
  tableAlias?: TTableAlias,
): BoundColumns<TColumns, TTableName, TTableAlias> => {
  const boundColumns = {} as BoundColumns<TColumns, TTableName, TTableAlias>;
  const physicalNames = new Map<string, string>();

  for (const [columnKey, column] of Object.entries(columns)) {
    const physicalName = column.configuredName ?? columnKey;
    const previousKey = physicalNames.get(physicalName);
    if (previousKey) {
      throw createClientValidationError(
        `Duplicate column name "${physicalName}" in table "${tableName}" for schema keys "${previousKey}" and "${columnKey}"`,
      );
    }
    physicalNames.set(physicalName, columnKey);

    boundColumns[columnKey as keyof BoundColumns<TColumns, TTableName, TTableAlias>] = column.bind({
      key: columnKey,
      name: physicalName,
      tableAlias,
      tableName,
    }) as BoundColumns<TColumns, TTableName, TTableAlias>[keyof BoundColumns<TColumns, TTableName, TTableAlias>];
  }

  return boundColumns;
};

const remapColumn = (boundColumns: Record<string, AnyColumn>, column: AnyColumn): AnyColumn => {
  const columnKey = column.key ?? column.name;
  return columnKey ? (boundColumns[columnKey] ?? column) : column;
};

const remapExpressionInput = (
  boundColumns: Record<string, AnyColumn>,
  value: AnyColumn | DdlFragmentInput,
): AnyColumn | DdlFragmentInput => {
  return isColumnLike(value) ? remapColumn(boundColumns, value) : value;
};

const remapExpressionListInput = (
  boundColumns: Record<string, AnyColumn>,
  value: TableOptions["partitionBy"] | TableOptions["primaryKey"],
) => {
  if (value === undefined) {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map((entry) => remapExpressionInput(boundColumns, entry));
  }

  return remapExpressionInput(boundColumns, value as AnyColumn | DdlFragmentInput);
};

// Reserved column-key names that would otherwise be shadowed by table metadata
// in the `Object.assign({}, boundColumns, tableBase)` layering below. The
// merge silently drops the colliding column refs from the top-level table
// object — they stay reachable via `table.columns[key]`, but `table.<key>`
// returns the metadata. This is a real friction: OpenTelemetry / SigNoz
// trace schemas use `kind` as a standard column name (span kind), so a hard
// `throw` would break legitimate users. Instead we emit a one-shot warning
// per (table, key) pair so the collision is visible without forcing a rename.
const RESERVED_TABLE_KEYS: ReadonlySet<string> = new Set([
  "kind",
  "tableName",
  "originalName",
  "alias",
  "columns",
  "options",
  "$inferSelect",
  "$inferInsert",
]);

// Module-scoped dedup so the same `ckTable(name, columns)` definition doesn't
// log every time the module that defines it is re-imported (multi-test-file
// scenarios). Keyed on `tableName + ":" + collidingKey` for stability across
// hot-reload-free environments.
const warnedReservedKeyCollisions = new Set<string>();

export const ckTable = <TName extends string, TColumns extends Record<string, AnyColumn>>(
  name: TName,
  columns: TColumns,
  options?: TableOptionsInput<TColumns, TName>,
): TableWithColumns<BoundColumns<TColumns, TName>, TName> => {
  const reservedCollisions = Object.keys(columns).filter((key) => RESERVED_TABLE_KEYS.has(key));
  if (reservedCollisions.length > 0) {
    const fresh = reservedCollisions.filter((key) => {
      const dedupKey = `${name}:${key}`;
      if (warnedReservedKeyCollisions.has(dedupKey)) return false;
      warnedReservedKeyCollisions.add(dedupKey);
      return true;
    });
    if (fresh.length > 0) {
      console.warn(
        `[ck-orm] ckTable("${name}") column keys collide with reserved table metadata fields: ${fresh.join(", ")}. ` +
          `The top-level \`table.<key>\` access returns the metadata field; access the column ref via \`table.columns[key]\` instead.`,
      );
    }
  }
  const boundColumns = bindColumns(name, columns);
  const tableBase = {
    kind: "table" as const,
    tableName: name,
    originalName: name,
    alias: undefined,
    columns: boundColumns,
    options: {} as TableOptions,
    $inferSelect: undefined as unknown as InferSelect<BoundColumns<TColumns, TName>>,
    $inferInsert: undefined as unknown as InferInsert<BoundColumns<TColumns, TName>>,
  };
  // Layer the bound columns BENEATH the table metadata. Otherwise a user-named
  // column like `kind` / `tableName` / `columns` / `options` / `$inferSelect`
  // / `$inferInsert` / `alias` / `originalName` overrides the corresponding
  // table marker and downstream code (e.g. ck.eq(`source.kind === "table"`))
  // misidentifies the table.
  const tableWithColumns = Object.assign({}, boundColumns, tableBase);
  const resolvedOptions = typeof options === "function" ? options(tableWithColumns) : (options ?? {});
  tableWithColumns.options = resolvedOptions;
  return tableWithColumns;
};

export const ckAlias = <TTable extends AnyTable, TAlias extends string>(
  table: TTable,
  aliasName: TAlias,
): TableWithColumns<
  BoundColumns<TTable["columns"], TTable["originalName"], TAlias>,
  TTable["tableName"],
  TAlias,
  TTable["originalName"]
> => {
  assertValidSqlIdentifier(aliasName);
  const boundColumns = bindColumns(table.originalName, table.columns, aliasName);
  // `column.key` is the logical schema key. It remains stable even when
  // `column.name` is an explicit database column name such as `user_id`.
  const remap = (column: AnyColumn): AnyColumn =>
    column.key ? (boundColumns[column.key as keyof typeof boundColumns] ?? column) : column;
  const mappedOptions: TableOptions = {
    ...table.options,
    partitionBy: remapExpressionListInput(boundColumns, table.options.partitionBy),
    primaryKey: remapExpressionListInput(boundColumns, table.options.primaryKey),
    orderBy: table.options.orderBy?.map((value) => remapExpressionInput(boundColumns, value)),
    sampleBy: table.options.sampleBy
      ? remapExpressionInput(boundColumns, table.options.sampleBy)
      : table.options.sampleBy,
    versionColumn: table.options.versionColumn ? remap(table.options.versionColumn) : table.options.versionColumn,
  };
  const aliasedTable = {
    ...table,
    alias: aliasName,
    columns: boundColumns,
    options: mappedOptions,
  };

  return Object.assign(aliasedTable, boundColumns) as unknown as TableWithColumns<
    BoundColumns<TTable["columns"], TTable["originalName"], TAlias>,
    TTable["tableName"],
    TAlias,
    TTable["originalName"]
  >;
};

// Tables are immutable, so the rendered identifier fragment is stable for
// the table's lifetime. WeakMap-keyed cache means a table that's garbage-
// collected drops its fragment with it.
const tableIdentifierCache = new WeakMap<AnyTable, SQLFragment>();

export const renderTableIdentifier = (table: AnyTable): SQLFragment => {
  const cached = tableIdentifierCache.get(table);
  if (cached) return cached;
  const fragment = table.alias
    ? sql.identifier({ table: table.originalName, as: table.alias })
    : sql.identifier({ table: table.originalName });
  tableIdentifierCache.set(table, fragment);
  return fragment;
};
