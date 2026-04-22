import type { AnyColumn, Column, DdlFragmentInput } from "./columns";
import { type SQLFragment, sql } from "./sql";

type InferSelect<TColumns extends Record<string, AnyColumn>> = {
  [K in keyof TColumns]: TColumns[K] extends Column<infer TData, string> ? TData : never;
};

type InferInsert<TColumns extends Record<string, AnyColumn>> = InferSelect<TColumns>;

type BoundColumns<
  TColumns extends Record<string, AnyColumn>,
  TTableName extends string,
  TTableAlias extends string | undefined = undefined,
> = {
  [K in keyof TColumns]: TColumns[K] extends Column<infer TData, infer TSqlType, string | undefined, string | undefined>
    ? Column<TData, TSqlType, TTableName, TTableAlias>
    : never;
};

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

  for (const [columnName, column] of Object.entries(columns)) {
    boundColumns[columnName as keyof BoundColumns<TColumns, TTableName, TTableAlias>] = column.bind({
      name: columnName,
      tableAlias,
      tableName,
    }) as BoundColumns<TColumns, TTableName, TTableAlias>[keyof BoundColumns<TColumns, TTableName, TTableAlias>];
  }

  return boundColumns;
};

const isColumnLike = (value: unknown): value is AnyColumn => {
  return typeof value === "object" && value !== null && "kind" in value && value.kind === "column";
};

const remapColumn = (boundColumns: Record<string, AnyColumn>, column: AnyColumn): AnyColumn => {
  return column.name ? (boundColumns[column.name] ?? column) : column;
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

export const chTable = <TName extends string, TColumns extends Record<string, AnyColumn>>(
  name: TName,
  columns: TColumns,
  options?: TableOptionsInput<TColumns, TName>,
): TableWithColumns<BoundColumns<TColumns, TName>, TName> => {
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
  const tableWithColumns = Object.assign(tableBase, boundColumns);
  const resolvedOptions = typeof options === "function" ? options(tableWithColumns) : (options ?? {});
  const table = {
    ...tableWithColumns,
    options: resolvedOptions,
  };

  return Object.assign(table, boundColumns);
};

export const alias = <TTable extends AnyTable, TAlias extends string>(
  table: TTable,
  aliasName: TAlias,
): TableWithColumns<
  BoundColumns<TTable["columns"], TTable["originalName"], TAlias>,
  TTable["tableName"],
  TAlias,
  TTable["originalName"]
> => {
  const boundColumns = bindColumns(table.originalName, table.columns, aliasName);
  // bindColumns guarantees `column.name === <schema key>` for every entry, so
  // `boundColumns[someColumn.name]` is the canonical lookup for finding the
  // re-bound counterpart of any column carried over from `table.options`.
  const remap = (column: AnyColumn): AnyColumn =>
    column.name ? (boundColumns[column.name as keyof typeof boundColumns] ?? column) : column;
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

export const renderTableIdentifier = (table: AnyTable) => {
  if (table.alias) {
    return sql.identifier({
      table: table.originalName,
      as: table.alias,
    });
  }

  return sql.identifier({
    table: table.originalName,
  });
};
