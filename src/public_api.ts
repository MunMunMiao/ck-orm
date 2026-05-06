import {
  aggregateFunction,
  array,
  bfloat16,
  bool,
  date,
  date32,
  dateTime,
  dateTime64,
  decimal,
  dynamic,
  enum8,
  enum16,
  fixedString,
  float32,
  float64,
  int8,
  int16,
  int32,
  int64,
  ipv4,
  ipv6,
  json,
  lineString,
  lowCardinality,
  map,
  multiLineString,
  multiPolygon,
  nested,
  nullable,
  point,
  polygon,
  qbit,
  ring,
  simpleAggregateFunction,
  string,
  time,
  time64,
  tuple,
  uint8,
  uint16,
  uint32,
  uint64,
  uuid,
  variant,
} from "./columns";
import { fn } from "./functions";
import {
  and,
  asc,
  between,
  contains,
  containsIgnoreCase,
  createSessionId,
  decodeRow,
  desc,
  endsWith,
  endsWithIgnoreCase,
  eq,
  exists,
  expr,
  gt,
  gte,
  has,
  hasAll,
  hasAny,
  hasSubstr,
  ilike,
  inArray,
  isNotNull,
  isNull,
  like,
  lt,
  lte,
  ne,
  not,
  notExists,
  notIlike,
  notInArray,
  notLike,
  or,
  startsWith,
  startsWithIgnoreCase,
} from "./query";
import { type SQLFragment, sql } from "./sql";

type CkNamespace = {
  fn: typeof fn;
  expr: typeof expr;
  createSessionId: typeof createSessionId;
  decodeRow: typeof decodeRow;
  and: typeof and;
  or: typeof or;
  not: typeof not;
  eq: typeof eq;
  ne: typeof ne;
  gt: typeof gt;
  gte: typeof gte;
  lt: typeof lt;
  lte: typeof lte;
  between: typeof between;
  inArray: typeof inArray;
  isNull: typeof isNull;
  isNotNull: typeof isNotNull;
  notInArray: typeof notInArray;
  contains: typeof contains;
  startsWith: typeof startsWith;
  endsWith: typeof endsWith;
  containsIgnoreCase: typeof containsIgnoreCase;
  startsWithIgnoreCase: typeof startsWithIgnoreCase;
  endsWithIgnoreCase: typeof endsWithIgnoreCase;
  like: typeof like;
  ilike: typeof ilike;
  notLike: typeof notLike;
  notIlike: typeof notIlike;
  has: typeof has;
  hasAll: typeof hasAll;
  hasAny: typeof hasAny;
  hasSubstr: typeof hasSubstr;
  exists: typeof exists;
  notExists: typeof notExists;
  asc: typeof asc;
  desc: typeof desc;
};

type CkSqlNamespace = {
  <TData = unknown>(strings: TemplateStringsArray, ...values: unknown[]): SQLFragment<TData>;
  join(parts: readonly SQLFragment<unknown>[], separator?: string | SQLFragment<unknown>): SQLFragment;
  identifier(
    value:
      | string
      | {
          readonly table?: string;
          readonly column?: string;
          readonly as?: string;
        },
  ): SQLFragment;
  /**
   * See `sql.decimal` — wraps the expression in `CAST(... AS Decimal(P, S))`
   * and decodes as `string` via `toStringValue`. Chain `.mapWith(...)` if you
   * need a different decoded shape.
   */
  decimal(expression: unknown, precision: number, scale: number): SQLFragment<string>;
};

type CkTypeNamespace = {
  aggregateFunction: typeof aggregateFunction;
  array: typeof array;
  bfloat16: typeof bfloat16;
  bool: typeof bool;
  date: typeof date;
  date32: typeof date32;
  dateTime: typeof dateTime;
  dateTime64: typeof dateTime64;
  decimal: typeof decimal;
  dynamic: typeof dynamic;
  enum8: typeof enum8;
  enum16: typeof enum16;
  fixedString: typeof fixedString;
  float32: typeof float32;
  float64: typeof float64;
  int8: typeof int8;
  int16: typeof int16;
  int32: typeof int32;
  int64: typeof int64;
  ipv4: typeof ipv4;
  ipv6: typeof ipv6;
  json: typeof json;
  lineString: typeof lineString;
  lowCardinality: typeof lowCardinality;
  map: typeof map;
  multiLineString: typeof multiLineString;
  multiPolygon: typeof multiPolygon;
  nested: typeof nested;
  nullable: typeof nullable;
  point: typeof point;
  polygon: typeof polygon;
  qbit: typeof qbit;
  ring: typeof ring;
  simpleAggregateFunction: typeof simpleAggregateFunction;
  string: typeof string;
  time: typeof time;
  time64: typeof time64;
  tuple: typeof tuple;
  uint8: typeof uint8;
  uint16: typeof uint16;
  uint32: typeof uint32;
  uint64: typeof uint64;
  uuid: typeof uuid;
  variant: typeof variant;
};

export type { AnyColumn, Column } from "./columns";
export type {
  ClickHouseORMError,
  ClickHouseORMErrorKind,
  ClickHouseORMExecutionState,
  DecodeError,
} from "./errors";
export { isClickHouseORMError, isDecodeError } from "./errors";
export type { JsonPathSegment } from "./functions";
export { fn } from "./functions";
export type {
  ClickHouseORMInstrumentation,
  ClickHouseORMLogger,
  ClickHouseORMLogLevel,
  ClickHouseORMQueryErrorEvent,
  ClickHouseORMQueryEvent,
  ClickHouseORMQueryResultEvent,
  ClickHouseORMQueryStatistics,
  ClickHouseORMTracingOptions,
} from "./observability";
export type { CompiledQuery, CompiledQueryMetadata } from "./query";
export type { Order, Predicate, Selection } from "./query-shared";
export {
  type ClickHouseBaseQueryOptions,
  type ClickHouseClientConfig,
  type ClickHouseEndpointOptions,
  type ClickHouseKnownSettingName,
  type ClickHouseKnownSettings,
  type ClickHouseORMClient,
  type ClickHouseQueryOptions,
  type ClickHouseSettings,
  type ClickHouseSettingValue,
  type ClickHouseStreamOptions,
  type CreateTemporaryTableOptions,
  clickhouseClient,
  type Session,
} from "./runtime";
export type { AnyTable, Table } from "./schema";
export {
  ckAlias,
  ckTable,
  type InferInsertModel,
  type InferInsertSchema,
  type InferSelectModel,
  type InferSelectSchema,
} from "./schema";
export type { SQLFragment } from "./sql";

const ckSqlTaggedTemplateError =
  '[ck-orm] ckSql only supports tagged-template usage. Use ckSql`...` instead of ckSql("...").';

const createCkSqlNamespace = (): CkSqlNamespace => {
  const ckSqlFactory = (<TData = unknown>(strings: TemplateStringsArray, ...values: unknown[]): SQLFragment<TData> => {
    if (Array.isArray(strings) && Array.isArray(strings.raw)) return sql<TData>(strings, ...values);
    throw new TypeError(ckSqlTaggedTemplateError);
  }) as CkSqlNamespace;

  ckSqlFactory.join = (parts, separator = ", ") => sql.join(parts, separator);
  ckSqlFactory.identifier = (value) => sql.identifier(value);
  ckSqlFactory.decimal = (expression: unknown, precision: number, scale: number) =>
    sql.decimal(expression, precision, scale);

  return ckSqlFactory;
};

export const ckSql = createCkSqlNamespace();

export const ckType: CkTypeNamespace = {
  aggregateFunction,
  array,
  bfloat16,
  bool,
  date,
  date32,
  dateTime,
  dateTime64,
  decimal,
  dynamic,
  enum8,
  enum16,
  fixedString,
  float32,
  float64,
  int8,
  int16,
  int32,
  int64,
  ipv4,
  ipv6,
  json,
  lineString,
  lowCardinality,
  map,
  multiLineString,
  multiPolygon,
  nested,
  nullable,
  point,
  polygon,
  qbit,
  ring,
  simpleAggregateFunction,
  string,
  time,
  time64,
  tuple,
  uint8,
  uint16,
  uint32,
  uint64,
  uuid,
  variant,
};

export const ck: CkNamespace = {
  fn,
  expr,
  createSessionId,
  decodeRow,
  and,
  or,
  not,
  eq,
  ne,
  gt,
  gte,
  lt,
  lte,
  between,
  inArray,
  isNull,
  isNotNull,
  notInArray,
  contains,
  startsWith,
  endsWith,
  containsIgnoreCase,
  startsWithIgnoreCase,
  endsWithIgnoreCase,
  like,
  ilike,
  notLike,
  notIlike,
  has,
  hasAll,
  hasAny,
  hasSubstr,
  exists,
  notExists,
  asc,
  desc,
};
