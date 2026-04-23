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
  createSessionId,
  decodeRow,
  desc,
  eq,
  escapeLike,
  exists,
  expr,
  gt,
  gte,
  has,
  hasAll,
  hasAny,
  ilike,
  inArray,
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
} from "./query";
import { sql } from "./sql";

type CkNamespace = {
  sql: typeof sql;
  fn: typeof fn;
  expr: typeof expr;
  createSessionId: typeof createSessionId;
  decodeRow: typeof decodeRow;
  escapeLike: typeof escapeLike;
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
  notInArray: typeof notInArray;
  like: typeof like;
  ilike: typeof ilike;
  notLike: typeof notLike;
  notIlike: typeof notIlike;
  has: typeof has;
  hasAll: typeof hasAll;
  hasAny: typeof hasAny;
  exists: typeof exists;
  notExists: typeof notExists;
  asc: typeof asc;
  desc: typeof desc;
};

type ChTypeNamespace = {
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
  ClickHouseOrmError,
  ClickHouseOrmErrorKind,
  ClickHouseOrmExecutionState,
  DecodeError,
} from "./errors";
export { isClickHouseOrmError, isDecodeError } from "./errors";
export { fn } from "./functions";
export type {
  ClickHouseOrmInstrumentation,
  ClickHouseOrmLogger,
  ClickHouseOrmLogLevel,
  ClickHouseOrmQueryErrorEvent,
  ClickHouseOrmQueryEvent,
  ClickHouseOrmQueryResultEvent,
  ClickHouseOrmTracingOptions,
} from "./observability";
export type { CompiledQuery, CompiledQueryMetadata } from "./query";
export type { Order, Predicate, Selection } from "./query-shared";
export {
  type ClickHouseBaseQueryOptions,
  type ClickHouseClientConfig,
  type ClickHouseEndpointOptions,
  type ClickHouseQueryOptions,
  type ClickHouseStreamOptions,
  type CreateTemporaryTableOptions,
  clickhouseClient,
  type Session,
} from "./runtime";
export type { AnyTable, Table } from "./schema";
export {
  alias,
  chTable,
  type InferInsertModel,
  type InferInsertSchema,
  type InferSelectModel,
  type InferSelectSchema,
} from "./schema";

export const chType: ChTypeNamespace = {
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
  sql,
  fn,
  expr,
  createSessionId,
  decodeRow,
  escapeLike,
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
  notInArray,
  like,
  ilike,
  notLike,
  notIlike,
  has,
  hasAll,
  hasAny,
  exists,
  notExists,
  asc,
  desc,
};
