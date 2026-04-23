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

export type { AnyColumn, Column } from "./columns";
export {
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
