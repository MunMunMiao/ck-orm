# `ck-orm` E2E API Matrix

This matrix records coverage from the real ClickHouse E2E suite only. It does not include pure type-level tests.

## Runtime API

| API | Coverage location |
| --- | --- |
| `clickhouseClient` | `dataset-smoke.e2e.test.ts`, `observability.e2e.test.ts` |
| `count` | `count-and-dynamic-filters.e2e.test.ts` |
| `select` | most E2E files |
| `insert` | `write-paths.e2e.test.ts` |
| `withSettings` | `session-cdc-stream.e2e.test.ts` |
| `execute` | `query-basics.e2e.test.ts`, `advanced-clickhouse-sql.e2e.test.ts`, `observability.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |
| `stream` | `session-cdc-stream.e2e.test.ts` |
| `command` | `advanced-clickhouse-sql.e2e.test.ts`, `session-cdc-stream.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |
| `ping` | `query-basics.e2e.test.ts` |
| `replicasStatus` | `query-basics.e2e.test.ts` |
| `insertJsonEachRow` | `write-paths.e2e.test.ts`, `session-cdc-stream.e2e.test.ts`, `seed.ts` |
| `runInSession` | `session-cdc-stream.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |
| `registerTempTable` | `session-cdc-stream.e2e.test.ts` |
| `createTemporaryTable` | `session-cdc-stream.e2e.test.ts`, `injection-identifiers.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |
| `createSessionId` | `query-basics.e2e.test.ts`, `session-cdc-stream.e2e.test.ts` |
| `decodeRow` | `query-basics.e2e.test.ts` |
| `expr` | `operators.e2e.test.ts`, `session-cdc-stream.e2e.test.ts` |

## Query operators and builder

| API | Coverage location |
| --- | --- |
| `and` | `operators.e2e.test.ts` |
| `or` | `operators.e2e.test.ts` |
| `not` | `operators.e2e.test.ts` |
| `eq` | `operators.e2e.test.ts`, `builder-analytics.e2e.test.ts`, `session-cdc-stream.e2e.test.ts` |
| `ne` | `operators.e2e.test.ts` |
| `gt` | `query-basics.e2e.test.ts`, `operators.e2e.test.ts` |
| `gte` | `operators.e2e.test.ts` |
| `has` | `operators.e2e.test.ts` |
| `hasAll` | `operators.e2e.test.ts` |
| `hasAny` | `operators.e2e.test.ts` |
| `lt` | `operators.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `lte` | `operators.e2e.test.ts` |
| `like` | `injection-values.e2e.test.ts` |
| `notLike` | `injection-values.e2e.test.ts` |
| `ilike` | `injection-values.e2e.test.ts` |
| `notIlike` | `injection-values.e2e.test.ts` |
| `escapeLike` | `injection-values.e2e.test.ts` |
| `between` | `operators.e2e.test.ts` |
| `inArray` | `operators.e2e.test.ts`, `builder-analytics.e2e.test.ts`, `injection-values.e2e.test.ts` |
| `notInArray` | `operators.e2e.test.ts` |
| `exists` | `operators.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `notExists` | `operators.e2e.test.ts` |
| `asc` | `operators.e2e.test.ts` |
| `desc` | `query-basics.e2e.test.ts`, `operators.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `from` | `query-basics.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `innerJoin` | `builder-analytics.e2e.test.ts`, `functions.e2e.test.ts` |
| `leftJoin` | `session-cdc-stream.e2e.test.ts` |
| `where` | most E2E files |
| `groupBy` | `builder-analytics.e2e.test.ts` |
| `having` | `builder-analytics.e2e.test.ts` |
| `orderBy` | most E2E files |
| `limit` | `query-basics.e2e.test.ts`, `builder-analytics.e2e.test.ts`, `session-cdc-stream.e2e.test.ts` |
| `offset` | `query-basics.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `final` | `builder-analytics.e2e.test.ts`, `session-cdc-stream.e2e.test.ts`, `dataset-smoke.e2e.test.ts` |
| `limitBy` | `builder-analytics.e2e.test.ts` |
| `$with` | `builder-analytics.e2e.test.ts` |
| `with` | `builder-analytics.e2e.test.ts` |
| `as` | `builder-analytics.e2e.test.ts`, `functions.e2e.test.ts`, `query-basics.e2e.test.ts` |
| `iterator` | `session-cdc-stream.e2e.test.ts` |

## SQL API

| API | Coverage location |
| --- | --- |
| `sql('...')` | `query-basics.e2e.test.ts` |
| `` sql`...` `` | most E2E files |
| `sql.raw` | `query-basics.e2e.test.ts`, `injection-raw-sql.e2e.test.ts` |
| `sql.join` | `query-basics.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |
| `sql.identifier` | `query-basics.e2e.test.ts`, `session-cdc-stream.e2e.test.ts`, `injection-identifiers.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |

## Function API

| API | Coverage location |
| --- | --- |
| `fn.call` | `query-basics.e2e.test.ts`, `functions.e2e.test.ts` |
| `fn.withParams` | `functions.e2e.test.ts`, `injection-identifiers.e2e.test.ts` |
| `fn.toString` | `functions.e2e.test.ts` |
| `fn.toDate` | `functions.e2e.test.ts` |
| `fn.toDateTime` | `functions.e2e.test.ts` |
| `fn.toStartOfMonth` | `functions.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `fn.count` | `dataset-smoke.e2e.test.ts`, `functions.e2e.test.ts` |
| `fn.countIf` | `functions.e2e.test.ts` |
| `fn.sum` | `functions.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `fn.sumIf` | `functions.e2e.test.ts` |
| `fn.avg` | `functions.e2e.test.ts` |
| `fn.min` | `functions.e2e.test.ts` |
| `fn.max` | `functions.e2e.test.ts` |
| `fn.uniqExact` | `functions.e2e.test.ts` |
| `fn.coalesce` | `functions.e2e.test.ts` |
| `fn.tuple` | `functions.e2e.test.ts` |
| `fn.arrayZip` | `functions.e2e.test.ts` |
| `fn.not` | `functions.e2e.test.ts` |
| `tableFn.call` | `functions.e2e.test.ts`, `injection-identifiers.e2e.test.ts` |

## Schema DSL

| API | Coverage location |
| --- | --- |
| `chTable` | `schema-roundtrip.e2e.test.ts` |
| `alias` | `schema-roundtrip.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `int8/int16/int32/int64` | `schema-roundtrip.e2e.test.ts` |
| `uint8/uint16/uint32/uint64` | `schema-roundtrip.e2e.test.ts` |
| `float32/float64/bfloat16` | `schema-roundtrip.e2e.test.ts` |
| `string/fixedString` | `schema-roundtrip.e2e.test.ts` |
| `decimal` | `schema-roundtrip.e2e.test.ts` |
| `date/date32/time/time64/dateTime/dateTime64` | `schema-roundtrip.e2e.test.ts` |
| `bool` | `schema-roundtrip.e2e.test.ts` |
| `uuid` | `schema-roundtrip.e2e.test.ts` |
| `ipv4/ipv6` | `schema-roundtrip.e2e.test.ts` |
| `json` | `schema-roundtrip.e2e.test.ts` |
| `dynamic` | `schema-roundtrip.e2e.test.ts` |
| `qbit` | `schema-roundtrip.e2e.test.ts` |
| `enum8/enum16` | `schema-roundtrip.e2e.test.ts` |
| `nullable` | `schema-roundtrip.e2e.test.ts` |
| `array` | `schema-roundtrip.e2e.test.ts` |
| `tuple` | `schema-roundtrip.e2e.test.ts` |
| `map` | `schema-roundtrip.e2e.test.ts` |
| `variant` | `schema-roundtrip.e2e.test.ts` |
| `lowCardinality` | `schema-roundtrip.e2e.test.ts` |
| `nested` | `schema-roundtrip.e2e.test.ts` |
| `aggregateFunction` | `schema-roundtrip.e2e.test.ts` |
| `simpleAggregateFunction` | `schema-roundtrip.e2e.test.ts` |
| `point/ring/lineString/multiLineString/polygon/multiPolygon` | `schema-roundtrip.e2e.test.ts` |

## Observability

| API | Coverage location |
| --- | --- |
| `logger` | `observability.e2e.test.ts` |
| `tracing` | `observability.e2e.test.ts` |
| `instrumentation` | `observability.e2e.test.ts` |

## Error contracts

| Scenario | Coverage location |
| --- | --- |
| invalid SQL syntax | `error-contracts.e2e.test.ts` |
| missing table | `error-contracts.e2e.test.ts` |
| accessing a temporary table after session end | `error-contracts.e2e.test.ts` |
| `insertJsonEachRow()` type mismatch | `error-contracts.e2e.test.ts` |
| `system.query_log` failure-stage validation | `error-contracts.e2e.test.ts` |

## Security and injection contexts

| Scenario | Coverage location |
| --- | --- |
| classic payloads in builder equality filters | `sql-injection.e2e.test.ts` |
| classic payloads in raw template literals | `sql-injection.e2e.test.ts` |
| Unicode line separators in parameter values | `sql-injection.e2e.test.ts`, `injection-values.e2e.test.ts` |
| set-membership payloads | `injection-values.e2e.test.ts` |
| LIKE / ILIKE payloads | `injection-values.e2e.test.ts` |
| literal wildcard matching via `escapeLike()` | `injection-values.e2e.test.ts` |
| string and object identifier rejection | `injection-identifiers.e2e.test.ts` |
| alias, temporary-table, and function-name rejection | `injection-identifiers.e2e.test.ts` |
| stacked raw SQL rejection and no-mutation checks | `injection-raw-sql.e2e.test.ts` |
| semicolons inside string literals and comments | `injection-raw-sql.e2e.test.ts` |
| `query_params`, `query_id`, and `session_id` validation | `injection-transport-and-boundaries.e2e.test.ts` |
| per-request `session_timeout` and continued-session `session_check` | `injection-transport-and-boundaries.e2e.test.ts` |
| `createTemporaryTable()` trusted-only boundary | `injection-transport-and-boundaries.e2e.test.ts` |
| `sql.join()` separator validation | `injection-transport-and-boundaries.e2e.test.ts` |
