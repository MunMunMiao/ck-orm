# `ck-orm` E2E API Matrix

This matrix records the public API coverage contract. Runtime behavior rows point to the real ClickHouse E2E suite; surface and version-gated rows point to unit/type coverage when a real-server assertion would be redundant, unstable, or outside the bundled `clickhouse/clickhouse-server:26.3` target.

## Public package surface

| API | Coverage location |
| --- | --- |
| root runtime exports: `ck`, `fn`, `ckSql`, `ckType`, `ckTable`, `ckAlias`, `clickhouseClient`, `isClickHouseORMError`, `isDecodeError` | `src/public_api.test.ts` |
| root type exports: columns, tables, query selections, compiled query metadata, runtime config/options/settings/session, observability events, error contracts, schema inference helpers, `SQLFragment`, `JsonPathSegment` | `src/public_api.test.ts`, `src/public_api.typecheck.ts`, `src/type-scenarios/public-api-matrix.typecheck.ts` |
| namespace key guards for `ck`, `fn`, `fn.table`, `ckSql`, `ckType` | `src/public_api.test.ts`, `src/type-scenarios/public-api-matrix.typecheck.ts` |

## Runtime API

| API | Coverage location |
| --- | --- |
| `clickhouseClient` | `dataset-smoke.e2e.test.ts`, `observability.e2e.test.ts`, `transport-contracts.e2e.test.ts` |
| `count`, `count().toUnsafe`, `count().toSafe`, `count().toMixed` | `count-and-dynamic-filters.e2e.test.ts` |
| `select` | most E2E files |
| `insert` | `write-paths.e2e.test.ts` |
| `withSettings` | `session-cdc-stream.e2e.test.ts` |
| `execute` | `query-basics.e2e.test.ts`, `advanced-clickhouse-sql.e2e.test.ts`, `observability.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |
| `stream` | `session-cdc-stream.e2e.test.ts`, `session-concurrency.e2e.test.ts` |
| `command` | `advanced-clickhouse-sql.e2e.test.ts`, `session-cdc-stream.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |
| `ping` | `query-basics.e2e.test.ts` |
| `replicasStatus` | `query-basics.e2e.test.ts` |
| `insertJsonEachRow` | `write-paths.e2e.test.ts`, `session-cdc-stream.e2e.test.ts`, `seed.ts` |
| `runInSession` | `session-cdc-stream.e2e.test.ts`, `session-security.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |
| `registerTempTable` | `session-cdc-stream.e2e.test.ts` |
| `createTemporaryTable` | `session-cdc-stream.e2e.test.ts`, `injection-identifiers.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |
| `ck.createSessionId` | `query-basics.e2e.test.ts`, `session-cdc-stream.e2e.test.ts` |
| `ck.decodeRow` | `query-basics.e2e.test.ts` |
| `ck.expr` | `operators.e2e.test.ts`, `session-cdc-stream.e2e.test.ts` |

## Transport behavior

| Scenario | Coverage location |
| --- | --- |
| `databaseUrl` credential parsing and stripped outgoing URLs | `transport-contracts.e2e.test.ts` |
| merged `http_headers` with `Authorization` precedence | `transport-contracts.e2e.test.ts` |
| repeated `role` query parameter propagation | `transport-contracts.e2e.test.ts` |
| real gzip response compression with `compression.response` | `transport-contracts.e2e.test.ts` |

## Session behavior

| Scenario | Coverage location |
| --- | --- |
| same explicit `session_id` is serialized by default | `session-concurrency.e2e.test.ts` |
| different `session_id` values remain parallel | `session-concurrency.e2e.test.ts` |
| client default `session_id` is serialized too | `session-concurrency.e2e.test.ts` |
| same-session raw streams hold the slot until explicitly closed | `session-concurrency.e2e.test.ts` |
| raising `session_max_concurrent_requests` above `1` can surface `SESSION_IS_LOCKED` | `session-concurrency.e2e.test.ts` |
| nested child sessions use distinct ids and cannot reuse ancestor ids | `session-security.e2e.test.ts` |
| sibling/child sessions stay isolated for temporary tables | `session-security.e2e.test.ts` |

## Query operators and builder

| API | Coverage location |
| --- | --- |
| `ck.and` | `operators.e2e.test.ts` |
| `ck.or` | `operators.e2e.test.ts` |
| `ck.not` | `operators.e2e.test.ts` |
| `ck.eq` | `operators.e2e.test.ts`, `builder-analytics.e2e.test.ts`, `session-cdc-stream.e2e.test.ts` |
| `ck.ne` | `operators.e2e.test.ts` |
| `ck.gt` | `query-basics.e2e.test.ts`, `operators.e2e.test.ts` |
| `ck.gte` | `operators.e2e.test.ts` |
| `ck.has` | `operators.e2e.test.ts` |
| `ck.hasAll` | `operators.e2e.test.ts` |
| `ck.hasAny` | `operators.e2e.test.ts` |
| `ck.hasSubstr` | `operators.e2e.test.ts` |
| `ck.lt` | `operators.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `ck.lte` | `operators.e2e.test.ts` |
| `ck.contains` | `injection-values.e2e.test.ts` |
| `ck.startsWith` | `injection-values.e2e.test.ts` |
| `ck.endsWith` | `injection-values.e2e.test.ts` |
| `ck.containsIgnoreCase` | `injection-values.e2e.test.ts` |
| `ck.startsWithIgnoreCase` | `injection-values.e2e.test.ts` |
| `ck.endsWithIgnoreCase` | `injection-values.e2e.test.ts` |
| `ck.like` | `injection-values.e2e.test.ts` |
| `ck.notLike` | `injection-values.e2e.test.ts` |
| `ck.ilike` | `injection-values.e2e.test.ts` |
| `ck.notIlike` | `injection-values.e2e.test.ts` |
| `ck.between` | `operators.e2e.test.ts` |
| `ck.inArray` | `operators.e2e.test.ts`, `builder-analytics.e2e.test.ts`, `injection-values.e2e.test.ts` |
| `ck.notInArray` | `operators.e2e.test.ts` |
| `ck.exists` | `operators.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `ck.notExists` | `operators.e2e.test.ts` |
| `ck.asc` | `operators.e2e.test.ts` |
| `ck.desc` | `query-basics.e2e.test.ts`, `operators.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
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
| `` ckSql`...` `` | most E2E files |
| `ckSql.join` | `query-basics.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |
| `ckSql.identifier` | `query-basics.e2e.test.ts`, `session-cdc-stream.e2e.test.ts`, `injection-identifiers.e2e.test.ts`, `injection-transport-and-boundaries.e2e.test.ts` |
| `ckSql.decimal` (CAST(... AS Decimal(P, S)) precision wrapper) | `builder-analytics.e2e.test.ts` |

## Function API

| API | Coverage location |
| --- | --- |
| `fn.call` | `query-basics.e2e.test.ts`, `functions.e2e.test.ts` |
| `fn.withParams` | `functions.e2e.test.ts`, `injection-identifiers.e2e.test.ts` |
| `fn.toString` | `functions.e2e.test.ts` |
| `fn.toDate`, `fn.toDate32` | `functions.e2e.test.ts` |
| `fn.toDateTime`, `fn.toDateTime32`, `fn.toDateTime64` | `functions.e2e.test.ts` |
| `fn.toUnixTimestamp`, `fn.toUnixTimestamp64Second`, `fn.toUnixTimestamp64Milli`, `fn.toUnixTimestamp64Micro`, `fn.toUnixTimestamp64Nano` | `functions.e2e.test.ts` |
| `fn.fromUnixTimestamp`, `fn.fromUnixTimestamp64Second`, `fn.fromUnixTimestamp64Milli`, `fn.fromUnixTimestamp64Micro`, `fn.fromUnixTimestamp64Nano` | `functions.e2e.test.ts` |
| `fn.toDecimal32`, `fn.toDecimal64`, `fn.toDecimal128`, `fn.toDecimal256` | `builder-analytics.e2e.test.ts` |
| `fn.toStartOfMonth` | `functions.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `fn.count`, `fn.count().toUnsafe`, `fn.count().toSafe`, `fn.count().toMixed` | `dataset-smoke.e2e.test.ts`, `functions.e2e.test.ts` |
| `fn.countIf`, `fn.countIf().toUnsafe`, `fn.countIf().toSafe`, `fn.countIf().toMixed` | `functions.e2e.test.ts` |
| `fn.sum` | `functions.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `fn.sumIf` | `functions.e2e.test.ts` |
| `fn.avg` | `functions.e2e.test.ts` |
| `fn.min` | `functions.e2e.test.ts` |
| `fn.max` | `functions.e2e.test.ts` |
| `fn.uniqExact`, `fn.uniqExact().toUnsafe`, `fn.uniqExact().toSafe`, `fn.uniqExact().toMixed` | `functions.e2e.test.ts` |
| `fn.coalesce` | `functions.e2e.test.ts` |
| `fn.jsonExtract` | `functions.e2e.test.ts` |
| `fn.tuple` | `functions.e2e.test.ts` |
| `fn.arrayZip` | `functions.e2e.test.ts` |
| `fn.arrayJoin` | `functions.e2e.test.ts` |
| `fn.tupleElement` | `functions.e2e.test.ts` |
| `fn.array` | `functions.e2e.test.ts` |
| `fn.arrayConcat` | `functions.e2e.test.ts` |
| `fn.arrayElement` | `functions.e2e.test.ts` |
| `fn.arrayElementOrNull` | `functions.e2e.test.ts` |
| `fn.arraySlice` | `functions.e2e.test.ts` |
| `fn.arrayFlatten` | `functions.e2e.test.ts` |
| `fn.arrayIntersect` | `functions.e2e.test.ts` |
| `fn.arrayExists`, `fn.arrayAll`, `fn.arrayCount` | `functions.e2e.test.ts` |
| `fn.arrayFilter`, `fn.arrayMap`, `fn.arrayFirst`, `fn.arrayFirstIndex`, `fn.arrayFirstOrNull`, `fn.arrayLast`, `fn.arrayLastIndex`, `fn.arrayLastOrNull` | `functions.e2e.test.ts` |
| `fn.arrayFill`, `fn.arrayReverseFill`, `fn.arraySplit`, `fn.arrayReverseSplit`, `fn.arrayFold` | `functions.e2e.test.ts` |
| `fn.arrayAvg`, `fn.arraySum`, `fn.arrayProduct`, `fn.arrayMax`, `fn.arrayMin`, `fn.arrayDotProduct`, `fn.arrayReduce` | `functions.e2e.test.ts` |
| `fn.arrayJaccardIndex`, `fn.arrayLevenshteinDistance`, `fn.arrayROCAUC` | `functions.e2e.test.ts` |
| `fn.arrayCompact`, `fn.arrayDistinct`, `fn.arrayDifference`, `fn.arrayCumSum`, `fn.arrayEnumerate`, `fn.arrayEnumerateDense`, `fn.arrayEnumerateUniq` | `functions.e2e.test.ts` |
| `fn.arraySort`, `fn.arrayReverseSort`, `fn.arrayReverse`, `fn.arrayRotateLeft`, `fn.arrayRotateRight`, `fn.arrayShiftLeft`, `fn.arrayShiftRight` | `functions.e2e.test.ts` |
| `fn.arrayPartialSort`, `fn.arrayPartialReverseSort`, `fn.arrayShuffle`, `fn.arrayPartialShuffle`, `fn.arrayRandomSample` | `functions.e2e.test.ts` structural assertions |
| `fn.arrayExcept`, `fn.arrayRemove`, `fn.arrayResize`, `fn.arrayUniq`, `fn.arrayUnion`, `fn.arraySymmetricDifference` | `functions.e2e.test.ts` |
| `fn.arrayShingles`, `fn.arrayWithConstant`, `fn.arrayPopBack`, `fn.arrayPopFront`, `fn.arrayPushBack`, `fn.arrayPushFront`, `fn.arrayZipUnaligned` | `functions.e2e.test.ts` |
| `fn.empty`, `fn.emptyArrayDate`, `fn.emptyArrayDateTime`, `fn.emptyArrayFloat32`, `fn.emptyArrayFloat64`, `fn.emptyArrayInt8`, `fn.emptyArrayInt16`, `fn.emptyArrayInt32`, `fn.emptyArrayInt64`, `fn.emptyArrayString`, `fn.emptyArrayUInt8`, `fn.emptyArrayUInt16`, `fn.emptyArrayUInt32`, `fn.emptyArrayUInt64`, `fn.emptyArrayToSingle` | `functions.e2e.test.ts` |
| `fn.has`, `fn.hasAll`, `fn.hasAny`, `fn.hasSubstr`, `fn.indexOfAssumeSorted`, `fn.range`, `fn.replicate`, `fn.kql_array_sort_asc`, `fn.kql_array_sort_desc`, `fn.reverse` | `functions.e2e.test.ts` |
| `fn.indexOf` | `functions.e2e.test.ts` |
| `fn.length` | `functions.e2e.test.ts` |
| `fn.notEmpty` | `functions.e2e.test.ts` |
| `fn.not` | `functions.e2e.test.ts` |
| `fn.table.call` | `functions.e2e.test.ts`, `injection-identifiers.e2e.test.ts` |
| version-gated or long-tail array helpers: `fn.arrayAUCPR`, `fn.arrayAutocorrelation`, `fn.arrayCumSumNonNegative`, `fn.arrayEnumerateDenseRanked`, `fn.arrayEnumerateUniqRanked`, `fn.arrayLevenshteinDistanceWeighted`, `fn.arrayNormalizedGini`, `fn.arrayReduceInRanges`, `fn.arraySimilarity`, `fn.arrayTranspose` | `src/functions.test.ts`, `src/type-scenarios/public-api-matrix.typecheck.ts` |

All `fn` keys are guarded by `src/public_api.test.ts` and `src/type-scenarios/public-api-matrix.typecheck.ts`; every official array helper also has SQL compile coverage in `src/functions.test.ts`.

## Schema DSL

| API | Coverage location |
| --- | --- |
| `ckTable` | `schema-roundtrip.e2e.test.ts` |
| `ckAlias` | `schema-roundtrip.e2e.test.ts`, `builder-analytics.e2e.test.ts` |
| `int8/int16/int32/int64` | `schema-roundtrip.e2e.test.ts`, `write-paths.e2e.test.ts` |
| `uint8/uint16/uint32/uint64` | `schema-roundtrip.e2e.test.ts`, `write-paths.e2e.test.ts` |
| `float32/float64/bfloat16` | `schema-roundtrip.e2e.test.ts` |
| `string/fixedString` | `schema-roundtrip.e2e.test.ts` |
| `decimal` | `schema-roundtrip.e2e.test.ts` |
| `decimal.cast(P, S)` (column-level precision cast) | `builder-analytics.e2e.test.ts` |
| Decimal column rejects object inputs (e.g. raw `decimal.js` instances) | `write-paths.e2e.test.ts` |
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
| `tracing`, including derived database/server fields and JSON response statistics | `observability.e2e.test.ts` |
| `instrumentation`, including derived database/server fields and JSON response statistics | `observability.e2e.test.ts` |

## Error contracts

| Scenario | Coverage location |
| --- | --- |
| invalid SQL syntax | `error-contracts.e2e.test.ts` |
| missing table | `error-contracts.e2e.test.ts` |
| accessing a temporary table after session end | `error-contracts.e2e.test.ts` |
| `insertJsonEachRow()` type mismatch | `error-contracts.e2e.test.ts` |
| `query_params` type mismatch | `error-contracts.e2e.test.ts` |
| partial `JSONEachRow` insert failure inside a session | `error-contracts.e2e.test.ts` |
| `system.query_log` failure-stage validation | `error-contracts.e2e.test.ts` |

## Security and injection contexts

| Scenario | Coverage location |
| --- | --- |
| classic payloads in builder equality filters | `sql-injection.e2e.test.ts` |
| classic payloads in raw template literals | `sql-injection.e2e.test.ts` |
| Unicode line separators in parameter values | `sql-injection.e2e.test.ts`, `injection-values.e2e.test.ts` |
| literal, array, map, DateTime64, NaN and Infinity `query_params` | `query-params-edge-cases.e2e.test.ts` |
| `Identifier` query parameters for table and column names | `query-params-edge-cases.e2e.test.ts` |
| malicious `Identifier` query parameters leave seeded tables untouched | `query-params-edge-cases.e2e.test.ts` |
| set-membership payloads | `injection-values.e2e.test.ts` |
| LIKE / ILIKE payloads | `injection-values.e2e.test.ts` |
| literal-text pattern matching via semantic helpers | `injection-values.e2e.test.ts` |
| string and object identifier rejection | `injection-identifiers.e2e.test.ts` |
| ckAlias, temporary-table, and function-name rejection | `injection-identifiers.e2e.test.ts` |
| stacked raw SQL rejection and no-mutation checks | `injection-raw-sql.e2e.test.ts` |
| semicolons inside string literals and comments | `injection-raw-sql.e2e.test.ts` |
| `query_params`, `query_id`, and `session_id` validation | `injection-transport-and-boundaries.e2e.test.ts` |
| per-request `session_timeout` and continued-session `session_check` | `injection-transport-and-boundaries.e2e.test.ts` |
| `createTemporaryTableRaw()` trusted-only boundary | `injection-transport-and-boundaries.e2e.test.ts` |
| `ckSql.join()` separator validation | `injection-transport-and-boundaries.e2e.test.ts` |
