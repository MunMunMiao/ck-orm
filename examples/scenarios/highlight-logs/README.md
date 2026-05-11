# Highlight.io application logs

> Replicates: **Highlight.io's open-source observability log store**
> Source: <https://highlight.io/blog/how-we-built-logging-with-clickhouse>

## What this example tests

A general application log schema following the OTel data model: severity,
service, body, and two attribute maps. Highlight uses tokenbf_v1 skip indexes
on `Body` to enable fast keyword search — omitted here for portability but
straightforward to add via `ckSql` index DDL.

## ck-orm features exercised

- OTel attribute `Map<String, String>` (resource + per-log)
- Composite ORDER BY `(service_name, severity_text, toUnixTimestamp(timestamp), trace_id)`
  — `service` and `severity` are first because dashboards filter on them most
- Partitioned by `toDate(timestamp)` with a 30-day TTL

## Key queries (in `index.ts`)

- `buildHighlightSeverityBreakdown()` — count by `severity_text` to power the
  "errors over time" widget.

## Why ClickHouse

Highlight ingests session-replay + log + trace data at scale and needs cheap
fan-out queries across all three. Putting everything in MergeTree tables with
shared bloom-filter and tokenbf indexes lets the dashboard join across them
without a separate analytics database.
