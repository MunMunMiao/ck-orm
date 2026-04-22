import { describe, expect, it } from "bun:test";
import { dateTime, int32, string } from "./columns";
import { alias, chTable } from "./schema";
import { buildCreateTableStatement, buildCreateTemporaryTableStatement, buildDropTableStatement } from "./schema-ddl";
import { sql } from "./sql";

const normalizeSql = (value: string) => value.replaceAll(/\s+/g, " ").trim();

describe("ck-orm schema ddl", function describeSchemaDdl() {
  it("renders full create table statements from schema metadata", function testBuildCreateTableStatement() {
    const events = chTable(
      "events",
      {
        id: int32(),
        created_at: dateTime().comment("creation time"),
        note: string().default(sql`'auto'`),
        shard_month: int32().materialized(sql`toYYYYMM(created_at)`),
        label: string().aliasExpr(sql`concat('event-', toString(id))`),
        expires_at: dateTime().ttl(sql`expires_at + INTERVAL 7 DAY`),
      },
      (table) => ({
        engine: "ReplacingMergeTree",
        partitionBy: sql`toYYYYMM(created_at)`,
        primaryKey: [table.id],
        orderBy: [table.id, sql`toDate(created_at)`],
        sampleBy: table.id,
        ttl: [sql`created_at + INTERVAL 30 DAY DELETE`],
        settings: {
          allow_nullable_key: true,
          storage_policy: "hot_to_cold",
        },
        comment: "event projections",
        versionColumn: table.created_at,
      }),
    );

    const statement = normalizeSql(buildCreateTableStatement(events));
    expect(statement).toContain("CREATE TABLE `events`");
    expect(statement).toContain("`note` String DEFAULT 'auto'");
    expect(statement).toContain("`shard_month` Int32 MATERIALIZED toYYYYMM(created_at)");
    expect(statement).toContain("`label` String ALIAS concat('event-', toString(id))");
    expect(statement).toContain("`created_at` DateTime COMMENT 'creation time'");
    expect(statement).toContain("`expires_at` DateTime TTL expires_at + INTERVAL 7 DAY");
    expect(statement).toContain("ENGINE = ReplacingMergeTree(`created_at`)");
    expect(statement).toContain("COMMENT 'event projections'");
    expect(statement).toContain("PARTITION BY toYYYYMM(created_at)");
    expect(statement).toContain("PRIMARY KEY (`id`)");
    expect(statement).toContain("ORDER BY (`id`, toDate(created_at))");
    expect(statement).toContain("SAMPLE BY `id`");
    expect(statement).toContain("TTL created_at + INTERVAL 30 DAY DELETE");
    expect(statement).toContain("SETTINGS allow_nullable_key = 1, storage_policy = 'hot_to_cold'");
  });

  it("renders structured temporary tables with default Memory engine and create modes", function testBuildCreateTemporaryTableStatement() {
    const scope = chTable("tmp_scope", {
      id: int32(),
      note: string().default(sql`'scoped'`),
    });

    const statement = normalizeSql(buildCreateTemporaryTableStatement(scope, "if_not_exists"));
    expect(statement).toContain("CREATE TEMPORARY TABLE IF NOT EXISTS `tmp_scope`");
    expect(statement).toContain("`note` String DEFAULT 'scoped'");
    expect(statement).toContain("ENGINE = Memory");
  });

  it("rejects unsupported temporary table engines and conflicting column generation modes", function testTempEngineValidation() {
    const replicatedScope = chTable(
      "tmp_replicated",
      {
        id: int32(),
      },
      {
        engine: sql.raw("ReplicatedMergeTree('/clickhouse/tables/{uuid}', '{replica}')"),
      },
    );

    expect(() => buildCreateTemporaryTableStatement(replicatedScope)).toThrow(
      "Temporary tables do not support engine ReplicatedMergeTree",
    );

    expect(() => string().default(sql`'x'`).materialized(sql`'y'`)).toThrow(
      "Column DDL cannot combine MATERIALIZED with DEFAULT",
    );
  });

  it("covers defensive ddl rendering branches without adding extra public API surface", function testDdlDefensiveBranches() {
    const metrics = chTable(
      "metrics",
      {
        id: int32(),
        expires_at: dateTime().ttl("expires_at + INTERVAL 1 DAY"),
        note: string().codec(sql`ZSTD(1)`),
      },
      {
        settings: {
          flatten_nested: false,
          index_granularity: 8192,
          min_bytes_for_wide_part: sql.raw("0"),
        },
      },
    );

    const metricsStatement = normalizeSql(buildCreateTableStatement(metrics));
    expect(metricsStatement).toContain("ENGINE = MergeTree");
    expect(metricsStatement).toContain("ORDER BY tuple()");
    expect(metricsStatement).toContain("`expires_at` DateTime TTL expires_at + INTERVAL 1 DAY");
    expect(metricsStatement).toContain("`note` String CODEC(ZSTD(1))");
    expect(metricsStatement).toContain(
      "SETTINGS flatten_nested = 0, index_granularity = 8192, min_bytes_for_wide_part = 0",
    );

    const logTable = chTable("event_log", { id: int32() }, { engine: "Log" });
    const logStatement = normalizeSql(buildCreateTableStatement(logTable));
    expect(logStatement).toContain("ENGINE = Log");
    expect(logStatement).not.toContain("ORDER BY tuple()");

    const opaqueEngine = chTable("opaque_engine", { id: int32() }, { engine: sql.raw("()") });
    const opaqueStatement = normalizeSql(buildCreateTableStatement(opaqueEngine));
    expect(opaqueStatement).toContain("ENGINE = ()");
    expect(opaqueStatement).toContain("ORDER BY tuple()");

    const retention = chTable(
      "retention",
      { id: int32(), expires_at: dateTime() },
      {
        ttl: "expires_at + INTERVAL 30 DAY DELETE",
      },
    );
    const retentionStatement = normalizeSql(buildCreateTableStatement(retention));
    expect(retentionStatement).toContain("TTL expires_at + INTERVAL 30 DAY DELETE");
  });

  it("rejects aliased tables, invalid bound columns, custom versionColumn engines and parameterized ddl fragments", function testDdlValidationEdges() {
    const events = chTable("events", { id: int32() });
    expect(() => buildCreateTableStatement(alias(events, "e"))).toThrow(
      "Schema DDL requires a base table, not an aliased table",
    );

    const brokenOrderBy = chTable(
      "broken_order_by",
      { id: int32() },
      {
        engine: "MergeTree",
        orderBy: [string() as never],
      },
    );
    expect(() => buildCreateTableStatement(brokenOrderBy)).toThrow("Expected bound column name for String");

    const customVersioned = chTable("custom_versioned", { id: int32(), version: int32() }, (table) => ({
      engine: sql.raw("CustomEngine()"),
      versionColumn: table.version,
    }));
    expect(() => buildCreateTableStatement(customVersioned)).toThrow("versionColumn only supports string engine names");

    const parameterizedDefault = chTable("parameterized_default", {
      note: string().default(sql`${"oops"}`),
    });
    expect(() => buildCreateTableStatement(parameterizedDefault)).toThrow(
      "column DEFAULT expression must not use SQL parameters",
    );
  });

  it("keeps DROP TABLE helpers aligned with the shared renderer", function testBuildDropTableStatement() {
    expect(buildDropTableStatement("events")).toBe("DROP TABLE IF EXISTS `events`");
  });
});
