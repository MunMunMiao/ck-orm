import { describe, expect, it } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { isClickHouseORMError } from "./errors";
import { prettyRows } from "./internal/pretty";
import { createInsertBuilder } from "./query";
import {
  appendRows,
  ckSql,
  ckTable,
  ckType,
  clickhouseClient,
  expectedBody,
  linearVisitBudget,
  makeOfflineDb,
  makeRows,
  makeTable,
  measureRowVisits,
} from "./query-values-performance-support.test";

const originalIterator = Array.prototype[Symbol.iterator];
const fixtureRoot = join(import.meta.dir, "query-values-gc-fixture.test.ts");
const restrictedEnv = { PATH: process.env.PATH ?? "", NO_COLOR: "1" };
const sentinel = "--ck-orm-fixture";
const bodyFor = (rows: readonly { id: number; label: string }[]) =>
  rows.map(({ id, label }) => `(${id},'${label}')`).join(", ");

const runBun = (fixture: string, args: string[]) =>
  spawnSync(process.execPath, ["--no-env-file", "--no-install", fixture, ...args], {
    encoding: "utf8",
    timeout: 8_000,
    env: restrictedEnv,
  });

describe("PERF-004 VALUES append construction", () => {
  it("PERF-004 RED: repeated values append has bounded linear row-reference traversal", () => {
    const db = makeOfflineDb();
    const table = makeTable();
    const results = [512, 1024].map((count) => {
      const rows = makeRows(count);
      const measured = measureRowVisits(rows, () => appendRows(db, table, rows));
      expect(Array.prototype[Symbol.iterator]).toBe(originalIterator);
      expect(measured.value.toSQL().body).toBe(expectedBody(rows));
      return { count, rowVisits: measured.rowVisits, budget: linearVisitBudget(count) };
    });
    for (const result of results) {
      expect(result.rowVisits).toBeLessThanOrEqual(result.budget);
    }
    const first = results[0];
    const second = results[1];
    if (first === undefined || second === undefined) {
      throw new Error("missing PERF-004 visit samples");
    }
    expect(second.rowVisits).toBeLessThanOrEqual(first.rowVisits * 2.25 + 128);
  });

  it("PERF-004 CONTROL: one batch passes the same deterministic budget", () => {
    const db = makeOfflineDb();
    const table = makeTable();
    const rows = makeRows(1024);
    const measured = measureRowVisits(rows, () => db.insert(table).values(rows));
    expect(Array.prototype[Symbol.iterator]).toBe(originalIterator);
    expect(measured.value.toSQL().body).toBe(expectedBody(rows));
    expect(measured.rowVisits).toBeGreaterThanOrEqual(rows.length);
    expect(measured.rowVisits).toBeLessThanOrEqual(linearVisitBudget(rows.length));
  });

  it("PERF-004 RED: 32000 tiny-appends stay inside the linear historical-copy budget", () => {
    const db = makeOfflineDb();
    const table = makeTable();
    const rows = makeRows(32_000);
    const measured = measureRowVisits(rows, () => appendRows(db, table, rows));
    expect(Array.prototype[Symbol.iterator]).toBe(originalIterator);
    expect(measured.value.toSQL().body).toBe(expectedBody(rows));
    expect(measured.rowVisits).toBeLessThanOrEqual(linearVisitBudget(rows.length));
  }, 180_000);

  it("PERF-004 REVIEW: mixed-batch append stays inside the linear visit budget", () => {
    const db = makeOfflineDb();
    const table = makeTable();
    const rows = makeRows(256);
    const sizes = [1, 2, 1, 3];
    const measured = measureRowVisits(rows, () => {
      let offset = 0;
      let builder = db.insert(table).values(rows[0] as (typeof rows)[0]);
      offset = 1;
      let sizeIndex = 0;
      while (offset < rows.length) {
        const size = Math.min(sizes[sizeIndex % sizes.length] as number, rows.length - offset);
        const part = rows.slice(offset, offset + size);
        builder = size === 1 ? builder.values(part[0] as (typeof rows)[0]) : builder.values(part);
        offset += size;
        sizeIndex += 1;
      }
      return builder;
    });
    expect(Array.prototype[Symbol.iterator]).toBe(originalIterator);
    expect(measured.value.toSQL().body).toBe(expectedBody(rows));
    expect(measured.rowVisits).toBeLessThanOrEqual(linearVisitBudget(rows.length));
  });
});

describe("PERF-004 VALUES branch, encoding, and execute semantics", () => {
  const db = makeOfflineDb();

  it("PERF-004 CONTROL: retained builders and branches stay independent across chunk boundaries", () => {
    const table = makeTable();
    const rows = makeRows(96);
    const parent = appendRows(db, table, rows.slice(0, 31));
    const snapshot = parent.toSQL();
    const left = parent.values(rows.slice(31, 65)).values(rows[65] as (typeof rows)[0]);
    const right = parent.values(rows[95] as (typeof rows)[0]);
    expect(parent.toSQL()).toEqual(snapshot);
    expect(left.toSQL().body).toBe(expectedBody(rows.slice(0, 66)));
    expect(right.toSQL().body).toBe(`${expectedBody(rows.slice(0, 31))}, (95,'row_95')`);
    expect(left.values(rows.slice(66)).toSQL().body).toBe(expectedBody(rows));
    expect(left.toSQL().body).toBe(expectedBody(rows.slice(0, 66)));
    expect(parent.toSQL()).toEqual(snapshot);
  });

  it("PERF-004 CONTROL: array membership is copied but row objects retain existing shallow semantics", () => {
    const table = makeTable();
    const first = { id: 1, label: "original" };
    const array = [first];
    const parent = db.insert(table).values(array);
    array.push({ id: 99, label: "not_in_builder" });
    const child = parent.values({ id: 2, label: "child" });
    expect(parent.toSQL().body).toBe("(1,'original')");
    first.label = "changed";
    expect(parent.toSQL().body).toBe("(1,'changed')");
    expect(child.toSQL().body).toBe("(1,'changed'), (2,'child')");
  });

  it("PERF-004 CONTROL: columns DEFAULT null Nested and encode transforms retain order and timing", () => {
    const calls: string[] = [];
    const schema = {
      "~standard": {
        version: 1 as const,
        vendor: "audit-perf004",
        validate(value: unknown) {
          if (typeof value !== "string") {
            return { issues: [{ message: "expected string" }] };
          }
          calls.push(value);
          return { value: `encoded_${value}` };
        },
      },
    };
    const table = ckTable("audit_perf004_semantics", {
      id: ckType.uint32("record_id"),
      label: ckType.string("text").$validator(schema).default(ckSql`'fallback'`),
      optional: ckType.nullable(ckType.string()),
      entries: ckType.nested({ sku: ckType.string().$validator(schema), amount: ckType.int32() }),
      generated: ckType.uint32().materialized(ckSql`record_id + 1`),
    });
    const parent = db.insert(table).values({
      id: 1,
      label: "a",
      optional: null,
      entries: [
        { sku: "x", amount: 3 },
        { sku: "y", amount: 4 },
      ],
    });
    const builder = parent.values({ id: 2 } as never).values({
      id: 3,
      label: "b",
      optional: "present",
      entries: [],
    });
    expect(calls).toEqual([]);
    const built = builder.toSQL();
    expect(built.sql).toBe(
      "insert into `audit_perf004_semantics` (`record_id`, `text`, `optional`, `entries`.`sku`, `entries`.`amount`) format Values",
    );
    expect(built.body).toBe(
      "(1,'encoded_a',NULL,['encoded_x','encoded_y'],[3,4]), (2,DEFAULT,DEFAULT,DEFAULT,DEFAULT), (3,'encoded_b','present',[],[])",
    );
    expect(built.params).toEqual({});
    expect(built.forcedSettings).toEqual({
      input_format_values_interpret_expressions: 0,
      input_format_values_deduce_templates_of_expressions: 0,
    });
    expect(calls).toEqual(["a", "x", "y", "b"]);
    calls.length = 0;
    expect(builder.toSQL()).toEqual(built);
    expect(calls).toEqual(["a", "x", "y", "b"]);
    expect(parent.toSQL().body).toBe("(1,'encoded_a',NULL,['encoded_x','encoded_y'],[3,4])");
    expect(prettyRows(built.body ?? "")).toBe(
      "(1,'encoded_a',NULL,['encoded_x','encoded_y'],[3,4]),\n(2,DEFAULT,DEFAULT,DEFAULT,DEFAULT),\n(3,'encoded_b','present',[],[])",
    );
    expect(builder.toSQL({ pretty: true }).body).toBe(prettyRows(built.body ?? ""));
  });

  it("PERF-004 CONTROL: invalid appends remain eager and do not corrupt a retained parent", () => {
    const table = ckTable("audit_perf004_validation", {
      id: ckType.uint32(),
      label: ckType.string(),
      computed: ckType.uint32().materialized(ckSql`id + 1`),
    });
    const parent = db.insert(table).values({ id: 1, label: "ok" });
    const before = parent.toSQL();
    expect(() => parent.values([])).toThrow("at least one row");
    expect(() => parent.values(null as never)).toThrow("must be an object");
    expect(() => parent.values({ id: 2, label: "ok", surprise: 3 } as never)).toThrow("unknown columns");
    expect(() => parent.values({ id: 2, label: "ok", computed: 3 } as never)).toThrow("generated columns");
    expect(parent.toSQL()).toEqual(before);
    expect(() => parent.values({ id: 2, label: ckSql`upper('x')` as never }).toSQL()).toThrow("takes data values only");
  });

  it("PERF-004 CONTROL: values and fromSelect remain mutually exclusive at runtime", () => {
    const table = makeTable();
    const source = db.select({ id: table.id, label: table.label }).from(table);
    const base = db.insert(table);
    const values = base.values({ id: 1, label: "value" });
    const selected = base.fromSelect(source);
    expect(() => (values as unknown as { fromSelect: (query: unknown) => unknown }).fromSelect(source)).toThrow(
      "cannot follow insert().values()",
    );
    expect(() =>
      (selected as unknown as { values: (row: unknown) => unknown }).values({ id: 2, label: "bad" }),
    ).toThrow("cannot follow insert().fromSelect()");
    expect(selected.toSQL().body).toBeUndefined();
    expect(() => base.toSQL()).toThrow("requires .values(rows) or .fromSelect(selectBuilder)");
  });

  it("PERF-004 REVIEW: mixed batch boundaries preserve every retained branch and input snapshot", () => {
    const table = makeTable();
    for (const prefixSize of [1, 31, 32, 33, 63, 64, 65]) {
      const prefix = makeRows(prefixSize);
      let parent = db.insert(table).values(prefix[0] as (typeof prefix)[0]);
      for (const row of prefix.slice(1)) {
        parent = parent.values(row);
      }
      const snapshot = parent.toSQL();
      for (const batchSize of [1, 2, 31, 32, 33, 64, 65]) {
        const incoming = makeRows(batchSize).map(({ id }) => ({ id: id + 100, label: `batch_${id}` }));
        const expected = [...prefix, ...incoming];
        const child = parent.values(incoming);
        const siblingRow = { id: 900, label: "sibling" };
        const sibling = parent.values(Object.freeze([siblingRow]));
        const last = { id: 901, label: "last" };
        const grandchild = child.values(last);
        incoming.fill({ id: 999, label: "not_part_of_the_builder" });
        incoming.length = 0;
        expect(child.toSQL().body).toBe(bodyFor(expected));
        expect(grandchild.toSQL().body).toBe(bodyFor([...expected, last]));
        expect(sibling.toSQL().body).toBe(bodyFor([...prefix, siblingRow]));
        expect(parent.toSQL()).toEqual(snapshot);
        const head = prefix[0];
        if (head === undefined) {
          throw new Error("missing prefix row");
        }
        head.label = "mutated_after_compile";
        expect(child.toSQL().body).toBe(bodyFor(expected));
        expect(sibling.toSQL().body).toBe(bodyFor([...prefix, siblingRow]));
        head.label = "row_0";
        expect(parent.toSQL()).toEqual(snapshot);
      }
    }
  });

  it("PERF-004 REVIEW: encoding failure and retries traverse all batches in order without cached results", () => {
    const calls: number[] = [];
    let failAt: number | undefined = 33;
    let offset = 0;
    const table = ckTable("audit_perf004_review_encoder", {
      id: ckType.uint32().$validator({
        "~standard": {
          version: 1,
          vendor: "audit-perf004-review",
          validate(value: unknown) {
            if (typeof value !== "number") {
              return { issues: [{ message: "expected number" }] };
            }
            calls.push(value);
            return value === failAt
              ? { issues: [{ message: "perf004-review-encoder-failure" }] }
              : { value: value + offset };
          },
        },
      }),
    });
    const rows = Array.from({ length: 65 }, (_, id) => ({ id }));
    let parent = db.insert(table).values(rows[0] as (typeof rows)[0]);
    for (const row of rows.slice(1, 32)) {
      parent = parent.values(row);
    }
    let child = parent;
    for (const row of rows.slice(32)) {
      child = child.values(row);
    }
    expect(calls).toEqual([]);
    expect(() => child.toSQL()).toThrow("perf004-review-encoder-failure");
    expect(calls).toEqual(rows.slice(0, 34).map(({ id }) => id));
    calls.length = 0;
    expect(parent.toSQL().body).toBe(
      rows
        .slice(0, 32)
        .map(({ id }) => `(${id})`)
        .join(", "),
    );
    expect(calls).toEqual(rows.slice(0, 32).map(({ id }) => id));
    failAt = undefined;
    for (offset of [0, 1000, 2000]) {
      calls.length = 0;
      expect(child.toSQL().body).toBe(rows.map(({ id }) => `(${id + offset})`).join(", "));
      expect(calls).toEqual(rows.map(({ id }) => id));
    }
  });

  it("PERF-004 REVIEW: repeated public execution sends fresh branch bodies and forced VALUES settings", async () => {
    const sent: { body: string; query: string | null; interpret: string | null; deduce: string | null }[] = [];
    const client = clickhouseClient({
      host: "http://127.0.0.1:1",
      fetch: async (request) => {
        const params = new URL(request.url).searchParams;
        sent.push({
          body: await request.text(),
          query: params.get("query"),
          interpret: params.get("input_format_values_interpret_expressions"),
          deduce: params.get("input_format_values_deduce_templates_of_expressions"),
        });
        return new Response("");
      },
    });
    const table = makeTable();
    const rows = makeRows(33);
    const parent = client
      .insert(table)
      .values(rows.slice(0, 31))
      .values(rows[31] as (typeof rows)[0])
      .values(rows[32] as (typeof rows)[0]);
    const extra = { id: 33, label: "extra" };
    const child = parent.values(extra);
    child.toSQL({ pretty: true });
    expect(sent).toEqual([]);
    expect(
      await child.execute({
        clickhouse_settings: {
          input_format_values_interpret_expressions: 1,
          input_format_values_deduce_templates_of_expressions: 1,
        },
      }),
    ).toBeUndefined();
    const firstBody = bodyFor([...rows, extra]);
    const head = rows[0];
    if (head === undefined) {
      throw new Error("missing execute-test head row");
    }
    head.label = "changed";
    expect(await parent).toBeUndefined();
    expect(await child.execute()).toBeUndefined();
    expect(sent.map(({ body }) => body)).toEqual([firstBody, bodyFor(rows), bodyFor([...rows, extra])]);
    expect(sent).toHaveLength(3);
    for (const request of sent) {
      expect(request.query).toBe("insert into `audit_perf004_rows` (`id`, `label`) format Values");
      expect(request.interpret).toBe("0");
      expect(request.deduce).toBe("0");
    }
  });

  it("PERF-004 REVIEW: direct empty values state still throws", () => {
    const empty = createInsertBuilder(makeTable(), undefined, { kind: "values", rows: [] });
    expect(() => empty.toSQL()).toThrow("insert().values() must be called with at least one row before execute()");
  });

  it("PERF-004 INTERNAL: malformed previous VALUES segment missing rows throws client_validation", () => {
    const table = makeTable();
    const head = [{ id: 1, label: "head" }];
    const malformedPrevious = { kind: "values" as const, rows: undefined };
    const state = {
      kind: "values" as const,
      rows: head,
      previous: malformedPrevious as unknown as { readonly kind: "values"; readonly rows: typeof head },
    };
    let thrown: unknown;
    try {
      createInsertBuilder(table, undefined, state).toSQL();
    } catch (error) {
      thrown = error;
    }
    expect(isClickHouseORMError(thrown)).toBe(true);
    expect(thrown).toEqual(
      expect.objectContaining({
        kind: "client_validation",
        executionState: "not_sent",
      }),
    );
    expect(String(thrown)).toContain("insert().values() compile encountered a missing VALUES segment");
  });

  it("PERF-004 REVIEW: seeded values state does not absorb a later append", () => {
    const table = makeTable();
    for (const count of [1, 40]) {
      const seed = makeRows(count);
      const parent = createInsertBuilder(table, undefined, { kind: "values", rows: seed });
      const before = parent.toSQL().body;
      const child = parent.values({ id: 900, label: "extra" });
      expect(seed).toHaveLength(count);
      expect(parent.toSQL().body).toBe(before);
      expect(child.toSQL().body).toBe(`${before}, (900,'extra')`);
    }
  });

  it("PERF-004 REVIEW: split batches match one values() batch including pretty recompile", () => {
    const table = ckTable("audit_perf004_review_rich", {
      id: ckType.uint32(),
      label: ckType.string(),
      note: ckType.nullable(ckType.string()),
      big: ckType.uint64(),
      tags: ckType.array(ckType.uint32()),
      events: ckType.nested({ name: ckType.string(), score: ckType.int32() }),
    });
    const richRows = Array.from({ length: 70 }, (_, id) => ({
      id,
      label: id % 7 === 0 ? "o'hare, (x) \\ -- /* DEFAULT NULL" : `row_${id}`,
      big: String(1000 + id),
      tags: id % 5 === 0 ? [] : [id, id + 1],
      events:
        id % 11 === 0
          ? []
          : [
              { name: `n_${id}`, score: id },
              { name: "a'b", score: 0 },
            ],
      note: id % 4 === 1 ? null : id % 4 === 2 ? "" : id % 4 === 3 ? `note_${id}` : null,
    }));
    const oracle = db.insert(table).values(richRows).toSQL();
    const prettyOracle = db.insert(table).values(richRows).toSQL({ pretty: true });
    expect(prettyOracle.body).toBe(prettyRows(oracle.body ?? ""));
    const chunkings = [
      [70],
      Array.from({ length: 70 }, () => 1),
      [31, 1, 1, 31, 6],
      [32, 32, 6],
      [33, 33, 4],
      [1, 31, 1, 32, 5],
    ];
    for (const chunks of chunkings) {
      let offset = 0;
      let builder = db.insert(table).values(richRows.slice(0, chunks[0]));
      offset = chunks[0] as number;
      for (const size of chunks.slice(1)) {
        const part = richRows.slice(offset, offset + size);
        const first = part[0];
        if (first === undefined) {
          throw new Error("empty PERF-004 review chunk");
        }
        builder = size === 1 ? builder.values(first) : builder.values(part);
        offset += size;
      }
      expect(offset).toBe(richRows.length);
      expect(builder.toSQL()).toEqual(oracle);
      expect(builder.toSQL({ pretty: true })).toEqual(prettyOracle);
    }
  });
});

describe("PERF-004 explicit GC subprocess and fixture CLI ownership", () => {
  it("PERF-004 CONTROL: dropped child rows collect across event-loop turns in an explicit-GC subprocess", async () => {
    const dir = await mkdtemp(join(tmpdir(), "ck-orm-perf004-"));
    const outfile = join(dir, "perf004-gc.mjs");
    try {
      const build = spawnSync(
        process.execPath,
        ["--no-env-file", "--no-install", "build", fixtureRoot, "--outfile", outfile, "--target=node"],
        { encoding: "utf8", timeout: 8_000, env: restrictedEnv },
      );
      expect(build.error, build.error?.message).toBeUndefined();
      expect(build.status, build.stderr).toBe(0);
      const child = spawnSync("node", ["--expose-gc", outfile, sentinel, "unit"], {
        encoding: "utf8",
        timeout: 30_000,
        env: restrictedEnv,
      });
      expect(child.error, child.error?.message).toBeUndefined();
      expect(child.signal, child.stderr).toBeNull();
      expect(child.status, `${child.stderr}\n${child.stdout}`).toBe(0);
      expect(child.stderr).toBe("");
      expect(JSON.parse(child.stdout)).toEqual({
        id: "PERF-004",
        layer: "explicit-gc-subprocess",
        negativeCollected: true,
        droppedCollected: [1, 32],
        heldRetained: true,
      });
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

  it("CONTROL: direct invocation without --ck-orm-fixture is not a driver invocation even when leftover args look like a mode", () => {
    for (const args of [["unit"], [], ["dropped-child"]] as const) {
      const result = runBun(fixtureRoot, [...args]);
      expect(result.error, `${args.join(" ") || "empty"} spawn`).toBeUndefined();
      expect(result.status, `${args.join(" ") || "empty"} without ${sentinel} is not a driver invocation`).toBe(0);
      expect(result.stdout).not.toContain('"id":"PERF-004"');
      expect(result.stderr).not.toContain("ck-orm fixture:");
    }
  });

  it("RED: explicit --ck-orm-fixture with missing or invalid mode must not silently succeed", () => {
    for (const args of [[sentinel], [sentinel, "database"]] as const) {
      const result = runBun(fixtureRoot, [...args]);
      expect(result.error).toBeUndefined();
      expect(result.status, args.join(" ")).not.toBe(0);
      expect(result.stderr).not.toBe("");
    }
  });
});
