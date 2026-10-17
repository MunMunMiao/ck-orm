import { describe, expect, it } from "bun:test";
import { ck, ckSql, ckTable, ckType, clickhouseClient } from "./public_api";
import type { AnyCte } from "./query";

const makeDb = () =>
  clickhouseClient({
    host: "http://127.0.0.1:1",
    fetch() {
      throw new Error("PERF-003 compile-only fixture attempted network access");
    },
  });

type Db = ReturnType<typeof makeDb>;

const constantChain = (
  db: Db,
  stages: number,
  options: { anonymous?: boolean; where?: boolean; key?: string; limit?: number } = {},
) => {
  const seed = db.select({ id: 7 });
  let last: AnyCte = options.anonymous ? db.$with().as(seed) : db.$with("stage_0").as(seed);
  let client = db.with(last);
  for (let index = 1; index < stages; index++) {
    let body = client.select({ id: last.columns.id }).from(last);
    if (options.where) {
      body = body.where(ck.eq(last.columns.id, 7));
    }
    last = options.anonymous ? client.$with().as(body) : client.$with(`stage_${index}`).as(body);
    client = client.with(last);
  }
  let query = client.select({ [options.key ?? "id"]: last.columns.id }).from(last);
  if (options.where) {
    query = query.where(ck.eq(last.columns.id, 7));
  }
  if (options.limit !== undefined) {
    query = query.limit(options.limit);
  }
  return query;
};

const definitionCount = (sql: string) => (sql.match(/ as \(/g) ?? []).length;
const withCount = (sql: string) => (sql.match(/\bwith /gi) ?? []).length;

const buildRawRootChain = (db: Db, stages: number) => {
  const seed = db.select({ id: ck.expr(ckSql`toInt32(7)`).mapWith(Number) });
  let last: AnyCte = db.$with("raw_0").as(seed);
  let client = db.with(last);
  const ctes: AnyCte[] = [last];
  for (let index = 1; index < stages; index++) {
    last = client.$with(`raw_${index}`).as(client.select({ id: last.columns.id }).from(last));
    ctes.push(last);
    client = client.with(last);
  }
  return { last, client, ctes };
};

const instrumentCteNameReads = (ctes: readonly AnyCte[]) => {
  let reads = 0;
  for (const cte of ctes) {
    const name = cte.name;
    Object.defineProperty(cte, "name", {
      configurable: true,
      enumerable: true,
      get() {
        reads += 1;
        return name;
      },
    });
  }
  return {
    reads: () => reads,
  };
};

const assertLinearNative = (sql: string, params: Record<string, unknown>, stages: number) => {
  expect(definitionCount(sql), "growth/size definition count").toBeLessThanOrEqual(stages);
  expect(Buffer.byteLength(sql), "growth/size bytes").toBeLessThanOrEqual(stages * 256);
  expect(withCount(sql), "growth/size with count").toBeLessThanOrEqual(1);
  expect(sql).not.toContain("toInt32");
  expect(sql).not.toContain("MATERIALIZED");
  expect(sql).not.toContain("enable_global_with_statement");
  expect(sql).toContain(":Int64}");
  for (const value of Object.values(params)) {
    expect(value).toBe(7);
  }
};

describe("PERF-003 S1-v3 native-constant lowering", () => {
  const db = makeDb();

  for (const stages of [5, 8, 12] as const) {
    for (const anonymous of [false, true]) {
      it(`growth/size ${anonymous ? "anonymous" : "named"} native-constant chain n=${stages}`, () => {
        const built = constantChain(db, stages, { anonymous }).toSQL();
        assertLinearNative(built.sql, built.params, stages);
        expect(Object.keys(built.params)).toHaveLength(1);
      }, 30_000);

      it(`growth/size ${anonymous ? "anonymous" : "named"} native-constant where chain n=${stages}`, () => {
        const built = constantChain(db, stages, { anonymous, where: true }).toSQL();
        assertLinearNative(built.sql, built.params, stages);
        expect(Object.keys(built.params).length).toBeLessThanOrEqual(stages + 1);
      }, 30_000);
    }
  }

  it("named n=2 native constants lower to nested derived tables", () => {
    const built = constantChain(db, 2).toSQL();
    expect(built.sql).toBe(
      "select `stage_1`.`id` as `id` from (select `stage_0`.`id` as `id` from (select {orm_param1:Int64} as `id`) as `stage_0`) as `stage_1`",
    );
    expect(built.params).toEqual({ orm_param1: 7 });
  });

  it("anonymous n=2 native constants lower without a WITH prefix", () => {
    const built = constantChain(db, 2, { anonymous: true }).toSQL();
    expect(built.sql).toBe(
      "select `__cte_2`.`id` as `id` from (select `__cte_1`.`id` as `id` from (select {orm_param1:Int64} as `id`) as `__cte_1`) as `__cte_2`",
    );
    expect(built.params).toEqual({ orm_param1: 7 });
  });

  it("projection alias is copied from the cached selection item", () => {
    const built = constantChain(db, 2, { key: "value" }).toSQL();
    expect(built.sql).toBe(
      "select `stage_1`.`id` as `value` from (select `stage_0`.`id` as `id` from (select {orm_param1:Int64} as `id`) as `stage_0`) as `stage_1`",
    );
  });

  it("structured WHERE renders eq/and/or/not/null from the same binding", () => {
    const seed = db.$with("stage_0").as(db.select({ id: 7 }));
    const base = db.with(seed).select({ id: seed.columns.id }).from(seed);
    expect(base.where(ck.eq(seed.columns.id, 7)).toSQL().sql).toBe(
      "select `stage_0`.`id` as `id` from (select {orm_param1:Int64} as `id`) as `stage_0` where `stage_0`.`id` = {orm_param2:Int64}",
    );
    expect(base.where(ck.ne(seed.columns.id, 7)).toSQL().sql).toBe(
      "select `stage_0`.`id` as `id` from (select {orm_param1:Int64} as `id`) as `stage_0` where `stage_0`.`id` != {orm_param2:Int64}",
    );
    expect(base.where(ck.and(ck.eq(seed.columns.id, 7), ck.gt(seed.columns.id, 1))).toSQL().sql).toBe(
      "select `stage_0`.`id` as `id` from (select {orm_param1:Int64} as `id`) as `stage_0` where (`stage_0`.`id` = {orm_param2:Int64} and `stage_0`.`id` > {orm_param3:Int64})",
    );
    expect(base.where(ck.or(ck.lt(seed.columns.id, 1), ck.gte(seed.columns.id, 7))).toSQL().sql).toBe(
      "select `stage_0`.`id` as `id` from (select {orm_param1:Int64} as `id`) as `stage_0` where (`stage_0`.`id` < {orm_param2:Int64} or `stage_0`.`id` >= {orm_param3:Int64})",
    );
    expect(base.where(ck.not(ck.eq(seed.columns.id, 7))).toSQL().sql).toBe(
      "select `stage_0`.`id` as `id` from (select {orm_param1:Int64} as `id`) as `stage_0` where not (`stage_0`.`id` = {orm_param2:Int64})",
    );
    expect(base.where(ck.isNull(seed.columns.id)).toSQL().sql).toBe(
      "select `stage_0`.`id` as `id` from (select {orm_param1:Int64} as `id`) as `stage_0` where `stage_0`.`id` is null",
    );
    expect(base.where(ck.isNotNull(seed.columns.id)).toSQL().sql).toBe(
      "select `stage_0`.`id` as `id` from (select {orm_param1:Int64} as `id`) as `stage_0` where `stage_0`.`id` is not null",
    );
    expect(base.where(ck.lte(seed.columns.id, seed.columns.id)).toSQL().sql).toBe(
      "select `stage_0`.`id` as `id` from (select {orm_param1:Int64} as `id`) as `stage_0` where `stage_0`.`id` <= `stage_0`.`id`",
    );
  });

  it("local where chain keeps the constant inside the derived table", () => {
    const outer = db.$with("base").as(db.select({ id: 59 }));
    const built = db
      .with(outer)
      .select({ id: outer.columns.id })
      .from(outer)
      .where(ck.eq(outer.columns.id, 59))
      .toSQL();
    expect(built.sql).not.toContain("with ");
    expect(Object.values(built.params)).toEqual([59, 59]);
    expect(built.sql).not.toContain("61");
  });
});

describe("PERF-003 S1-v3 fallback, getters, and SEC-002", () => {
  const db = makeDb();

  it("primitive params keep the no-cte SQL", () => {
    expect(db.select({ id: 7 }).toSQL()).toEqual({
      sql: "select {orm_param1:Int64} as `id`",
      params: { orm_param1: 7 },
    });
    expect(db.select({ id: 1.5 }).toSQL().sql).toBe("select {orm_param1:Float64} as `id`");
    expect(db.select({ id: "a" }).toSQL().sql).toBe("select {orm_param1:String} as `id`");
    expect(db.select({ id: true }).toSQL().sql).toBe("select {orm_param1:Bool} as `id`");
    const bigint = db.select({ id: 7n }).toSQL();
    expect(bigint.sql).toBe("select {orm_param1:Int64} as `id`");
    expect(bigint.params.orm_param1).toBe(7n);
  });

  it("boxed primitives and unsupported values stay on the original error path", () => {
    const boxed = db.select({ id: Object(7) }).toSQL();
    expect(boxed.sql).toBe("select {orm_param1:Map(String, String)} as `id`");
    expect(String(boxed.params.orm_param1)).toBe("7");
    expect(() => db.select({ id: Symbol("x") }).toSQL()).toThrow("[ck-orm] Unsupported SQL parameter value: Symbol(x)");
    expect(() => db.select({ id: null }).toSQL()).toThrow("[ck-orm] Unsupported SQL parameter value: null");
    expect(() => db.select({ id: undefined }).toSQL()).toThrow("[ck-orm] Unsupported SQL parameter value: undefined");
  });

  it("select().from() reads a selection getter twice and toSQL adds none", () => {
    let reads = 0;
    const source = db.$with("s").as(db.select({ id: 1 }));
    const selection = {
      get id() {
        reads += 1;
        return source.columns.id;
      },
    };
    const query = db.with(source).select(selection).from(source);
    expect(reads).toBe(2);
    query.toSQL();
    expect(reads).toBe(2);
  });

  it("a later toSQL does not reuse mutated params", () => {
    const query = db.select({ id: 7 });
    const first = query.toSQL();
    first.params.orm_param1 = 99;
    const second = query.toSQL();
    expect(second.params.orm_param1).toBe(7);
    expect(first.params).not.toBe(second.params);
    const chain = constantChain(db, 3);
    const chainFirst = chain.toSQL();
    chainFirst.params.orm_param1 = 99;
    const chainSecond = chain.toSQL();
    expect(chainSecond.params.orm_param1).toBe(7);
    expect(chainFirst.params).not.toBe(chainSecond.params);
  });

  it("raw, fn.toInt32, limit, join, and ambiguous names keep the original WITH renderer", () => {
    const rawSeed = db.select({ id: ck.expr(ckSql`toInt32(7)`).mapWith(Number) });
    let rawLast: AnyCte = db.$with("stage_0").as(rawSeed);
    let rawClient = db.with(rawLast);
    for (let index = 1; index < 5; index++) {
      rawLast = rawClient.$with(`stage_${index}`).as(rawClient.select({ id: rawLast.columns.id }).from(rawLast));
      rawClient = rawClient.with(rawLast);
    }
    const raw = rawClient.select({ id: rawLast.columns.id }).from(rawLast).toSQL();
    expect(raw.sql).toContain("with ");
    expect(definitionCount(raw.sql)).toBe(31);

    const convertedSeed = db.select({ id: ck.fn.toInt32(7) });
    let convertedLast: AnyCte = db.$with("stage_0").as(convertedSeed);
    let convertedClient = db.with(convertedLast);
    for (let index = 1; index < 5; index++) {
      convertedLast = convertedClient
        .$with(`stage_${index}`)
        .as(convertedClient.select({ id: convertedLast.columns.id }).from(convertedLast));
      convertedClient = convertedClient.with(convertedLast);
    }
    expect(
      definitionCount(convertedClient.select({ id: convertedLast.columns.id }).from(convertedLast).toSQL().sql),
    ).toBe(31);

    const opaque = db.$with("stage_0").as(db.select({ id: ck.expr(ckSql`(select 7)`).mapWith(Number) }));
    expect(db.with(opaque).select({ id: opaque.columns.id }).from(opaque).toSQL().sql).toContain("with ");

    const limited = constantChain(db, 4, { limit: 1 }).toSQL();
    expect(limited.sql).toContain("with ");
    expect(limited.sql).toContain("limit");

    const left = db.$with("lhs").as(db.select({ id: 1 }));
    const right = db.$with("rhs").as(db.select({ id: 2 }));
    const joined = db
      .with(left, right)
      .select({ id: right.columns.id })
      .from(left)
      .innerJoin(right, ck.eq(left.columns.id, right.columns.id))
      .toSQL();
    expect(joined.sql).toContain("with ");
    expect(joined.sql).toContain("inner join");

    const first = db.$with("base").as(db.select({ id: 1 }));
    const second = db.$with("base").as(db.select({ id: 2 }));
    expect(db.with(first, second).select({ id: first.columns.id }).from(first).toSQL().sql).toContain("with ");
  });

  it("replaced compile stays on the original path and is not invoked while planning", () => {
    let calls = 0;
    const seed = db.select({ id: 7 });
    const expression = seed.buildSelectionItems()[0]?.expression;
    if (!expression) {
      throw new Error("missing seed expression");
    }
    const original = expression.compile;
    expression.compile = (ctx) => {
      calls += 1;
      return original.call(expression, ctx);
    };
    let last: AnyCte = db.$with("stage_0").as(seed);
    let client = db.with(last);
    for (let index = 1; index < 5; index++) {
      last = client.$with(`stage_${index}`).as(client.select({ id: last.columns.id }).from(last));
      client = client.with(last);
    }
    const query = client.select({ id: last.columns.id }).from(last);
    expect(calls).toBe(0);
    query.toSQL();
    expect(calls).toBe(16);
    query.toSQL();
    expect(calls).toBe(32);
  });

  it("rebound shared definition keeps original WITH rendering and both bindings", () => {
    const outerBase = db.$with("base").as(db.select({ id: 31 }));
    const innerBase = db.$with("base").as(db.select({ id: 47 }));
    const shared = db.$with("shared").as(db.select({ id: outerBase.columns.id }).from(outerBase));
    const inner = db.with(innerBase, shared).select({ id: shared.columns.id }).from(shared).as("rebound");
    const built = db.with(outerBase, shared).select({ id: inner.columns.id }).from(inner).toSQL();
    expect(built.sql).toContain("with ");
    expect(Object.values(built.params)).toEqual(expect.arrayContaining([31, 47]));
  });

  it("SEC-002 CTE template binding survives the integrated optimizer", () => {
    const cte = db.$with("seed").as(db.select({ id: ck.expr(ckSql`${7}`) }));
    const built = db.with(cte).select({ id: cte.columns.id }).from(cte).toSQL();
    expect(built.sql).toContain("{orm_param1:Int64}");
    expect(built.params).toEqual({ orm_param1: 7 });
  });
});

describe("PERF-003 S1-v3 revision guards and early fallback", () => {
  const db = makeDb();

  for (const invalidKind of ["cte-name", "projection-alias"] as const) {
    it(`unused ${invalidKind} retains client validation`, () => {
      const unused = db
        .$with(invalidKind === "cte-name" ? "not valid" : "unused")
        .as(db.select(invalidKind === "projection-alias" ? { "not valid": 1 } : { id: 1 }));
      const used = db.$with("ok").as(db.select({ id: 7 }));
      expect(() => db.with(unused, used).select({ id: used.id }).from(used).toSQL()).toThrow(
        "Invalid SQL identifier: not valid",
      );
    });
  }

  it("duplicate anonymous source uses the ambiguous-scope original path", () => {
    const cte = db.$with().as(db.select({ id: 7 }));
    const built = db.with(cte, cte).select({ id: cte.columns.id }).from(cte).toSQL();
    expect(built.sql).toContain("with ");
    expect(Object.values(built.params).every((value) => value === 7)).toBe(true);
    expect(Object.keys(built.params).length).toBeGreaterThanOrEqual(1);
  });

  for (const empty of [false, true]) {
    it(`unrelated ${empty ? "empty" : "valid"} sibling retains the original WITH path`, () => {
      const unused = db.$with("unrelated").as(db.select(empty ? {} : { id: 3 }));
      const used = db.$with("used").as(db.select({ id: 7 }));
      const built = db.with(unused, used).select({ id: used.id }).from(used).toSQL();
      expect(built.sql).toContain("with ");
      expect(built.sql).toContain("`unrelated`");
      expect(built.sql).toContain("`used`");
    });
  }

  for (const stages of [8, 12]) {
    it(`MECHANISM: shared leaf inspection is bounded at n=${stages}`, () => {
      const seed = db.select({ id: 7 });
      const expression = seed.buildSelectionItems()[0]?.expression;
      if (!expression) {
        throw new Error("missing probe expression");
      }
      const compile = expression.compile;
      let reads = 0;
      Object.defineProperty(expression, "compile", {
        get() {
          reads += 1;
          return compile;
        },
      });
      let last: AnyCte = db.$with("probe_0").as(seed);
      let client = db.with(last);
      for (let index = 1; index < stages; index++) {
        last = client.$with(`probe_${index}`).as(client.select({ id: last.columns.id }).from(last));
        client = client.with(last);
      }
      const query = client.select({ id: last.columns.id }).from(last);
      reads = 0;
      const built = query.toSQL();
      expect(built.params).toEqual({ orm_param1: 7 });
      expect(reads).toBeLessThanOrEqual(stages * stages);
    });
  }

  it("early fallback: real-table FROM keeps unused CTE compile off the planning path", () => {
    let compileReads = 0;
    const users = ckTable("users", { id: ckType.int32() });
    const seed = db.select({ id: 7 });
    const expression = seed.buildSelectionItems()[0]?.expression;
    if (!expression) {
      throw new Error("missing unused seed");
    }
    const compile = expression.compile;
    Object.defineProperty(expression, "compile", {
      get() {
        compileReads += 1;
        return compile;
      },
    });
    const unused = db.$with("unused").as(seed);
    compileReads = 0;
    const query = db.with(unused).select({ id: users.id }).from(users);
    expect(compileReads, "planning a real-table FROM must not probe unused CTE compile").toBe(0);
    const built = query.toSQL();
    expect(built.sql).toContain("with ");
    expect(built.sql).toContain("`users`");
    expect(compileReads).toBeGreaterThan(0);
  });

  it("early fallback: ineligible raw ancestor keeps exponential WITH prefixes", () => {
    const seed = db.select({ id: ck.expr(ckSql`toInt32(7)`).mapWith(Number) });
    let last: AnyCte = db.$with("stage_0").as(seed);
    let client = db.with(last);
    for (let index = 1; index < 5; index++) {
      last = client.$with(`stage_${index}`).as(client.select({ id: last.columns.id }).from(last));
      client = client.with(last);
    }
    const built = client.select({ id: last.columns.id }).from(last).toSQL();
    expect(built.sql).toContain("with ");
    expect(definitionCount(built.sql)).toBe(31);
  });

  for (const stages of [8, 32, 64] as const) {
    it(`early fallback: real-table FROM does not scan CTE name prefixes at n=${stages}`, () => {
      const { client, ctes } = buildRawRootChain(db, stages);
      const table = ckTable(`fallback_users_${stages}`, { id: ckType.int32() });
      const names = instrumentCteNameReads(ctes);
      const pending = client.select({ id: table.id });
      expect(names.reads(), "select() must not walk CTE names while FROM is still unknown").toBe(0);
      const query = pending.from(table);
      expect(names.reads(), "real-table FROM must not scan CTE name prefixes while planning").toBe(0);
      if (stages === 8) {
        const built = query.toSQL();
        expect(built.sql).toContain("with ");
        expect(built.sql).toContain("`fallback_users_8`");
      }
    }, 30_000);

    it(`early fallback: ineligible raw FROM does not scan CTE name prefixes at n=${stages}`, () => {
      const { last, client, ctes } = buildRawRootChain(db, stages);
      const names = instrumentCteNameReads(ctes);
      const pending = client.select({ id: last.columns.id });
      expect(names.reads(), "select() must not walk CTE names before FROM").toBe(0);
      const query = pending.from(last);
      expect(names.reads(), "raw-ancestor FROM must not scan CTE name prefixes while planning").toBe(0);
      if (stages === 8) {
        const built = query.toSQL();
        expect(built.sql).toContain("with ");
        expect(definitionCount(built.sql)).toBe(255);
      }
    }, 30_000);
  }

  it("early fallback: real-table FROM keeps selection getter reads at two", () => {
    const { client } = buildRawRootChain(db, 8);
    const table = ckTable("fallback_getter_users", { id: ckType.int32() });
    let reads = 0;
    const selection = {
      get id() {
        reads += 1;
        return table.id;
      },
    };
    const query = client.select(selection).from(table);
    expect(reads).toBe(2);
    query.toSQL();
    expect(reads).toBe(2);
  });

  it("early fallback: implicit projection does not eagerly read table columns while planning", () => {
    const { client, ctes } = buildRawRootChain(db, 8);
    const table = ckTable("fallback_implicit_users", { id: ckType.int32() });
    const column = table.columns.id;
    let columnReads = 0;
    Object.defineProperty(table.columns, "id", {
      configurable: true,
      enumerable: true,
      get() {
        columnReads += 1;
        return column;
      },
    });
    const names = instrumentCteNameReads(ctes);
    const query = client.select().from(table);
    expect(names.reads(), "implicit projection must not scan CTE names while planning").toBe(0);
    expect(columnReads, "implicit projection must not getItems during ineligible planning").toBe(0);
    query.buildSelectionItems();
    expect(columnReads).toBeGreaterThan(0);
  });
});

describe("PERF-003 S1-v3 BigInt follow-up", () => {
  const huge = 1n << 256n;
  const rangeError = "is out of all ClickHouse integer ranges (max UInt256)";

  for (const kind of ["projection", "predicate", "nested"] as const) {
    it(`unused ${kind} retains BigInt validation`, () => {
      const db = makeDb();
      const seed = db.$with("seed").as(db.select({ id: kind === "nested" ? huge : 7 }));
      const child = db.with(seed).select({ id: seed.id }).from(seed);
      const unused = db
        .$with("unused")
        .as(
          kind === "projection"
            ? db.select({ id: huge })
            : kind === "predicate"
              ? child.where(ck.eq(seed.id, huge))
              : child,
        );
      const used = db.$with("ok").as(db.select({ id: 7 }));
      expect(() => db.with(unused, used).select({ id: used.id }).from(used).toSQL()).toThrow(rangeError);
    });
  }

  it("insert.fromSelect retains unused BigInt validation", () => {
    const db = makeDb();
    const unused = db.$with("unused").as(db.select({ id: huge }));
    const used = db.$with("ok").as(db.select({ id: 7 }));
    const query = db
      .with(unused, used)
      .select({ id: used.id.mapWith(Number) })
      .from(used);
    const target = ckTable("review_target", { id: ckType.int32() });
    expect(() => db.insert(target).fromSelect(query).toSQL()).toThrow(rangeError);
  });

  it("count retains unused BigInt validation before fetch", async () => {
    let fetchCalls = 0;
    const db = clickhouseClient({
      host: "http://127.0.0.1:1",
      fetch() {
        fetchCalls += 1;
        throw new Error("离线测试不允许网络访问");
      },
    });
    const unused = db.$with("unused").as(db.select({ id: huge }));
    const used = db.$with("ok").as(db.select({ id: 7 }));
    const query = db.with(unused, used).select({ id: used.id }).from(used);
    const execute = async () => db.count(query.as("count_source")).execute();
    await expect(execute()).rejects.toThrow(rangeError);
    expect(fetchCalls).toBe(0);
  });

  it("used invalid BigInt fails on repeated compilation", () => {
    const db = makeDb();
    const used = db.$with("ok").as(db.select({ id: huge }));
    const query = db.with(used).select({ id: used.id }).from(used);
    expect(() => query.toSQL()).toThrow(rangeError);
    expect(() => query.toSQL()).toThrow(rangeError);
  });
});
