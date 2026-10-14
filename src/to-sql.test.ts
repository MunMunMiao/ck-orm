import { describe, expect, it } from "bun:test";
import { ckTable, ckType } from "./public_api";
import { and, createQueryClient, eq, gte, inArray } from "./query";
import { clickhouseClient } from "./runtime";

const users = ckTable("users", { id: ckType.int32(), name: ckType.string(), region: ckType.string() });
const pets = ckTable("pets", { id: ckType.int32(), ownerId: ckType.int32("owner_id") });

describe("toSQL", () => {
  const db = createQueryClient();

  it("returns the statement and the raw parameter values without running the query", () => {
    const built = db
      .select({ id: users.id })
      .from(users)
      .where(and(eq(users.name, "alice"), gte(users.id, 7)))
      .toSQL();

    expect(built).toEqual({
      sql: "select `users`.`id` as `id` from `users` where (`users`.`name` = {orm_param1:String} and `users`.`id` >= {orm_param2:Int32})",
      params: { orm_param1: "alice", orm_param2: 7 },
    });
  });

  it("reports the settings a builder forces", () => {
    const built = db.select().from(users).leftJoin(pets, eq(users.id, pets.ownerId)).toSQL();

    expect(built.forcedSettings).toEqual({ join_use_nulls: 1 });
  });

  it("returns the body of insert().values() apart from the statement", () => {
    const built = db.insert(users).values({ id: 1, name: "alice", region: "eu" }).toSQL();

    expect(built.sql).toBe("insert into `users` (`id`, `name`, `region`) format Values");
    expect(built.body).toBe("(1,'alice','eu')");
    expect(built.params).toEqual({});
    expect(built.forcedSettings).toEqual({
      input_format_values_interpret_expressions: 0,
      input_format_values_deduce_templates_of_expressions: 0,
    });
  });

  it("compiles insert().fromSelect() with the parameters of its select", () => {
    const built = db
      .insert(users)
      .fromSelect(
        db.select({ id: users.id, name: users.name, region: users.region }).from(users).where(eq(users.region, "eu")),
      )
      .toSQL();

    expect(built.sql).toStartWith("insert into `users` (`id`, `name`, `region`) select ");
    expect(built.params).toEqual({ orm_param1: "eu" });
    expect(built.body).toBeUndefined();
  });

  it("rejects an insert without rows, as executing it does", () => {
    expect(() => db.insert(users).toSQL()).toThrow("insert() requires .values(rows) or .fromSelect(selectBuilder)");
  });

  it("returns copies, so changing the result does not change the builder", () => {
    const query = db.select({ id: users.id }).from(users).where(eq(users.name, "alice"));
    const first = query.toSQL();
    (first.params as Record<string, unknown>).orm_param1 = "mallory";

    expect(query.toSQL().params).toEqual({ orm_param1: "alice" });
  });

  it("is not shadowed by a selected column called toSQL", () => {
    const query = db.select({ toSQL: users.id }).from(users);

    expect(query.toSQL().sql).toBe("select `users`.`id` as `toSQL` from `users`");
  });

  describe("pretty", () => {
    const query = () =>
      db
        .select({ id: users.id, name: users.name })
        .from(users)
        .leftJoin(pets, eq(users.id, pets.ownerId))
        .where(and(eq(users.region, "eu"), gte(users.id, 7)))
        .orderBy(users.name)
        .limit(5);

    it("lays the statement out over several lines", () => {
      expect(query().toSQL({ pretty: true }).sql).toBe(
        [
          "select",
          "  `users`.`id` as `id`,",
          "  `users`.`name` as `name`",
          "from `users`",
          "left join `pets` on `users`.`id` = `pets`.`owner_id`",
          "where (",
          "  `users`.`region` = {orm_param1:String}",
          "  and `users`.`id` >= {orm_param2:Int32}",
          ")",
          "order by `users`.`name` ASC",
          "limit 5",
        ].join("\n"),
      );
    });

    it("changes only whitespace and leaves everything else as toSQL() returns it", () => {
      const plain = query().toSQL();
      const pretty = query().toSQL({ pretty: true });

      expect(pretty.sql.replace(/\s+/g, "")).toBe(plain.sql.replace(/\s+/g, ""));
      expect(pretty.params).toEqual(plain.params);
      expect(pretty.forcedSettings).toEqual(plain.forcedSettings);
      expect(query().toSQL({ pretty: false })).toEqual(plain);
    });

    it("indents subqueries", () => {
      const inner = db.select({ ownerId: pets.ownerId }).from(pets);
      const built = db.select({ id: users.id }).from(users).where(inArray(users.id, inner)).toSQL({ pretty: true });

      expect(built.sql).toBe(
        [
          "select",
          "  `users`.`id` as `id`",
          "from `users`",
          "where `users`.`id` in (",
          "  select",
          "    `pets`.`owner_id` as `ownerId`",
          "  from `pets`",
          ")",
        ].join("\n"),
      );
    });

    it("puts each inserted row on its own line", () => {
      const built = db
        .insert(users)
        .values([
          { id: 1, name: "a, b", region: "eu" },
          { id: 2, name: "c", region: "us" },
        ])
        .toSQL({ pretty: true });

      expect(built.sql).toBe("insert into `users` (`id`, `name`, `region`) format Values");
      expect(built.body).toBe("(1,'a, b','eu'),\n(2,'c','us')");
    });

    it("does not change what the client sends", async () => {
      const statements: string[] = [];
      const client = clickhouseClient({
        host: "http://clickhouse.test",
        fetch: async (request) => {
          statements.push(String((await request.formData()).get("query")).replaceAll("\r\n", "\n"));
          return Response.json({ data: [] });
        },
      });
      const run = client.select({ id: users.id }).from(users).where(eq(users.region, "eu"));

      run.toSQL({ pretty: true });
      await run;

      expect(statements).toEqual([`${run.toSQL().sql}\nFORMAT JSON`]);
    });
  });

  it("matches what the client sends, apart from the response format the transport appends", async () => {
    const sent: { statement: string; params: Record<string, string> }[] = [];
    const client = clickhouseClient({
      host: "http://clickhouse.test",
      fetch: async (request) => {
        const form = await request.formData();
        sent.push({
          // Multipart form fields carry CRLF line breaks.
          statement: String(form.get("query")).replaceAll("\r\n", "\n"),
          params: Object.fromEntries(
            [...form].filter(([key]) => key.startsWith("param_")).map(([key, value]) => [key.slice(6), String(value)]),
          ),
        });
        return Response.json({ data: [] });
      },
    });
    const query = client
      .select({ id: users.id })
      .from(users)
      .where(and(eq(users.name, "alice"), gte(users.id, 7)))
      .limit(5);

    const built = query.toSQL();
    await query;

    expect(sent).toEqual([
      { statement: `${built.sql}\nFORMAT JSON`, params: { orm_param1: "alice", orm_param2: "7" } },
    ]);
    expect(Object.keys(built.params)).toEqual(Object.keys(sent[0]?.params ?? {}));
  });
});
