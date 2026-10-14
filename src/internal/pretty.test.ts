import { describe, expect, it } from "bun:test";
import { prettyRows, prettyStatement } from "./pretty";

describe("prettyStatement", () => {
  it("starts a line for each clause and puts select items one per line", () => {
    expect(
      prettyStatement(
        "select a, f(b, c) as d from t inner join u on t.id = u.id where x = 1 group by a having n > 1 order by a ASC limit 5 offset 2",
      ),
    ).toBe(
      [
        "select",
        "  a,",
        "  f(b, c) as d",
        "from t",
        "inner join u on t.id = u.id",
        "where x = 1",
        "group by a",
        "having n > 1",
        "order by a ASC",
        "limit 5",
        "offset 2",
      ].join("\n"),
    );
  });

  it("indents subqueries and CTE bodies", () => {
    expect(
      prettyStatement(
        "with a as (select 1 as x), b as (select 2 as y) select x from (select x from a) as s where x in (select y from b)",
      ),
    ).toBe(
      [
        "with",
        "  a as (",
        "    select",
        "      1 as x",
        "  ),",
        "  b as (",
        "    select",
        "      2 as y",
        "  )",
        "select",
        "  x",
        "from (",
        "  select",
        "    x",
        "  from a",
        ") as s",
        "where x in (",
        "  select",
        "    y",
        "  from b",
        ")",
      ].join("\n"),
    );
  });

  it("puts each condition of an and/or group on its own line", () => {
    expect(
      prettyStatement(
        "select a from t where ((x = 1 or y between 2 and 3) and not (z = 4 or w = 5) and f(a and b, c) and v in (1, 2))",
      ),
    ).toBe(
      [
        "select",
        "  a",
        "from t",
        "where (",
        "  (",
        "    x = 1",
        "    or y between 2 and 3",
        "  )",
        "  and not (",
        "    z = 4",
        "    or w = 5",
        "  )",
        "  and f(a and b, c)",
        "  and v in (1, 2)",
        ")",
      ].join("\n"),
    );
  });

  it("does not count the and of a between as a condition", () => {
    expect(prettyStatement("select a from t where (x between 1 and 2)")).toBe(
      ["select", "  a", "from t", "where (x between 1 and 2)"].join("\n"),
    );
    expect(prettyStatement("select a from t where (x between 1 and 2 and y = 3)")).toBe(
      ["select", "  a", "from t", "where (", "  x between 1 and 2", "  and y = 3", ")"].join("\n"),
    );
  });

  it("puts the select of an insert on its own line", () => {
    expect(prettyStatement("insert into t (a, b) select a, b from s")).toBe(
      ["insert into t (a, b)", "select", "  a,", "  b", "from s"].join("\n"),
    );
  });

  it("leaves quoted text, brackets and non-clause words alone", () => {
    const statement =
      "select 'a, from b where \\' c', \"x, y\", `from`, [1, 2], {p:Array(Int32)}, left(s, 2), constructor from t where s = 'it''s'";
    expect(prettyStatement(statement)).toBe(
      [
        "select",
        "  'a, from b where \\' c',",
        '  "x, y",',
        "  `from`,",
        "  [1, 2],",
        "  {p:Array(Int32)},",
        "  left(s, 2),",
        "  constructor",
        "from t",
        "where s = 'it''s'",
      ].join("\n"),
    );
  });

  it("only changes whitespace", () => {
    const statement = "select  a ,b   from t where (x = 1 and y in (select z from u))";
    const strip = (text: string) => text.replace(/\s+/g, "");
    expect(strip(prettyStatement(statement))).toBe(strip(statement));
  });

  it("returns the statement unchanged when line breaks could change its meaning or it is unbalanced", () => {
    for (const statement of [
      "select 1 -- comment\nfrom t",
      "select 1 /* c */ from t",
      "select $$a$$ from t",
      "select 'open from t",
      "select (1 from t",
      "select 1) from t",
      "select (1] from t",
      "",
    ]) {
      expect(prettyStatement(statement)).toBe(statement);
    }
  });
});

describe("prettyRows", () => {
  it("puts each row on its own line", () => {
    expect(prettyRows("(1,'a,b',[1,2]), (2,'(x)',(3,4))")).toBe("(1,'a,b',[1,2]),\n(2,'(x)',(3,4))");
  });

  it("keeps a single row as it is", () => {
    expect(prettyRows("(1,'a')")).toBe("(1,'a')");
  });

  it("keeps the spaces inside a row", () => {
    expect(prettyRows("(1, 'a b'), (2, 'c')")).toBe("(1, 'a b'),\n(2, 'c')");
  });

  it("returns the body unchanged when it cannot lay it out with certainty", () => {
    for (const body of ["(1,'open), (2,'x')", "(1,2), (3,4", "(1,2)) , (3,4)"]) {
      expect(prettyRows(body)).toBe(body);
    }
  });
});
