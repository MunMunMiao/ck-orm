import { describe, expect, it } from "bun:test";
import {
  aggregateFunction,
  dateTime64,
  decimal,
  enum8,
  enum16,
  fixedString,
  float32,
  int32,
  nested,
  qbit,
  simpleAggregateFunction,
  string,
  time64,
} from "./columns";
import { fn, tableFn } from "./functions";
import {
  compileQuerySymbol,
  contains,
  containsIgnoreCase,
  createSelectBuilder,
  endsWith,
  endsWithIgnoreCase,
  ilike,
  like,
  notLike,
  startsWith,
  startsWithIgnoreCase,
} from "./query";
import { normalizeSingleStatementSql } from "./runtime/sql-scan";
import { ckTable } from "./schema";
import { compileSql, sql } from "./sql";

const _normalizeSql = (value: string) => value.replace(/\s+/g, " ").trim();

describe("ck-orm sql security", function describeSqlSecurity() {
  it("rejects identifiers with backticks", function testIdentifierEscaping() {
    expect(() => compileSql(sql.identifier("users`--"))).toThrow("Invalid SQL identifier: users`--");
    expect(() => compileSql(sql.identifier({ table: "tab`le", column: "id" }))).toThrow(
      "Invalid SQL identifier: tab`le",
    );
    expect(() => compileSql(sql.identifier({ table: "users", column: "id; DROP TABLE" }))).toThrow(
      "Invalid SQL identifier: id; DROP TABLE",
    );
  });

  it("rejects identifiers with spaces and special chars", function testIdentifierCharsetValidation() {
    expect(() => compileSql(sql.identifier("users table"))).toThrow("Invalid SQL identifier: users table");
    expect(() => compileSql(sql.identifier('users"table'))).toThrow('Invalid SQL identifier: users"table');
    expect(() => compileSql(sql.identifier("users'table"))).toThrow("Invalid SQL identifier: users'table");
    expect(() => compileSql(sql.identifier("123table"))).toThrow("Invalid SQL identifier: 123table");
    expect(() => compileSql(sql.identifier(""))).toThrow("Invalid SQL identifier:");
  });

  it("allows valid identifiers", function testValidIdentifiers() {
    expect(compileSql(sql.identifier("users")).query).toBe("`users`");
    expect(compileSql(sql.identifier("_users")).query).toBe("`_users`");
    expect(compileSql(sql.identifier({ table: "users", column: "id" })).query).toBe("`users`.`id`");
  });

  it("escapes string parameters with unicode line separators", function testUnicodeLineSeparators() {
    const evil = "'; DROP TABLE users; --";
    const compiled = compileSql(sql`SELECT * FROM users WHERE name = ${evil}`);
    expect(compiled.query).toContain("{orm_param1:String}");
    expect(compiled.params.orm_param1).toBe(evil);

    const lineSep = "hello\u2028world";
    const paraSep = "hello\u2029world";
    const compiled2 = compileSql(sql`SELECT * WHERE a = ${lineSep} AND b = ${paraSep}`);
    expect(compiled2.params.orm_param1).toBe(lineSep);
    expect(compiled2.params.orm_param2).toBe(paraSep);
  });

  it("rejects invalid function names", function testFunctionNameWhitelist() {
    const buildCtx = () => ({ params: {}, nextParamIndex: 0 });
    expect(() => fn.call("; DROP TABLE users; --", []).compile(buildCtx())).toThrow(
      "Invalid function name: ; DROP TABLE users; --",
    );
    expect(() => fn.call("sum`drop", []).compile(buildCtx())).toThrow("Invalid function name: sum`drop");
    expect(() => fn.call("sum drop", []).compile(buildCtx())).toThrow("Invalid function name: sum drop");
    expect(() => fn.call("1sum", []).compile(buildCtx())).toThrow("Invalid function name: 1sum");
    expect(() => fn.withParams("; DROP TABLE users; --", [0.95], []).compile(buildCtx())).toThrow(
      "Invalid function name: ; DROP TABLE users; --",
    );
    expect(() => fn.withParams("sum`drop", [0.95], []).compile(buildCtx())).toThrow("Invalid function name: sum`drop");
  });

  it("allows valid function names", function testValidFunctionNames() {
    const buildCtx = () => ({ params: {}, nextParamIndex: 0 });
    expect(() => fn.call("sum", []).compile(buildCtx())).not.toThrow();
    expect(() => fn.call("_sum", []).compile(buildCtx())).not.toThrow();
    expect(() => fn.call("avgIf", []).compile(buildCtx())).not.toThrow();
    expect(() => fn.call("groupArrayArray", []).compile(buildCtx())).not.toThrow();
  });

  it("escapes enum keys with single quotes", function testEnumKeyEscaping() {
    const col = enum8({ "active' OR '1'='1": 1, normal: 2 });
    expect(col.sqlType).toBe("Enum8('active\\' OR \\'1\\'=\\'1' = 1, 'normal' = 2)");
  });

  it("escapes enum16 keys with single quotes", function testEnum16KeyEscaping() {
    const col = enum16({ "paused'; DROP": 100 });
    expect(col.sqlType).toBe("Enum16('paused\\'; DROP' = 100)");
  });

  it("escapes dateTime64 timezone with single quotes", function testDateTime64TimezoneEscaping() {
    const col = dateTime64({ precision: 3, timezone: "Asia/Shanghai' OR '1'='1" });
    expect(col.sqlType).toBe("DateTime64(3, 'Asia/Shanghai\\' OR \\'1\\'=\\'1')");
  });

  it("rejects invalid aggregate function names", function testAggregateFunctionValidation() {
    expect(() => aggregateFunction("; DROP", string())).toThrow("Invalid aggregate function name: ; DROP");
    expect(() => aggregateFunction("sum drop", string())).toThrow("Invalid aggregate function name: sum drop");
    expect(() => simpleAggregateFunction("; DROP", string())).toThrow("Invalid simple aggregate function name: ; DROP");
  });

  it("rejects invalid nested column names", function testNestedColumnNameValidation() {
    expect(() => nested({ "id; DROP": int32() })).toThrow("Invalid nested column name: id; DROP");
    expect(() => nested({ "123id": int32() })).toThrow("Invalid nested column name: 123id");
    expect(() => nested({ "col name": int32() })).toThrow("Invalid nested column name: col name");
  });

  it("allows valid nested column names", function testValidNestedColumnNames() {
    const col = nested({ id: int32(), name: string() });
    expect(col.sqlType).toBe("Nested(id Int32, name String)");
  });

  it("rejects column constructors with invalid parameters", function testColumnParameterValidation() {
    expect(() => fixedString({ length: 0 })).toThrow("fixedString length must be a positive integer, got 0");
    expect(() => fixedString({ length: -1 })).toThrow("fixedString length must be a positive integer, got -1");
    expect(() => fixedString({ length: 1.5 })).toThrow("fixedString length must be a positive integer, got 1.5");

    expect(() => decimal({ precision: 0, scale: 0 })).toThrow(
      "decimal precision must be an integer between 1 and 76, got 0",
    );
    expect(() => decimal({ precision: 77, scale: 0 })).toThrow(
      "decimal precision must be an integer between 1 and 76, got 77",
    );
    expect(() => decimal({ precision: 10, scale: -1 })).toThrow(
      "decimal scale must be an integer between 0 and precision (10), got -1",
    );
    expect(() => decimal({ precision: 10, scale: 11 })).toThrow(
      "decimal scale must be an integer between 0 and precision (10), got 11",
    );
    expect(() => decimal({ precision: 10.5, scale: 2 })).toThrow(
      "decimal precision must be an integer between 1 and 76, got 10.5",
    );

    expect(() => time64({ precision: -1 })).toThrow("time64 precision must be an integer between 0 and 9, got -1");
    expect(() => time64({ precision: 10 })).toThrow("time64 precision must be an integer between 0 and 9, got 10");
    expect(() => time64({ precision: 1.5 })).toThrow("time64 precision must be an integer between 0 and 9, got 1.5");

    expect(() => qbit(float32(), { dimensions: 0 })).toThrow("qbit dimensions must be a positive integer, got 0");
    expect(() => qbit(float32(), { dimensions: -1 })).toThrow("qbit dimensions must be a positive integer, got -1");
    expect(() => qbit(float32(), { dimensions: 1.5 })).toThrow("qbit dimensions must be a positive integer, got 1.5");
  });

  it("escapes backslashes before single quotes in enum keys to prevent boundary escape", function testEnumBackslashEscaping() {
    const trailingBackslash = enum8({ "a\\": 1 });
    expect(trailingBackslash.sqlType).toBe("Enum8('a\\\\' = 1)");

    const slashThenInjection = enum16({ "x\\' OR 1=1 --": 9 });
    expect(slashThenInjection.sqlType).toBe("Enum16('x\\\\\\' OR 1=1 --' = 9)");
  });

  it("escapes backslashes before single quotes in dateTime64 timezone", function testDateTime64BackslashEscaping() {
    const col = dateTime64({ precision: 3, timezone: "Asia/Shanghai\\" });
    expect(col.sqlType).toBe("DateTime64(3, 'Asia/Shanghai\\\\')");

    const evil = dateTime64({ precision: 3, timezone: "UTC\\' OR '1'='1" });
    expect(evil.sqlType).toBe("DateTime64(3, 'UTC\\\\\\' OR \\'1\\'=\\'1')");
  });

  it("rejects dangerous sql.join separator patterns", function testJoinSeparatorSafety() {
    const evilSeparator = "`) UNION ALL SELECT password FROM users; -- ";
    expect(() => compileSql(sql.join([sql.identifier("a"), sql.identifier("b")], evilSeparator))).toThrow(
      "Invalid SQL join separator",
    );

    expect(() => compileSql(sql.join([sql.identifier("a")], "'; DROP"))).toThrow("Invalid SQL join separator");
    expect(() => compileSql(sql.join([sql.identifier("a")], "a--b"))).toThrow("Invalid SQL join separator");
    expect(() => compileSql(sql.join([sql.identifier("a")], "/*x*/"))).toThrow("Invalid SQL join separator");
    expect(() => compileSql(sql.join([sql.identifier("a")], "a;b"))).toThrow("Invalid SQL join separator");

    // Tightened allowlist also rejects SQL keywords disguised as separators —
    // these previously slipped through because the regex permitted ASCII
    // letters. Reproduces a real PoC: `sql.join([cond1, cond2], " OR 1=1 OR ")`
    // would compile to `cond1 OR 1=1 OR cond2`, neutralising the conditions.
    expect(() => compileSql(sql.join([sql.identifier("a")], " OR 1=1 OR "))).toThrow("Invalid SQL join separator");
    expect(() => compileSql(sql.join([sql.identifier("a")], " AND "))).toThrow("Invalid SQL join separator");
    expect(() => compileSql(sql.join([sql.identifier("a")], " OR "))).toThrow("Invalid SQL join separator");
    expect(() => compileSql(sql.join([sql.identifier("a")], " UNION ALL "))).toThrow("Invalid SQL join separator");
    expect(() => compileSql(sql.join([sql.identifier("a")], " == "))).toThrow("Invalid SQL join separator");
    expect(() => compileSql(sql.join([sql.identifier("a")], " < "))).toThrow("Invalid SQL join separator");
    expect(() => compileSql(sql.join([sql.identifier("a")], " + "))).toThrow("Invalid SQL join separator");
  });

  it("allows safe sql.join separators", function testJoinSafeSeparators() {
    // String separators are now restricted to whitespace and grouping
    // punctuation only. Anything more (keywords, operators) must travel as
    // an SQLFragment to make the raw-SQL intent explicit at the call site.
    expect(compileSql(sql.join([sql.identifier("a"), sql.identifier("b")], ", ")).query).toBe("`a`, `b`");
    expect(compileSql(sql.join([sql.identifier("a"), sql.identifier("b")], " ")).query).toBe("`a` `b`");
    expect(compileSql(sql.join([sql.identifier("a"), sql.identifier("b")], "(),")).query).toBe("`a`(),`b`");
  });

  it("supports keyword separators when wrapped as SQLFragments", function testJoinFragmentSeparators() {
    expect(compileSql(sql.join([sql.identifier("a"), sql.identifier("b")], sql` AND `)).query).toBe("`a` AND `b`");
    expect(compileSql(sql.join([sql.identifier("a"), sql.identifier("b")], sql` OR `)).query).toBe("`a` OR `b`");
    expect(compileSql(sql.join([sql.identifier("a"), sql.identifier("b")], sql.raw(" UNION ALL "))).query).toBe(
      "`a` UNION ALL `b`",
    );
  });

  it("rejects invalid table function names at compile time", function testTableFunctionNameValidation() {
    const badFn = tableFn.call("; DROP TABLE", 100);
    expect(() => compileSql(badFn.compileSource({ params: {}, nextParamIndex: 0 }))).toThrow("Invalid function name");
    const badFn2 = tableFn.call("numbers`evil", 100);
    expect(() => compileSql(badFn2.compileSource({ params: {}, nextParamIndex: 0 }))).toThrow("Invalid function name");
  });

  it("rejects null and undefined parameters", function testNullUndefinedParameters() {
    expect(() => compileSql(sql`SELECT * WHERE id = ${null}`)).toThrow(
      "Raw SQL parameters do not support null or undefined",
    );
    expect(() => compileSql(sql`SELECT * WHERE id = ${undefined}`)).toThrow(
      "Raw SQL parameters do not support null or undefined",
    );
  });

  it("safely handles parameter name collision attempts", function testParamNameCollision() {
    const evil = "{orm_param1:String}";
    const compiled = compileSql(sql`SELECT * WHERE name = ${evil}`);
    // The parameter name should use orm_param1, and the value should be the literal string
    expect(compiled.params.orm_param1).toBe(evil);
  });

  it("does not trust user-shaped compile objects inside ckSql templates", function testFakeCompileObjects() {
    const fakeExpression = {
      compile() {
        return sql.raw("1 OR 1=1");
      },
    };
    const fakeSource = {
      compileSource() {
        return sql.raw("numbers(1)");
      },
    };

    expect(() => compileSql(sql`SELECT ${fakeExpression}`)).toThrow("Unsupported SQL parameter value");
    expect(() => compileSql(sql`SELECT * FROM ${fakeSource}`)).toThrow("Unsupported SQL parameter value");
  });

  it("rejects empty structured identifiers", function testEmptyStructuredIdentifiers() {
    expect(() => compileSql(sql.identifier({}))).toThrow("Invalid SQL identifier object");
    expect(() => compileSql(sql.identifier({ table: "" }))).toThrow("table must be a non-empty identifier");
    expect(() => compileSql(sql.identifier({ column: "" }))).toThrow("column must be a non-empty identifier");
    expect(() => compileSql(sql.identifier({ as: "" }))).toThrow("as must be a non-empty identifier");
  });

  it("allows semicolons inside comments and string literals", function testSemicolonInComments() {
    const commentResult = normalizeSingleStatementSql("SELECT 1 /* this; is; ok */", "x");
    expect(commentResult).toContain("SELECT 1");

    const stringResult = normalizeSingleStatementSql("SELECT '; DROP' AS test", "x");
    expect(stringResult).toContain("SELECT");
  });

  it("rejects actual multi-statements", function testActualMultiStatements() {
    expect(() => normalizeSingleStatementSql("SELECT 1; DROP TABLE users", "multiple statements not allowed")).toThrow(
      "multiple statements not allowed",
    );
  });

  it("rejects raw AggregateFunction type arguments with unsafe tokens", function testAggregateFunctionArgTypes() {
    expect(aggregateFunction("quantile(0.5)", "Float64").sqlType).toBe("AggregateFunction(quantile(0.5), Float64)");
    expect(() => aggregateFunction("quantile(now())", "Float64")).toThrow("Invalid aggregate function name");
    expect(() => aggregateFunction("sum", "UInt64; DROP TABLE users")).toThrow(
      "Invalid AggregateFunction argument type",
    );
    expect(() => aggregateFunction("sum", "String -- comment")).toThrow("Invalid AggregateFunction argument type");
    expect(aggregateFunction("sum", "UInt64").sqlType).toBe("AggregateFunction(sum, UInt64)");
  });

  it("rejects identifiers with Unicode symbols", function testUnicodeIdentifierRejection() {
    expect(() => compileSql(sql.identifier("user\u0000name"))).toThrow("Invalid SQL identifier");
    expect(() => compileSql(sql.identifier("user\u2028name"))).toThrow("Invalid SQL identifier");
    expect(() => compileSql(sql.identifier("用户表"))).toThrow("Invalid SQL identifier");
  });

  it("documents orderBy sql.raw behavior", function testOrderByRawBehavior() {
    const testTable = ckTable("users", { id: int32(), name: string() });
    const builder = createSelectBuilder({ fromSource: testTable });
    const rawExpr = sql.raw("1; DROP TABLE users; --");
    const query = builder.orderBy(rawExpr);
    const compiled = query[compileQuerySymbol]();
    expect(compiled.statement).toContain("1; DROP TABLE users; --");
  });

  it("like compiles to parameterized query", function testLikeParameterization() {
    const testTable = ckTable("users", { id: int32(), name: string() });
    const builder = createSelectBuilder({ fromSource: testTable });
    const query = builder.where(like(testTable.name, "'; DROP TABLE users; --"));
    const compiled = query[compileQuerySymbol]();
    expect(compiled.statement).toContain("like");
    expect(compiled.statement).toContain("{orm_param1:String}");
    expect(compiled.params.orm_param1).toBe("'; DROP TABLE users; --");
  });

  it("keeps raw LIKE and ILIKE patterns unchanged", function testRawLikePatternSemantics() {
    const testTable = ckTable("users", { id: int32(), name: string() });
    const builder = createSelectBuilder({ fromSource: testTable });
    const rawPattern = "50%_\\";

    const likeCompiled = builder.where(like(testTable.name, rawPattern))[compileQuerySymbol]();
    expect(likeCompiled.statement).toContain("like");
    expect(likeCompiled.params.orm_param1).toBe(rawPattern);

    const ilikeCompiled = builder.where(ilike(testTable.name, rawPattern))[compileQuerySymbol]();
    expect(ilikeCompiled.statement).toContain("ilike");
    expect(ilikeCompiled.params.orm_param1).toBe(rawPattern);
  });

  it("notLike compiles to parameterized query", function testNotLikeParameterization() {
    const testTable = ckTable("users", { id: int32(), name: string() });
    const builder = createSelectBuilder({ fromSource: testTable });
    const query = builder.where(notLike(testTable.name, "admin%"));
    const compiled = query[compileQuerySymbol]();
    expect(compiled.statement).toContain("not like");
    expect(compiled.statement).toContain("{orm_param1:String}");
    expect(compiled.params.orm_param1).toBe("admin%");
  });

  it("semantic pattern helpers escape wildcard characters internally", function testSemanticPatternHelpers() {
    const testTable = ckTable("users", { id: int32(), name: string() });
    const builder = createSelectBuilder({ fromSource: testTable });
    const literal = "50%_\\";

    const containsCompiled = builder.where(contains(testTable.name, literal))[compileQuerySymbol]();
    expect(containsCompiled.statement).toContain("like");
    expect(containsCompiled.params.orm_param1).toBe("%50\\%\\_\\\\%");

    const startsWithCompiled = builder.where(startsWith(testTable.name, literal))[compileQuerySymbol]();
    expect(startsWithCompiled.params.orm_param1).toBe("50\\%\\_\\\\%");

    const endsWithCompiled = builder.where(endsWith(testTable.name, literal))[compileQuerySymbol]();
    expect(endsWithCompiled.params.orm_param1).toBe("%50\\%\\_\\\\");

    const containsIgnoreCaseCompiled = builder.where(containsIgnoreCase(testTable.name, literal))[compileQuerySymbol]();
    expect(containsIgnoreCaseCompiled.statement).toContain("ilike");
    expect(containsIgnoreCaseCompiled.params.orm_param1).toBe("%50\\%\\_\\\\%");

    const startsWithIgnoreCaseCompiled = builder
      .where(startsWithIgnoreCase(testTable.name, literal))
      [compileQuerySymbol]();
    expect(startsWithIgnoreCaseCompiled.statement).toContain("ilike");
    expect(startsWithIgnoreCaseCompiled.params.orm_param1).toBe("50\\%\\_\\\\%");

    const endsWithIgnoreCaseCompiled = builder.where(endsWithIgnoreCase(testTable.name, literal))[compileQuerySymbol]();
    expect(endsWithIgnoreCaseCompiled.statement).toContain("ilike");
    expect(endsWithIgnoreCaseCompiled.params.orm_param1).toBe("%50\\%\\_\\\\");
  });
});
