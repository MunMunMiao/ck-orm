// Type-only contract file. It exists to keep the public runtime API honest
// under `tsc --noEmit`; it is not runtime library code.
import { float64, int32, nested, string } from "./columns";
import { ckSql } from "./public_api";
import { clickhouseClient, type Session } from "./runtime";
import { ckTable } from "./schema";

const users = ckTable("users", {
  id: int32(),
  name: string(),
});
const tempUsers = ckTable("tmp_users", {
  id: int32(),
  name: string().default("anonymous"),
});
const fromSelectTarget = ckTable("from_select_target", {
  id: int32(),
  name: string(),
  amount: float64(),
  note: string().default("''"),
});
const fromSelectSource = ckTable("from_select_source", {
  id: int32(),
  name: string(),
  amount: float64(),
  vendor: string(),
});

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  session_max_concurrent_requests: 2,
});

db.runInSession(
  async (session: Session) => {
    await session.createTemporaryTable(tempUsers, { mode: "if_not_exists" });
    await session.createTemporaryTableRaw("tmp_users_raw", "(id Int32)");
    await session.execute(ckSql`select 1`, {
      session_timeout: 30,
      session_check: 1,
    });
    return await session.execute(ckSql`select 1`, {
      session_timeout: 30,
      session_check: 1,
    });
  },
  {
    session_timeout: 30,
    session_check: 1,
  },
);

db.runInSession(async (session: Session) => {
  await session.runInSession(async (nestedSession) => {
    const outerId: string = session.sessionId;
    const innerId: string = nestedSession.sessionId;
    void outerId;
    void innerId;
  });

  await session.withSettings({ max_threads: 2 }).runInSession(async (nestedSession) => {
    await nestedSession.command(ckSql`select 1`);
  });
});

db.insert(users).values({
  id: 1,
  name: "alice",
});

db.insert(users).values([
  {
    id: 2,
    name: "bob",
  },
]);

db.execute(ckSql`select 1`, {
  format: "JSON",
});

db.stream(ckSql`select 1`, {
  format: "JSONEachRow",
});

db.select({
  id: users.id,
})
  .from(users)
  .execute({
    query_id: "typed_query",
  });

// @ts-expect-error insert rows should reject unknown columns
db.insert(users).values({ typo_name: "alice" });

// @ts-expect-error raw eager queries only support JSON output
db.execute(ckSql`select 1`, { format: "JSONEachRow" });

// @ts-expect-error raw streaming queries only support JSONEachRow output
db.stream(ckSql`select 1`, { format: "JSON" });

// @ts-expect-error raw query execution no longer accepts plain strings
db.execute("select 1");

// @ts-expect-error raw command execution no longer accepts plain strings
db.command("select 1");

// @ts-expect-error raw streaming no longer accepts plain strings
db.stream("select 1");

// @ts-expect-error typed builder queries do not expose format overrides
db.select({ id: users.id }).from(users).execute({ format: "JSON" });

// @ts-expect-error typed builder iterators do not expose format overrides
db.select({ id: users.id }).from(users).iterator({ format: "JSONEachRow" });

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  // @ts-expect-error client config no longer accepts session_timeout defaults
  session_timeout: 30,
});

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  // @ts-expect-error client config no longer accepts session_check defaults
  session_check: 1,
});

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  // @ts-expect-error client config no longer accepts custom json hooks
  json: {
    parse: (text: string) => JSON.parse(text) as unknown,
    stringify: (value: unknown) => JSON.stringify(value),
  },
});

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  // @ts-expect-error session_max_concurrent_requests must be a number
  session_max_concurrent_requests: "2",
});

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  tracing: {
    // @ts-expect-error tracing database name is derived from the client config
    dbName: "typecheck_db",
  },
});

// --- insert().fromSelect() positive cases ---

// 1) Exact-shape projection compiles.
db.insert(fromSelectTarget).fromSelect(
  db
    .select({
      id: fromSelectSource.id,
      name: fromSelectSource.name,
      amount: fromSelectSource.amount,
    })
    .from(fromSelectSource),
);

// 2) Projection-key order is irrelevant (alignment is by key, not position).
db.insert(fromSelectTarget).fromSelect(
  db
    .select({
      amount: fromSelectSource.amount,
      id: fromSelectSource.id,
      name: fromSelectSource.name,
    })
    .from(fromSelectSource),
);

// 3) Columns with DEFAULT (`note`) may be omitted from the projection.
db.insert(fromSelectTarget).fromSelect(
  db
    .select({
      id: fromSelectSource.id,
      name: fromSelectSource.name,
      amount: fromSelectSource.amount,
    })
    .from(fromSelectSource),
);

// 4) Columns with DEFAULT may also be explicitly provided.
db.insert(fromSelectTarget).fromSelect(
  db
    .select({
      id: fromSelectSource.id,
      name: fromSelectSource.name,
      amount: fromSelectSource.amount,
      note: fromSelectSource.vendor,
    })
    .from(fromSelectSource),
);

// --- insert().fromSelect() negative cases (compile-time errors) ---

// 5) Missing required column `amount`.
db.insert(fromSelectTarget).fromSelect(
  // @ts-expect-error insert.fromSelect requires all required columns; `amount` is missing
  db
    .select({
      id: fromSelectSource.id,
      name: fromSelectSource.name,
    })
    .from(fromSelectSource),
);

// 6) Unknown column `extra` in projection.
db.insert(fromSelectTarget).fromSelect(
  // @ts-expect-error insert.fromSelect rejects projections that include columns absent from the target table
  db
    .select({
      id: fromSelectSource.id,
      name: fromSelectSource.name,
      amount: fromSelectSource.amount,
      extra: fromSelectSource.vendor,
    })
    .from(fromSelectSource),
);

// 7) Type mismatch — `name` should be string, but we projected an int32 column.
db.insert(fromSelectTarget).fromSelect(
  // @ts-expect-error insert.fromSelect rejects per-column type mismatches between projection and target column
  db
    .select({
      id: fromSelectSource.id,
      name: fromSelectSource.id,
      amount: fromSelectSource.amount,
    })
    .from(fromSelectSource),
);

// 8) After `.values(...)` there is no `.fromSelect(...)` on the narrowed builder.
db.insert(fromSelectTarget)
  .values({ id: 1, name: "alice", amount: 1 })
  // @ts-expect-error InsertValuesBuilder does not expose fromSelect
  .fromSelect(
    db
      .select({
        id: fromSelectSource.id,
        name: fromSelectSource.name,
        amount: fromSelectSource.amount,
      })
      .from(fromSelectSource),
  );

// 9) After `.fromSelect(...)` there is no `.values(...)` on the narrowed builder.
db.insert(fromSelectTarget)
  .fromSelect(
    db
      .select({
        id: fromSelectSource.id,
        name: fromSelectSource.name,
        amount: fromSelectSource.amount,
      })
      .from(fromSelectSource),
  )
  // @ts-expect-error InsertFromSelectBuilder does not expose values
  .values({ id: 1, name: "alice", amount: 1 });

// --- nested column matrix on insert paths ---

const nestedSource = ckTable("nti_source", {
  id: int32(),
  events: nested({ name: string(), score: int32() }),
});
const nestedSink = ckTable("nti_sink", {
  id: int32(),
  events: nested({ name: string(), score: int32() }),
});
const nestedSinkWithType = ckTable("nti_sink_typed", {
  id: int32(),
  // `.$type<…>()` would historically reset the column's `ColumnIoMarker`
  // brand back to `<…, false, false>` — pinning it as required. The
  // orthogonal `NestedColumnBrand` keeps it optional. This case is the
  // regression hot-spot.
  events: nested({ name: string(), score: int32() }).$type<{ name: string; score: number; tag: "ok" | "fail" }[]>(),
});
const nestedSinkRequired = ckTable("nti_sink_required", {
  id: int32(),
  events: nested({ name: string(), score: int32() }).requiredOnInsert(),
});

// 10) `.values()` may omit the nested column on a default-optional table.
db.insert(nestedSink).values({ id: 1 });

// 11) `.values()` may also fill the nested column explicitly.
db.insert(nestedSink).values({ id: 2, events: [{ name: "x", score: 1 }] });

// 12) `.fromSelect()` may omit the nested column too.
db.insert(nestedSink).fromSelect(db.select({ id: nestedSource.id }).from(nestedSource));

// 13) `.fromSelect()` accepts a direct nested column reference (wrap-subquery
//     fans this out into per-field dot-path projections at compile time).
db.insert(nestedSink).fromSelect(db.select({ id: nestedSource.id, events: nestedSource.events }).from(nestedSource));

// 14) Same as 12) but on a `.$type<…>()`-chained nested column — the brand
//     must survive the `.$type` chain or this would TS-error on missing
//     `events`.
db.insert(nestedSinkWithType).values({ id: 3 });
db.insert(nestedSinkWithType).fromSelect(db.select({ id: nestedSource.id }).from(nestedSource));

// 15) `.requiredOnInsert()` flips nested back to required.
//     Omitting `events` must TS-error on both insert paths.
// @ts-expect-error requiredOnInsert nested column must be supplied via .values
db.insert(nestedSinkRequired).values({ id: 4 });
db.insert(nestedSinkRequired).values({ id: 4, events: [{ name: "y", score: 2 }] });
// @ts-expect-error requiredOnInsert nested column must be projected via fromSelect
db.insert(nestedSinkRequired).fromSelect(db.select({ id: nestedSource.id }).from(nestedSource));

// 16) `.requiredOnInsert()` still allows a direct nested column reference
//     through `.fromSelect()` — projection of the source's nested column
//     satisfies the requirement.
db.insert(nestedSinkRequired).fromSelect(
  db.select({ id: nestedSource.id, events: nestedSource.events }).from(nestedSource),
);

// --- nested column $inferInsert chain matrix
// These assertions live in a typecheck file (rather than a *.test.ts file)
// because tsconfig.json `excludes` test files — type-only assertions there
// silently pass. The tuple binding below also forces tsc to evaluate each
// alias eagerly.
import { sql as _sqlForChainMatrix } from "./sql";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Expect<T extends true> = T;

type _SharedNestedShape = { name: string; score: number };
type _CustomNestedEvent = { name: string; score: number; tag: "ok" | "fail" };

const _matrixBare = ckTable("ni_bare", {
  id: int32(),
  events: nested({ name: string(), score: int32() }),
});
type _MatrixBare = Expect<Equal<typeof _matrixBare.$inferInsert, { id: number; events?: _SharedNestedShape[] }>>;

const _matrixDefault = ckTable("ni_default", {
  id: int32(),
  events: nested({ name: string(), score: int32() }).default(_sqlForChainMatrix`[]`),
});
type _MatrixDefault = Expect<Equal<typeof _matrixDefault.$inferInsert, { id: number; events?: _SharedNestedShape[] }>>;

const _matrixMetadata = ckTable("ni_meta", {
  id: int32(),
  events: nested({ name: string(), score: int32() }).comment("audit").codec(_sqlForChainMatrix`ZSTD(3)`),
});
type _MatrixMetadata = Expect<
  Equal<typeof _matrixMetadata.$inferInsert, { id: number; events?: _SharedNestedShape[] }>
>;

const _matrixType = ckTable("ni_type", {
  id: int32(),
  events: nested({ name: string(), score: int32() }).$type<_CustomNestedEvent[]>(),
});
type _MatrixType = Expect<Equal<typeof _matrixType.$inferInsert, { id: number; events?: _CustomNestedEvent[] }>>;

const _matrixMaterialized = ckTable("ni_mat", {
  id: int32(),
  events: nested({ name: string(), score: int32() }).materialized(_sqlForChainMatrix`[]`),
});
type _MatrixMaterialized = Expect<Equal<typeof _matrixMaterialized.$inferInsert, { id: number }>>;

const _matrixRequired = ckTable("ni_req", {
  id: int32(),
  events: nested({ name: string(), score: int32() }).requiredOnInsert(),
});
type _MatrixRequired = Expect<Equal<typeof _matrixRequired.$inferInsert, { id: number; events: _SharedNestedShape[] }>>;

const _matrixRequiredThenType = ckTable("ni_req_type", {
  id: int32(),
  events: nested({ name: string(), score: int32() }).requiredOnInsert().$type<_CustomNestedEvent[]>(),
});
type _MatrixRequiredThenType = Expect<
  Equal<typeof _matrixRequiredThenType.$inferInsert, { id: number; events: _CustomNestedEvent[] }>
>;

// Force eager evaluation of every Expect alias above.
const _nestedChainMatrixAssertions: [
  _MatrixBare,
  _MatrixDefault,
  _MatrixMetadata,
  _MatrixType,
  _MatrixMaterialized,
  _MatrixRequired,
  _MatrixRequiredThenType,
] = [true, true, true, true, true, true, true];
void _nestedChainMatrixAssertions;
