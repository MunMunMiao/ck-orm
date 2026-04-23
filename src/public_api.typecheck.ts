import type * as RootApi from "./index";
import {
  chTable,
  ck,
  clickhouseClient,
  fn,
  int32,
  type Order,
  type Predicate,
  type Selection,
  type Session,
  string,
} from "./index";

const users = chTable("users", {
  id: int32(),
  name: string(),
});
const tempUsers = chTable("tmp_users", {
  id: int32(),
  name: string().default(ck.sql`'anonymous'`),
});

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/public_api_typecheck",
  schema: { users },
});

db.runInSession(async (session: Session) => {
  await session.createTemporaryTable(tempUsers);
  await session.createTemporaryTableRaw("tmp_users_raw", "(id Int32)");
  await session.select({ id: tempUsers.id }).from(tempUsers).execute();
});

const nameSelection: Selection<string> = fn.toString(users.name);
const constantSelection: Selection<number> = ck.expr(ck.sql.raw("1"), {
  decoder: (value) => Number(value),
  sqlType: "UInt8",
});
const groupedSelection: Selection<number> = ck.expr(ck.sql.raw("toUInt8(1)"), {
  decoder: (value) => Number(value),
  sqlType: "UInt8",
});
const idPredicate: Predicate = ck.eq(users.id, 1);
const sortOrder: Order = ck.asc(nameSelection);
const namespaceFlag: Selection<boolean> = ck.expr<boolean>(ck.sql.raw("1"), {
  decoder: (value) => Number(value) === 1,
  sqlType: "UInt8",
});
const namespacePredicate: Predicate = ck.eq(users.id, 1);
const namespacePredicateGroup: Predicate = ck.and(namespacePredicate, ck.eq(users.id, 2));
const namespaceSortOrder: Order = ck.desc(namespaceFlag);
const namespaceCount: Selection<string> = ck.fn.count();

const groupedSelections = [users.id, groupedSelection] satisfies Selection[];
const orderedSelections = [sortOrder, ck.asc(users.id), ck.asc(groupedSelection)] satisfies Order[];

const builder = db
  .select({
    id: users.id,
    name: nameSelection,
    constantOne: constantSelection,
  })
  .from(users)
  .where(idPredicate)
  .groupBy(...groupedSelections)
  .orderBy(...orderedSelections, users.id, groupedSelection)
  .limitBy(groupedSelections, 1);

void builder;
void namespacePredicateGroup;
void namespaceCount;

const composed = fn
  .sum(users.id)
  .mapWith((value) => Number(value))
  .as("total_count");
void composed;

const namespaceBuilder = db
  .select({
    id: users.id,
    flag: namespaceFlag,
    total: ck.fn.count().as("total"),
  })
  .from(users)
  .where(namespacePredicate)
  .orderBy(namespaceSortOrder);

void namespaceBuilder;

const numbers = fn.table.call("numbers", 3).as("n");

const tableFunctionBuilder = db
  .select({
    total: fn.count().as("total"),
  })
  .from(numbers);

void tableFunctionBuilder;

// @ts-expect-error Selection should not expose compile
nameSelection.compile;
// @ts-expect-error Selection should not expose decoder
nameSelection.decoder;
// @ts-expect-error Selection should not expose sqlType
nameSelection.sqlType;
// @ts-expect-error Selection should not expose sourceKey
nameSelection.sourceKey;

// @ts-expect-error Predicate should not expose compile
idPredicate.compile;
// @ts-expect-error Predicate should not expose decoder
idPredicate.decoder;

// @ts-expect-error SqlExpression should remain internal to the package root
const hiddenSqlExpression: RootApi.SqlExpression | undefined = undefined;
void hiddenSqlExpression;

// @ts-expect-error Grouping should remain internal to the package root
const hiddenGrouping: RootApi.Grouping | undefined = undefined;
void hiddenGrouping;
