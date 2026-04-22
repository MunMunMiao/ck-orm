import type * as RootApi from "./index";
import {
  asc,
  chTable,
  clickhouseClient,
  eq,
  expr,
  fn,
  int32,
  type Order,
  type Predicate,
  type Selection,
  sql,
  string,
} from "./index";

const users = chTable("users", {
  id: int32(),
  name: string(),
});

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/public_api_typecheck",
  schema: { users },
});

const nameSelection: Selection<string> = fn.toString(users.name);
const constantSelection: Selection<number> = expr(sql.raw("1"), {
  decoder: (value) => Number(value),
  sqlType: "UInt8",
});
const groupedSelection: Selection<number> = expr(sql.raw("toUInt8(1)"), {
  decoder: (value) => Number(value),
  sqlType: "UInt8",
});
const idPredicate: Predicate = eq(users.id, 1);
const sortOrder: Order = asc(nameSelection);

const groupedSelections = [users.id, groupedSelection] satisfies Selection[];
const orderedSelections = [sortOrder, asc(users.id), asc(groupedSelection)] satisfies Order[];

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

const composed = fn
  .sum(users.id)
  .mapWith((value) => Number(value))
  .as("total_count");
void composed;

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
