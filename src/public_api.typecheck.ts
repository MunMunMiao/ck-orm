import type * as RootApi from "./index";
import {
  ck,
  ckTable,
  ckType,
  clickhouseClient,
  csql,
  fn,
  type Order,
  type Predicate,
  type Selection,
  type Session,
} from "./index";

const users = ckTable("users", {
  id: ckType.int32(),
  name: ckType.string(),
});
const tempUsers = ckTable("tmp_users", {
  id: ckType.int32(),
  name: ckType.string().default(csql`'anonymous'`),
});
const stringArray = ckType.array(ckType.string());
const nestedUsers = ckType.nested({
  id: ckType.int32(),
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
const constantSelection: Selection<number> = ck.expr(csql`1`, {
  decoder: (value) => Number(value),
  sqlType: "UInt8",
});
const groupedSelection: Selection<number> = ck.expr(csql`toUInt8(1)`, {
  decoder: (value) => Number(value),
  sqlType: "UInt8",
});
const idPredicate: Predicate = ck.eq(users.id, 1);
const sortOrder: Order = ck.asc(nameSelection);
const namespaceFlag: Selection<boolean> = ck.expr<boolean>(csql`1`, {
  decoder: (value) => Number(value) === 1,
  sqlType: "UInt8",
});
const namespacePredicate: Predicate = ck.eq(users.id, 1);
const namespacePredicateGroup: Predicate = ck.and(namespacePredicate, ck.eq(users.id, 2));
const namespaceSortOrder: Order = ck.desc(namespaceFlag);
const namespaceCount: Selection<string> = ck.fn.count();
const namespaceContains: Predicate = ck.contains(users.name, "user_100%");
const namespaceStartsWith: Predicate = ck.startsWith(users.name, "arch_");
const namespaceEndsWith: Predicate = ck.endsWith(users.name, "_done");
const namespaceContainsIgnoreCase: Predicate = ck.containsIgnoreCase(users.name, "user_100%");
const namespaceStartsWithIgnoreCase: Predicate = ck.startsWithIgnoreCase(users.name, "arch_");
const namespaceEndsWithIgnoreCase: Predicate = ck.endsWithIgnoreCase(users.name, "_done");
const jsonArraySelection: Selection<string[]> = fn.jsonExtract(csql`payload`, stringArray, "regulatory");
const namespaceJsonArraySelection: Selection<string[]> = ck.fn.jsonExtract(csql`payload`, stringArray);
const arraySelection: Selection<string[]> = fn.array<string>("vip", "pro");
const arrayConcatSelection: Selection<string[]> = fn.arrayConcat<string>(["vip"], ["pro"]);
const arrayElementSelection: Selection<string> = fn.arrayElement<string>(arraySelection, 1);
const arrayElementOrNullSelection: Selection<string | null> = fn.arrayElementOrNull<string>(arraySelection, 2);
const arraySliceSelection: Selection<string[]> = fn.arraySlice<string>(arraySelection, 1, 2);
const arrayFlattenSelection: Selection<string[]> = fn.arrayFlatten<string>([["vip"], ["pro"]]);
const arrayIntersectSelection: Selection<string[]> = fn.arrayIntersect<string>(["vip"], ["pro", "vip"]);
const arrayExistsSelection: Selection<boolean> = fn.arrayExists(csql`x -> x = 'vip'`, arraySelection);
const arrayFilterSelection: Selection<string[]> = fn.arrayFilter<string>(csql`x -> x != ''`, arraySelection);
const arrayFirstSelection: Selection<string> = fn.arrayFirst<string>(csql`x -> x != ''`, arraySelection);
const arrayFirstOrNullSelection: Selection<string | null> = fn.arrayFirstOrNull<string>(
  csql`x -> x = 'missing'`,
  arraySelection,
);
const emptyArraySelection: Selection<string[]> = fn.emptyArrayString();
const kqlArraySortSelection: Selection<readonly [string[]]> =
  fn.kql_array_sort_asc<readonly [string[]]>(arraySelection);
const hasSubstrPredicate: Predicate = ck.hasSubstr(arraySelection, ["vip"]);
const hasSubstrSelection: Selection<boolean> = fn.hasSubstr(arraySelection, ["vip"]);
const arrayIndexSelection: Selection<string> = fn.indexOf(arraySelection, "vip");
const arrayLengthSelection: Selection<string> = fn.length(arraySelection);
const notEmptySelection: Selection<boolean> = fn.notEmpty(arraySelection);
const targetOrderTupleSelection: Selection<unknown> = fn.arrayJoin(fn.arrayZip([10001], [9001]));
const tupleElementSelection: Selection<string> = fn.tupleElement<string>(fn.tuple(users.id, users.name), 2);
const namespaceTupleElementSelection: Selection<number> = ck.fn.tupleElement<number>(fn.tuple(users.id, users.name), 1);

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
void namespaceContains;
void namespaceStartsWith;
void namespaceEndsWith;
void namespaceContainsIgnoreCase;
void namespaceStartsWithIgnoreCase;
void namespaceEndsWithIgnoreCase;
void jsonArraySelection;
void namespaceJsonArraySelection;
void arraySelection;
void arrayConcatSelection;
void arrayElementSelection;
void arrayElementOrNullSelection;
void arraySliceSelection;
void arrayFlattenSelection;
void arrayIntersectSelection;
void arrayExistsSelection;
void arrayFilterSelection;
void arrayFirstSelection;
void arrayFirstOrNullSelection;
void emptyArraySelection;
void kqlArraySortSelection;
void hasSubstrPredicate;
void hasSubstrSelection;
void arrayIndexSelection;
void arrayLengthSelection;
void notEmptySelection;
void targetOrderTupleSelection;
void tupleElementSelection;
void namespaceTupleElementSelection;
void stringArray;
void nestedUsers;

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

// @ts-expect-error escapeLike should no longer be part of the public ck namespace
ck.escapeLike("literal");

// @ts-expect-error ck.sql should no longer be part of the public ck namespace
ck.sql;

// @ts-expect-error csql only supports tagged-template usage
csql("select 1");

// @ts-expect-error jsonExtract return type must come from ckType
fn.jsonExtract(csql`payload`, "Array(String)");

// @ts-expect-error SqlExpression should remain internal to the package root
const hiddenSqlExpression: RootApi.SqlExpression | undefined = undefined;
void hiddenSqlExpression;

type HasCkType = "ckType" extends keyof typeof import("./index") ? true : false;
const _hasChType: HasCkType = true;

type HasRootInt32 = "int32" extends keyof typeof import("./index") ? true : false;
const _hasRootInt32: HasRootInt32 = false;

// @ts-expect-error schema factory values should no longer stay root-exported
const hiddenInt32 = RootApi.int32;
void hiddenInt32;

// @ts-expect-error Grouping should remain internal to the package root
const hiddenGrouping: RootApi.Grouping | undefined = undefined;
void hiddenGrouping;
