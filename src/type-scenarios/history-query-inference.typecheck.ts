import { ck, ckSql, ckTable, ckType, fn, type Selection } from "../index";
import type { Equal, Expect } from "./helpers";

type SelectionData<TValue> = TValue extends Selection<infer TData> ? TData : never;

const historyOrder = ckTable("history_order", {
  closedAt: ckType.dateTime64({ precision: 3 }),
  active: ckType.bool(),
  id: ckType.int32(),
  amount: ckType.decimal({ precision: 18, scale: 5 }),
  nullableAmount: ckType.nullable(ckType.decimal({ precision: 18, scale: 5 })),
  lowCardinalityAmount: ckType.lowCardinality(ckType.decimal({ precision: 18, scale: 5 })),
  ratio: ckType.float64(),
  lowCardinalityRatio: ckType.lowCardinality(ckType.float64()),
  nullableRatio: ckType.nullable(ckType.float64()),
  largeId: ckType.int64(),
});

type HistoryPageTuple = [Date, boolean, number];

const pageRow = fn.tuple(historyOrder.closedAt, historyOrder.active, historyOrder.id);
type _TupleData = Expect<Equal<SelectionData<typeof pageRow>, HistoryPageTuple>>;

const rawTuple = fn.tuple(ckSql<string>`'history'`, ckSql<number>`1`);
type _RawTupleData = Expect<Equal<SelectionData<typeof rawTuple>, [string, number]>>;

const groupedRows = fn.withParams("groupArray", [21], pageRow);
type _GroupedRowsData = Expect<Equal<SelectionData<typeof groupedRows>, HistoryPageTuple[]>>;

const sortedRows = fn.arrayReverseSort(groupedRows);
type _SortedRowsData = Expect<Equal<SelectionData<typeof sortedRows>, HistoryPageTuple[]>>;

const explicitGroupedRows = fn.withParams<HistoryPageTuple[]>("groupArray", [21], pageRow);
type _ExplicitGroupedRowsData = Expect<Equal<SelectionData<typeof explicitGroupedRows>, HistoryPageTuple[]>>;

const explicitSortedRows = fn.arrayReverseSort<HistoryPageTuple>(explicitGroupedRows);
type _ExplicitSortedRowsData = Expect<Equal<SelectionData<typeof explicitSortedRows>, HistoryPageTuple[]>>;

const lambdaSortedRows = fn.arrayReverseSort<number>(ckSql`x -> -x`, [1, 2], [3, 4]);
type _LambdaSortedRowsData = Expect<Equal<SelectionData<typeof lambdaSortedRows>, number[]>>;

const broadSum = fn.sum(ck.expr(ckSql`value`));
const broadSumIf = fn.sumIf(ck.expr(ckSql`value`), historyOrder.active);
const decimalAmountSum = fn.sum(historyOrder.amount);
const nullableDecimalAmountSum = fn.sumIf(historyOrder.nullableAmount, historyOrder.active);
const lowCardinalityDecimalAmountSum = fn.sumIf(historyOrder.lowCardinalityAmount, historyOrder.active);
const floatSum = fn.sumIf(historyOrder.ratio, historyOrder.active);
const lowCardinalityFloatSum = fn.sumIf(historyOrder.lowCardinalityRatio, historyOrder.active);
const nullableFloatSum = fn.sumIf(historyOrder.nullableRatio, historyOrder.active);
const intSum = fn.sumIf(historyOrder.largeId, historyOrder.active);
const rankInTier = fn.over(fn.rowNumber(), {
  partitionBy: [historyOrder.active],
  orderBy: [ck.desc(historyOrder.id)],
});
const safeRankInTier = fn.over(fn.rowNumber().toSafe(), {
  partitionBy: [historyOrder.active],
  orderBy: [ck.desc(historyOrder.id)],
});
const mixedRankInTier = fn.over(fn.rowNumber().toMixed(), {
  partitionBy: [historyOrder.active],
  orderBy: [ck.desc(historyOrder.id)],
});
const windowedDecimalAmountSum = fn.over(fn.sum(historyOrder.amount));
const decimalAmountMaximum = fn.maxIf(historyOrder.amount, historyOrder.active);
const computedAmountMaximum = fn.maxIf(fn.toString(historyOrder.id), historyOrder.active);
const decimalAmountDivision = fn.divideDecimal(historyOrder.amount, fn.toDecimal64("3.00", 2), 5);

type _BroadSumData = Expect<Equal<SelectionData<typeof broadSum>, number | string>>;
type _BroadSumIfData = Expect<Equal<SelectionData<typeof broadSumIf>, number | string>>;
type _DecimalAmountSumData = Expect<Equal<SelectionData<typeof decimalAmountSum>, string>>;
type _NullableDecimalAmountSumData = Expect<Equal<SelectionData<typeof nullableDecimalAmountSum>, string>>;
type _LowCardinalityDecimalAmountSumData = Expect<Equal<SelectionData<typeof lowCardinalityDecimalAmountSum>, string>>;
type _FloatSumData = Expect<Equal<SelectionData<typeof floatSum>, number>>;
type _LowCardinalityFloatSumData = Expect<Equal<SelectionData<typeof lowCardinalityFloatSum>, number>>;
type _NullableFloatSumData = Expect<Equal<SelectionData<typeof nullableFloatSum>, number>>;
type _IntSumData = Expect<Equal<SelectionData<typeof intSum>, number | string>>;
type _RankInTierData = Expect<Equal<SelectionData<typeof rankInTier>, number>>;
type _SafeRankInTierData = Expect<Equal<SelectionData<typeof safeRankInTier>, string>>;
type _MixedRankInTierData = Expect<Equal<SelectionData<typeof mixedRankInTier>, number | string>>;
type _WindowedDecimalAmountSumData = Expect<Equal<SelectionData<typeof windowedDecimalAmountSum>, string>>;
type _DecimalAmountMaximumData = Expect<Equal<SelectionData<typeof decimalAmountMaximum>, string>>;
type _ComputedAmountMaximumData = Expect<Equal<SelectionData<typeof computedAmountMaximum>, string>>;
type _DecimalAmountDivisionData = Expect<Equal<SelectionData<typeof decimalAmountDivision>, string>>;

const untypedRaw = ck.expr(ckSql`1`);
type _UntypedRawData = Expect<Equal<SelectionData<typeof untypedRaw>, unknown>>;

const typedRaw = ck.expr(ckSql<string>`'history'`);
type _TypedRawData = Expect<Equal<SelectionData<typeof typedRaw>, string>>;

const decoderOverride = ck.expr<boolean>(ckSql`1`, {
  decoder: (value) => Number(value) === 1,
  sqlType: "UInt8",
});
type _DecoderOverrideData = Expect<Equal<SelectionData<typeof decoderOverride>, boolean>>;
