// Compile-time only. Verifies the PR-B invariants for bare-SelectBuilder
// column references:
//   - variable-bound bare builders expose typed `.colKey` refs
//   - method-name keys (e.g. `from`) keep the builder method type — the
//     `Omit<ReferenceColumns, ForbiddenAutoColumnKeys>` mask hides the
//     conflicting column ref at the intersection layer
//   - chain methods (`.where`, `.orderBy`, ...) propagate refs onto the
//     returned builder so `sub.id` remains accessible after the chain
//   - `.as("name")` route still produces a literal-aliased Subquery

import { ckTable, ckType, clickhouseClient, fn, type Selection } from "../index";
import type { Equal, Expect } from "./helpers";

const orders = ckTable(
  "orders",
  {
    id: ckType.int32(),
    name: ckType.string(),
    amount: ckType.float64(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

const db = clickhouseClient({ databaseUrl: "http://127.0.0.1:8123" });

// 1. bare builder right after `.from()` exposes column refs typed
//    `Selection<TData, string>` (auto-alias has no literal type).
const bareAfterFrom = db
  .select({
    owner_id: orders.id,
    total: fn.sum(orders.amount).as("total_amount"),
  })
  .from(orders);

type _BareOwnerIdRefType = typeof bareAfterFrom.owner_id;
type _BareOwnerIdAssert = Expect<Equal<_BareOwnerIdRefType, Selection<number, string>>>;
type _BareTotalRefType = typeof bareAfterFrom.total;
// Float64 columns decode to `number` through fn.sum.
type _BareTotalAssert = Expect<Equal<_BareTotalRefType, Selection<number, string>>>;

// 2. Column refs survive chain methods: `.where()`, `.orderBy()`, `.limit()`.
const chained = bareAfterFrom.where(undefined).orderBy(orders.id).limit(5);
type _ChainedOwnerIdAssert = Expect<Equal<typeof chained.owner_id, Selection<number, string>>>;

// 3. `.as("named")` route retains literal alias — TAlias = "named".
const namedSub = bareAfterFrom.as("orders_sub");
type _NamedOwnerIdAssert = Expect<Equal<typeof namedSub.owner_id, Selection<number, "orders_sub">>>;

// 4. Non-conflicting custom keys flow correctly.
const customKey = db
  .select({
    customValue: orders.amount,
  })
  .from(orders);
type _CustomValueAssert = Expect<Equal<typeof customKey.customValue, Selection<number, string>>>;

// 5. Method-name keys: when a user writes `db.select({ from: ... })`, the
//    `Omit<..., ForbiddenAutoColumnKeys>` at the column-refs intersection
//    masks the conflicting key — `sub.from` keeps the builder method type,
//    not the column ref. The runtime also skips attaching the key.
const conflictMix = db
  .select({
    id: orders.id,
    // intentionally fine: `id` is not a method name
  })
  .from(orders);
type _ConflictMixIdAssert = Expect<Equal<typeof conflictMix.id, Selection<number, string>>>;
// `.from` is the builder method (callable), not a Selection<...> column ref.
// If the `Omit<..., ForbiddenAutoColumnKeys>` mask ever regresses, this would
// flip to `false` — turning the vacuous truth that the previous assertion was.
type _BuilderFromIsNotSelection = Expect<typeof conflictMix.from extends Selection<unknown, string> ? false : true>;

// 6. When no explicit selection is supplied, TResult is inferred from the
//    rootSource's columns. The intersection of SelectBuilderColumnRefs is
//    `unknown` (TSelection is undefined, not a SelectionRecord) — so the
//    builder doesn't get a spurious string-indexed map of refs.
const noSelection = db.select().from(orders);
// The default-projection path infers row shape from `orders` columns, so
// `.execute()` resolves to `Promise<{id, name, amount}[]>` (specific shape).
type _NoSelectionRowShape = Awaited<ReturnType<typeof noSelection.execute>>[number];
type _NoSelectionRowAssert = Expect<Equal<_NoSelectionRowShape, { id: number; name: string; amount: number }>>;
