import { ck, ckAlias, ckTable, ckType, clickhouseClient } from "../index";
import type { Equal, Expect, InferBuilderResult } from "./helpers";

const trades = ckTable("trades", {
  tradeId: ckType.uint32("trade_id"),
  symbol: ckType.string(),
  tradeTime: ckType.dateTime64("trade_time", { precision: 3, timezone: "UTC" }),
});

const quotes = ckTable("quotes", {
  symbol: ckType.string(),
  quoteTime: ckType.dateTime64("quote_time", { precision: 3, timezone: "UTC" }),
  price: ckType.decimal({ precision: 20, scale: 5 }),
});

const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123",
});

const on = ck.and(ck.eq(trades.symbol, quotes.symbol), ck.gte(trades.tradeTime, quotes.quoteTime));

const left = db.select({ tradeId: trades.tradeId, price: quotes.price }).from(trades).asofLeftJoin(quotes, on);
type _Left = Expect<Equal<InferBuilderResult<typeof left>, { tradeId: number; price: string | null }>>;

const inner = db.select({ tradeId: trades.tradeId, price: quotes.price }).from(trades).asofJoin(quotes, on);
type _Inner = Expect<Equal<InferBuilderResult<typeof inner>, { tradeId: number; price: string }>>;

const implicit = db.select().from(trades).asofLeftJoin(quotes, on);
type _Implicit = Expect<
  Equal<
    InferBuilderResult<typeof implicit>,
    { trades: typeof trades.$inferSelect } & { quotes: typeof quotes.$inferSelect | null }
  >
>;

const zero = db
  .withSettings({ join_use_nulls: 0 })
  .select({ tradeId: trades.tradeId, price: quotes.price })
  .from(trades)
  .asofLeftJoin(quotes, on);
type _Zero = Expect<Equal<InferBuilderResult<typeof zero>, { tradeId: number; price: string }>>;

const callback = db
  .select({ tradeId: trades.tradeId, price: quotes.price })
  .from(trades)
  .asofLeftJoin(quotes, (joined) =>
    ck.and(ck.eq(trades.symbol, joined.symbol), ck.gte(trades.tradeTime, joined.quoteTime)),
  );
type _Callback = Expect<Equal<InferBuilderResult<typeof callback>, { tradeId: number; price: string | null }>>;

const innerCallback = db
  .select({ tradeId: trades.tradeId, price: quotes.price })
  .from(trades)
  .asofJoin(quotes, (joined) =>
    ck.and(ck.eq(trades.symbol, joined.symbol), ck.gte(trades.tradeTime, joined.quoteTime)),
  );
type _InnerCallback = Expect<Equal<InferBuilderResult<typeof innerCallback>, { tradeId: number; price: string }>>;

const laterQuotes = ckAlias(quotes, "later_quotes");
const selfJoin = db
  .select({ price: quotes.price, laterPrice: laterQuotes.price })
  .from(quotes)
  .asofJoin(
    laterQuotes,
    ck.and(ck.eq(quotes.symbol, laterQuotes.symbol), ck.gt(quotes.quoteTime, laterQuotes.quoteTime)),
  );
type _SelfJoin = Expect<Equal<InferBuilderResult<typeof selfJoin>, { price: string; laterPrice: string }>>;

const bareQuotes = db.select({ symbol: quotes.symbol, price: quotes.price }).from(quotes);
const bareJoined = db
  .select({ tradeId: trades.tradeId, price: bareQuotes.price })
  .from(trades)
  .asofLeftJoin(bareQuotes, (joined) => ck.eq(trades.symbol, joined.symbol));
type _Bare = Expect<Equal<InferBuilderResult<typeof bareJoined>, { tradeId: number; price: string | null }>>;
