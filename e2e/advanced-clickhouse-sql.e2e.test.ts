import { beforeEach, expect, it } from "bun:test";
import { ckSql } from "./ck-orm";
import { createE2EDb } from "./shared";
import { describeE2E } from "./test-helpers";

describeE2E("ck-orm e2e advanced clickhouse sql", function describeAdvancedClickHouseSql() {
  beforeEach(async function truncateDailySummary() {
    const db = createE2EDb();
    await db.command(ckSql`TRUNCATE TABLE user_daily_summary`);
  });

  it("supports scalar WITH and ARRAY JOIN in raw SQL", async function testScalarWithAndArrayJoin() {
    const db = createE2EDb();

    expect(
      await db.execute(ckSql`
        WITH 10 AS min_user_id
        SELECT count() AS total
        FROM users
        WHERE id > min_user_id
      `),
    ).toEqual([{ total: "4990" }]);

    expect(
      await db.execute(ckSql`
        SELECT
          event_id,
          tag
        FROM web_events
        ARRAY JOIN tags AS tag
        WHERE event_id = 1
        ORDER BY tag
      `),
    ).toEqual([
      { event_id: "1", tag: "segment_0" },
      { event_id: "1", tag: "tag_0" },
    ]);
  });

  it("supports window functions over real datasets", async function testWindowFunctions() {
    const db = createE2EDb();

    const rows = await db.execute(ckSql`
      SELECT
        id,
        tier,
        row_number() OVER (PARTITION BY tier ORDER BY id) AS rank_in_tier,
        lag(id, 1, 0) OVER (PARTITION BY tier ORDER BY id) AS previous_id
      FROM users
      WHERE id <= 10
      ORDER BY tier, id
    `);

    expect(rows).toEqual([
      { id: 4, tier: "standard", rank_in_tier: "1", previous_id: 0 },
      { id: 7, tier: "standard", rank_in_tier: "2", previous_id: 4 },
      { id: 10, tier: "standard", rank_in_tier: "3", previous_id: 7 },
      { id: 2, tier: "trial", rank_in_tier: "1", previous_id: 0 },
      { id: 3, tier: "trial", rank_in_tier: "2", previous_id: 2 },
      { id: 5, tier: "trial", rank_in_tier: "3", previous_id: 3 },
      { id: 6, tier: "trial", rank_in_tier: "4", previous_id: 5 },
      { id: 9, tier: "trial", rank_in_tier: "5", previous_id: 6 },
      { id: 1, tier: "vip", rank_in_tier: "1", previous_id: 0 },
      { id: 8, tier: "vip", rank_in_tier: "2", previous_id: 1 },
    ]);
  });

  it("supports ASOF JOIN over time-series tables", async function testAsofJoin() {
    const db = createE2EDb();

    const rows = await db.execute(ckSql`
      SELECT
        f.trade_id,
        f.symbol,
        q.bid
      FROM
      (
        SELECT trade_id, symbol, filled_at
        FROM trade_fills
        WHERE trade_id IN (1, 2, 3)
        ORDER BY symbol, filled_at
      ) AS f
      ASOF LEFT JOIN
      (
        SELECT symbol, quote_time, bid
        FROM quote_snapshots
        ORDER BY symbol, quote_time
      ) AS q
      ON f.symbol = q.symbol AND f.filled_at >= q.quote_time
      ORDER BY f.trade_id
    `);

    expect(rows).toEqual([
      { trade_id: "1", symbol: "EURUSD", bid: "1.1" },
      { trade_id: "2", symbol: "XAUUSD", bid: "2350.001" },
      { trade_id: "3", symbol: "BTCUSD", bid: "65000.002" },
    ]);
  });

  it("supports multi-cte reporting SQL and insert into select pipelines", async function testCteReportAndInsertSelect() {
    const db = createE2EDb();

    const reportRows = await db.execute(ckSql`
      WITH scoped_users AS
      (
        SELECT id, tier
        FROM users
        WHERE id <= 6
      ),
      tier_totals AS
      (
        SELECT tier, count() AS cnt
        FROM scoped_users
        GROUP BY tier
      ),
      ranked AS
      (
        SELECT
          tier,
          cnt,
          row_number() OVER (ORDER BY cnt DESC, tier ASC) AS rank
        FROM tier_totals
      )
      SELECT tier, cnt, rank
      FROM ranked
      ORDER BY rank
    `);

    expect(reportRows).toEqual([
      { tier: "trial", cnt: "4", rank: "1" },
      { tier: "standard", cnt: "1", rank: "2" },
      { tier: "vip", cnt: "1", rank: "3" },
    ]);

    const expectedRows = await db.execute(ckSql`
      SELECT
        toDate(viewed_at) AS day,
        user_id,
        count() AS total_events,
        CAST(sum(revenue) AS Decimal(18, 2)) AS total_revenue
      FROM web_events
      WHERE user_id <= 3
      GROUP BY day, user_id
      ORDER BY day, user_id
    `);

    await db.command(ckSql`
      INSERT INTO user_daily_summary
      SELECT
        toDate(viewed_at) AS day,
        user_id,
        count() AS total_events,
        CAST(sum(revenue) AS Decimal(18, 2)) AS total_revenue
      FROM web_events
      WHERE user_id <= 3
      GROUP BY day, user_id
    `);

    const insertedRows = await db.execute(ckSql`
      SELECT day, user_id, total_events, total_revenue
      FROM user_daily_summary
      ORDER BY day, user_id
    `);

    expect(insertedRows).toEqual(expectedRows);
  });
});
