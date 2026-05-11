// NYC TLC taxi trips — see ./README.md.
import { ck, ckSql, fn } from "../../ck-orm";
import { nycTaxiTrips } from "../../schema/scenarios";

export { nycTaxiTrips };

/** Trips + avg fare + tip% per payment type — classic ClickHouse demo query. */
export const buildNycPaymentBreakdownExample = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        paymentType: nycTaxiTrips.payment_type,
        trips: fn.count().as("trips"),
        avgFare: fn.avg(nycTaxiTrips.total_amount).as("avg_fare"),
        totalTipPct: ck
          .expr<number>(ckSql`round(sum(${nycTaxiTrips.tip_amount}) / sum(${nycTaxiTrips.fare_amount}) * 100, 2)`, {
            decoder: (value) => Number(value),
            sqlType: "Float64",
          })
          .as("total_tip_pct"),
      })
      .from(nycTaxiTrips)
      .groupBy(nycTaxiTrips.payment_type),
});
