import { type CompiledQuery, ck, clickhouseClient, csql, fn } from "./ck-orm";
import { commerceSchema, customerInvoice } from "./schema/commerce";

const createCommerceDb = () => {
  return clickhouseClient({
    host: "http://127.0.0.1:8123",
    database: "demo_store",
    username: "default",
    password: "<password>",
    schema: commerceSchema,
  });
};

const oneQuery = {
  kind: "compiled-query",
  mode: "query",
  statement: "SELECT 1 AS one",
  params: {},
  selection: [
    {
      key: "one",
      sqlAlias: "one",
      path: ["one"],
      decoder(value: unknown) {
        return Number(value);
      },
    },
  ],
  metadata: {
    tags: ["example", "compiled"],
  },
} satisfies CompiledQuery<{ one: number }>;

export const runExecuteCompiledExample = async () => {
  const commerceDb = createCommerceDb();
  const sessionId = ck.createSessionId();

  return commerceDb.executeCompiled<{ one: number }>(oneQuery, {
    session_id: sessionId,
  });
};

export const runIteratorCompiledExample = async () => {
  const commerceDb = createCommerceDb();
  const rows: Array<{ one: number }> = [];

  for await (const row of commerceDb.iteratorCompiled<{ one: number }>(oneQuery)) {
    rows.push(row);
  }

  return rows;
};

export const decodeCompiledRowExample = () => {
  return ck.decodeRow<{ one: number }>({ one: "1" }, oneQuery.selection);
};

/**
 * Decimal precision example: every numeric value travels as `string` end to end.
 *
 * - `fn.sum(decimalColumn)` auto-injects `CAST(... AS Decimal(38, 5))`.
 * - `csql.decimal(...)` wraps a hand-written expression into a precision cast.
 * - The decoded row keeps the value as `string`, ready for `decimal.js` on the consumer side.
 */
export const runDecimalPrecisionAggregate = async () => {
  const commerceDb = createCommerceDb();

  const summary = await commerceDb
    .select({
      userId: customerInvoice.userId,
      grossTotal: fn.sum(customerInvoice.totalAmount).as("gross_total"),
      netTotal: csql
        .decimal(csql`sum(${customerInvoice.totalAmount}) - sum(${customerInvoice.feeAmount})`, 20, 5)
        .as("net_total"),
    })
    .from(customerInvoice)
    .groupBy(customerInvoice.userId)
    .execute();

  return summary;
};
