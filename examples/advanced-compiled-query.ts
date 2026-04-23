import { type CompiledQuery, ck, clickhouseClient } from "./ck-orm";
import { commerceSchema } from "./schema/commerce";

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
