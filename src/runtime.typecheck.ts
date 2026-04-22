// Type-only contract file. It exists to keep the public runtime API honest
// under `tsc --noEmit`; it is not runtime library code.
import { int32, string } from "./columns";
import { clickhouseClient } from "./runtime";
import { chTable } from "./schema";

const users = chTable("users", {
  id: int32(),
  name: string(),
});

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  schema: { users },
});

db.runInSession(
  async (sessionDb) => {
    await sessionDb.execute("select 1", {
      session_timeout: 30,
      session_check: 1,
    });
    return await sessionDb.execute("select 1", {
      session_timeout: 30,
      session_check: 1,
    });
  },
  {
    session_timeout: 30,
    session_check: 1,
  },
);

db.insert(users).values({
  id: 1,
  name: "alice",
});

db.insert(users).values([
  {
    id: 2,
  },
]);

db.execute("select 1", {
  format: "JSON",
});

db.stream("select 1", {
  format: "JSONEachRow",
});

db.select({
  id: users.id,
})
  .from(users)
  .execute({
    query_id: "typed_query",
  });

// @ts-expect-error insert rows should reject unknown columns
db.insert(users).values({ typo_name: "alice" });

// @ts-expect-error raw eager queries only support JSON output
db.execute("select 1", { format: "JSONEachRow" });

// @ts-expect-error raw streaming queries only support JSONEachRow output
db.stream("select 1", { format: "JSON" });

// @ts-expect-error typed builder queries do not expose format overrides
db.select({ id: users.id }).from(users).execute({ format: "JSON" });

// @ts-expect-error typed builder iterators do not expose format overrides
db.select({ id: users.id }).from(users).iterator({ format: "JSONEachRow" });

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  schema: { users },
  // @ts-expect-error client config no longer accepts session_timeout defaults
  session_timeout: 30,
});

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  schema: { users },
  // @ts-expect-error client config no longer accepts session_check defaults
  session_check: 1,
});

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  schema: { users },
  // @ts-expect-error client config no longer accepts custom json hooks
  json: {
    parse: (text: string) => JSON.parse(text) as unknown,
    stringify: (value: unknown) => JSON.stringify(value),
  },
});
