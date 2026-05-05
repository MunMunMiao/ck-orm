// Type-only contract file. It exists to keep the public runtime API honest
// under `tsc --noEmit`; it is not runtime library code.
import { int32, string } from "./columns";
import { ckSql } from "./public_api";
import { clickhouseClient, type Session } from "./runtime";
import { ckTable } from "./schema";

const users = ckTable("users", {
  id: int32(),
  name: string(),
});
const tempUsers = ckTable("tmp_users", {
  id: int32(),
  name: string().default("anonymous"),
});

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  session_max_concurrent_requests: 2,
});

db.runInSession(
  async (session: Session) => {
    await session.createTemporaryTable(tempUsers, { mode: "if_not_exists" });
    await session.createTemporaryTableRaw("tmp_users_raw", "(id Int32)");
    await session.execute(ckSql`select 1`, {
      session_timeout: 30,
      session_check: 1,
    });
    return await session.execute(ckSql`select 1`, {
      session_timeout: 30,
      session_check: 1,
    });
  },
  {
    session_timeout: 30,
    session_check: 1,
  },
);

db.runInSession(async (session: Session) => {
  await session.runInSession(async (nestedSession) => {
    const outerId: string = session.sessionId;
    const innerId: string = nestedSession.sessionId;
    void outerId;
    void innerId;
  });

  await session.withSettings({ max_threads: 2 }).runInSession(async (nestedSession) => {
    await nestedSession.command(ckSql`select 1`);
  });
});

db.insert(users).values({
  id: 1,
  name: "alice",
});

db.insert(users).values([
  {
    id: 2,
  },
]);

db.execute(ckSql`select 1`, {
  format: "JSON",
});

db.stream(ckSql`select 1`, {
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
db.execute(ckSql`select 1`, { format: "JSONEachRow" });

// @ts-expect-error raw streaming queries only support JSONEachRow output
db.stream(ckSql`select 1`, { format: "JSON" });

// @ts-expect-error raw query execution no longer accepts plain strings
db.execute("select 1");

// @ts-expect-error raw command execution no longer accepts plain strings
db.command("select 1");

// @ts-expect-error raw streaming no longer accepts plain strings
db.stream("select 1");

// @ts-expect-error typed builder queries do not expose format overrides
db.select({ id: users.id }).from(users).execute({ format: "JSON" });

// @ts-expect-error typed builder iterators do not expose format overrides
db.select({ id: users.id }).from(users).iterator({ format: "JSONEachRow" });

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  // @ts-expect-error client config no longer accepts session_timeout defaults
  session_timeout: 30,
});

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  // @ts-expect-error client config no longer accepts session_check defaults
  session_check: 1,
});

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  // @ts-expect-error client config no longer accepts custom json hooks
  json: {
    parse: (text: string) => JSON.parse(text) as unknown,
    stringify: (value: unknown) => JSON.stringify(value),
  },
});

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  // @ts-expect-error session_max_concurrent_requests must be a number
  session_max_concurrent_requests: "2",
});

clickhouseClient({
  databaseUrl: "http://localhost:8123/typecheck_db",
  tracing: {
    // @ts-expect-error tracing database name is derived from the client config
    dbName: "typecheck_db",
  },
});
