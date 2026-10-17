import { clickhouseClient } from "./runtime";
import { dsnMarker, dsnUsername, invalidDsns, serializeError, validDsn } from "./runtime-boundaries-support.test";
import { sql } from "./sql";

const main = async () => {
  const scenario = process.argv[3];
  let fetchCalls = 0;
  const noNetwork = () => {
    fetchCalls += 1;
    throw new Error("Unexpected fixture network request");
  };

  if (scenario === "valid-dsn") {
    let strippedUrl = false;
    let correctAuth = false;
    const db = clickhouseClient({
      databaseUrl: validDsn,
      fetch: (request) => {
        fetchCalls += 1;
        const url = new URL(request.url);
        strippedUrl = url.username === "" && url.password === "" && !url.href.includes(dsnMarker);
        correctAuth =
          request.headers.get("authorization") ===
          `Basic ${Buffer.from(`${dsnUsername}:${dsnMarker}`).toString("base64")}`;
        return Response.json({ data: [{ ok: 1 }] });
      },
    });
    const rows = await db.execute(sql`SELECT 1 AS ok`);
    process.stdout.write(`${JSON.stringify({ scenario, fetchCalls, strippedUrl, correctAuth, rows })}\n`);
    return;
  }

  const fixture = invalidDsns.find(([name]) => name === scenario);
  if (!fixture && scenario !== "separate-credentials") {
    throw new Error("Unknown DSN fixture scenario");
  }
  let error: unknown;
  try {
    clickhouseClient(
      fixture
        ? { databaseUrl: fixture[1], fetch: noNetwork }
        : { host: "http://127.0.0.1:INVALID", username: dsnUsername, password: dsnMarker, fetch: noNetwork },
    );
  } catch (caught) {
    error = caught;
  }
  if (!(error instanceof Error)) {
    throw new Error("DSN fixture did not produce the expected construction error");
  }
  process.stdout.write(
    `${JSON.stringify({
      scenario,
      fetchCalls,
      errorCaught: true,
      serialized: serializeError(error),
      kind: Reflect.get(error, "kind"),
      executionState: Reflect.get(error, "executionState"),
    })}\n`,
  );
};

if (import.meta.main && process.argv[2] === "--ck-orm-fixture") {
  main().catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`);
    process.exitCode = 1;
  });
}
