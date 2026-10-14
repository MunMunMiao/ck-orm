import { expect, test } from "bun:test";
import { clickhouseClient } from "ck-orm";
import { run } from "./main";

test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL)(
  "includes the new signal's threshold boundary but not lower or missing scores",
  async () => {
    const db = clickhouseClient({
      databaseUrl: process.env.CLICKHOUSE_EXAMPLES_URL ?? "",
    });

    // 0.9 and exactly 0.8 qualify; 0.79, zero and absent keys do not.
    expect(await run(db)).toEqual([
      { interactionId: "above", score: 0.9 },
      { interactionId: "boundary", score: 0.8 },
    ]);
  },
);

test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL)(
  "keeps a real zero score but excludes absent keys at a zero threshold",
  async () => {
    const db = clickhouseClient({
      databaseUrl: process.env.CLICKHOUSE_EXAMPLES_URL ?? "",
    });

    // Missing keys also read as 0 in ClickHouse; mapContains must exclude them.
    expect(await run(db, { threshold: 0 })).toEqual([
      { interactionId: "above", score: 0.9 },
      { interactionId: "below", score: 0.79 },
      { interactionId: "boundary", score: 0.8 },
      { interactionId: "zero", score: 0 },
    ]);
  },
);

test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL)(
  "filters a different model signal without changing the table schema",
  async () => {
    const db = clickhouseClient({
      databaseUrl: process.env.CLICKHOUSE_EXAMPLES_URL ?? "",
    });

    // Only missing has legacy_model >= 0.8; above has legacy_model = 0.2.
    expect(await run(db, { signal: "legacy_model", threshold: 0.8 })).toEqual([
      { interactionId: "missing", score: 0.99 },
    ]);
  },
);
