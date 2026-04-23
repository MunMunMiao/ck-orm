import { describe, expect } from "bun:test";
import { type ClickHouseOrmError, ck, csql, isClickHouseOrmError, isDecodeError } from "./ck-orm";
import { createAdminDb, createE2EDb, datasetCounts, hasE2EEnv, users } from "./shared";

export const describeE2E = hasE2EEnv ? describe : describe.skip;

export const expectDate = (value: unknown) => {
  expect(value).toBeInstanceOf(Date);
};

export const expectPresent = <TValue>(value: TValue | null | undefined, label = "value"): TValue => {
  expect(value).toBeDefined();
  expect(value).not.toBeNull();
  if (value === null || value === undefined) {
    throw new Error(`Expected ${label} to be present`);
  }
  return value;
};

export const takeAsync = async <TValue>(iterable: AsyncIterable<TValue>, limit: number) => {
  const rows: TValue[] = [];
  for await (const row of iterable) {
    rows.push(row);
    if (rows.length >= limit) {
      break;
    }
  }
  return rows;
};

export const expectClickhouseError = (error: unknown, expected: Record<string, unknown>) => {
  expect(isClickHouseOrmError(error)).toBe(true);
  expect(isDecodeError(error)).toBe(expected.kind === "decode");
  for (const [key, value] of Object.entries(expected)) {
    expect((error as Record<string, unknown>)[key]).toEqual(value);
  }
};

const resolveMaybeThrowing = async (input: Promise<unknown> | (() => Promise<unknown> | unknown)) => {
  if (typeof input === "function") {
    return await input();
  }
  return await input;
};

export const expectRejectsWithClickhouseError = async (
  input: Promise<unknown> | (() => Promise<unknown> | unknown),
  expected: Record<string, unknown>,
) => {
  try {
    await resolveMaybeThrowing(input);
    throw new Error("Expected promise to reject with ClickHouseOrmError");
  } catch (error) {
    expectClickhouseError(error, expected);
    return error as ClickHouseOrmError;
  }
};

export const expectClientValidationNotSent = async (
  input: Promise<unknown> | (() => Promise<unknown> | unknown),
  expected?: Record<string, unknown>,
) => {
  return expectRejectsWithClickhouseError(input, {
    kind: "client_validation",
    executionState: "not_sent",
    ...(expected ?? {}),
  });
};

export const expectNoMutationAfterRejectedInjection = async (options?: {
  readonly expectedUserCount?: number;
  readonly probeUserId?: number;
  readonly probeUserName?: string;
}) => {
  const db = createE2EDb();
  const expectedUserCount = options?.expectedUserCount ?? datasetCounts.users;
  const probeUserId = options?.probeUserId ?? 1;
  const probeUserName = options?.probeUserName ?? "alice";

  expect(await db.count(users)).toBe(expectedUserCount);

  const [probeUser] = await db
    .select({
      id: users.id,
      name: users.name,
    })
    .from(users)
    .where(ck.eq(users.id, probeUserId))
    .limit(1);

  expect(expectPresent(probeUser, "probeUser")).toEqual({
    id: probeUserId,
    name: probeUserName,
  });
};

export type QueryLogExceptionRow = {
  readonly type: string;
  readonly exception_code: number;
  readonly exception: string;
  readonly query_id: string;
  readonly http_method: string | number;
  readonly interface: string | number;
  readonly query_duration_ms: string | number;
  readonly read_rows: string | number;
  readonly written_rows: string | number;
};

export const waitForQueryLogException = async (
  queryId: string,
  options?: {
    readonly timeoutMs?: number;
  },
): Promise<QueryLogExceptionRow> => {
  const adminDb = createAdminDb();
  const timeoutMs = options?.timeoutMs ?? 10_000;
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    await adminDb.command(csql`SYSTEM FLUSH LOGS`);

    const rows = await adminDb.execute(csql`
      select
        type,
        exception_code,
        exception,
        query_id,
        http_method,
        interface,
        query_duration_ms,
        read_rows,
        written_rows
      from system.query_log
      where query_id = ${queryId}
        and type in ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
      order by event_time_microseconds desc
      limit 1
    `);

    if (rows.length > 0) {
      return rows[0] as QueryLogExceptionRow;
    }

    await Bun.sleep(250);
  }

  throw new Error(`Expected system.query_log exception row for query_id=${queryId}`);
};
