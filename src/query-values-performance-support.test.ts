import { ckSql, ckTable, ckType, clickhouseClient } from "./public_api";

export { ckSql, ckTable, ckType, clickhouseClient };

export const makeOfflineDb = () =>
  clickhouseClient({
    host: "http://127.0.0.1:1",
    fetch: () => {
      throw new Error("PERF-004 compile-only test attempted network I/O");
    },
  });

export const makeTable = (name = "audit_perf004_rows") =>
  ckTable(name, {
    id: ckType.uint32(),
    label: ckType.string(),
  });

export type Row = { id: number; label: string };

export const makeRows = (count: number): Row[] =>
  Array.from({ length: count }, (_, id) => ({ id, label: `row_${id}` }));

const requireRow = (rows: readonly Row[], index: number): Row => {
  const row = rows[index];
  if (row === undefined) {
    throw new Error(`missing PERF-004 row at ${index}`);
  }
  return row;
};

export const appendRows = (
  db: ReturnType<typeof makeOfflineDb>,
  table: ReturnType<typeof makeTable>,
  rows: readonly Row[],
) => {
  if (rows.length === 0) {
    throw new Error("The workload requires non-empty rows");
  }
  let builder = db.insert(table).values(requireRow(rows, 0));
  for (let index = 1; index < rows.length; index++) {
    builder = builder.values(requireRow(rows, index));
  }
  return builder;
};

export const measureRowVisits = <T>(rows: readonly object[], build: () => T) => {
  const tracked = new WeakSet(rows);
  const original = Array.prototype[Symbol.iterator];
  let rowVisits = 0;
  Array.prototype[Symbol.iterator] = function* (this: unknown[]) {
    const iterator = original.call(this);
    for (let next = iterator.next(); !next.done; next = iterator.next()) {
      const value = next.value;
      if (typeof value === "object" && value !== null && tracked.has(value)) {
        rowVisits += 1;
      }
      yield value;
    }
  } as typeof original;
  try {
    return { value: build(), rowVisits };
  } finally {
    Array.prototype[Symbol.iterator] = original;
  }
};

export const expectedBody = (rows: readonly Row[]) => rows.map(({ id, label }) => `(${id},'${label}')`).join(", ");

export const linearVisitBudget = (rowCount: number) => 40 * rowCount;
