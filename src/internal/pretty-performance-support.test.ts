import { ckTable, ckType, clickhouseClient } from "../public_api";

export const workloadTable = (name = "audit_perf005_values") =>
  ckTable(name, { id: ckType.uint32(), text: ckType.string() });

export const offlineClient = () =>
  clickhouseClient({
    host: "http://127.0.0.1:1",
    fetch: () => {
      throw new Error("PERF-005 compile-only fixture must not execute requests");
    },
  });

export const makeWorkload = (count: number) => {
  const text = "x".repeat(90);
  const rows = Array.from({ length: count }, (_, id) => ({ id, text }));
  const literals = rows.map(({ id }) => `(${id},'${text}')`);
  const builder = offlineClient().insert(workloadTable()).values(rows);
  return {
    builder,
    rows,
    plainBody: literals.join(", "),
    prettyBody: literals.join(",\n"),
  };
};

export type TrimWork = { calls: number; receiverChars: number; maxReceiverChars: number };

export const measureTrimWork = <T>(run: () => T): { value: T; work: TrimWork } => {
  const descriptor = Object.getOwnPropertyDescriptor(String.prototype, "trimEnd");
  if (!descriptor || typeof descriptor.value !== "function") {
    throw new Error("trimEnd is unavailable");
  }
  const original = descriptor.value as (this: string) => string;
  const work: TrimWork = { calls: 0, receiverChars: 0, maxReceiverChars: 0 };
  Object.defineProperty(String.prototype, "trimEnd", {
    ...descriptor,
    value: function (this: string) {
      const length = String(this).length;
      work.calls += 1;
      work.receiverChars += length;
      work.maxReceiverChars = Math.max(work.maxReceiverChars, length);
      return original.call(this);
    },
  });
  try {
    return { value: run(), work };
  } finally {
    Object.defineProperty(String.prototype, "trimEnd", descriptor);
  }
};

export const trimEndDescriptor = () => Object.getOwnPropertyDescriptor(String.prototype, "trimEnd");
