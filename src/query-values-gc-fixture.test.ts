import assert from "node:assert/strict";
import { setImmediate as nextTurn } from "node:timers/promises";
import { makeOfflineDb, makeRows, makeTable } from "./query-values-performance-support.test";

const collect = async () => {
  const gc = globalThis.gc;
  if (typeof gc !== "function") {
    throw new Error("Explicit GC is required; run Node with --expose-gc");
  }
  for (let attempt = 0; attempt < 8; attempt++) {
    await nextTurn();
    gc();
  }
  await nextTurn();
};

const droppedChild = async () => {
  const db = makeOfflineDb();
  const table = makeTable("audit_perf004_turn_retention");
  const held = await new Promise<
    {
      prefix: number;
      parent: ReturnType<ReturnType<typeof db.insert>["values"]>;
      parentBody: string | undefined;
      childBody: string | undefined;
      ref: WeakRef<{ id: number; label: string }>;
    }[]
  >((resolve) => {
    setTimeout(() => {
      resolve(
        [1, 32].map((prefix) => {
          const row = { id: 800 + prefix, label: "only-child" };
          const parent = db.insert(table).values(makeRows(prefix));
          const child = parent.values(row);
          return {
            prefix,
            parent,
            parentBody: parent.toSQL().body,
            childBody: child.toSQL().body,
            ref: new WeakRef(row),
          };
        }),
      );
    }, 0);
  });
  const control = await new Promise<WeakRef<object>>((resolve) => {
    setTimeout(() => resolve(new WeakRef({ unused: true })), 0);
  });
  const liveChildRow = { id: 864, label: "held-child" };
  const liveParent = db.insert(table).values(makeRows(32));
  const liveChild = liveParent.values(liveChildRow);
  const liveRef = new WeakRef(liveChildRow);
  const liveChildBody = liveChild.toSQL().body;
  await collect();
  assert.equal(control.deref(), undefined, "negative-control unused object must collect across event-loop turns");
  for (const item of held) {
    assert.equal(item.ref.deref(), undefined, `dropped child row at prefix ${item.prefix} must collect`);
    assert.equal(item.parent.toSQL().body, item.parentBody);
    assert.equal(item.childBody, `${item.parentBody}, (${800 + item.prefix},'only-child')`);
  }
  assert.equal(liveRef.deref(), liveChildRow, "a retained child builder must keep its own row");
  assert.equal(liveChild.toSQL().body, liveChildBody);
  process.stdout.write(
    `${JSON.stringify({
      id: "PERF-004",
      layer: "explicit-gc-subprocess",
      negativeCollected: true,
      droppedCollected: held.map((item) => item.prefix),
      heldRetained: true,
    })}\n`,
  );
};

const main = async () => {
  const mode = process.argv[3];
  if (mode !== "unit") {
    throw new Error(`ck-orm fixture: unknown or missing GC mode ${mode ?? "(missing)"}`);
  }
  assert.equal(typeof globalThis.gc, "function", "Explicit GC is required; run Node with --expose-gc");
  globalThis.fetch = (() => {
    throw new Error("Network is forbidden in local PERF-004 probes");
  }) as unknown as typeof fetch;
  await droppedChild();
};

if (import.meta.main && process.argv[2] === "--ck-orm-fixture") {
  main().catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`);
    process.exitCode = 1;
  });
}
