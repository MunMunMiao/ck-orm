import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { createSessionConcurrencyController } from "./runtime/session-concurrency";

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));
const payloadBytesEach = 4096;
const gcRoundsPerCheckpoint = 6;
const checkpoints = 3;

type Refs = { reasons: WeakRef<object>[]; signals: WeakRef<object>[]; payloads: WeakRef<object>[] };
type Starter = (signal: AbortSignal) => Promise<unknown>;

const live = (refs: WeakRef<object>[]) => refs.reduce((sum, ref) => sum + Number(ref.deref() !== undefined), 0);
const emptyRefs = (): Refs => ({ reasons: [], signals: [], payloads: [] });
const countLive = (refs: Refs) => ({
  reasons: live(refs.reasons),
  signals: live(refs.signals),
  payloads: live(refs.payloads),
});
const memory = () => {
  const { heapUsed, heapTotal, external, arrayBuffers, rss } = process.memoryUsage();
  return { heapUsed, heapTotal, external, arrayBuffers, rss };
};

const collect = async () => {
  const gc = globalThis.gc;
  if (typeof gc !== "function") {
    throw new Error("Explicit GC is required; run Node with --expose-gc");
  }
  for (let round = 0; round < gcRoundsPerCheckpoint; round++) {
    await tick();
    gc();
  }
  await tick();
};

const cancelOne = async (start: Starter, refs: Refs, preAborted = false) => {
  const controller = new AbortController();
  const reason = Object.assign(new Error("audit queued cancellation"), { payload: new Uint8Array(payloadBytesEach) });
  reason.payload.fill(0x42);
  if (preAborted) {
    controller.abort(reason);
  }
  const pending = start(controller.signal);
  assert.equal(getEventListeners(controller.signal, "abort").length, preAborted ? 0 : 1);
  if (!preAborted) {
    controller.abort(reason);
  }
  await pending.then(
    () => assert.fail("Cancelled queued operation ran"),
    (error: { kind?: unknown; cause?: unknown; message?: string }) => {
      assert.equal(error.kind, "aborted");
      assert.equal(error.cause, reason);
      assert.match(error.message ?? "", /audit queued cancellation/);
    },
  );
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  refs.reasons.push(new WeakRef(reason));
  refs.signals.push(new WeakRef(controller.signal));
  refs.payloads.push(new WeakRef(reason.payload));
};

const freeReferences = () =>
  Array.from({ length: 16 }, () => new WeakRef({ payload: new Uint8Array(payloadBytesEach) }));

const coreSlot = async () => {
  const controller = createSessionConcurrencyController(1);
  let cancelledExecuted = 0;
  let sentinelStarted = false;
  const stream = controller.runStream("audit_perf002_unit", async function* () {
    yield 0;
    yield 1;
  });
  assert.deepEqual(await stream.next(), { done: false, value: 0 });
  const sentinel = controller.run("audit_perf002_unit", async () => {
    sentinelStarted = true;
    return 7;
  });
  await tick();
  assert.equal(sentinelStarted, false, "Precondition: the live stream must hold the slot");
  return {
    start: (signal: AbortSignal) =>
      controller.run(
        "audit_perf002_unit",
        async () => {
          cancelledExecuted += 1;
        },
        signal,
      ),
    assertHeld: async () => {
      assert.equal(sentinelStarted, false);
      assert.equal(await controller.run("audit_perf002_unit_other", async () => 11), 11);
      assert.equal(cancelledExecuted, 0);
    },
    release: async () => {
      await stream.return(undefined);
      await stream.return(undefined);
      assert.equal(await sentinel, 7);
      assert.equal(sentinelStarted, true);
      assert.equal(cancelledExecuted, 0);
    },
    evidence: () => ({ slotHeldBeforeCancellation: true, cancelledOperationsExecuted: cancelledExecuted }),
  };
};

const retention = async (count: number) => {
  const slot = await coreSlot();
  const refs = emptyRefs();
  const preAborted = emptyRefs();
  const free = freeReferences();
  const rooted = { marker: "positive-live-control" };
  const rootedRef = new WeakRef(rooted);
  await collect();
  const before = memory();
  const held: Array<{
    cancelled: ReturnType<typeof countLive>;
    preAborted: ReturnType<typeof countLive>;
    free: number;
    memory: ReturnType<typeof memory>;
  }> = [];
  let released = false;
  try {
    for (let index = 0; index < 16; index++) {
      await cancelOne(slot.start, preAborted, true);
    }
    for (let index = 0; index < count; index++) {
      await cancelOne(slot.start, refs);
    }
    for (let checkpoint = 0; checkpoint < checkpoints; checkpoint++) {
      await collect();
      await slot.assertHeld();
      held.push({
        cancelled: countLive(refs),
        preAborted: countLive(preAborted),
        free: live(free),
        memory: memory(),
      });
    }
    assert.equal(rootedRef.deref(), rooted, "Positive GC control must remain live");
    await slot.release();
    released = true;
    await collect();
    const afterRelease = { cancelled: countLive(refs), preAborted: countLive(preAborted), memory: memory() };
    return {
      id: "PERF-002",
      runtime: process.version,
      layer: "isolated-controller-process",
      count,
      payloadBytesEach,
      gc: { roundsPerCheckpoint: gcRoundsPerCheckpoint, checkpoints, explicit: true },
      deterministicControls: {
        listenersAttachedBeforeCancel: count,
        listenersRemainingAfterCancel: 0,
        ...slot.evidence(),
      },
      before,
      held,
      afterRelease,
    };
  } finally {
    if (!released) {
      await slot.release();
    }
  }
};

const main = async () => {
  const mode = process.argv[3];
  const options = JSON.parse(process.argv[4] ?? "{}") as { count?: number };
  const count = options.count ?? 128;
  assert.equal(mode, "unit", "ck-orm fixture: unknown or missing GC mode");
  assert(Number.isInteger(count) && count > 0 && count <= 2000, "waiter count must be within 1..2000");
  assert.equal(typeof globalThis.gc, "function", "Explicit GC is required; run Node with --expose-gc");
  globalThis.fetch = (() => {
    throw new Error("Network is forbidden in local PERF-002 probes");
  }) as unknown as typeof fetch;
  process.stdout.write(`${JSON.stringify(await retention(count))}\n`);
};

if (import.meta.main && process.argv[2] === "--ck-orm-fixture") {
  main().catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`);
    process.exitCode = 1;
  });
}
