import { beforeAll, describe, expect, it } from "bun:test";
import { spawnSync } from "node:child_process";
import { getEventListeners } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { clickhouseClient } from "./runtime";
import { createIdempotentRelease, createSessionConcurrencyController } from "./runtime/session-concurrency";
import { sql } from "./sql";

const fixtureRoot = join(import.meta.dir, "runtime-session-gc-fixture.test.ts");
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
const nextTurn = () => new Promise<void>((done) => setImmediate(done));
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
};

type QueueNode = { next?: QueueNode; previous?: QueueNode };
type QueueStorage = { waiters?: unknown[]; head?: QueueNode; tail?: QueueNode };

const requireItem = <T>(items: T[], index: number, label: string): T => {
  const item = items[index];
  if (item === undefined) {
    throw new Error(`missing ${label} at ${index}`);
  }
  return item;
};

// Observe the private queue without modifying source or depending on its
// compaction implementation. Both array slots and linked nodes count as
// retained storage; cleared callbacks alone must not satisfy this assertion.
const retainedSlots = (entry: QueueStorage): number => {
  if (Array.isArray(entry.waiters)) {
    return entry.waiters.length;
  }
  const seen = new Set<QueueNode>();
  let previous: QueueNode | undefined;
  for (let node = entry.head; node; node = node.next) {
    expect(seen.has(node), "linked queue must not contain a cycle").toBe(false);
    expect(node.previous).toBe(previous);
    seen.add(node);
    previous = node;
  }
  expect(entry.tail).toBe(previous);
  return seen.size;
};

type LiveCounts = { reasons: number; signals: number; payloads: number };
type ProbeResult = {
  count: number;
  deterministicControls: {
    slotHeldBeforeCancellation: boolean;
    listenersAttachedBeforeCancel: number;
    listenersRemainingAfterCancel: number;
  };
  held: Array<{ cancelled: LiveCounts; preAborted: LiveCounts; free: number }>;
  afterRelease: { cancelled: LiveCounts };
};

const restrictedEnv = { PATH: process.env.PATH ?? "", NO_COLOR: "1" };

const runProbe = async (): Promise<ProbeResult> => {
  const dir = await mkdtemp(join(tmpdir(), "ck-orm-perf002-"));
  const outfile = join(dir, "perf002-gc.mjs");
  try {
    const build = spawnSync(
      process.execPath,
      ["--no-env-file", "--no-install", "build", fixtureRoot, "--outfile", outfile, "--target=node"],
      { encoding: "utf8", timeout: 8_000, env: restrictedEnv },
    );
    expect(build.status, build.stderr).toBe(0);
    const child = spawnSync(
      "node",
      ["--expose-gc", outfile, "--ck-orm-fixture", "unit", JSON.stringify({ count: 127 })],
      {
        encoding: "utf8",
        timeout: 30_000,
        env: restrictedEnv,
      },
    );
    expect(child.error, child.error?.message).toBeUndefined();
    expect(child.signal, child.stderr).toBeNull();
    expect(child.status, `${child.stderr}\n${child.stdout}`).toBe(0);
    expect(child.stderr).toBe("");
    return JSON.parse(child.stdout) as ProbeResult;
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
};

const assertGCControls = (value: ProbeResult) => {
  const zero = { reasons: 0, signals: 0, payloads: 0 };
  expect(value.deterministicControls.slotHeldBeforeCancellation).toBe(true);
  expect(value.deterministicControls.listenersAttachedBeforeCancel).toBe(value.count);
  expect(value.deterministicControls.listenersRemainingAfterCancel).toBe(0);
  for (const checkpoint of value.held) {
    expect(checkpoint.free, "INCONCLUSIVE: unrelated weak objects were not collected").toBe(0);
    expect(checkpoint.preAborted, "INCONCLUSIVE: pre-aborted negative control was retained").toEqual(zero);
  }
  expect(value.afterRelease.cancelled, "INCONCLUSIVE: release negative control was not collected").toEqual(zero);
  for (const checkpoint of value.held) {
    expect(checkpoint.cancelled, "INCONCLUSIVE: unstable GC checkpoints cannot establish retention").toEqual(
      value.held[0]?.cancelled,
    );
  }
};

describe("PERF-002 session waiter queue", () => {
  let result: ProbeResult;

  beforeAll(async () => {
    result = await runProbe();
  }, 35_000);

  it("PERF-002 CONTROL: explicit GC, real queued listeners and release negative controls are valid", () => {
    assertGCControls(result);
  });

  it("PERF-002 RED: cancelled waiter references are collectible while the active stream still holds its slot", () => {
    assertGCControls(result);
    expect(result.held.map((checkpoint) => checkpoint.cancelled)).toEqual([
      { reasons: 0, signals: 0, payloads: 0 },
      { reasons: 0, signals: 0, payloads: 0 },
      { reasons: 0, signals: 0, payloads: 0 },
    ]);
  });

  it("PERF-002 CONTROL: cancellation storms preserve FIFO and max concurrency across repeated queue maintenance", async () => {
    const controller = createSessionConcurrencyController(2);
    const gates = [deferred(), deferred()];
    let active = 0;
    let maximum = 0;
    const started: number[] = [];
    const first = gates.map((gate) =>
      controller.run("shared", async () => {
        active += 1;
        maximum = Math.max(maximum, active);
        await gate.promise;
        active -= 1;
      }),
    );
    await tick();
    expect(active).toBe(2);
    const signals: AbortController[] = [];
    const expected: number[] = [];
    const pending = Array.from({ length: 192 }, (_, index) => {
      const abort = new AbortController();
      signals.push(abort);
      const keep = index % 7 === 3;
      if (keep) {
        expected.push(index);
      }
      return controller
        .run(
          "shared",
          async () => {
            active += 1;
            maximum = Math.max(maximum, active);
            started.push(index);
            await Promise.resolve();
            active -= 1;
            return index;
          },
          abort.signal,
        )
        .then(
          (value) => {
            expect(keep).toBe(true);
            expect(value).toBe(index);
          },
          (error: { kind?: unknown; cause?: unknown }) => {
            expect(keep).toBe(false);
            expect(error.kind).toBe("aborted");
            expect(error.cause).toBe(`stop-${index}`);
          },
        );
    });
    for (let index = signals.length - 1; index >= 0; index--) {
      if (index % 7 !== 3) {
        const abort = signals[index];
        expect(abort).toBeDefined();
        abort.abort(`stop-${index}`);
      }
    }
    await tick();
    expect(started).toEqual([]);
    const firstGate = gates[0];
    expect(firstGate).toBeDefined();
    firstGate.resolve();
    await Promise.all(pending);
    expect(started).toEqual(expected);
    expect(maximum).toBe(2);
    for (const abort of signals) {
      expect(getEventListeners(abort.signal, "abort")).toHaveLength(0);
    }
    const secondGate = gates[1];
    expect(secondGate).toBeDefined();
    secondGate.resolve();
    await Promise.all(first);
    expect(await controller.run("shared", async () => "reused")).toBe("reused");
  });

  it("PERF-002 CONTROL: sequential cancellations and later FIFO admissions do not accumulate live callbacks", async () => {
    const controller = createSessionConcurrencyController(1);
    const gate = deferred();
    const held = controller.run("held", () => gate.promise);
    const survivor = controller.run("held", async () => "survivor");
    let executed = 0;
    for (let index = 0; index < 96; index++) {
      const abort = new AbortController();
      const request = controller.run(
        "held",
        async () => {
          executed += 1;
        },
        abort.signal,
      );
      expect(getEventListeners(abort.signal, "abort")).toHaveLength(1);
      const reason = new Error(`cancel-${index}`);
      abort.abort(reason);
      await request.catch((error: { kind?: unknown; cause?: unknown }) => {
        expect(error.kind).toBe("aborted");
        expect(error.cause).toBe(reason);
      });
      expect(getEventListeners(abort.signal, "abort")).toHaveLength(0);
    }
    const last = controller.run("held", async () => "last");
    gate.resolve();
    await held;
    expect(await Promise.all([survivor, last])).toEqual(["survivor", "last"]);
    expect(executed).toBe(0);
  });

  it("PERF-002 CONTROL: queued and pre-aborted reasons retain their public error identity", async () => {
    const controller = createSessionConcurrencyController(1);
    const gate = deferred();
    const held = controller.run("held", () => gate.promise);
    for (const preAborted of [false, true]) {
      for (const reason of [new Error("synthetic reason"), "string reason", { marker: "object reason" }, undefined]) {
        const abort = new AbortController();
        if (preAborted) {
          abort.abort(reason);
        }
        const request = controller.run(
          "held",
          async () => {
            throw new Error("Must not execute");
          },
          abort.signal,
        );
        if (!preAborted) {
          abort.abort(reason);
        }
        await request.then(
          () => {
            throw new Error("Must reject");
          },
          (error: { kind?: unknown; executionState?: unknown; cause?: unknown }) => {
            expect(error.kind).toBe("aborted");
            expect(error.executionState).toBe("unknown");
            expect(error.cause).toBe(abort.signal.reason);
          },
        );
        expect(getEventListeners(abort.signal, "abort")).toHaveLength(0);
      }
    }
    gate.resolve();
    await held;
  });

  it("PERF-002 CONTROL: active failures release once and do not strand the next waiter", async () => {
    const controller = createSessionConcurrencyController(1);
    const gate = deferred();
    const reason = new Error("active operation failed");
    const held = controller.run("held", async () => {
      await gate.promise;
      throw reason;
    });
    const next = controller.run("held", async () => 11);
    const observed = held.catch((error) => {
      expect(error).toBe(reason);
    });
    gate.resolve();
    await observed;
    expect(await next).toBe(11);
    let released = 0;
    const release = createIdempotentRelease(() => {
      released += 1;
    });
    release();
    release();
    release();
    expect(released).toBe(1);
  });

  it("PERF-002 CONTROL: early stream return waits for server release but normal exhaustion does not", async () => {
    const controller = createSessionConcurrencyController(1);
    const server = deferred();
    let releaseWaits = 0;
    const stream = controller.runStream(
      "held",
      async function* () {
        yield 1;
        yield 2;
      },
      undefined,
      () => {
        releaseWaits += 1;
        return server.promise;
      },
    );
    expect((await stream.next()).value).toBe(1);
    let started = false;
    const next = controller.run("held", async () => {
      started = true;
    });
    await stream.return(undefined);
    await stream.return(undefined);
    await tick();
    expect(started).toBe(false);
    expect(releaseWaits).toBe(1);
    server.resolve();
    await next;
    const exhausted = controller.runStream(
      "held",
      async function* () {
        yield 3;
      },
      undefined,
      async () => {
        releaseWaits += 1;
      },
    );
    expect((await exhausted.next()).value).toBe(3);
    expect((await exhausted.next()).done).toBe(true);
    expect(releaseWaits).toBe(1);
  });

  it("PERF-002 CONTROL: session bypass, independent sessions and queued stream abort remain intact", async () => {
    const controller = createSessionConcurrencyController(1);
    const gate = deferred();
    const held = controller.run("held", () => gate.promise);
    expect(await controller.run(undefined, async () => 1)).toBe(1);
    expect(await controller.run("", async () => 2)).toBe(2);
    expect(await controller.run("other", async () => 3)).toBe(3);
    const abort = new AbortController();
    let entered = false;
    const queued = controller.runStream(
      "held",
      async function* () {
        entered = true;
        yield 9;
      },
      abort.signal,
    );
    const pending = queued.next();
    abort.abort("queued stream cancelled");
    await expect(pending).rejects.toThrow("queued stream cancelled");
    expect(entered).toBe(false);
    gate.resolve();
    await held;
  });

  it("PERF-002 CONTROL: public API rejects failed requests, queues survivors, and sends no cancelled query", async () => {
    const gate = deferred();
    const sent: string[] = [];
    const db = clickhouseClient({
      host: "http://127.0.0.1:1",
      session_id: "audit_control",
      fetch: async (request) => {
        const id = new URL(request.url).searchParams.get("query_id") ?? "";
        sent.push(id);
        if (id === "first") {
          await gate.promise;
          return new Response("synthetic backend rejection", { status: 500 });
        }
        return Response.json({ data: [{ value: 7 }] });
      },
    });
    const failed = db.execute(sql`SELECT 1`, { query_id: "first" }).catch((error: { kind?: unknown }) => {
      expect(error.kind).toBe("request_failed");
    });
    const abort = new AbortController();
    const cancelled = db
      .execute(sql`SELECT 2`, { query_id: "cancelled", abort_signal: abort.signal })
      .catch((error: { kind?: unknown }) => {
        expect(error.kind).toBe("aborted");
      });
    const next = db.execute(sql`SELECT 7 AS value`, { query_id: "last" });
    abort.abort("cancel before transport");
    await tick();
    expect(sent).toEqual(["first"]);
    gate.resolve();
    await failed;
    await cancelled;
    expect(await next).toEqual([{ value: 7 }]);
    expect(sent).toEqual(["first", "last"]);
  });

  it("PERF-002 supplementary: settled cancellation churn bounds retained queue storage before the active slot releases", async () => {
    const controller = createSessionConcurrencyController(1);
    const gate = deferred();
    const session = "audit_perf002_review_storage";
    let entry: QueueStorage | undefined;
    const originalSet = Map.prototype.set;
    Map.prototype.set = function (this: Map<unknown, unknown>, key: unknown, value: unknown) {
      if (key === session) {
        entry = value as QueueStorage;
      }
      return originalSet.call(this, key, value);
    };
    let held: Promise<void>;
    try {
      held = controller.run(session, () => gate.promise);
    } finally {
      Map.prototype.set = originalSet;
    }
    let survivorStarted = false;
    const survivor = controller.run(session, async () => {
      survivorStarted = true;
      return 91;
    });
    const counts: number[] = [];
    try {
      expect(entry).toBeDefined();
      if (entry === undefined) {
        throw new Error("missing queue storage");
      }
      const queue = entry;
      await nextTurn();
      for (let index = 0; index < 127; index++) {
        const abort = new AbortController();
        const reason = new Error(`review-cancel-${index}`);
        const pending = controller.run(
          session,
          async () => {
            throw new Error("Cancelled operation executed");
          },
          abort.signal,
        );
        abort.abort(reason);
        await pending.then(
          () => {
            throw new Error("Cancelled operation resolved");
          },
          (error: { kind?: unknown; cause?: unknown }) => {
            expect(error.kind).toBe("aborted");
            expect(error.cause).toBe(reason);
          },
        );
        expect(getEventListeners(abort.signal, "abort")).toHaveLength(0);
        counts.push(retainedSlots(queue));
      }
      expect(survivorStarted).toBe(false);
      const liveWaiters = 1;
      expect(
        Math.max(...counts),
        "Storage must track live waiters, not total settled cancellations",
      ).toBeLessThanOrEqual(2 * liveWaiters + 16);
    } finally {
      gate.resolve();
      await held;
      expect(await survivor).toBe(91);
    }
  });

  it("PERF-002 supplementary CONTROL: an independent FIFO model agrees during interleaved admission, cancellation, failure and refill", async () => {
    for (const capacity of [1, 2, 3]) {
      const controller = createSessionConcurrencyController(capacity);
      type Job = {
        id: number;
        abort: AbortController;
        gate: ReturnType<typeof deferred>;
        error: Error;
        state: "queued" | "active" | "cancelled" | "done";
        observed: "pending" | "started" | "cancelled" | "done";
        result: Promise<void>;
      };
      const jobs: Job[] = [];
      const expectedStarts: number[] = [];
      const actualStarts: number[] = [];
      let active = 0;
      let maxActive = 0;
      let seed = 1009 + capacity;
      const random = () => {
        seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
        return seed;
      };
      const fill = () => {
        let slots = capacity - jobs.filter((job) => job.state === "active").length;
        for (const job of jobs) {
          if (slots === 0) {
            break;
          }
          if (job.state !== "queued") {
            continue;
          }
          job.state = "active";
          expectedStarts.push(job.id);
          slots -= 1;
        }
      };
      const enqueue = () => {
        const id = jobs.length;
        const job: Job = {
          id,
          abort: new AbortController(),
          gate: deferred(),
          error: new Error(`job-${id}`),
          state: "queued",
          observed: "pending",
          result: Promise.resolve(),
        };
        jobs.push(job);
        fill();
        job.result = controller
          .run(
            `audit_perf002_review_model_${capacity}`,
            async () => {
              expect(job.observed).toBe("pending");
              job.observed = "started";
              actualStarts.push(id);
              active += 1;
              maxActive = Math.max(maxActive, active);
              try {
                await job.gate.promise;
                if (id % 7 === 0) {
                  throw job.error;
                }
                return { id };
              } finally {
                active -= 1;
              }
            },
            job.abort.signal,
          )
          .then(
            (value) => {
              expect(value).toEqual({ id });
              job.observed = "done";
            },
            (error: { kind?: unknown; cause?: unknown }) => {
              if (job.state === "cancelled") {
                expect(error.kind).toBe("aborted");
                expect(error.cause).toBe(job.error);
                job.observed = "cancelled";
              } else {
                expect(id % 7).toBe(0);
                expect(error).toBe(job.error);
                job.observed = "done";
              }
            },
          );
      };
      const cancel = (job: Job) => {
        expect(job.state).toBe("queued");
        job.state = "cancelled";
        job.abort.abort(job.error);
      };
      const finish = (job: Job) => {
        expect(job.state).toBe("active");
        job.state = "done";
        job.gate.resolve();
        fill();
      };
      const check = async () => {
        await nextTurn();
        expect(actualStarts).toEqual(expectedStarts);
        expect(active).toBe(jobs.filter((job) => job.state === "active").length);
        expect(maxActive).toBeLessThanOrEqual(capacity);
      };
      try {
        for (let index = 0; index < 67; index++) {
          enqueue();
        }
        await check();
        for (const job of [...jobs].reverse()) {
          if (job.state === "queued" && job.id % 5 !== 2) {
            cancel(job);
          }
        }
        await check();
        for (let step = 0; step < 72; step++) {
          const queued = jobs.filter((job) => job.state === "queued");
          const running = jobs.filter((job) => job.state === "active");
          const action = random() % 3;
          if (action === 0 && queued.length) {
            cancel(requireItem(queued, random() % queued.length, "queued job"));
          } else if (action === 1 && running.length) {
            finish(requireItem(running, random() % running.length, "active job"));
          } else {
            enqueue();
          }
          await check();
        }
        while (jobs.some((job) => job.state === "active")) {
          const running = jobs.find((job) => job.state === "active");
          if (running === undefined) {
            throw new Error("missing active job");
          }
          finish(running);
          await check();
        }
        await Promise.all(jobs.map((job) => job.result));
        for (const job of jobs) {
          expect(job.observed).toBe(job.state === "cancelled" ? "cancelled" : "done");
          expect(getEventListeners(job.abort.signal, "abort")).toHaveLength(0);
        }
        expect(await controller.run(`audit_perf002_review_model_${capacity}`, async () => "reused")).toBe("reused");
      } finally {
        for (const job of jobs) {
          job.gate.resolve();
        }
        await Promise.all(jobs.map((job) => job.result));
      }
    }
  }, 35_000);

  it("PERF-002 supplementary CONTROL: shared-signal abort and reentrant reason conversion preserve surviving FIFO nodes", async () => {
    const controller = createSessionConcurrencyController(1);
    const session = "audit_perf002_review_reentrant";
    const gate = deferred();
    const held = controller.run(session, () => gate.promise);
    const starts: number[] = [];
    const pending: Promise<unknown>[] = [];
    const shared = new AbortController();
    const cascade = new AbortController();
    const run = (id: number, signal?: AbortSignal) =>
      controller.run(
        session,
        async () => {
          starts.push(id);
          return id;
        },
        signal,
      );
    const expectCancelled = (request: Promise<unknown>, reason: unknown) =>
      request.then(
        () => {
          throw new Error("Cancelled request resolved");
        },
        (error: { kind?: unknown; cause?: unknown }) => {
          expect(error.kind).toBe("aborted");
          expect(error.cause).toBe(reason);
        },
      );
    let conversions = 0;
    const reason = {
      toString() {
        conversions += 1;
        if (conversions === 1) {
          cascade.abort("cascade");
          pending.push(run(99));
        }
        return "shared cancellation";
      },
    };
    try {
      for (let id = 0; id < 8; id++) {
        pending.push(expectCancelled(run(id, shared.signal), reason));
        if (id === 2 || id === 5) {
          pending.push(run(20 + id));
        }
      }
      pending.push(expectCancelled(run(88, cascade.signal), "cascade"));
      shared.abort(reason);
      await nextTurn();
      expect(conversions).toBe(8);
      expect(starts).toEqual([]);
      expect(getEventListeners(shared.signal, "abort")).toHaveLength(0);
      expect(getEventListeners(cascade.signal, "abort")).toHaveLength(0);
      gate.resolve();
      await held;
      await Promise.all(pending);
      expect(starts).toEqual([22, 25, 99]);
    } finally {
      gate.resolve();
      await held;
      await Promise.all(pending);
    }
  });
});
