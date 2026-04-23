import { describe, expect, it } from "bun:test";
import { createIdempotentRelease, createSessionConcurrencyController } from "./session-concurrency";

type Deferred<TValue> = {
  promise: Promise<TValue>;
  reject: (reason?: unknown) => void;
  resolve: (value: TValue | PromiseLike<TValue>) => void;
};

const createDeferred = <TValue = void>(): Deferred<TValue> => {
  let resolve!: Deferred<TValue>["resolve"];
  let reject!: Deferred<TValue>["reject"];
  const promise = new Promise<TValue>((innerResolve, innerReject) => {
    resolve = innerResolve;
    reject = innerReject;
  });
  return { promise, resolve, reject };
};

const flushAsyncWork = async () => {
  await Promise.resolve();
  await Promise.resolve();
};

describe("session concurrency controller", function describeSessionConcurrencyController() {
  it("keeps private slot release callbacks idempotent", function testIdempotentReleaseCallback() {
    let releaseCount = 0;
    const release = createIdempotentRelease(() => {
      releaseCount += 1;
    });

    release();
    release();

    expect(releaseCount).toBe(1);
  });

  it("does not throttle operations that do not target a session", async function testNoSessionBypass() {
    const controller = createSessionConcurrencyController(1);
    const firstGate = createDeferred<void>();
    const secondGate = createDeferred<void>();
    let inFlight = 0;
    let maxInFlight = 0;

    const first = controller.run(undefined, async () => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await firstGate.promise;
      inFlight -= 1;
      return "first";
    });

    const second = controller.run(undefined, async () => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await secondGate.promise;
      inFlight -= 1;
      return "second";
    });

    await flushAsyncWork();
    expect(maxInFlight).toBe(2);

    firstGate.resolve();
    secondGate.resolve();

    await expect(Promise.all([first, second])).resolves.toEqual(["first", "second"]);
  });

  it("serializes same-session operations in FIFO order", async function testSameSessionFifo() {
    const controller = createSessionConcurrencyController(1);
    const started: string[] = [];
    const firstGate = createDeferred<void>();
    const secondGate = createDeferred<void>();
    const thirdGate = createDeferred<void>();

    const first = controller.run("shared_session", async () => {
      started.push("first");
      await firstGate.promise;
      return "first";
    });
    const second = controller.run("shared_session", async () => {
      started.push("second");
      await secondGate.promise;
      return "second";
    });
    const third = controller.run("shared_session", async () => {
      started.push("third");
      await thirdGate.promise;
      return "third";
    });

    await flushAsyncWork();
    expect(started).toEqual(["first"]);

    firstGate.resolve();
    await expect(first).resolves.toBe("first");

    await flushAsyncWork();
    expect(started).toEqual(["first", "second"]);

    secondGate.resolve();
    await expect(second).resolves.toBe("second");

    await flushAsyncWork();
    expect(started).toEqual(["first", "second", "third"]);

    thirdGate.resolve();
    await expect(third).resolves.toBe("third");
  });

  it("keeps different sessions independent while still queuing within each session", async function testDifferentSessions() {
    const controller = createSessionConcurrencyController(1);
    const started: string[] = [];
    const sessionAGate = createDeferred<void>();
    const sessionAQueuedGate = createDeferred<void>();
    const sessionBGate = createDeferred<void>();
    let inFlight = 0;
    let maxInFlight = 0;

    const trackStart = (label: string) => {
      started.push(label);
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
    };

    const trackEnd = () => {
      inFlight -= 1;
    };

    const firstA = controller.run("session_a", async () => {
      trackStart("session_a:first");
      await sessionAGate.promise;
      trackEnd();
      return "session_a:first";
    });
    const secondA = controller.run("session_a", async () => {
      trackStart("session_a:second");
      await sessionAQueuedGate.promise;
      trackEnd();
      return "session_a:second";
    });
    const firstB = controller.run("session_b", async () => {
      trackStart("session_b:first");
      await sessionBGate.promise;
      trackEnd();
      return "session_b:first";
    });

    await flushAsyncWork();
    expect(started).toEqual(["session_a:first", "session_b:first"]);
    expect(maxInFlight).toBe(2);

    sessionBGate.resolve();
    await expect(firstB).resolves.toBe("session_b:first");

    await flushAsyncWork();
    expect(started).toEqual(["session_a:first", "session_b:first"]);

    sessionAGate.resolve();
    await expect(firstA).resolves.toBe("session_a:first");

    await flushAsyncWork();
    expect(started).toEqual(["session_a:first", "session_b:first", "session_a:second"]);

    sessionAQueuedGate.resolve();
    await expect(secondA).resolves.toBe("session_a:second");
  });

  it("releases the queued slot after a same-session failure", async function testFailureRelease() {
    const controller = createSessionConcurrencyController(1);
    const firstGate = createDeferred<void>();
    let secondStarted = false;

    const first = controller.run("shared_session", async () => {
      await firstGate.promise;
      throw new Error("boom");
    });
    const second = controller.run("shared_session", async () => {
      secondStarted = true;
      return "second";
    });

    await flushAsyncWork();
    expect(secondStarted).toBe(false);

    firstGate.resolve();
    await expect(first).rejects.toThrow("boom");

    await flushAsyncWork();
    expect(secondStarted).toBe(true);
    await expect(second).resolves.toBe("second");
  });

  it("holds a same-session slot until the stream iterator closes", async function testStreamSlotLifetime() {
    const controller = createSessionConcurrencyController(1);
    const events: string[] = [];

    const stream = controller.runStream("shared_session", async function* generateRows() {
      events.push("stream:start");
      yield 1;
      yield 2;
    });

    expect(await stream.next()).toEqual({
      done: false,
      value: 1,
    });

    const queued = controller.run("shared_session", async () => {
      events.push("query:start");
      return "query";
    });

    await flushAsyncWork();
    expect(events).toEqual(["stream:start"]);

    await stream.return(undefined);
    await stream.return(undefined);

    await flushAsyncWork();
    expect(events).toEqual(["stream:start", "query:start"]);
    await expect(queued).resolves.toBe("query");
  });

  it("respects configurable same-session parallelism above one", async function testConfigurableParallelism() {
    const controller = createSessionConcurrencyController(2);
    const started: string[] = [];
    const gates = [createDeferred<void>(), createDeferred<void>(), createDeferred<void>()];
    let inFlight = 0;
    let maxInFlight = 0;

    const run = (label: string, gate: Deferred<void>) =>
      controller.run("shared_session", async () => {
        started.push(label);
        inFlight += 1;
        maxInFlight = Math.max(maxInFlight, inFlight);
        await gate.promise;
        inFlight -= 1;
        return label;
      });

    const first = run("first", gates[0]);
    const second = run("second", gates[1]);
    const third = run("third", gates[2]);

    await flushAsyncWork();
    expect(started).toEqual(["first", "second"]);
    expect(maxInFlight).toBe(2);

    gates[0].resolve();
    await expect(first).resolves.toBe("first");

    await flushAsyncWork();
    expect(started).toEqual(["first", "second", "third"]);

    gates[1].resolve();
    gates[2].resolve();
    await expect(Promise.all([second, third])).resolves.toEqual(["second", "third"]);
  });
});
