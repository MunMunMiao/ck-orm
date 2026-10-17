import { createAbortedError } from "../errors";

type SessionQueueEntry = {
  activeCount: number;
  head: SessionWaiter | undefined;
  tail: SessionWaiter | undefined;
};

type SessionWaiter = {
  previous: SessionWaiter | undefined;
  next: SessionWaiter | undefined;
  resume: (() => void) | undefined;
};

const removeWaiter = (entry: SessionQueueEntry, waiter: SessionWaiter) => {
  if (waiter.previous) waiter.previous.next = waiter.next;
  else entry.head = waiter.next;
  if (waiter.next) waiter.next.previous = waiter.previous;
  else entry.tail = waiter.previous;
  waiter.previous = undefined;
  waiter.next = undefined;
};

export type SessionConcurrencyController = {
  run<TValue>(
    sessionId: string | undefined,
    operation: () => Promise<TValue>,
    abortSignal?: AbortSignal,
  ): Promise<TValue>;
  runStream<TValue>(
    sessionId: string | undefined,
    operation: () => AsyncGenerator<TValue, void, unknown>,
    abortSignal?: AbortSignal,
    waitForServerRelease?: () => Promise<void>,
  ): AsyncGenerator<TValue, void, unknown>;
};

const createSessionQueueEntry = (): SessionQueueEntry => ({
  activeCount: 0,
  head: undefined,
  tail: undefined,
});

export const createIdempotentRelease = (releaseSlot: () => void): (() => void) => {
  let released = false;

  return () => {
    if (released) {
      return;
    }
    released = true;
    releaseSlot();
  };
};

export const createSessionConcurrencyController = (maxConcurrentRequests: number): SessionConcurrencyController => {
  const sessions = new Map<string, SessionQueueEntry>();

  const releaseSlot = (sessionId: string, entry: SessionQueueEntry) => {
    entry.activeCount -= 1;

    while (entry.activeCount < maxConcurrentRequests && entry.head) {
      const waiter = entry.head;
      removeWaiter(entry, waiter);
      const resume = waiter.resume;
      waiter.resume = undefined;
      resume?.();
    }

    if (entry.activeCount === 0 && !entry.head) {
      sessions.delete(sessionId);
    }
  };

  const createQueuedAbortError = (signal: AbortSignal) => {
    if (signal.reason instanceof Error) {
      return createAbortedError(signal.reason.message, { cause: signal.reason });
    }
    if (signal.reason !== undefined) {
      return createAbortedError(String(signal.reason), { cause: signal.reason });
    }
    return createAbortedError();
  };

  const acquireSlot = async (sessionId: string, abortSignal?: AbortSignal): Promise<() => void> => {
    const entry = sessions.get(sessionId) ?? createSessionQueueEntry();
    sessions.set(sessionId, entry);

    const buildRelease = () => createIdempotentRelease(() => releaseSlot(sessionId, entry));

    if (entry.activeCount < maxConcurrentRequests) {
      entry.activeCount += 1;
      return buildRelease();
    }

    if (abortSignal?.aborted) {
      if (entry.activeCount === 0 && !entry.head) {
        sessions.delete(sessionId);
      }
      throw createQueuedAbortError(abortSignal);
    }

    const signal = abortSignal;
    return new Promise<() => void>((resolve, reject) => {
      let settled = false;
      const cleanup = () => {
        signal?.removeEventListener("abort", onAbort);
      };
      const onAbort = () => {
        if (settled) return;
        settled = true;
        cleanup();
        removeWaiter(entry, waiter);
        waiter.resume = undefined;
        if (entry.activeCount === 0 && !entry.head) sessions.delete(sessionId);
        reject(signal ? createQueuedAbortError(signal) : createAbortedError());
      };
      const waiter: SessionWaiter = {
        previous: entry.tail,
        next: undefined,
        resume() {
          if (settled) return;
          settled = true;
          cleanup();
          entry.activeCount += 1;
          resolve(buildRelease());
        },
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      if (entry.tail) entry.tail.next = waiter;
      else entry.head = waiter;
      entry.tail = waiter;
    });
  };

  return {
    async run<TValue>(
      sessionId: string | undefined,
      operation: () => Promise<TValue>,
      abortSignal?: AbortSignal,
    ): Promise<TValue> {
      if (!sessionId) {
        return await operation();
      }

      const release = await acquireSlot(sessionId, abortSignal);
      try {
        return await operation();
      } finally {
        release();
      }
    },

    async *runStream<TValue>(
      sessionId: string | undefined,
      operation: () => AsyncGenerator<TValue, void, unknown>,
      abortSignal?: AbortSignal,
      waitForServerRelease?: () => Promise<void>,
    ): AsyncGenerator<TValue, void, unknown> {
      if (!sessionId) {
        yield* operation();
        return;
      }

      const release = await acquireSlot(sessionId, abortSignal);
      let completed = false;
      try {
        yield* operation();
        completed = true;
      } finally {
        // A stream left early (break, error, abort) can still be running on the
        // server, which keeps the session locked until it notices the closed
        // connection. Hold the local slot until then, without blocking the
        // caller, so the next same-session request does not hit SESSION_IS_LOCKED.
        if (completed || !waitForServerRelease) {
          release();
        } else {
          void waitForServerRelease().finally(release);
        }
      }
    },
  };
};
