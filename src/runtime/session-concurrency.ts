import { createAbortedError } from "../errors";

type SessionQueueEntry = {
  activeCount: number;
  waiters: SessionWaiter[];
  // Head index — first non-yet-shifted waiter slot. Avoids `Array.shift()`
  // in the drain loop, which is O(N) per call (memmove of every later
  // element); under a long backlog the loop would degrade to O(N²).
  head: number;
};

// `cancelled` waiters are tombstones skipped at drain time. We favour O(1)
// flag-flipping over O(N) `splice` removal so abort storms on a busy
// session don't degrade to quadratic queue maintenance.
type SessionWaiter = {
  cancelled: boolean;
  resume(): void;
};

// Compact the waiter array once `head` has eaten through enough of it that
// we'd otherwise grow without bound. Threshold tuned to keep the drained
// prefix cheap to discard while only running on already-busy sessions.
const SESSION_QUEUE_COMPACT_THRESHOLD = 16;

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
  ): AsyncGenerator<TValue, void, unknown>;
};

const createSessionQueueEntry = (): SessionQueueEntry => ({
  activeCount: 0,
  waiters: [],
  head: 0,
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

    while (entry.activeCount < maxConcurrentRequests && entry.head < entry.waiters.length) {
      const waiter = entry.waiters[entry.head];
      entry.head += 1;
      // Skip tombstones from cancelled waiters; do not consume a slot for them.
      if (waiter && !waiter.cancelled) {
        waiter.resume();
      }
    }

    // Compact when the head has chewed through a non-trivial prefix so the
    // backing array doesn't grow indefinitely on long-lived sessions.
    if (entry.head >= SESSION_QUEUE_COMPACT_THRESHOLD && entry.head * 2 >= entry.waiters.length) {
      entry.waiters = entry.waiters.slice(entry.head);
      entry.head = 0;
    }

    if (entry.activeCount === 0 && entry.head >= entry.waiters.length) {
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
      if (entry.activeCount === 0 && entry.head >= entry.waiters.length) {
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
        // Tombstone the waiter; releaseSlot will skip it. We can't drop the
        // session entry here (live waiters may exist behind us) — releaseSlot
        // already collapses it once the queue is empty.
        waiter.cancelled = true;
        reject(signal ? createQueuedAbortError(signal) : createAbortedError());
      };
      const waiter: SessionWaiter = {
        cancelled: false,
        resume() {
          if (settled) return;
          settled = true;
          cleanup();
          entry.activeCount += 1;
          resolve(buildRelease());
        },
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      entry.waiters.push(waiter);
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
    ): AsyncGenerator<TValue, void, unknown> {
      if (!sessionId) {
        yield* operation();
        return;
      }

      const release = await acquireSlot(sessionId, abortSignal);
      try {
        yield* operation();
      } finally {
        release();
      }
    },
  };
};
