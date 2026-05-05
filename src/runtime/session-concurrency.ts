import { createAbortedError } from "../errors";

type SessionQueueEntry = {
  activeCount: number;
  waiters: SessionWaiter[];
};

type SessionWaiter = {
  resume(): void;
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
  ): AsyncGenerator<TValue, void, unknown>;
};

const createSessionQueueEntry = (): SessionQueueEntry => ({
  activeCount: 0,
  waiters: [],
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

    while (entry.activeCount < maxConcurrentRequests && entry.waiters.length > 0) {
      entry.waiters.shift()?.resume();
    }

    if (entry.activeCount === 0 && entry.waiters.length === 0) {
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
      if (entry.activeCount === 0 && entry.waiters.length === 0) {
        sessions.delete(sessionId);
      }
      throw createQueuedAbortError(abortSignal);
    }

    const signal = abortSignal;
    return await new Promise<() => void>((resolve, reject) => {
      let settled = false;
      const cleanup = () => {
        signal?.removeEventListener("abort", onAbort);
      };
      const removeWaiter = () => {
        const index = entry.waiters.indexOf(waiter);
        if (index >= 0) {
          entry.waiters.splice(index, 1);
        }
        if (entry.activeCount === 0 && entry.waiters.length === 0) {
          sessions.delete(sessionId);
        }
      };
      const onAbort = () => {
        if (settled) return;
        settled = true;
        cleanup();
        removeWaiter();
        reject(signal ? createQueuedAbortError(signal) : createAbortedError());
      };
      const waiter: SessionWaiter = {
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
