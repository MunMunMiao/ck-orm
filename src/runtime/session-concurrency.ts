type SessionQueueEntry = {
  activeCount: number;
  waiters: Array<() => void>;
};

export type SessionConcurrencyController = {
  run<TValue>(sessionId: string | undefined, operation: () => Promise<TValue>): Promise<TValue>;
  runStream<TValue>(
    sessionId: string | undefined,
    operation: () => AsyncGenerator<TValue, void, unknown>,
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
      entry.waiters.shift()?.();
    }

    if (entry.activeCount === 0 && entry.waiters.length === 0) {
      sessions.delete(sessionId);
    }
  };

  const acquireSlot = async (sessionId: string): Promise<() => void> => {
    const entry = sessions.get(sessionId) ?? createSessionQueueEntry();
    sessions.set(sessionId, entry);

    const buildRelease = () => createIdempotentRelease(() => releaseSlot(sessionId, entry));

    if (entry.activeCount < maxConcurrentRequests) {
      entry.activeCount += 1;
      return buildRelease();
    }

    return await new Promise<() => void>((resolve) => {
      entry.waiters.push(() => {
        entry.activeCount += 1;
        resolve(buildRelease());
      });
    });
  };

  return {
    async run<TValue>(sessionId: string | undefined, operation: () => Promise<TValue>): Promise<TValue> {
      if (!sessionId) {
        return await operation();
      }

      const release = await acquireSlot(sessionId);
      try {
        return await operation();
      } finally {
        release();
      }
    },

    async *runStream<TValue>(
      sessionId: string | undefined,
      operation: () => AsyncGenerator<TValue, void, unknown>,
    ): AsyncGenerator<TValue, void, unknown> {
      if (!sessionId) {
        yield* operation();
        return;
      }

      const release = await acquireSlot(sessionId);
      try {
        yield* operation();
      } finally {
        release();
      }
    },
  };
};
