import { createAbortedError, createTimeoutError } from "../errors";

/**
 * Wires together a request timeout and an external user signal into a single
 * AbortController.
 *
 * Listener leak prevention:
 *   - cleanup() removes the external listener and clears the timer.
 *   - As a belt-and-suspenders, the controller's own abort event also triggers
 *     cleanup, so listeners are dropped even if the caller forgets to call
 *     cleanup() explicitly.
 *
 * If the external signal is already aborted on entry, abort propagation runs
 * synchronously.
 */
export const createAbortController = (requestTimeout: number, externalSignal?: AbortSignal) => {
  const controller = new AbortController();
  let cleaned = false;

  const cleanup = () => {
    if (cleaned) {
      return;
    }
    cleaned = true;
    clearTimeout(timer);
    if (externalSignal) {
      externalSignal.removeEventListener("abort", onAbort);
    }
  };

  controller.signal.addEventListener("abort", cleanup, { once: true });

  const timer = setTimeout(() => {
    controller.abort(createTimeoutError(requestTimeout));
  }, requestTimeout);

  const onAbort = () => {
    if (externalSignal?.reason instanceof Error) {
      controller.abort(
        createAbortedError(externalSignal.reason.message, {
          cause: externalSignal.reason,
        }),
      );
      return;
    }
    if (externalSignal?.reason !== undefined) {
      controller.abort(
        createAbortedError(String(externalSignal.reason), {
          cause: externalSignal.reason,
        }),
      );
      return;
    }
    controller.abort(createAbortedError());
  };
  if (externalSignal?.aborted) {
    onAbort();
  } else {
    externalSignal?.addEventListener("abort", onAbort, { once: true });
  }

  return {
    signal: controller.signal,
    cleanup,
  };
};
