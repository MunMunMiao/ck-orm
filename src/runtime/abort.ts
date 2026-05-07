import { createAbortedError, createTimeoutError } from "../errors";

/**
 * Wires together an optional request timeout and an external user signal into
 * a single AbortController.
 *
 * Behavior:
 *   - When `requestTimeout` is undefined, no client-side timer is scheduled.
 *     Request lifetime is controlled entirely by the underlying fetch, the
 *     server, the platform's default socket timeout, or the caller-supplied
 *     `externalSignal`.
 *   - When `requestTimeout` is a number, the timer is driven by the platform's
 *     `AbortSignal.timeout()` rather than a hand-rolled `setTimeout`. The
 *     fired reason is then translated into ck-orm's `createTimeoutError(...)`
 *     so downstream consumers see the existing error shape (kind: "timeout",
 *     message "...timed out after Nms; execution state is unknown").
 *
 * Listener leak prevention:
 *   - cleanup() removes the external listener and the timeout listener.
 *   - As a belt-and-suspenders, the controller's own abort event also triggers
 *     cleanup, so listeners are dropped even if the caller forgets to call
 *     cleanup() explicitly.
 *
 * If the external signal (or `AbortSignal.timeout`) is already aborted on
 * entry, abort propagation runs synchronously.
 */
export const createAbortController = (requestTimeout: number | undefined, externalSignal?: AbortSignal) => {
  const controller = new AbortController();
  let cleaned = false;
  let timeoutSignal: AbortSignal | undefined;
  let onTimeout: (() => void) | undefined;

  const cleanup = () => {
    if (cleaned) {
      return;
    }
    cleaned = true;
    if (timeoutSignal !== undefined && onTimeout !== undefined) {
      timeoutSignal.removeEventListener("abort", onTimeout);
    }
    if (externalSignal) {
      externalSignal.removeEventListener("abort", onAbort);
    }
  };

  controller.signal.addEventListener("abort", cleanup, { once: true });

  if (requestTimeout !== undefined) {
    const ms = requestTimeout;
    onTimeout = () => {
      controller.abort(createTimeoutError(ms));
    };
    // AbortSignal.timeout(ms) is guaranteed to fire asynchronously (the spec
    // schedules the timer as a global task), so it cannot be observed as
    // already-aborted at this point — addEventListener is the only path needed.
    timeoutSignal = AbortSignal.timeout(ms);
    timeoutSignal.addEventListener("abort", onTimeout, { once: true });
  }

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
