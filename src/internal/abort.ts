import { Result } from "better-result";
import { Cancelled, abortError, isAbortCause } from "../errors/index";

/**
 * Returns `Cancelled` when the signal is already aborted.
 *
 * @param signal - Optional caller abort signal.
 */
export function cancelledIfAborted(signal?: AbortSignal): Cancelled | undefined {
  if (signal?.aborted) {
    return Cancelled.fromCause(signal.reason);
  }
  return undefined;
}

/**
 * Converts an abort/rejection cause into `Cancelled` when it is cancellation.
 *
 * @param cause - Unknown catch value.
 * @param signal - Optional signal that may already be aborted.
 */
export function toCancelled(cause: unknown, signal?: AbortSignal): Cancelled | undefined {
  if (signal?.aborted) {
    return Cancelled.fromCause(signal.reason ?? cause);
  }
  if (isAbortCause(cause)) {
    return Cancelled.fromCause(cause);
  }
  if (Cancelled.is(cause)) {
    return cause;
  }
  return undefined;
}

/**
 * Wraps a promise, mapping cancellation to `Result.err(Cancelled)`.
 * Non-cancellation rejections are rethrown (caller/defect failures).
 *
 * @template A
 * @param promise - In-flight work.
 * @param signal - Optional signal used for classification.
 */
export async function resultFromCancellable<A>(
  promise: Promise<A>,
  signal?: AbortSignal,
): Promise<Result<A, Cancelled>> {
  try {
    return Result.ok(await promise);
  } catch (cause: unknown) {
    const cancelled = toCancelled(cause, signal);
    if (cancelled) return Result.err(cancelled);
    throw cause;
  }
}

/**
 * Merges signals so the result aborts when any input aborts.
 *
 * @param signals - Parent signals.
 */
export function anySignal(...signals: AbortSignal[]): AbortSignal {
  const filtered = signals.filter((s) => s != null);
  if (filtered.length === 0) return new AbortController().signal;
  const only = filtered[0];
  if (filtered.length === 1 && only) return only;

  const controller = new AbortController();
  const onAbort = (event: Event) => {
    const target = event.target;
    if (target instanceof AbortSignal) {
      controller.abort(target.reason);
    }
  };

  for (const signal of filtered) {
    if (signal.aborted) {
      controller.abort(signal.reason);
      return controller.signal;
    }
    signal.addEventListener("abort", onAbort, { once: true });
  }

  controller.signal.addEventListener(
    "abort",
    () => {
      for (const signal of filtered) {
        signal.removeEventListener("abort", onAbort);
      }
    },
    { once: true },
  );

  return controller.signal;
}

/**
 * Links a child AbortController to a parent signal.
 *
 * @param parent - Optional parent signal.
 * @param child - Child controller.
 * @returns Unlink function.
 */
export function linkChild(parent: AbortSignal | undefined, child: AbortController): () => void {
  if (!parent) return () => {};
  if (parent.aborted) {
    child.abort(parent.reason);
    return () => {};
  }
  const onAbort = () => child.abort(parent.reason);
  parent.addEventListener("abort", onAbort, { once: true });
  return () => parent.removeEventListener("abort", onAbort);
}

export { abortError };
