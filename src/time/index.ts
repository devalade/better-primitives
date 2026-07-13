import { Result } from "better-result";
import { TimeoutError, abortError } from "../errors/index";
import { linkChild } from "../internal/abort";
import type { Task } from "../task/index";

function assertDuration(ms: number, operation: string): void {
  if (!Number.isFinite(ms) || ms < 0) {
    throw new RangeError(`${operation} duration must be a finite non-negative number`);
  }
}

/** Timer Tasks with caller-owned cancellation and typed timeout failures. */
export const Time = {
  /** Creates a Task that succeeds after `ms`; cancellation is classified by the execution boundary. */
  sleep(ms: number): Task<void, never> {
    assertDuration(ms, "Time.sleep");
    return async (signal) => {
      if (signal.aborted) throw abortError(signal.reason);
      if (ms === 0) return Result.ok(undefined);

      await new Promise<void>((resolve, reject) => {
        const id = setTimeout(() => {
          cleanup();
          resolve();
        }, ms);
        const onAbort = () => {
          cleanup();
          reject(abortError(signal.reason));
        };
        const cleanup = () => {
          clearTimeout(id);
          signal.removeEventListener("abort", onAbort);
        };
        signal.addEventListener("abort", onAbort, { once: true });
      });
      return Result.ok(undefined);
    };
  },

  /** Adds a typed timeout to a Task while preserving the Task's expected failures. */
  timeout<A, E>(ms: number, task: Task<A, E>): Task<A, E | TimeoutError> {
    assertDuration(ms, "Time.timeout");
    return async (signal) => {
      const controller = new AbortController();
      const unlink = linkChild(signal, controller);
      let timer: ReturnType<typeof setTimeout> | undefined;
      let timedOut = false;
      const running = Promise.resolve().then(() => task(controller.signal));
      const timeout = new Promise<Result<A, TimeoutError>>((resolve) => {
        timer = setTimeout(() => {
          timedOut = true;
          const error = TimeoutError.after(ms);
          controller.abort(error);
          resolve(Result.err(error));
        }, ms);
      });

      try {
        return await Promise.race([running, timeout]);
      } finally {
        if (timer !== undefined) clearTimeout(timer);
        if (!controller.signal.aborted) controller.abort(abortError());
        unlink();
        if (timedOut) {
          void running.then(
            () => undefined,
            () => undefined,
          );
        }
      }
    };
  },

  /** Adds a typed timeout using an absolute `Date.now()`-based deadline. */
  deadline<A, E>(at: number, task: Task<A, E>): Task<A, E | TimeoutError> {
    if (!Number.isFinite(at)) {
      throw new RangeError("Time.deadline timestamp must be finite");
    }
    return Time.timeout(Math.max(0, at - Date.now()), task);
  },
} as const;
