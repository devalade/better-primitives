import { Result } from "better-result";
import { abortError } from "../errors/index";
import { linkChild } from "../internal/abort";
import { Scope } from "../scope/index";
import type { Task as TaskType } from "../task/index";

/**
 * A scoped resource with typed acquisition failure.
 *
 * Cleanup rejection remains a defect and is never folded into `E`.
 *
 * @template A - Acquired value.
 * @template E - Expected acquisition failure.
 */
export interface Resource<A, E = never> {
  /** Creates an acquisition Task that registers cleanup in `scope` after successful acquisition. */
  readonly acquire: (scope: Scope) => TaskType<A, E>;
}

/** Typed scoped-resource constructors and combinators. */
export const Resource = {
  /** Creates a Resource from a typed acquisition Task and deterministic release function. */
  make<A, E>(acquire: TaskType<A, E>, release: (value: A) => void | Promise<void>): Resource<A, E> {
    return {
      acquire(scope) {
        return async (signal) => {
          const controller = new AbortController();
          const unlinkScope = linkChild(scope.signal, controller);
          const unlinkCaller = linkChild(signal, controller);
          try {
            if (controller.signal.aborted) throw abortError(controller.signal.reason);
            const result = await acquire(controller.signal);
            if (controller.signal.aborted) {
              if (Result.isOk(result)) await release(result.value);
              throw abortError(controller.signal.reason);
            }
            if (Result.isError(result)) return result;
            scope.addFinalizer(() => release(result.value));
            return result;
          } finally {
            unlinkCaller();
            unlinkScope();
          }
        };
      },
    };
  },

  /**
   * Creates a Task that acquires into a fresh Scope, runs `use`, and always releases afterward.
   * Acquisition, use, and cancellation failures remain explicit; cleanup rejection is a defect.
   */
  use<A, E, B, E2>(
    resource: Resource<A, E>,
    use: (value: A) => TaskType<B, E2>,
  ): TaskType<B, E | E2> {
    return async (signal) => {
      const scope = Scope.make({ signal });
      try {
        const acquired = await resource.acquire(scope)(signal);
        if (Result.isError(acquired)) {
          return Result.err<B, E | E2>(acquired.error);
        }
        return await use(acquired.value)(scope.signal);
      } finally {
        await scope.close();
      }
    };
  },

  /** Creates a Task that acquires a Resource into an existing Scope. */
  add<A, E>(scope: Scope, resource: Resource<A, E>): TaskType<A, E> {
    return resource.acquire(scope);
  },
} as const;
