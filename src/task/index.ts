import { Result } from "better-result";
import { Cancelled, abortError } from "../errors/index";
import { linkChild, toCancelled } from "../internal/abort";
import type { CancellableOptions } from "../options";
import type { Schedule } from "../schedule/index";
import type { Scope } from "../scope/index";

/**
 * A lazy asynchronous computation with an explicit expected-failure channel.
 *
 * Rejection is reserved for cancellation control or defects. Execute through {@link Task.execute}
 * or {@link Task.run} to classify cancellation as {@link Cancelled}.
 *
 * @template A - Success value.
 * @template E - Expected failure value.
 */
export type Task<A, E = never> = (signal: AbortSignal) => Promise<Result<A, E>>;

/** Extracts a Task success type. */
export type TaskSuccess<T> = T extends Task<infer A, infer _E> ? A : never;

/** Extracts a Task expected-failure type. */
export type TaskError<T> = T extends Task<infer _A, infer E> ? E : never;

type SomeTask = (signal: AbortSignal) => Promise<unknown>;
type EnsureTask<T> = T extends Task<infer _A, infer _E> ? T : never;
type NonPromise<T> = T extends PromiseLike<unknown> ? never : T;

/**
 * A running Task whose result preserves both its expected failure and cancellation.
 *
 * @template A - Success value.
 * @template E - Expected failure value.
 */
export interface Fiber<A, E = never> extends AsyncDisposable {
  readonly signal: AbortSignal;
  readonly isDone: boolean;
  /** Requests cooperative cancellation. */
  abort(reason?: unknown): void;
  /** Awaits success, expected failure, or cancellation. Defects still reject. */
  result(): Promise<Result<A, E | Cancelled>>;
}

class FiberImpl<A, E> implements Fiber<A, E> {
  readonly #controller = new AbortController();
  readonly #promise: Promise<Result<A, E>>;
  #done = false;

  constructor(task: Task<A, E>, parent?: AbortSignal) {
    const unlink = linkChild(parent, this.#controller);
    this.#promise = Promise.resolve()
      .then(() => {
        if (this.#controller.signal.aborted) {
          throw abortError(this.#controller.signal.reason);
        }
        return task(this.#controller.signal);
      })
      .finally(() => {
        this.#done = true;
        unlink();
      });

    void this.#promise.then(
      () => undefined,
      () => undefined,
    );
  }

  get signal(): AbortSignal {
    return this.#controller.signal;
  }

  get isDone(): boolean {
    return this.#done;
  }

  abort(reason?: unknown): void {
    if (!this.#done && !this.#controller.signal.aborted) {
      this.#controller.abort(reason === undefined ? abortError() : reason);
    }
  }

  async result(): Promise<Result<A, E | Cancelled>> {
    try {
      const result = await this.#promise;
      if (this.#controller.signal.aborted) {
        return Result.err(Cancelled.fromCause(this.#controller.signal.reason));
      }
      return result;
    } catch (cause: unknown) {
      const cancelled = toCancelled(cause, this.#controller.signal);
      if (cancelled) return Result.err(cancelled);
      throw cause;
    }
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.abort();
    await this.result();
  }
}

type Tagged = { readonly _tag: string };

function firstSuccess<A, E>(tasks: ReadonlyArray<Task<A, E>>): Task<A, E> {
  return async (signal) => {
    const controller = new AbortController();
    const unlink = linkChild(signal, controller);
    type Settled =
      | { readonly index: number; readonly succeeded: true; readonly result: Result<A, E> }
      | { readonly index: number; readonly succeeded: false; readonly cause: unknown };
    const pending = new Map<number, Promise<Settled>>();
    let firstFailure: { readonly error: E } | undefined;

    for (const [index, task] of tasks.entries()) {
      pending.set(
        index,
        Promise.resolve()
          .then(() => task(controller.signal))
          .then(
            (result): Settled => ({ index, succeeded: true, result }),
            (cause: unknown): Settled => ({ index, succeeded: false, cause }),
          ),
      );
    }

    try {
      while (pending.size > 0) {
        const settled = await Promise.race(pending.values());
        pending.delete(settled.index);
        if (!settled.succeeded) throw settled.cause;
        if (Result.isOk(settled.result)) return settled.result;
        firstFailure ??= { error: settled.result.error };
      }

      if (!firstFailure) {
        throw new Error("Task first-success race completed without a branch outcome");
      }
      return Result.err(firstFailure.error);
    } finally {
      if (!controller.signal.aborted) controller.abort(abortError());
      unlink();
      await Promise.allSettled(pending.values());
    }
  };
}

function delay(signal: AbortSignal, ms: number): Promise<void> {
  if (signal.aborted) return Promise.reject(abortError(signal.reason));
  if (ms === 0) return Promise.resolve();
  return new Promise((resolve, reject) => {
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
}

/** Typed asynchronous computation constructors, combinators, and execution boundaries. */
export const Task = {
  /** Creates a successful Task. */
  succeed<A>(value: A): Task<A, never> {
    return async () => Result.ok(value);
  },

  /** Creates a Task that fails through its expected-failure channel. */
  fail<E>(error: E): Task<never, E> {
    return async () => Result.err(error);
  },

  /** Defines a Task that already obeys the typed computation contract. */
  from<A, E = never>(task: Task<A, E>): Task<A, E> {
    return task;
  },

  /**
   * Adapts a rejection-based Promise boundary into a typed Task.
   *
   * Cancellation is rethrown for classification by the execution boundary; other rejection values
   * are translated with `onError`.
   */
  tryPromise<A, E>(
    evaluate: (signal: AbortSignal) => Promise<A>,
    onError: (cause: unknown) => E,
  ): Task<A, E> {
    return async (signal) => {
      try {
        return Result.ok(await evaluate(signal));
      } catch (cause: unknown) {
        const cancelled = toCancelled(cause, signal);
        if (cancelled) throw cause;
        return Result.err(onError(cause));
      }
    };
  },

  /** Maps a Task success value without changing its expected failures. */
  map<A, E, B>(task: Task<A, E>, f: (value: A) => B & NonPromise<B>): Task<B, E> {
    return async (signal) => {
      const result = await task(signal);
      return Result.isError(result)
        ? Result.err<B, E>(result.error)
        : Result.ok<B, E>(f(result.value));
    };
  },

  /** Maps a Task expected failure without changing its success value. */
  mapError<A, E, E2>(task: Task<A, E>, f: (error: E) => E2): Task<A, E2> {
    return async (signal) => {
      const result = await task(signal);
      return Result.isError(result) ? Result.err(f(result.error)) : result;
    };
  },

  /** Sequentially composes Tasks and unions their expected-failure channels. */
  flatMap<A, E, B, E2>(task: Task<A, E>, f: (value: A) => Task<B, E2>): Task<B, E | E2> {
    return async (signal) => {
      const result = await task(signal);
      if (Result.isError(result)) return Result.err<B, E | E2>(result.error);
      return f(result.value)(signal);
    };
  },

  /** Recovers from every expected failure and unions the recovery Task's failure channel. */
  catchAll<A, E, B, E2>(task: Task<A, E>, f: (error: E) => Task<B, E2>): Task<A | B, E2> {
    return async (signal) => {
      const result = await task(signal);
      return Result.isError(result) ? f(result.error)(signal) : Result.ok<A | B, E2>(result.value);
    };
  },

  /** Recovers one tagged expected-failure variant while preserving the remaining union. */
  catchTag<A, E extends Tagged, K extends E["_tag"], B, E2>(
    task: Task<A, E>,
    tag: K,
    f: (error: Extract<E, { readonly _tag: K }>) => Task<B, E2>,
  ): Task<A | B, Exclude<E, { readonly _tag: K }> | E2> {
    return async (signal) => {
      const result = await task(signal);
      if (Result.isOk(result)) {
        return Result.ok<A | B, Exclude<E, { readonly _tag: K }> | E2>(result.value);
      }
      if (result.error._tag !== tag) {
        // SAFETY: a non-matching discriminant excludes the handled member from the remaining union.
        return Result.err<A | B, Exclude<E, { readonly _tag: K }> | E2>(
          result.error as Exclude<E, { readonly _tag: K }>,
        );
      }
      // SAFETY: matching the discriminant refines the selected member of the tagged error union.
      return f(result.error as Extract<E, { readonly _tag: K }>)(signal);
    };
  },

  /** Retries expected Task failures according to a pure Schedule. Defects are not retried. */
  retry<A, E>(task: Task<A, E>, schedule: Schedule<E>): Task<A, E> {
    return async (signal) => {
      let attempt = 0;
      while (true) {
        const result = await task(signal);
        if (Result.isOk(result)) return result;
        const wait = schedule.next(attempt, result.error);
        if (wait === undefined) return result;
        if (!Number.isFinite(wait) || wait < 0) {
          throw new RangeError("Task.retry schedule must return a finite non-negative delay");
        }
        await delay(signal, wait);
        attempt += 1;
      }
    };
  },

  /** Starts a Task and returns its cancellable Fiber. */
  run<A, E>(task: Task<A, E>, options?: CancellableOptions): Fiber<A, E> {
    return new FiberImpl(task, options?.signal);
  },

  /** Executes a Task to a typed result, adding cancellation to its error channel. */
  execute<A, E>(task: Task<A, E>, options?: CancellableOptions): Promise<Result<A, E | Cancelled>> {
    return new FiberImpl(task, options?.signal).result();
  },

  /** Starts a Task supervised by `scope`. */
  fork<A, E>(scope: Scope, task: Task<A, E>): Fiber<A, E> {
    const fiber = new FiberImpl(task, scope.signal);
    scope.addFinalizer(async () => {
      fiber.abort();
      await fiber.result();
    });
    return fiber;
  },

  /** Runs Tasks concurrently, preserving value order and cancelling siblings after failure. */
  all<const Tasks extends readonly unknown[]>(
    tasks: Tasks & { readonly [K in keyof Tasks]: EnsureTask<Tasks[K]> },
  ): Task<{ readonly [K in keyof Tasks]: TaskSuccess<Tasks[K]> }, TaskError<Tasks[number]>> {
    return async (signal) => {
      const controller = new AbortController();
      const unlink = linkChild(signal, controller);
      const values: unknown[] = [];
      let firstFailure: { readonly error: TaskError<Tasks[number]> } | undefined;
      let firstDefect: { readonly cause: unknown } | undefined;

      try {
        await Promise.all(
          tasks.map(async (task, index) => {
            try {
              // SAFETY: the mapped parameter constraint admits only Task values.
              const unresolved = await (task as SomeTask)(controller.signal);
              const result = unresolved as Result<unknown, TaskError<Tasks[number]>>;
              if (Result.isError(result)) {
                firstFailure ??= { error: result.error };
                if (!controller.signal.aborted) controller.abort(abortError());
                return;
              }
              values[index] = result.value;
            } catch (cause: unknown) {
              if (!firstFailure && !firstDefect) firstDefect = { cause };
              if (!controller.signal.aborted) controller.abort(abortError());
            }
          }),
        );
        if (firstDefect) throw firstDefect.cause;
        if (firstFailure) {
          return Result.err<
            { readonly [K in keyof Tasks]: TaskSuccess<Tasks[K]> },
            TaskError<Tasks[number]>
          >(firstFailure.error);
        }
        if (controller.signal.aborted) throw abortError(controller.signal.reason);
        // SAFETY: every Task succeeded and wrote its value at the corresponding tuple index.
        return Result.ok(values as { readonly [K in keyof Tasks]: TaskSuccess<Tasks[K]> });
      } finally {
        if (!controller.signal.aborted) controller.abort(abortError());
        unlink();
      }
    };
  },

  /**
   * Creates a first-success race. Typed failures are ignored while another branch can succeed; if
   * every branch fails, the first observed typed failure is returned. Losers are aborted and settled.
   */
  race<const Tasks extends readonly [unknown, ...unknown[]]>(
    tasks: Tasks & { readonly [K in keyof Tasks]: EnsureTask<Tasks[K]> },
  ): Task<TaskSuccess<Tasks[number]>, TaskError<Tasks[number]>> {
    // SAFETY: the mapped parameter constraint admits only Tasks; the homogeneous view widens their
    // success and failure types to the exact unions returned below.
    return firstSuccess(
      tasks as ReadonlyArray<Task<TaskSuccess<Tasks[number]>, TaskError<Tasks[number]>>>,
    );
  },

  /** Named first-success race preserving correlated branch keys, values, and failure unions. */
  select<const Branches extends Readonly<Record<string, unknown>>>(
    branches: Branches & { readonly [K in keyof Branches]: EnsureTask<Branches[K]> },
  ): Task<
    {
      [K in keyof Branches]: { readonly key: K; readonly value: TaskSuccess<Branches[K]> };
    }[keyof Branches],
    TaskError<Branches[keyof Branches]>
  > {
    const keys = Object.keys(branches) as Array<keyof Branches & string>;
    if (keys.length === 0) throw new TypeError("Task.select requires at least one branch");
    const tasks = keys.map((key) => {
      const branch = branches[key];
      if (!branch) throw new TypeError(`Task.select missing branch: ${key}`);
      // SAFETY: the mapped parameter constraint establishes the branch Task contract.
      return Task.map(
        branch as Task<TaskSuccess<typeof branch>, TaskError<typeof branch>>,
        (value) => ({ key, value }),
      );
    });
    // SAFETY: each mapped Task preserves its literal branch key and corresponding success/error type.
    return firstSuccess(tasks) as Task<
      {
        [K in keyof Branches]: { readonly key: K; readonly value: TaskSuccess<Branches[K]> };
      }[keyof Branches],
      TaskError<Branches[keyof Branches]>
    >;
  },

  /** Maps an iterable with bounded concurrency and an explicit expected-failure channel. */
  forEach<A, B, E>(
    items: Iterable<A>,
    f: (item: A, index: number) => Task<B, E>,
    options?: { readonly concurrency?: number },
  ): Task<ReadonlyArray<B>, E> {
    const list = Array.from(items);
    const concurrency = options?.concurrency ?? (list.length === 0 ? 1 : list.length);
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new RangeError("Task.forEach concurrency must be a positive integer");
    }

    return async (signal) => {
      const controller = new AbortController();
      const unlink = linkChild(signal, controller);
      const results: B[] = Array.from({ length: list.length });
      let nextIndex = 0;
      let firstFailure: { readonly error: E } | undefined;
      let firstDefect: { readonly cause: unknown } | undefined;
      const worker = async (): Promise<void> => {
        while (!firstFailure && !firstDefect) {
          const index = nextIndex;
          nextIndex += 1;
          if (index >= list.length) return;
          const item = list[index];
          try {
            // SAFETY: index is within list.length; noUncheckedIndexedAccess does not model that refinement.
            const result = await f(item as A, index)(controller.signal);
            if (Result.isError(result)) {
              firstFailure ??= { error: result.error };
              if (!controller.signal.aborted) controller.abort(abortError());
              return;
            }
            results[index] = result.value;
          } catch (cause: unknown) {
            if (!firstFailure && !firstDefect) firstDefect = { cause };
            if (!controller.signal.aborted) controller.abort(abortError());
            return;
          }
        }
      };

      const workerCount = Math.min(concurrency, list.length === 0 ? 1 : list.length);
      try {
        await Promise.all(Array.from({ length: workerCount }, () => worker()));
        if (firstDefect) throw firstDefect.cause;
        if (firstFailure) return Result.err(firstFailure.error);
        if (controller.signal.aborted) throw abortError(controller.signal.reason);
        return Result.ok(results);
      } finally {
        if (!controller.signal.aborted) controller.abort(abortError());
        unlink();
      }
    };
  },

  /** A Task that yields to the microtask queue. */
  yieldNow: (): Task<void, never> => async () => {
    await new Promise<void>((resolve) => queueMicrotask(resolve));
    return Result.ok(undefined);
  },
} as const;
