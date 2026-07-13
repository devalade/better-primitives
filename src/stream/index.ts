import { Result } from "better-result";
import { Cancelled, abortError } from "../errors/index";
import { linkChild, toCancelled } from "../internal/abort";
import type { CancellableOptions } from "../options";
import type { Task } from "../task/index";
import { Time } from "../time/index";

/** Left-to-right function composition. */
export function pipe<A>(a: A): A;
/** Left-to-right function composition. */
export function pipe<A, B>(a: A, ab: (a: A) => B): B;
/** Left-to-right function composition. */
export function pipe<A, B, C>(a: A, ab: (a: A) => B, bc: (b: B) => C): C;
/** Left-to-right function composition. */
export function pipe<A, B, C, D>(a: A, ab: (a: A) => B, bc: (b: B) => C, cd: (c: C) => D): D;
/** Left-to-right function composition. */
export function pipe<A, B, C, D, E>(
  a: A,
  ab: (a: A) => B,
  bc: (b: B) => C,
  cd: (c: C) => D,
  de: (d: D) => E,
): E;
/** Left-to-right function composition. */
export function pipe<A, B, C, D, E, F>(
  a: A,
  ab: (a: A) => B,
  bc: (b: B) => C,
  cd: (c: C) => D,
  de: (d: D) => E,
  ef: (e: E) => F,
): F;
/** Left-to-right function composition. */
export function pipe<A, B, C, D, E, F, G>(
  a: A,
  ab: (a: A) => B,
  bc: (b: B) => C,
  cd: (c: C) => D,
  de: (d: D) => E,
  ef: (e: E) => F,
  fg: (f: F) => G,
): G;
export function pipe(value: unknown, ...fns: Array<(value: unknown) => unknown>): unknown {
  let output = value;
  for (const fn of fns) output = fn(output);
  return output;
}

const streamSource: unique symbol = Symbol("better-primitives/Stream/source");

/**
 * An opaque asynchronous stream with an explicit terminal expected-failure channel.
 *
 * Use {@link Stream.from} and the Stream combinators to construct values. Convert to raw
 * `AsyncIterable` only through the explicitly rejection-based {@link Stream.toAsyncIterable} boundary.
 *
 * @template A - Emitted value.
 * @template E - Expected terminal failure.
 */
export interface Stream<A, E = never> {
  readonly [streamSource]: (signal: AbortSignal) => AsyncIterable<Result<A, E>>;
}

/** Extracts a Stream value type. */
export type StreamValue<S> = S extends Stream<infer A, infer _E> ? A : never;

/** Extracts a Stream expected-failure type. */
export type StreamError<S> = S extends Stream<infer _A, infer E> ? E : never;

function makeStream<A, E>(
  source: (signal: AbortSignal) => AsyncIterable<Result<A, E>>,
): Stream<A, E> {
  return { [streamSource]: source };
}

function sourceOf<A, E>(stream: Stream<A, E>, signal: AbortSignal): AsyncIterable<Result<A, E>> {
  return stream[streamSource](signal);
}

function assertPositiveInteger(value: number, operation: string): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(`${operation} must be a positive integer`);
  }
}

function isAsyncIterable<A>(source: unknown): source is AsyncIterable<A> {
  return source != null && typeof (source as AsyncIterable<A>)[Symbol.asyncIterator] === "function";
}

type SourceIterator<A> = Iterator<A, unknown> | AsyncIterator<A, unknown>;

function nextOrAbort<A>(
  iterator: SourceIterator<A>,
  signal: AbortSignal,
): Promise<IteratorResult<A, unknown>> {
  if (signal.aborted) return Promise.reject(abortError(signal.reason));

  return new Promise((resolve, reject) => {
    let settled = false;
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    const onAbort = () => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(abortError(signal.reason));
    };

    signal.addEventListener("abort", onAbort, { once: true });
    Promise.resolve()
      .then(() => iterator.next())
      .then(
        (step) => {
          if (settled) return;
          settled = true;
          cleanup();
          resolve(step);
        },
        (cause: unknown) => {
          if (settled) return;
          settled = true;
          cleanup();
          reject(cause);
        },
      );
  });
}

/** Typed asynchronous stream constructors, combinators, interop boundaries, and runners. */
export const Stream = {
  /**
   * Lifts a raw iterable and closes its iterator when the Stream is cancelled or consumed early.
   * Rejection from a raw async source remains an untyped defect boundary.
   */
  from<A>(source: AsyncIterable<A> | Iterable<A>): Stream<A, never> {
    return makeStream(async function* (signal) {
      const iterator: SourceIterator<A> = isAsyncIterable<A>(source)
        ? source[Symbol.asyncIterator]()
        : source[Symbol.iterator]();
      try {
        while (true) {
          const step = await nextOrAbort(iterator, signal);
          if (step.done) return;
          yield Result.ok(step.value);
        }
      } finally {
        const closing = iterator.return?.();
        if (closing) await closing;
      }
    });
  },

  /** Creates a stream that terminates with an expected failure. */
  fail<E>(error: E): Stream<never, E> {
    return makeStream(async function* () {
      yield Result.err(error);
    });
  },

  /** Emits DOM events until the runner's signal is aborted or the consumer returns. */
  fromEvent<T = Event>(target: EventTarget, name: string): Stream<T, never> {
    return makeStream(async function* (signal) {
      const queue: T[] = [];
      let wake: (() => void) | undefined;
      const onEvent = (event: Event) => {
        queue.push(event as T);
        wake?.();
      };
      const onAbort = () => wake?.();
      target.addEventListener(name, onEvent);
      signal.addEventListener("abort", onAbort, { once: true });
      try {
        while (true) {
          if (signal.aborted) throw abortError(signal.reason);
          if (queue.length > 0) {
            // SAFETY: the positive length check establishes that shift removed a T.
            yield Result.ok(queue.shift() as T);
            continue;
          }
          await new Promise<void>((resolve) => {
            wake = resolve;
          });
          wake = undefined;
        }
      } finally {
        target.removeEventListener(name, onEvent);
        signal.removeEventListener("abort", onAbort);
      }
    });
  },

  /** Unfolds state with a typed Task until the Task succeeds with `null`. */
  unfold<S, A, E>(seed: S, step: (state: S) => Task<readonly [A, S] | null, E>): Stream<A, E> {
    return makeStream(async function* (signal) {
      let state = seed;
      while (true) {
        const next = await step(state)(signal);
        if (Result.isError(next)) {
          yield Result.err<A, E>(next.error);
          return;
        }
        if (next.value === null) return;
        const [value, nextState] = next.value;
        state = nextState;
        yield Result.ok(value);
      }
    });
  },

  /** Emits incrementing indices separated by `ms`. */
  tick(ms: number): Stream<number, never> {
    const sleep = Time.sleep(ms);
    return makeStream(async function* (signal) {
      let index = 0;
      while (true) {
        await sleep(signal);
        yield Result.ok(index);
        index += 1;
      }
    });
  },

  /** Pure synchronous map preserving the stream's expected failures. */
  map<A, B>(f: (value: A) => B): <E>(stream: Stream<A, E>) => Stream<B, E> {
    return function <E>(stream: Stream<A, E>): Stream<B, E> {
      return makeStream(async function* (signal) {
        for await (const item of sourceOf(stream, signal)) {
          if (Result.isError(item)) {
            yield Result.err<B, E>(item.error);
            return;
          }
          yield Result.ok(f(item.value));
        }
      });
    };
  },

  /** Sequential Task map that unions stream and mapper expected failures. */
  mapEffect<A, E2, B>(
    f: (value: A) => Task<B, E2>,
  ): <E>(stream: Stream<A, E>) => Stream<B, E | E2> {
    return function <E>(stream: Stream<A, E>): Stream<B, E | E2> {
      return makeStream(async function* (signal) {
        for await (const item of sourceOf(stream, signal)) {
          if (Result.isError(item)) {
            yield Result.err<B, E | E2>(item.error);
            return;
          }
          const mapped = await f(item.value)(signal);
          if (Result.isError(mapped)) {
            yield Result.err<B, E | E2>(mapped.error);
            return;
          }
          yield Result.ok<B, E | E2>(mapped.value);
        }
      });
    };
  },

  /** Filters values while preserving expected failures. */
  filter<A>(predicate: (value: A) => boolean): <E>(stream: Stream<A, E>) => Stream<A, E> {
    return function <E>(stream: Stream<A, E>): Stream<A, E> {
      return makeStream(async function* (signal) {
        for await (const item of sourceOf(stream, signal)) {
          if (Result.isError(item)) {
            yield item;
            return;
          }
          if (predicate(item.value)) yield item;
        }
      });
    };
  },

  /** Takes at most `count` values and closes the upstream stream on completion. */
  take(count: number): <A, E>(stream: Stream<A, E>) => Stream<A, E> {
    if (!Number.isInteger(count) || count < 0) {
      throw new RangeError("Stream.take count must be a non-negative integer");
    }
    return (stream) =>
      makeStream(async function* (signal) {
        if (count === 0) return;
        let emitted = 0;
        for await (const item of sourceOf(stream, signal)) {
          yield item;
          if (Result.isError(item)) return;
          emitted += 1;
          if (emitted >= count) return;
        }
      });
  },

  /** Observes each success value synchronously while preserving the stream. */
  tap<A>(observe: (value: A) => void): <E>(stream: Stream<A, E>) => Stream<A, E> {
    return (stream) =>
      makeStream(async function* (signal) {
        for await (const item of sourceOf(stream, signal)) {
          if (Result.isOk(item)) observe(item.value);
          yield item;
          if (Result.isError(item)) return;
        }
      });
  },

  /** Unordered bounded Task map with typed failure union and owned mapper cleanup. */
  mapConcurrent<A, B, E2>(
    f: (value: A) => Task<B, E2>,
    concurrency: number,
  ): <E>(stream: Stream<A, E>) => Stream<B, E | E2> {
    assertPositiveInteger(concurrency, "Stream.mapConcurrent concurrency");
    return function <E>(stream: Stream<A, E>): Stream<B, E | E2> {
      return makeStream(async function* (signal) {
        const controller = new AbortController();
        const unlink = linkChild(signal, controller);
        const iterator = sourceOf(stream, controller.signal)[Symbol.asyncIterator]();
        type TaskSettled =
          | {
              readonly kind: "task";
              readonly id: number;
              readonly succeeded: true;
              readonly result: Result<B, E2>;
            }
          | {
              readonly kind: "task";
              readonly id: number;
              readonly succeeded: false;
              readonly cause: unknown;
            };
        type PullSettled =
          | {
              readonly kind: "pull";
              readonly succeeded: true;
              readonly step: IteratorResult<Result<A, E>>;
            }
          | { readonly kind: "pull"; readonly succeeded: false; readonly cause: unknown };
        const inFlight = new Map<number, Promise<TaskSettled>>();
        let nextId = 0;
        let sourceDone = false;
        let pull: Promise<PullSettled> | undefined;

        const startPull = (): void => {
          if (pull || sourceDone || inFlight.size >= concurrency) return;
          pull = iterator.next().then(
            (step): PullSettled => ({ kind: "pull", succeeded: true, step }),
            (cause: unknown): PullSettled => ({ kind: "pull", succeeded: false, cause }),
          );
        };

        const launch = (value: A): void => {
          const id = nextId;
          nextId += 1;
          inFlight.set(
            id,
            Promise.resolve()
              .then(() => f(value)(controller.signal))
              .then(
                (result): TaskSettled => ({ kind: "task", id, succeeded: true, result }),
                (cause: unknown): TaskSettled => ({ kind: "task", id, succeeded: false, cause }),
              ),
          );
        };

        try {
          startPull();
          while (pull || inFlight.size > 0) {
            const candidates: Array<Promise<PullSettled | TaskSettled>> = [...inFlight.values()];
            if (pull) candidates.push(pull);
            const settled = await Promise.race(candidates);
            if (settled.kind === "pull") {
              pull = undefined;
              if (!settled.succeeded) throw settled.cause;
              if (settled.step.done) {
                sourceDone = true;
              } else if (Result.isError(settled.step.value)) {
                yield Result.err<B, E | E2>(settled.step.value.error);
                return;
              } else {
                launch(settled.step.value.value);
              }
              startPull();
              continue;
            }

            inFlight.delete(settled.id);
            if (!settled.succeeded) throw settled.cause;
            if (Result.isError(settled.result)) {
              yield Result.err<B, E | E2>(settled.result.error);
              return;
            }
            yield Result.ok<B, E | E2>(settled.result.value);
            startPull();
          }
        } finally {
          if (!controller.signal.aborted) controller.abort(abortError());
          unlink();
          const cleanup: Array<Promise<unknown>> = [...inFlight.values()];
          if (pull) cleanup.push(pull);
          const closing = iterator.return?.();
          if (closing) cleanup.push(closing);
          await Promise.allSettled(cleanup);
        }
      });
    };
  },

  /** Ordered bounded Task map with typed failure union and owned mapper cleanup. */
  buffer<A, B, E2>(
    f: (value: A) => Task<B, E2>,
    concurrency: number,
  ): <E>(stream: Stream<A, E>) => Stream<B, E | E2> {
    assertPositiveInteger(concurrency, "Stream.buffer concurrency");
    return function <E>(stream: Stream<A, E>): Stream<B, E | E2> {
      return makeStream(async function* (signal) {
        const controller = new AbortController();
        const unlink = linkChild(signal, controller);
        const iterator = sourceOf(stream, controller.signal)[Symbol.asyncIterator]();
        type TaskSettled =
          | {
              readonly kind: "task";
              readonly id: number;
              readonly succeeded: true;
              readonly result: Result<B, E2>;
            }
          | {
              readonly kind: "task";
              readonly id: number;
              readonly succeeded: false;
              readonly cause: unknown;
            };
        type PullSettled =
          | {
              readonly kind: "pull";
              readonly succeeded: true;
              readonly step: IteratorResult<Result<A, E>>;
            }
          | { readonly kind: "pull"; readonly succeeded: false; readonly cause: unknown };
        const pending = new Map<number, Promise<TaskSettled>>();
        let nextRead = 0;
        let nextWrite = 0;
        let sourceDone = false;
        let pull: Promise<PullSettled> | undefined;
        const sourceState: { failure: { readonly error: E } | undefined } = {
          failure: undefined,
        };

        const startPull = (): void => {
          if (pull || sourceDone || pending.size >= concurrency) return;
          pull = iterator.next().then(
            (step): PullSettled => ({ kind: "pull", succeeded: true, step }),
            (cause: unknown): PullSettled => ({ kind: "pull", succeeded: false, cause }),
          );
        };

        const launch = (value: A): void => {
          const id = nextWrite;
          nextWrite += 1;
          pending.set(
            id,
            Promise.resolve()
              .then(() => f(value)(controller.signal))
              .then(
                (result): TaskSettled => ({ kind: "task", id, succeeded: true, result }),
                (cause: unknown): TaskSettled => ({ kind: "task", id, succeeded: false, cause }),
              ),
          );
        };

        try {
          startPull();
          while (pull || pending.size > 0) {
            const current = pending.get(nextRead);
            const candidates: Array<Promise<PullSettled | TaskSettled>> = [];
            if (current) candidates.push(current);
            if (pull) candidates.push(pull);
            if (candidates.length === 0) {
              throw new Error("Stream.buffer lost an in-flight result");
            }
            const settled = await Promise.race(candidates);
            if (settled.kind === "pull") {
              pull = undefined;
              if (!settled.succeeded) throw settled.cause;
              if (settled.step.done) {
                sourceDone = true;
              } else if (Result.isError(settled.step.value)) {
                sourceDone = true;
                sourceState.failure = { error: settled.step.value.error };
              } else {
                launch(settled.step.value.value);
              }
              startPull();
              continue;
            }

            pending.delete(nextRead);
            nextRead += 1;
            if (!settled.succeeded) throw settled.cause;
            if (Result.isError(settled.result)) {
              yield Result.err<B, E | E2>(settled.result.error);
              return;
            }
            yield Result.ok<B, E | E2>(settled.result.value);
            startPull();
          }
          if (sourceState.failure) {
            yield Result.err<B, E | E2>(sourceState.failure.error);
          }
        } finally {
          if (!controller.signal.aborted) controller.abort(abortError());
          unlink();
          const cleanup: Array<Promise<unknown>> = [...pending.values()];
          if (pull) cleanup.push(pull);
          const closing = iterator.return?.();
          if (closing) cleanup.push(closing);
          await Promise.allSettled(cleanup);
        }
      });
    };
  },

  /** Groups success values into non-empty fixed-size chunks. */
  chunks(size: number): <A, E>(stream: Stream<A, E>) => Stream<ReadonlyArray<A>, E> {
    assertPositiveInteger(size, "Stream.chunks size");
    return function <A, E>(stream: Stream<A, E>): Stream<ReadonlyArray<A>, E> {
      return makeStream(async function* (signal) {
        let chunk: A[] = [];
        for await (const item of sourceOf(stream, signal)) {
          if (Result.isError(item)) {
            yield Result.err<ReadonlyArray<A>, E>(item.error);
            return;
          }
          chunk.push(item.value);
          if (chunk.length === size) {
            yield Result.ok<ReadonlyArray<A>, E>(chunk);
            chunk = [];
          }
        }
        if (chunk.length > 0) yield Result.ok<ReadonlyArray<A>, E>(chunk);
      });
    };
  },

  /** Merges streams by completion order and unions their expected failures. */
  merge<const Streams extends readonly Stream<unknown, unknown>[]>(
    ...streams: Streams
  ): Stream<StreamValue<Streams[number]>, StreamError<Streams[number]>> {
    type A = StreamValue<Streams[number]>;
    type E = StreamError<Streams[number]>;
    return makeStream(async function* (signal) {
      const controller = new AbortController();
      const unlink = linkChild(signal, controller);
      const iterators = streams.map((stream) =>
        sourceOf(stream as Stream<A, E>, controller.signal)[Symbol.asyncIterator](),
      );
      type Settled =
        | {
            readonly index: number;
            readonly succeeded: true;
            readonly step: IteratorResult<Result<A, E>>;
          }
        | { readonly index: number; readonly succeeded: false; readonly cause: unknown };
      const pending = new Map<number, Promise<Settled>>();
      const pull = (index: number): void => {
        const iterator = iterators[index];
        if (!iterator) return;
        pending.set(
          index,
          iterator.next().then(
            (step): Settled => ({ index, succeeded: true, step }),
            (cause: unknown): Settled => ({ index, succeeded: false, cause }),
          ),
        );
      };

      try {
        for (let index = 0; index < iterators.length; index += 1) pull(index);
        while (pending.size > 0) {
          const settled = await Promise.race(pending.values());
          pending.delete(settled.index);
          if (!settled.succeeded) throw settled.cause;
          if (settled.step.done) continue;
          yield settled.step.value;
          if (Result.isError(settled.step.value)) return;
          pull(settled.index);
        }
      } finally {
        if (!controller.signal.aborted) controller.abort(abortError());
        unlink();
        const cleanup: Array<Promise<unknown>> = [...pending.values()];
        for (const iterator of iterators) {
          const closing = iterator.return?.();
          if (closing) cleanup.push(closing);
        }
        await Promise.allSettled(cleanup);
      }
    });
  },

  /** Zips two streams until either ends and unions their expected failures. */
  zip<A, E, B, E2>(left: Stream<A, E>, right: Stream<B, E2>): Stream<readonly [A, B], E | E2> {
    return makeStream(async function* (signal) {
      const controller = new AbortController();
      const unlink = linkChild(signal, controller);
      const leftIterator = sourceOf(left, controller.signal)[Symbol.asyncIterator]();
      const rightIterator = sourceOf(right, controller.signal)[Symbol.asyncIterator]();
      let pending: Array<Promise<unknown>> = [];
      try {
        while (true) {
          const leftNext = leftIterator.next();
          const rightNext = rightIterator.next();
          pending = [leftNext, rightNext];
          const [leftStep, rightStep] = await Promise.all([leftNext, rightNext]);
          pending = [];
          if (leftStep.done || rightStep.done) return;
          if (Result.isError(leftStep.value)) {
            yield Result.err<readonly [A, B], E | E2>(leftStep.value.error);
            return;
          }
          if (Result.isError(rightStep.value)) {
            yield Result.err<readonly [A, B], E | E2>(rightStep.value.error);
            return;
          }
          yield Result.ok([leftStep.value.value, rightStep.value.value] as const);
        }
      } finally {
        if (!controller.signal.aborted) controller.abort(abortError());
        unlink();
        const closeLeft = leftIterator.return?.();
        const closeRight = rightIterator.return?.();
        if (closeLeft) pending.push(closeLeft);
        if (closeRight) pending.push(closeRight);
        await Promise.allSettled(pending);
      }
    });
  },

  /** Collects all values, returning typed stream failure or cancellation explicitly. */
  async runCollect<A, E>(
    stream: Stream<A, E>,
    options?: CancellableOptions,
  ): Promise<Result<ReadonlyArray<A>, E | Cancelled>> {
    const controller = new AbortController();
    const unlink = linkChild(options?.signal, controller);
    const values: A[] = [];
    try {
      for await (const item of sourceOf(stream, controller.signal)) {
        if (Result.isError(item)) return Result.err<ReadonlyArray<A>, E | Cancelled>(item.error);
        values.push(item.value);
      }
      if (controller.signal.aborted) {
        return Result.err(Cancelled.fromCause(controller.signal.reason));
      }
      return Result.ok(values);
    } catch (cause: unknown) {
      const cancelled = toCancelled(cause, controller.signal);
      if (cancelled) return Result.err(cancelled);
      throw cause;
    } finally {
      unlink();
    }
  },

  /** Runs a Task for each value and unions stream, Task, and cancellation failures. */
  async runForEach<A, E, E2>(
    stream: Stream<A, E>,
    f: (value: A) => Task<void, E2>,
    options?: CancellableOptions,
  ): Promise<Result<void, E | E2 | Cancelled>> {
    const controller = new AbortController();
    const unlink = linkChild(options?.signal, controller);
    try {
      for await (const item of sourceOf(stream, controller.signal)) {
        if (Result.isError(item)) return Result.err<void, E | E2 | Cancelled>(item.error);
        const effect = await f(item.value)(controller.signal);
        if (Result.isError(effect)) return Result.err<void, E | E2 | Cancelled>(effect.error);
      }
      if (controller.signal.aborted) {
        return Result.err(Cancelled.fromCause(controller.signal.reason));
      }
      return Result.ok(undefined);
    } catch (cause: unknown) {
      const cancelled = toCancelled(cause, controller.signal);
      if (cancelled) return Result.err(cancelled);
      throw cause;
    } finally {
      unlink();
    }
  },

  /** Folds values while preserving typed stream failure and cancellation. */
  async runFold<A, E, B>(
    stream: Stream<A, E>,
    seed: B,
    fold: (accumulator: B, value: A) => B | Promise<B>,
    options?: CancellableOptions,
  ): Promise<Result<B, E | Cancelled>> {
    const controller = new AbortController();
    const unlink = linkChild(options?.signal, controller);
    let accumulator = seed;
    try {
      for await (const item of sourceOf(stream, controller.signal)) {
        if (Result.isError(item)) return Result.err<B, E | Cancelled>(item.error);
        accumulator = await fold(accumulator, item.value);
      }
      if (controller.signal.aborted) {
        return Result.err(Cancelled.fromCause(controller.signal.reason));
      }
      return Result.ok(accumulator);
    } catch (cause: unknown) {
      const cancelled = toCancelled(cause, controller.signal);
      if (cancelled) return Result.err(cancelled);
      throw cause;
    } finally {
      unlink();
    }
  },

  /**
   * Converts to raw AsyncIterable. A typed stream failure is deliberately rethrown because the raw
   * protocol has no expected-failure channel.
   */
  toAsyncIterable<A, E>(stream: Stream<A, E>, options?: CancellableOptions): AsyncIterable<A> {
    return {
      async *[Symbol.asyncIterator]() {
        const controller = new AbortController();
        const unlink = linkChild(options?.signal, controller);
        try {
          for await (const item of sourceOf(stream, controller.signal)) {
            if (Result.isError(item)) throw item.error;
            yield item.value;
          }
        } finally {
          if (!controller.signal.aborted) controller.abort(abortError());
          unlink();
        }
      },
    };
  },
} as const;
