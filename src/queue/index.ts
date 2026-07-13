import { Result } from "better-result";
import { Cancelled, QueueClosedError, QueueEmptyError } from "../errors/index";
import { cancelledIfAborted } from "../internal/abort";
import type { CancellableOptions } from "../options";

/**
 * Backpressure queue (Effect-like). Not an actor mailbox.
 *
 * @template A
 */
export interface Queue<A> extends AsyncIterable<A> {
  readonly capacity: number;
  readonly size: number;
  readonly isClosed: boolean;
  /**
   * Offers a value, waiting when bounded and full.
   *
   * @param value - Value to enqueue.
   * @returns False if the queue is closed.
   */
  offer(value: A, options?: CancellableOptions): Promise<boolean | Cancelled>;
  /**
   * Non-blocking offer.
   *
   * @param value - Value to enqueue.
   */
  tryOffer(value: A): boolean;
  /**
   * Takes the next value.
   *
   * @returns `Err(QueueClosedError)` when closed and empty.
   */
  take(options?: CancellableOptions): Promise<Result<A, QueueClosedError | Cancelled>>;
  /**
   * Non-blocking take.
   *
   * @returns `Err(QueueEmptyError)` when no value is immediately available.
   */
  tryTake(): Result<A, QueueEmptyError>;
  /** Closes the queue; pending offers resolve false; pending takes err. */
  close(): void;
}

type Strategy = "bounded" | "unbounded" | "dropping" | "sliding";

type Taker<A> = {
  resolve: (value: Result<A, QueueClosedError | Cancelled>) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
};

type Offerer<A> = {
  value: A;
  resolve: (ok: boolean | Cancelled) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
};

class QueueImpl<A> implements Queue<A> {
  readonly #capacity: number;
  readonly #strategy: Strategy;
  readonly #buffer: A[] = [];
  readonly #takers: Taker<A>[] = [];
  readonly #offerers: Offerer<A>[] = [];
  #closed = false;

  constructor(capacity: number, strategy: Strategy) {
    this.#capacity = capacity;
    this.#strategy = strategy;
  }

  get capacity(): number {
    return this.#capacity;
  }

  get size(): number {
    return this.#buffer.length;
  }

  get isClosed(): boolean {
    return this.#closed;
  }

  tryOffer(value: A): boolean {
    if (this.#closed) return false;

    const taker = this.#takers.shift();
    if (taker) {
      this.#removeAbortListener(taker);
      taker.resolve(Result.ok(value));
      return true;
    }

    if (this.#strategy === "unbounded" || this.#buffer.length < this.#capacity) {
      this.#buffer.push(value);
      return true;
    }

    if (this.#strategy === "dropping") {
      return false;
    }

    if (this.#strategy === "sliding") {
      this.#buffer.shift();
      this.#buffer.push(value);
      return true;
    }

    return false;
  }

  offer(value: A, options?: CancellableOptions): Promise<boolean | Cancelled> {
    if (this.#closed) return Promise.resolve(false);
    const early = cancelledIfAborted(options?.signal);
    if (early) return Promise.resolve(early);

    if (this.tryOffer(value)) {
      return Promise.resolve(true);
    }

    if (this.#strategy !== "bounded") {
      return Promise.resolve(false);
    }

    return new Promise<boolean | Cancelled>((resolve) => {
      const offerer: Offerer<A> = { value, resolve, signal: options?.signal };
      const onAbort = () => {
        const index = this.#offerers.indexOf(offerer);
        if (index >= 0) {
          this.#offerers.splice(index, 1);
          resolve(Cancelled.fromCause(options?.signal?.reason));
        }
      };
      offerer.onAbort = onAbort;
      this.#offerers.push(offerer);
      options?.signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  tryTake(): Result<A, QueueEmptyError> {
    if (this.#buffer.length === 0) return Result.err(QueueEmptyError.instance);
    const value = this.#buffer.shift();
    this.#pumpOfferers();
    // SAFETY: the non-empty length check establishes that shift removed an A, including when A is undefined.
    return Result.ok(value as A);
  }

  take(options?: CancellableOptions): Promise<Result<A, QueueClosedError | Cancelled>> {
    const early = cancelledIfAborted(options?.signal);
    if (early) return Promise.resolve(Result.err(early));
    const value = this.tryTake();
    if (Result.isOk(value)) return Promise.resolve(value);

    if (this.#closed) {
      return Promise.resolve(Result.err(QueueClosedError.instance));
    }
    return new Promise((resolve) => {
      const taker: Taker<A> = { resolve, signal: options?.signal };
      const onAbort = () => {
        const index = this.#takers.indexOf(taker);
        if (index >= 0) {
          this.#takers.splice(index, 1);
          resolve(Result.err(Cancelled.fromCause(options?.signal?.reason)));
        }
      };
      taker.onAbort = onAbort;
      this.#takers.push(taker);
      options?.signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;

    for (const offerer of this.#offerers.splice(0)) {
      this.#removeAbortListener(offerer);
      offerer.resolve(false);
    }
    for (const taker of this.#takers.splice(0)) {
      this.#removeAbortListener(taker);
      taker.resolve(Result.err(QueueClosedError.instance));
    }
  }

  #pumpOfferers(): void {
    while (this.#offerers.length > 0 && this.#buffer.length < this.#capacity) {
      const offerer = this.#offerers.shift();
      if (!offerer) return;
      this.#removeAbortListener(offerer);
      this.#buffer.push(offerer.value);
      offerer.resolve(true);
    }
  }

  #removeAbortListener(waiter: {
    readonly signal?: AbortSignal;
    readonly onAbort?: () => void;
  }): void {
    if (waiter.signal && waiter.onAbort) waiter.signal.removeEventListener("abort", waiter.onAbort);
  }

  async *[Symbol.asyncIterator](): AsyncIterator<A> {
    while (true) {
      const next = await this.take();
      if (Result.isError(next)) return;
      yield next.value;
    }
  }
}

/**
 * Queue constructors.
 */
export const Queue = {
  /**
   * Bounded queue with backpressure on `offer`.
   *
   * @template A
   * @param capacity - Buffer capacity (>= 1).
   */
  bounded<A>(capacity: number): Queue<A> {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError("Queue.bounded capacity must be a positive integer");
    }
    return new QueueImpl<A>(capacity, "bounded");
  },

  /** Unbounded queue; `offer` never waits. */
  unbounded<A>(): Queue<A> {
    return new QueueImpl<A>(Number.POSITIVE_INFINITY, "unbounded");
  },

  /**
   * Drops the newest value when full (`tryOffer`/`offer` return false).
   *
   * @template A
   * @param capacity - Buffer capacity (>= 1).
   */
  dropping<A>(capacity: number): Queue<A> {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError("Queue.dropping capacity must be a positive integer");
    }
    return new QueueImpl<A>(capacity, "dropping");
  },

  /**
   * Drops the oldest value when full to accept the newest.
   *
   * @template A
   * @param capacity - Buffer capacity (>= 1).
   */
  sliding<A>(capacity: number): Queue<A> {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError("Queue.sliding capacity must be a positive integer");
    }
    return new QueueImpl<A>(capacity, "sliding");
  },
} as const;
