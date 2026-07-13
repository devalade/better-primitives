import { Result } from "better-result";
import { QueueClosedError, QueueEmptyError } from "../errors/index";

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
  offer(value: A): Promise<boolean>;
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
  take(): Promise<Result<A, QueueClosedError>>;
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
  resolve: (value: Result<A, QueueClosedError>) => void;
};

type Offerer<A> = {
  value: A;
  resolve: (ok: boolean) => void;
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

  offer(value: A): Promise<boolean> {
    if (this.#closed) return Promise.resolve(false);

    if (this.tryOffer(value)) {
      return Promise.resolve(true);
    }

    if (this.#strategy !== "bounded") {
      return Promise.resolve(false);
    }

    return new Promise<boolean>((resolve) => {
      this.#offerers.push({ value, resolve });
    });
  }

  tryTake(): Result<A, QueueEmptyError> {
    if (this.#buffer.length === 0) return Result.err(QueueEmptyError.instance);
    const value = this.#buffer.shift();
    this.#pumpOfferers();
    // SAFETY: the non-empty length check establishes that shift removed an A, including when A is undefined.
    return Result.ok(value as A);
  }

  take(): Promise<Result<A, QueueClosedError>> {
    const value = this.tryTake();
    if (Result.isOk(value)) return Promise.resolve(value);

    if (this.#closed) {
      return Promise.resolve(Result.err(QueueClosedError.instance));
    }

    return new Promise((resolve) => {
      this.#takers.push({ resolve });
    });
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;

    for (const offerer of this.#offerers.splice(0)) {
      offerer.resolve(false);
    }
    for (const taker of this.#takers.splice(0)) {
      taker.resolve(Result.err(QueueClosedError.instance));
    }
  }

  #pumpOfferers(): void {
    while (this.#offerers.length > 0 && this.#buffer.length < this.#capacity) {
      const offerer = this.#offerers.shift();
      if (!offerer) return;
      this.#buffer.push(offerer.value);
      offerer.resolve(true);
    }
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
