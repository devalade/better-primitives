import { Result } from "better-result";
import { Cancelled } from "../errors/index";
import { cancelledIfAborted } from "../internal/abort";
import type { CancellableOptions } from "../options";

/**
 * Mutex lock guard. Prefer {@link Mutex.withLock}.
 */
export interface MutexGuard extends AsyncDisposable {
  /** Releases the lock. Idempotent. */
  release(): void;
}

/**
 * Async mutex for exclusive critical sections across await points.
 */
export class Mutex {
  #locked = false;
  readonly #waiters: Array<() => void> = [];

  /**
   * Acquires the lock. Release via the guard or `await using`.
   */
  async lock(): Promise<MutexGuard> {
    if (this.#locked) {
      await new Promise<void>((resolve) => {
        this.#waiters.push(resolve);
      });
    }
    this.#locked = true;

    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      const next = this.#waiters.shift();
      if (next) {
        next();
      } else {
        this.#locked = false;
      }
    };

    return {
      release,
      async [Symbol.asyncDispose]() {
        release();
      },
    };
  }

  /**
   * Runs `fn` while holding the lock.
   *
   * @template A
   * @param fn - Critical section.
   */
  async withLock<A>(fn: () => Promise<A> | A): Promise<A> {
    const guard = await this.lock();
    try {
      return await fn();
    } finally {
      guard.release();
    }
  }
}

/**
 * Semaphore permit. Prefer {@link Semaphore.withPermit}.
 */
export interface Permit extends AsyncDisposable {
  /** Releases the permit. Idempotent. */
  release(): void;
}

/**
 * Counting semaphore for limiting concurrency.
 */
export class Semaphore {
  #available: number;
  readonly #waiters: Array<() => void> = [];

  /**
   * @param permits - Initial permit count.
   */
  constructor(permits: number) {
    if (!Number.isInteger(permits) || permits < 0) {
      throw new RangeError("Semaphore permits must be a non-negative integer");
    }
    this.#available = permits;
  }

  /** Currently available permits. */
  get availablePermits(): number {
    return this.#available;
  }

  /** Acquires one permit. */
  async acquire(): Promise<Permit> {
    if (this.#available > 0) {
      this.#available -= 1;
    } else {
      await new Promise<void>((resolve) => {
        this.#waiters.push(resolve);
      });
    }
    return this.#createPermit();
  }

  /** Tries to acquire without waiting. */
  tryAcquire(): Permit | undefined {
    if (this.#available <= 0) return undefined;
    this.#available -= 1;
    return this.#createPermit();
  }

  /**
   * Runs `fn` while holding a permit.
   *
   * @template A
   * @param fn - Critical section.
   */
  async withPermit<A>(fn: () => Promise<A> | A): Promise<A> {
    const permit = await this.acquire();
    try {
      return await fn();
    } finally {
      permit.release();
    }
  }

  #createPermit(): Permit {
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      const next = this.#waiters.shift();
      if (next) {
        next();
      } else {
        this.#available += 1;
      }
    };
    return {
      release,
      async [Symbol.asyncDispose]() {
        release();
      },
    };
  }
}

/**
 * Readers-writer lock: concurrent readers or one writer.
 */
export class RwLock {
  #readers = 0;
  #writer = false;
  readonly #readWaiters: Array<() => void> = [];
  readonly #writeWaiters: Array<() => void> = [];

  async #acquireRead(): Promise<() => void> {
    while (this.#writer || this.#writeWaiters.length > 0) {
      await new Promise<void>((resolve) => {
        this.#readWaiters.push(resolve);
      });
    }
    this.#readers += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.#readers -= 1;
      this.#wake();
    };
  }

  async #acquireWrite(): Promise<() => void> {
    while (this.#writer || this.#readers > 0) {
      await new Promise<void>((resolve) => {
        this.#writeWaiters.push(resolve);
      });
    }
    this.#writer = true;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.#writer = false;
      this.#wake();
    };
  }

  #wake(): void {
    if (this.#writer) return;
    if (this.#writeWaiters.length > 0 && this.#readers === 0) {
      const next = this.#writeWaiters.shift();
      next?.();
      return;
    }
    if (this.#writeWaiters.length === 0) {
      while (this.#readWaiters.length > 0) {
        const next = this.#readWaiters.shift();
        next?.();
      }
    }
  }

  /**
   * Runs `fn` with a shared read lock.
   *
   * @template A
   * @param fn - Reader section.
   */
  async withRead<A>(fn: () => Promise<A> | A): Promise<A> {
    const release = await this.#acquireRead();
    try {
      return await fn();
    } finally {
      release();
    }
  }

  /**
   * Runs `fn` with an exclusive write lock.
   *
   * @template A
   * @param fn - Writer section.
   */
  async withWrite<A>(fn: () => Promise<A> | A): Promise<A> {
    const release = await this.#acquireWrite();
    try {
      return await fn();
    } finally {
      release();
    }
  }
}

/**
 * Single-assignment async value.
 *
 * Prefer {@link Deferred.await}; this type is intentionally not thenable.
 *
 * @template A
 * @template E
 */
export class Deferred<A, E = never> {
  #settle: ((value: Result<A, E>) => void) | undefined;
  readonly #promise: Promise<Result<A, E>>;
  #done = false;

  private constructor() {
    this.#promise = new Promise<Result<A, E>>((resolve) => {
      this.#settle = resolve;
    });
  }

  /** Creates an empty deferred. */
  static make<A, E = never>(): Deferred<A, E> {
    return new Deferred<A, E>();
  }

  /** Whether the deferred has been completed. */
  get isDone(): boolean {
    return this.#done;
  }

  /**
   * Completes successfully.
   *
   * @param value - Success value.
   * @returns False if already completed.
   */
  succeed(value: A): boolean {
    if (this.#done) return false;
    this.#done = true;
    this.#settle?.(Result.ok(value));
    return true;
  }

  /**
   * Completes with an expected failure.
   *
   * @param error - Failure value.
   * @returns False if already completed.
   */
  fail(error: E): boolean {
    if (this.#done) return false;
    this.#done = true;
    this.#settle?.(Result.err(error));
    return true;
  }

  /** Awaits completion as a `Result`. */
  await(): Promise<Result<A, E>> {
    return this.#promise;
  }
}

/**
 * Countdown latch: opens after `count` calls to {@link Latch.open}.
 */
export class Latch {
  #remaining: number;
  readonly #waiters: Array<() => void> = [];
  #isOpen = false;

  private constructor(count: number) {
    this.#remaining = count;
    if (count <= 0) this.#isOpen = true;
  }

  /**
   * @param count - Number of `open` calls required (default 1).
   */
  static make(count = 1): Latch {
    if (!Number.isInteger(count) || count < 0) {
      throw new RangeError("Latch count must be a non-negative integer");
    }
    return new Latch(count);
  }

  /** Decrements the remaining count; opens at zero. */
  open(): void {
    if (this.#isOpen) return;
    this.#remaining = Math.max(0, this.#remaining - 1);
    if (this.#remaining === 0) {
      this.#isOpen = true;
      while (this.#waiters.length > 0) {
        const next = this.#waiters.shift();
        next?.();
      }
    }
  }

  /** Resolves when the latch is open. */
  await(): Promise<void> {
    if (this.#isOpen) return Promise.resolve();
    return new Promise<void>((resolve) => {
      this.#waiters.push(resolve);
    });
  }
}

/**
 * Simple condition variable: wait / notify one / notify all.
 */
export class Notify {
  readonly #waiters: Array<() => void> = [];

  /**
   * Waits until notified or cancelled.
   *
   * @param options - Caller cancellation.
   */
  wait(options?: CancellableOptions): Promise<Result<void, Cancelled>> {
    const signal = options?.signal;
    const early = cancelledIfAborted(signal);
    if (early) return Promise.resolve(Result.err(early));

    return new Promise((resolve) => {
      const wake = () => {
        cleanup();
        resolve(Result.ok(undefined));
      };

      const onAbort = () => {
        cleanup();
        resolve(Result.err(Cancelled.fromCause(signal?.reason)));
      };

      const cleanup = () => {
        const idx = this.#waiters.indexOf(wake);
        if (idx >= 0) this.#waiters.splice(idx, 1);
        signal?.removeEventListener("abort", onAbort);
      };

      this.#waiters.push(wake);
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  /** Wakes one waiter. */
  notify(): void {
    const wake = this.#waiters.shift();
    wake?.();
  }

  /** Wakes all waiters. */
  notifyAll(): void {
    const waiters = this.#waiters.splice(0);
    for (const wake of waiters) wake();
  }
}

/**
 * Async lazy cell: compute once, share the result.
 *
 * @template A
 */
export class Once<A> {
  #value: A | undefined;
  #hasValue = false;
  #inflight: Promise<A> | undefined;

  /**
   * Returns the cached value or runs `fn` once.
   *
   * @param fn - Initializer.
   */
  async getOrSet(fn: () => Promise<A>): Promise<A> {
    if (this.#hasValue) {
      // SAFETY: #hasValue guarantees #value was assigned.
      return this.#value as A;
    }
    if (this.#inflight) return this.#inflight;

    this.#inflight = (async () => {
      const value = await fn();
      this.#value = value;
      this.#hasValue = true;
      this.#inflight = undefined;
      return value;
    })();

    try {
      return await this.#inflight;
    } catch (cause: unknown) {
      this.#inflight = undefined;
      throw cause;
    }
  }

  /** Returns the value if already initialized. */
  get(): A | undefined {
    return this.#hasValue ? this.#value : undefined;
  }
}
