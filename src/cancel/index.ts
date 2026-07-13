import { Result } from "better-result";
import { Cancelled, abortError } from "../errors/index";
import { anySignal, cancelledIfAborted, linkChild } from "../internal/abort";
import type { CancellableOptions } from "../options";

/**
 * Tree-structured cancellation token over `AbortSignal`.
 */
export interface Cancel extends Disposable {
  readonly signal: AbortSignal;
  readonly isCancelled: boolean;
  /**
   * Cancels this token and descendants.
   *
   * @param reason - Optional abort reason.
   */
  cancel(reason?: unknown): void;
  /** Creates a child token cancelled with this token. */
  child(): Cancel;
  /**
   * Resolves when this token is cancelled.
   *
   * @returns Always `Ok` after cancellation is observed.
   */
  cancelled(): Promise<Result<void, never>>;
}

class CancelImpl implements Cancel {
  readonly #controller: AbortController;
  readonly #unlink: () => void;

  constructor(parent?: AbortSignal) {
    this.#controller = new AbortController();
    this.#unlink = linkChild(parent, this.#controller);
  }

  get signal(): AbortSignal {
    return this.#controller.signal;
  }

  get isCancelled(): boolean {
    return this.#controller.signal.aborted;
  }

  cancel(reason?: unknown): void {
    this.#unlink();
    if (!this.#controller.signal.aborted) {
      this.#controller.abort(reason === undefined ? abortError() : reason);
    }
  }

  child(): Cancel {
    return new CancelImpl(this.signal);
  }

  cancelled(): Promise<Result<void, never>> {
    const signal = this.signal;
    if (signal.aborted) return Promise.resolve(Result.ok(undefined));
    return new Promise((resolve) => {
      signal.addEventListener("abort", () => resolve(Result.ok(undefined)), { once: true });
    });
  }

  [Symbol.dispose](): void {
    this.cancel();
  }
}

class FromSignalCancel implements Cancel {
  readonly #signal: AbortSignal;

  constructor(signal: AbortSignal) {
    this.#signal = signal;
  }

  get signal(): AbortSignal {
    return this.#signal;
  }

  get isCancelled(): boolean {
    return this.#signal.aborted;
  }

  cancel(_reason?: unknown): void {
    // External signals cannot be aborted from here.
  }

  child(): Cancel {
    return new CancelImpl(this.#signal);
  }

  cancelled(): Promise<Result<void, never>> {
    const signal = this.#signal;
    if (signal.aborted) return Promise.resolve(Result.ok(undefined));
    return new Promise((resolve) => {
      signal.addEventListener("abort", () => resolve(Result.ok(undefined)), { once: true });
    });
  }

  [Symbol.dispose](): void {
    // External signal — nothing to dispose.
  }
}

/**
 * Cancellation helpers built on `AbortSignal`.
 */
export const Cancel = {
  /**
   * Creates a new cancellation token.
   *
   * @param options - Optional parent signal linkage.
   */
  make(options?: CancellableOptions): Cancel {
    return new CancelImpl(options?.signal);
  },

  /**
   * Wraps an existing signal as a `Cancel` view.
   *
   * @param signal - Existing abort signal.
   */
  from(signal: AbortSignal): Cancel {
    return new FromSignalCancel(signal);
  },

  /**
   * Creates a token that cancels after `ms`, optionally linked to a parent.
   *
   * @param ms - Timeout in milliseconds.
   * @param options - Optional parent signal.
   */
  timeout(ms: number, options?: CancellableOptions): Cancel {
    const cancel = new CancelImpl(options?.signal);
    if (ms <= 0) {
      cancel.cancel(abortError("Timeout"));
      return cancel;
    }
    const id = setTimeout(() => {
      cancel.cancel(abortError("Timeout"));
    }, ms);
    cancel.signal.addEventListener("abort", () => clearTimeout(id), { once: true });
    return cancel;
  },

  /**
   * Returns a signal aborted when any input signal aborts.
   *
   * @param signals - Signals to merge.
   */
  any(...signals: AbortSignal[]): AbortSignal {
    return anySignal(...signals);
  },

  /**
   * Returns `Err(Cancelled)` when already aborted, otherwise `Ok`.
   *
   * @param options - Optional signal to inspect.
   */
  check(options?: CancellableOptions): Result<void, Cancelled> {
    const cancelled = cancelledIfAborted(options?.signal);
    return cancelled ? Result.err(cancelled) : Result.ok(undefined);
  },
} as const;
