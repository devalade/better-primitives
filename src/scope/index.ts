import { abortError } from "../errors/index";
import { linkChild } from "../internal/abort";
import type { CancellableOptions } from "../options";

/**
 * Structured lifetime: owns an abort signal and LIFO finalizers.
 */
export interface Scope extends AsyncDisposable {
  readonly signal: AbortSignal;
  readonly isClosed: boolean;
  /**
   * Registers cleanup run in LIFO order on close.
   *
   * @param fn - Finalizer.
   */
  addFinalizer(fn: () => void | Promise<void>): void;
  /** Nested scope closed when this scope closes. */
  fork(): Scope;
  /**
   * Aborts the scope signal and runs finalizers. Idempotent.
   *
   * @param reason - Optional abort reason.
   */
  close(reason?: unknown): Promise<void>;
}

class ScopeImpl implements Scope {
  readonly #controller: AbortController;
  readonly #unlink: () => void;
  readonly #finalizers: Array<() => void | Promise<void>> = [];
  #closed = false;
  #closing: Promise<void> | undefined;

  constructor(parent?: AbortSignal) {
    this.#controller = new AbortController();
    this.#unlink = linkChild(parent, this.#controller);
  }

  get signal(): AbortSignal {
    return this.#controller.signal;
  }

  get isClosed(): boolean {
    return this.#closed;
  }

  addFinalizer(fn: () => void | Promise<void>): void {
    if (this.#closed) {
      void Promise.resolve()
        .then(fn)
        .catch(() => {
          // Finalizer errors after close are swallowed; prefer registering before close.
        });
      return;
    }
    this.#finalizers.push(fn);
  }

  fork(): Scope {
    const child = new ScopeImpl(this.signal);
    this.addFinalizer(() => child.close());
    return child;
  }

  close(reason?: unknown): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    this.#closing = this.#runClose(reason);
    return this.#closing;
  }

  async #runClose(reason?: unknown): Promise<void> {
    this.#unlink();
    if (!this.#controller.signal.aborted) {
      this.#controller.abort(reason === undefined ? abortError() : reason);
    }

    const errors: unknown[] = [];
    for (let i = this.#finalizers.length - 1; i >= 0; i -= 1) {
      const finalizer = this.#finalizers[i];
      if (!finalizer) continue;
      try {
        await finalizer();
      } catch (cause: unknown) {
        errors.push(cause);
      }
    }
    this.#finalizers.length = 0;

    if (errors.length === 1) {
      const only = errors[0];
      throw only;
    }
    if (errors.length > 1) {
      throw new AggregateError(errors, "Multiple finalizers failed");
    }
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}

/**
 * Structured concurrency scopes.
 */
export const Scope = {
  /**
   * Creates a scope optionally linked to a parent signal.
   *
   * @param options - Optional parent cancellation.
   */
  make(options?: CancellableOptions): Scope {
    return new ScopeImpl(options?.signal);
  },

  /**
   * Runs `fn` with a scope and always closes it afterward.
   *
   * @template A
   * @param fn - Work that receives the scope.
   */
  async use<A>(fn: (scope: Scope) => Promise<A>): Promise<A> {
    const scope = new ScopeImpl();
    try {
      return await fn(scope);
    } finally {
      await scope.close();
    }
  },
} as const;
