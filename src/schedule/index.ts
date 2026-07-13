/**
 * Pure retry policy. `attempt` starts at zero for the first retry opportunity.
 * Returning `undefined` stops the retry loop; otherwise the returned delay is in milliseconds.
 */
export type Schedule<E = unknown> = {
  readonly next: (attempt: number, error: E) => number | undefined;
};

type RetryOptions = {
  readonly maxRetries?: number;
};

function retries(options?: RetryOptions): number {
  const maxRetries = options?.maxRetries ?? Number.POSITIVE_INFINITY;
  if (
    maxRetries !== Number.POSITIVE_INFINITY &&
    (!Number.isInteger(maxRetries) || maxRetries < 0)
  ) {
    throw new RangeError("Schedule maxRetries must be a non-negative integer");
  }
  return maxRetries;
}

function assertDelay(delay: number): void {
  if (!Number.isFinite(delay) || delay < 0) {
    throw new RangeError("Schedule delay must be a finite non-negative number");
  }
}

/** Retry schedules and pure schedule combinators. */
export const Schedule = {
  /** Retries immediately until the optional retry limit is reached. */
  immediate<E = unknown>(options?: RetryOptions): Schedule<E> {
    const maxRetries = retries(options);
    return {
      next: (attempt) => (attempt < maxRetries ? 0 : undefined),
    };
  },

  /** Retries after a fixed delay until the optional retry limit is reached. */
  fixed<E = unknown>(delay: number, options?: RetryOptions): Schedule<E> {
    assertDelay(delay);
    const maxRetries = retries(options);
    return {
      next: (attempt) => (attempt < maxRetries ? delay : undefined),
    };
  },

  /** Retries with exponential backoff, capped optionally, until the retry limit is reached. */
  exponential<E = unknown>(
    initialDelay: number,
    options?: RetryOptions & { readonly factor?: number; readonly maxDelay?: number },
  ): Schedule<E> {
    assertDelay(initialDelay);
    const factor = options?.factor ?? 2;
    if (!Number.isFinite(factor) || factor < 1) {
      throw new RangeError("Schedule exponential factor must be finite and at least 1");
    }
    const maxDelay = options?.maxDelay ?? Number.POSITIVE_INFINITY;
    if (maxDelay !== Number.POSITIVE_INFINITY) assertDelay(maxDelay);
    const maxRetries = retries(options);
    return {
      next: (attempt) => {
        if (attempt >= maxRetries) return undefined;
        const calculated = initialDelay * factor ** attempt;
        return Math.min(
          maxDelay,
          Number.isFinite(calculated) ? calculated : Number.MAX_SAFE_INTEGER,
        );
      },
    };
  },

  /** Limits another schedule to at most `maxRetries` retry opportunities. */
  limit<E>(schedule: Schedule<E>, maxRetries: number): Schedule<E> {
    const limit = retries({ maxRetries });
    return {
      next: (attempt, error) => (attempt < limit ? schedule.next(attempt, error) : undefined),
    };
  },

  /** Adds multiplicative jitter around a schedule delay. Provide the random source explicitly. */
  jitter<E>(schedule: Schedule<E>, amount: number, random: () => number): Schedule<E> {
    if (!Number.isFinite(amount) || amount < 0 || amount > 1) {
      throw new RangeError("Schedule jitter amount must be between 0 and 1");
    }
    return {
      next: (attempt, error) => {
        const delay = schedule.next(attempt, error);
        if (delay === undefined || amount === 0) return delay;
        const sample = random();
        if (!Number.isFinite(sample) || sample < 0 || sample > 1) {
          throw new RangeError("Schedule random source must return a number between 0 and 1");
        }
        return delay * (1 - amount + sample * 2 * amount);
      },
    };
  },
} as const;
