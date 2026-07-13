import { TaggedError } from "better-result";

/**
 * Expected failure when an operation observes caller cancellation.
 *
 * @remarks Prefer classifying abort causes with {@link isAbortCause} before wrapping unknown failures.
 */
export class Cancelled extends TaggedError("Cancelled")<{
  readonly message: string;
  readonly cause: unknown;
}>() {
  /**
   * @param cause - Abort reason or unknown rejection cause.
   */
  static fromCause(cause: unknown): Cancelled {
    return new Cancelled({
      message: "Operation was cancelled",
      cause,
    });
  }
}

/**
 * Expected failure when a timed operation exceeds its deadline.
 */
export class TimeoutError extends TaggedError("TimeoutError")<{
  readonly message: string;
  readonly ms: number;
}>() {
  /**
   * @param ms - Timeout duration in milliseconds.
   */
  static after(ms: number): TimeoutError {
    return new TimeoutError({
      message: `Operation timed out after ${ms}ms`,
      ms,
    });
  }
}

/**
 * Expected failure when taking from a closed queue with no buffered values.
 */
export class QueueClosedError extends TaggedError("QueueClosedError")<{
  readonly message: string;
}>() {
  static readonly instance = new QueueClosedError({
    message: "Queue is closed",
  });
}

/**
 * Expected failure when a non-blocking queue take observes an empty queue.
 */
export class QueueEmptyError extends TaggedError("QueueEmptyError")<{
  readonly message: string;
}>() {
  static readonly instance = new QueueEmptyError({
    message: "Queue is empty",
  });
}

/**
 * Returns true when `cause` represents AbortSignal cancellation.
 *
 * @param cause - Unknown catch/rejection value.
 */
export function isAbortCause(cause: unknown): boolean {
  if (cause == null) return false;
  if (typeof DOMException !== "undefined" && cause instanceof DOMException) {
    return cause.name === "AbortError";
  }
  if (cause instanceof Error) {
    return cause.name === "AbortError";
  }
  return false;
}

/**
 * Builds a DOM-compatible abort error for AbortController.abort.
 *
 * @param reason - Optional abort reason.
 */
export function abortError(reason?: unknown): Error {
  if (reason instanceof Error) return reason;
  const message =
    reason === undefined || reason === null ? "This operation was aborted" : String(reason);
  if (typeof DOMException !== "undefined") {
    return new DOMException(message, "AbortError");
  }
  const err = new Error(message);
  err.name = "AbortError";
  return err;
}
