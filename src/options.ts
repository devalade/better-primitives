/**
 * Final options object for caller-owned cancellation.
 */
export type CancellableOptions = {
  readonly signal?: AbortSignal;
};
