import { Result } from "better-result";
import { Cancelled, QueueClosedError } from "../errors/index";
import { cancelledIfAborted } from "../internal/abort";
import type { CancellableOptions } from "../options";
import { Queue } from "../queue/index";

export type ChannelResult<A> = Result<A, QueueClosedError | Cancelled>;

export interface Sender<A> {
  readonly isClosed: boolean;
  send(value: A, options?: CancellableOptions): Promise<Result<void, QueueClosedError | Cancelled>>;
  close(): void;
}

export interface Receiver<A> extends AsyncIterable<A> {
  readonly isClosed: boolean;
  recv(options?: CancellableOptions): Promise<ChannelResult<A>>;
  close(): void;
}

class SenderImpl<A> implements Sender<A> {
  constructor(private readonly queue: Queue<A>) {}

  get isClosed(): boolean {
    return this.queue.isClosed;
  }

  async send(
    value: A,
    options?: CancellableOptions,
  ): Promise<Result<void, QueueClosedError | Cancelled>> {
    const result = await this.queue.offer(value, options);
    if (Cancelled.is(result)) return Result.err(result);
    return result ? Result.ok(undefined) : Result.err(QueueClosedError.instance);
  }

  close(): void {
    this.queue.close();
  }
}

class ReceiverImpl<A> implements Receiver<A> {
  constructor(private readonly queue: Queue<A>) {}

  get isClosed(): boolean {
    return this.queue.isClosed;
  }

  recv(options?: CancellableOptions): Promise<ChannelResult<A>> {
    return this.queue.take(options);
  }

  close(): void {
    this.queue.close();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<A> {
    while (true) {
      const result = await this.recv();
      if (Result.isError(result)) return;
      yield result.value;
    }
  }
}

/** Creates a bounded multi-producer, single-consumer channel. */
function mpsc<A>(capacity: number): readonly [Sender<A>, Receiver<A>] {
  const queue = Queue.bounded<A>(capacity);
  return [new SenderImpl(queue), new ReceiverImpl(queue)];
}

export interface OneshotSender<A> {
  readonly isClosed: boolean;
  send(value: A): boolean;
  close(): void;
}

export interface OneshotReceiver<A> {
  readonly isClosed: boolean;
  recv(options?: CancellableOptions): Promise<ChannelResult<A>>;
  close(): void;
}

/** Creates a single-value channel. */
function oneshot<A>(): readonly [OneshotSender<A>, OneshotReceiver<A>] {
  const queue = Queue.bounded<A>(1);
  let received = false;
  const sender: OneshotSender<A> = {
    get isClosed() {
      return queue.isClosed;
    },
    send(value) {
      return queue.tryOffer(value);
    },
    close() {
      queue.close();
    },
  };
  const receiver: OneshotReceiver<A> = {
    get isClosed() {
      return queue.isClosed;
    },
    recv(options) {
      if (received) return Promise.resolve(Result.err(QueueClosedError.instance));
      return queue.take(options).then((result) => {
        if (Result.isOk(result)) {
          received = true;
          queue.close();
        }
        return result;
      });
    },
    close() {
      received = true;
      queue.close();
    },
  };
  return [sender, receiver];
}

export interface BroadcastSender<A> {
  readonly isClosed: boolean;
  /** Returns false when closed or any receiver buffer is full. */
  send(value: A): boolean;
  subscribe(): BroadcastReceiver<A>;
  close(): void;
}

export interface BroadcastReceiver<A> extends AsyncIterable<A> {
  readonly isClosed: boolean;
  recv(options?: CancellableOptions): Promise<ChannelResult<A>>;
  close(): void;
}

class BroadcastReceiverImpl<A> implements BroadcastReceiver<A> {
  constructor(
    readonly queue: Queue<A>,
    private readonly onClose: () => void,
  ) {}

  get isClosed(): boolean {
    return this.queue.isClosed;
  }

  recv(options?: CancellableOptions): Promise<ChannelResult<A>> {
    return this.queue.take(options);
  }

  close(): void {
    if (this.queue.isClosed) return;
    this.queue.close();
    this.onClose();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<A> {
    while (true) {
      const result = await this.recv();
      if (Result.isError(result)) return;
      yield result.value;
    }
  }
}

/** Creates a broadcast channel with a bounded per-receiver buffer. */
function broadcast<A>(capacity: number): readonly [BroadcastSender<A>, BroadcastReceiver<A>] {
  if (!Number.isInteger(capacity) || capacity < 1) {
    throw new RangeError("broadcast capacity must be a positive integer");
  }
  const receivers = new Set<BroadcastReceiverImpl<A>>();
  let closed = false;
  const createReceiver = () => {
    let receiver: BroadcastReceiverImpl<A>;
    receiver = new BroadcastReceiverImpl(Queue.bounded<A>(capacity), () => {
      receivers.delete(receiver);
    });
    receivers.add(receiver);
    return receiver;
  };
  const sender: BroadcastSender<A> = {
    get isClosed() {
      return closed;
    },
    send(value) {
      if (closed) return false;
      for (const receiver of receivers) {
        if (!receiver.isClosed && receiver.queue.size >= receiver.queue.capacity) return false;
      }
      for (const receiver of receivers) {
        if (!receiver.isClosed) receiver.queue.tryOffer(value);
      }
      return true;
    },
    subscribe() {
      return createReceiver();
    },
    close() {
      if (closed) return;
      closed = true;
      for (const receiver of receivers) receiver.close();
    },
  };
  return [sender, createReceiver()];
}

export interface WatchSender<A> {
  send(value: A): void;
  close(): void;
}

export interface WatchReceiver<A> {
  readonly isClosed: boolean;
  borrow(): A;
  changed(options?: CancellableOptions): Promise<Result<A, QueueClosedError | Cancelled>>;
}

/** Creates a latest-value watch channel. */
function watch<A>(initial: A): readonly [WatchSender<A>, WatchReceiver<A>] {
  let value = initial;
  let version = 0;
  let observedVersion = 0;
  let closed = false;
  let flushScheduled = false;
  const waiters = new Set<{
    readonly resolve: (result: Result<A, QueueClosedError | Cancelled>) => void;
    readonly signal: AbortSignal | undefined;
    onAbort: (() => void) | undefined;
  }>();
  const sender: WatchSender<A> = {
    send(next) {
      if (closed) return;
      value = next;
      version += 1;
      if (waiters.size > 0 && !flushScheduled) {
        flushScheduled = true;
        queueMicrotask(() => {
          flushScheduled = false;
          if (closed || waiters.size === 0 || version === observedVersion) return;
          observedVersion = version;
          for (const waiter of waiters) {
            if (waiter.signal && waiter.onAbort)
              waiter.signal.removeEventListener("abort", waiter.onAbort);
            waiter.resolve(Result.ok(value));
          }
          waiters.clear();
        });
      }
    },
    close() {
      if (closed) return;
      closed = true;
      for (const waiter of waiters) {
        if (waiter.signal && waiter.onAbort)
          waiter.signal.removeEventListener("abort", waiter.onAbort);
        waiter.resolve(Result.err(QueueClosedError.instance));
      }
      waiters.clear();
    },
  };
  const receiver: WatchReceiver<A> = {
    get isClosed() {
      return closed;
    },
    borrow() {
      return value;
    },
    changed(options) {
      const early = cancelledIfAborted(options?.signal);
      if (early) return Promise.resolve(Result.err(early));
      if (closed) return Promise.resolve(Result.err(QueueClosedError.instance));
      if (version > observedVersion) {
        observedVersion = version;
        return Promise.resolve(Result.ok(value));
      }
      return new Promise((resolve) => {
        const waiter: {
          readonly resolve: (result: Result<A, QueueClosedError | Cancelled>) => void;
          readonly signal: AbortSignal | undefined;
          onAbort: (() => void) | undefined;
        } = { resolve, signal: options?.signal, onAbort: undefined };
        const onAbort = () => {
          if (waiters.delete(waiter))
            resolve(Result.err(Cancelled.fromCause(options?.signal?.reason)));
        };
        waiter.onAbort = onAbort;
        waiters.add(waiter);
        options?.signal?.addEventListener("abort", onAbort, { once: true });
      });
    },
  };
  return [sender, receiver];
}

export const Channel = { mpsc, oneshot, broadcast, watch } as const;
