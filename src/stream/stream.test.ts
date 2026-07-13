import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";
import { Cancelled } from "../errors/index";
import { Task } from "../task/index";
import { Time } from "../time/index";
import { Stream, pipe } from "./index";

function waitForAbort(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    signal.addEventListener("abort", () => reject(signal.reason), { once: true });
  });
}

describe("Stream.mapConcurrent", () => {
  it("rejects invalid concurrency", () => {
    expect(() => Stream.mapConcurrent(Task.succeed, 1.5)).toThrow(RangeError);
  });

  it("does not drop immediately resolved mapper results", async () => {
    const result = await pipe(
      Stream.from([1, 2, 3]),
      Stream.mapConcurrent((value) => Task.succeed(value * 2), 2),
      Stream.runCollect,
    );
    expect(Result.isOk(result)).toBe(true);
    if (Result.isOk(result)) expect([...result.value].sort()).toEqual([2, 4, 6]);
  });

  it("emits delayed results in completion order", async () => {
    const result = await pipe(
      Stream.from([1, 2, 3]),
      Stream.mapConcurrent((value) => Task.map(Time.sleep((4 - value) * 10), () => value), 3),
      Stream.runCollect,
    );
    expect(result).toEqual(Result.ok([3, 2, 1]));
  });

  it("stays bounded on an infinite immediately resolving source", async () => {
    let sourceClosed = false;
    async function* source(): AsyncGenerator<number> {
      try {
        let value = 0;
        while (true) {
          yield value;
          value += 1;
          if (value % 20 === 0) await new Promise<void>((resolve) => setTimeout(resolve, 0));
        }
      } finally {
        sourceClosed = true;
      }
    }

    const result = await pipe(
      Stream.from(source()),
      Stream.mapConcurrent(Task.succeed, 2),
      Stream.take(3),
      Stream.runCollect,
    );
    expect(result).toEqual(Result.ok([0, 1, 2]));
    expect(sourceClosed).toBe(true);
  });

  it("emits a completed mapper without waiting for a blocked prefetch", async () => {
    const source = Stream.unfold(0, (state) =>
      state === 0
        ? Task.succeed([1, 1] as const)
        : Task.map(Time.sleep(500), () => [2, 2] as const),
    );
    const startedAt = Date.now();
    const result = await pipe(
      source,
      Stream.mapConcurrent(Task.succeed, 2),
      Stream.take(1),
      Stream.runCollect,
    );
    expect(result).toEqual(Result.ok([1]));
    expect(Date.now() - startedAt).toBeLessThan(200);
  });

  it("closes source and aborts mapper work when a raw consumer returns", async () => {
    let sourceClosed = false;
    let siblingAborted = false;
    async function* source(): AsyncGenerator<number> {
      try {
        yield 1;
        yield 2;
      } finally {
        sourceClosed = true;
      }
    }
    const mapped = pipe(
      Stream.from(source()),
      Stream.mapConcurrent(
        (value) =>
          value === 1
            ? Task.map(Time.sleep(5), () => value)
            : Task.from(async (signal) => {
                try {
                  return await waitForAbort(signal);
                } finally {
                  siblingAborted = signal.aborted;
                }
              }),
        2,
      ),
    );
    const iterator = Stream.toAsyncIterable(mapped)[Symbol.asyncIterator]();
    expect(await iterator.next()).toEqual({ done: false, value: 1 });
    await iterator.return?.();
    expect(sourceClosed).toBe(true);
    expect(siblingAborted).toBe(true);
  });

  it("propagates a typed mapper failure and aborts siblings", async () => {
    const failure = { _tag: "MapError" as const };
    let siblingAborted = false;
    const result = await pipe(
      Stream.from([1, 2]),
      Stream.mapConcurrent(
        (value) =>
          value === 1
            ? Task.flatMap(Time.sleep(5), () => Task.fail(failure))
            : Task.from(async (signal) => {
                try {
                  return await waitForAbort(signal);
                } finally {
                  siblingAborted = signal.aborted;
                }
              }),
        2,
      ),
      Stream.runCollect,
    );
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(result.error).toBe(failure);
    expect(siblingAborted).toBe(true);
  });

  it("keeps mapper defects outside the typed failure channel", async () => {
    const defect = new Error("mapper defect");
    const stream = pipe(
      Stream.from([1]),
      Stream.mapConcurrent(() => Task.from<never>(async () => Promise.reject(defect)), 1),
    );
    await expect(Stream.runCollect(stream)).rejects.toBe(defect);
  });

  it("returns cancellation and closes the source when the caller aborts", async () => {
    const controller = new AbortController();
    let sourceClosed = false;
    let started = 0;
    let notifyStarted: (() => void) | undefined;
    const bothStarted = new Promise<void>((resolve) => {
      notifyStarted = resolve;
    });
    async function* source(): AsyncGenerator<number> {
      try {
        yield 1;
        yield 2;
      } finally {
        sourceClosed = true;
      }
    }
    const pending = pipe(
      Stream.from(source()),
      Stream.mapConcurrent(
        () =>
          Task.from(async (signal) => {
            started += 1;
            if (started === 2) notifyStarted?.();
            return waitForAbort(signal);
          }),
        2,
      ),
      (stream) => Stream.runCollect(stream, { signal: controller.signal }),
    );
    await bothStarted;
    controller.abort("stop");
    const result = await pending;
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(Cancelled.is(result.error)).toBe(true);
    expect(sourceClosed).toBe(true);
  });
});

describe("Stream.buffer", () => {
  it("rejects invalid concurrency", () => {
    expect(() => Stream.buffer(Task.succeed, Number.NaN)).toThrow(RangeError);
  });

  it("preserves order and undefined values", async () => {
    const ordered = await pipe(
      Stream.from([1, 2, 3]),
      Stream.buffer((value) => Task.map(Time.sleep((4 - value) * 5), () => value * 10), 3),
      Stream.runCollect,
    );
    expect(ordered).toEqual(Result.ok([10, 20, 30]));
    const undefinedValues = await pipe(
      Stream.from([1, 2]),
      Stream.buffer(() => Task.succeed(undefined), 2),
      Stream.runCollect,
    );
    expect(undefinedValues).toEqual(Result.ok([undefined, undefined]));
  });

  it("emits the next ordered result without waiting for a blocked prefetch", async () => {
    const source = Stream.unfold(0, (state) =>
      state === 0
        ? Task.succeed([1, 1] as const)
        : Task.map(Time.sleep(500), () => [2, 2] as const),
    );
    const startedAt = Date.now();
    const result = await pipe(
      source,
      Stream.buffer(Task.succeed, 2),
      Stream.take(1),
      Stream.runCollect,
    );
    expect(result).toEqual(Result.ok([1]));
    expect(Date.now() - startedAt).toBeLessThan(200);
  });

  it("propagates typed mapper failure and aborts siblings", async () => {
    const failure = { _tag: "BufferError" as const };
    let siblingAborted = false;
    const result = await pipe(
      Stream.from([1, 2]),
      Stream.buffer(
        (value) =>
          value === 1
            ? Task.flatMap(Time.sleep(5), () => Task.fail(failure))
            : Task.from(async (signal) => {
                try {
                  return await waitForAbort(signal);
                } finally {
                  siblingAborted = signal.aborted;
                }
              }),
        2,
      ),
      Stream.runCollect,
    );
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(result.error).toBe(failure);
    expect(siblingAborted).toBe(true);
  });
});

describe("Stream composition and interop", () => {
  it("keeps synchronous collection behind an asynchronous execution boundary", async () => {
    let mapped = false;
    const collecting = pipe(
      Stream.from([1]),
      Stream.map((value) => {
        mapped = true;
        return value;
      }),
      Stream.runCollect,
    );

    expect(mapped).toBe(false);
    expect(await collecting).toEqual(Result.ok([1]));
    expect(mapped).toBe(true);
  });

  it("cooperatively yields large synchronous collections for queued cancellation", async () => {
    const controller = new AbortController();
    let cancellationQueued = false;
    const result = await pipe(
      Stream.from(Array.from({ length: 5_000 }, (_, index) => index)),
      Stream.tap(() => {
        if (cancellationQueued) return;
        cancellationQueued = true;
        queueMicrotask(() => controller.abort("stop"));
      }),
      (stream) => Stream.runCollect(stream, { signal: controller.signal }),
    );

    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(Cancelled.is(result.error)).toBe(true);
  });

  it("composes synchronous pure operators without changing values or observation order", async () => {
    const observed: number[] = [];
    const result = await pipe(
      Stream.from([1, 2, 3, 4, 5, 6]),
      Stream.tap((value) => observed.push(value)),
      Stream.filter((value) => value % 2 === 0),
      Stream.take(2),
      Stream.chunks(2),
      Stream.runCollect,
    );

    expect(result).toEqual(Result.ok([[2, 4]]));
    expect(observed).toEqual([1, 2, 3, 4]);
  });

  it("runs pure maps in order and keeps thrown defects outside the typed channel", async () => {
    const calls: string[] = [];
    const mapped = pipe(
      Stream.from([1, 2]),
      Stream.map((value) => {
        calls.push(`first:${value}`);
        return value + 1;
      }),
      Stream.map((value) => {
        calls.push(`second:${value}`);
        return value * 2;
      }),
    );

    expect(await Stream.runCollect(mapped)).toEqual(Result.ok([4, 6]));
    expect(calls).toEqual(["first:1", "second:2", "first:2", "second:3"]);

    const defect = new Error("pure map defect");
    const failed = pipe(
      Stream.from([1]),
      Stream.map(() => {
        throw defect;
      }),
    );
    await expect(Stream.runCollect(failed)).rejects.toBe(defect);
  });

  it("checks cancellation between synchronous pulls and closes the iterator", async () => {
    const controller = new AbortController();
    let sourceClosed = false;
    function* source(): Generator<number> {
      try {
        yield 1;
        yield 2;
      } finally {
        sourceClosed = true;
      }
    }

    const result = await pipe(
      Stream.from(source()),
      Stream.map((value) => {
        controller.abort("stop");
        return value;
      }),
      (stream) => Stream.runCollect(stream, { signal: controller.signal }),
    );

    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(Cancelled.is(result.error)).toBe(true);
    expect(sourceClosed).toBe(true);
  });

  it("cancels and closes a raw async source blocked on its next pull", async () => {
    const controller = new AbortController();
    let notifyPullStarted: (() => void) | undefined;
    const pullStarted = new Promise<void>((resolve) => {
      notifyPullStarted = resolve;
    });
    let returned = false;
    const source: AsyncIterable<number> = {
      [Symbol.asyncIterator]() {
        return {
          next() {
            notifyPullStarted?.();
            return new Promise<IteratorResult<number>>(() => {});
          },
          async return() {
            returned = true;
            return { done: true, value: undefined };
          },
        };
      },
    };

    const collecting = Stream.runCollect(Stream.from(source), { signal: controller.signal });
    await pullStarted;
    controller.abort("stop");
    const result = await Promise.race([
      collecting,
      new Promise<"timed-out">((resolve) => setTimeout(() => resolve("timed-out"), 50)),
    ]);

    expect(result).not.toBe("timed-out");
    if (result === "timed-out") return;
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(Cancelled.is(result.error)).toBe(true);
    expect(returned).toBe(true);
  });

  it("propagates typed source failures through pure operators", async () => {
    const failure = { _tag: "SourceError" as const };
    const result = await pipe(
      Stream.fail(failure),
      Stream.map((value: never) => value),
      Stream.runCollect,
    );
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(result.error).toBe(failure);
  });

  it("throws typed failure only at the explicit AsyncIterable boundary", async () => {
    const failure = { _tag: "InteropError" as const };
    const iterator = Stream.toAsyncIterable(Stream.fail(failure))[Symbol.asyncIterator]();
    await expect(iterator.next()).rejects.toBe(failure);
  });

  it("merge closes every source when the consumer returns early", async () => {
    const closed = new Set<string>();
    async function* source(name: string, value: number): AsyncGenerator<number> {
      try {
        yield value;
      } finally {
        closed.add(name);
      }
    }
    const merged = Stream.merge(Stream.from(source("left", 1)), Stream.from(source("right", 2)));
    const iterator = Stream.toAsyncIterable(merged)[Symbol.asyncIterator]();
    expect((await iterator.next()).done).toBe(false);
    await iterator.return?.();
    expect(closed).toEqual(new Set(["left", "right"]));
  });

  it("merge closes siblings when a raw source defects", async () => {
    const defect = new Error("source defect");
    let siblingClosed = false;
    const failed: AsyncIterable<number> = {
      [Symbol.asyncIterator]() {
        return {
          async next() {
            throw defect;
          },
        };
      },
    };
    async function* sibling(): AsyncGenerator<number> {
      try {
        yield 1;
      } finally {
        siblingClosed = true;
      }
    }
    await expect(
      Stream.runCollect(Stream.merge(Stream.from(failed), Stream.from(sibling()))),
    ).rejects.toBe(defect);
    expect(siblingClosed).toBe(true);
  });

  it("zip closes both sources when the consumer returns early", async () => {
    const closed = new Set<string>();
    async function* source(name: string, value: number): AsyncGenerator<number> {
      try {
        yield value;
      } finally {
        closed.add(name);
      }
    }
    const zipped = Stream.zip(Stream.from(source("left", 1)), Stream.from(source("right", 2)));
    const iterator = Stream.toAsyncIterable(zipped)[Symbol.asyncIterator]();
    expect(await iterator.next()).toEqual({ done: false, value: [1, 2] });
    await iterator.return?.();
    expect(closed).toEqual(new Set(["left", "right"]));
  });
});
