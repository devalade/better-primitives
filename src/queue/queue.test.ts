import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";
import { QueueClosedError, QueueEmptyError } from "../errors/index";
import { Stream, pipe } from "../stream/index";
import { Task } from "../task/index";
import { Time } from "../time/index";
import { Queue } from "./index";

describe("Queue", () => {
  it("rejects non-integer capacities", () => {
    expect(() => Queue.bounded(1.5)).toThrow(RangeError);
    expect(() => Queue.dropping(Number.NaN)).toThrow(RangeError);
    expect(() => Queue.sliding(Number.POSITIVE_INFINITY)).toThrow(RangeError);
  });

  it("bounded offer/take", async () => {
    const q = Queue.bounded<number>(2);
    expect(await q.offer(1)).toBe(true);
    expect(await q.offer(2)).toBe(true);
    expect(await q.take()).toEqual(Result.ok(1));
    expect(await q.take()).toEqual(Result.ok(2));
  });

  it("bounded backpressure", async () => {
    const q = Queue.bounded<number>(1);
    await q.offer(1);
    let resolved = false;
    const p = q.offer(2).then((ok) => {
      resolved = true;
      return ok;
    });
    await Task.execute(Time.sleep(15));
    expect(resolved).toBe(false);
    expect(await q.take()).toEqual(Result.ok(1));
    expect(await p).toBe(true);
  });

  it("close errs take", async () => {
    const q = Queue.bounded<number>(1);
    const p = q.take();
    q.close();
    const result = await p;
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) {
      expect(QueueClosedError.is(result.error)).toBe(true);
    }
  });

  it("sliding drops oldest", async () => {
    const q = Queue.sliding<number>(2);
    q.tryOffer(1);
    q.tryOffer(2);
    q.tryOffer(3);
    expect(await q.take()).toEqual(Result.ok(2));
    expect(await q.take()).toEqual(Result.ok(3));
  });

  it("takes undefined as a queued value", async () => {
    const q = Queue.unbounded<undefined>();
    await q.offer(undefined);

    const taken = q.take();
    q.close();

    expect(await taken).toEqual(Result.ok(undefined));
  });

  it("distinguishes an empty tryTake from an undefined value", () => {
    const q = Queue.unbounded<undefined>();

    const empty = q.tryTake();
    expect(Result.isError(empty)).toBe(true);
    if (Result.isError(empty)) {
      expect(QueueEmptyError.is(empty.error)).toBe(true);
    }

    q.tryOffer(undefined);
    expect(q.tryTake()).toEqual(Result.ok(undefined));
  });
});

describe("Stream", () => {
  it("pipe map filter collect", async () => {
    const out = await pipe(
      Stream.from([1, 2, 3, 4]),
      Stream.map((n) => n * 2),
      Stream.filter((n) => n > 4),
      Stream.runCollect,
    );
    expect(out).toEqual(Result.ok([6, 8]));
  });

  it("buffer preserves order", async () => {
    const out = await pipe(
      Stream.from([1, 2, 3]),
      Stream.buffer((n) => Task.map(Time.sleep((4 - n) * 8), () => n * 10), 3),
      Stream.runCollect,
    );
    expect(out).toEqual(Result.ok([10, 20, 30]));
  });

  it("chunks", async () => {
    const out = await pipe(Stream.from([1, 2, 3, 4, 5]), Stream.chunks(2), Stream.runCollect);
    expect(out).toEqual(Result.ok([[1, 2], [3, 4], [5]]));
  });

  it("chunks rejects non-integer sizes", () => {
    expect(() => Stream.chunks(1.5)).toThrow(RangeError);
  });

  it("fold", async () => {
    const sum = await Stream.runFold(
      Stream.unfold(0, (n) => Task.succeed(n >= 3 ? null : ([n, n + 1] as const))),
      0,
      (a, b) => a + b,
    );
    expect(sum).toEqual(Result.ok(3));
  });
});
