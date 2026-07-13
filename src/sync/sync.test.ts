import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";
import { Cancelled } from "../errors/index";
import { Time } from "../time/index";
import { Task } from "../task/index";
import { Deferred, Latch, Mutex, Notify, Once, RwLock, Semaphore } from "./index";

describe("Mutex", () => {
  it("serializes withLock", async () => {
    const mutex = new Mutex();
    const order: number[] = [];
    await Promise.all([
      mutex.withLock(async () => {
        order.push(1);
        await Task.execute(Time.sleep(20));
        order.push(2);
      }),
      mutex.withLock(async () => {
        order.push(3);
      }),
    ]);
    expect(order).toEqual([1, 2, 3]);
  });
});

describe("Semaphore", () => {
  it("rejects fractional permit counts", () => {
    expect(() => new Semaphore(1.5)).toThrow(RangeError);
  });

  it("limits concurrency", async () => {
    const sem = new Semaphore(2);
    let active = 0;
    let max = 0;
    await Promise.all(
      Array.from({ length: 5 }, () =>
        sem.withPermit(async () => {
          active += 1;
          max = Math.max(max, active);
          await Task.execute(Time.sleep(15));
          active -= 1;
        }),
      ),
    );
    expect(max).toBeLessThanOrEqual(2);
  });

  it("transfers and restores a permit after contention", async () => {
    const semaphore = new Semaphore(1);
    const first = await semaphore.acquire();
    const secondPending = semaphore.acquire();

    first.release();
    const second = await secondPending;
    expect(semaphore.availablePermits).toBe(0);

    second.release();
    expect(semaphore.availablePermits).toBe(1);

    const reused = await semaphore.acquire();
    reused.release();
    expect(semaphore.availablePermits).toBe(1);
  });
});

describe("Deferred", () => {
  it("succeeds once", async () => {
    const d = Deferred.make<number>();
    expect(d.succeed(42)).toBe(true);
    expect(d.succeed(1)).toBe(false);
    expect(await d.await()).toEqual(Result.ok(42));
  });

  it("fails as Result err", async () => {
    const d = Deferred.make<number, string>();
    d.fail("nope");
    const result = await d.await();
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) {
      expect(result.error).toBe("nope");
    }
  });
});

describe("Latch", () => {
  it("rejects fractional counts", () => {
    expect(() => Latch.make(1.5)).toThrow(RangeError);
  });

  it("opens after count", async () => {
    const latch = Latch.make(2);
    let opened = false;
    const waiter = latch.await().then(() => {
      opened = true;
    });
    latch.open();
    expect(opened).toBe(false);
    latch.open();
    await waiter;
    expect(opened).toBe(true);
  });
});

describe("Notify", () => {
  it("wakes one waiter", async () => {
    const n = new Notify();
    const p = n.wait();
    n.notify();
    expect(await p).toEqual(Result.ok(undefined));
  });

  it("cancels wait", async () => {
    const n = new Notify();
    const c = new AbortController();
    const p = n.wait({ signal: c.signal });
    c.abort();
    const result = await p;
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) {
      expect(Cancelled.is(result.error)).toBe(true);
    }
  });
});

describe("Once", () => {
  it("computes once", async () => {
    let calls = 0;
    const once = new Once<number>();
    const [a, b] = await Promise.all([
      once.getOrSet(async () => {
        calls += 1;
        await Task.execute(Time.sleep(10));
        return 9;
      }),
      once.getOrSet(async () => {
        calls += 1;
        return 1;
      }),
    ]);
    expect(a).toBe(9);
    expect(b).toBe(9);
    expect(calls).toBe(1);
  });
});

describe("RwLock", () => {
  it("allows concurrent readers", async () => {
    const lock = new RwLock();
    let concurrent = 0;
    let max = 0;
    await Promise.all([
      lock.withRead(async () => {
        concurrent += 1;
        max = Math.max(max, concurrent);
        await Task.execute(Time.sleep(20));
        concurrent -= 1;
      }),
      lock.withRead(async () => {
        concurrent += 1;
        max = Math.max(max, concurrent);
        await Task.execute(Time.sleep(20));
        concurrent -= 1;
      }),
    ]);
    expect(max).toBe(2);
  });
});
