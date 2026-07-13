import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";
import { Cancelled } from "../errors/index";
import { Resource } from "../resource/index";
import { Task } from "../task/index";
import { Time } from "../time/index";
import { Scope } from "./index";

describe("Scope", () => {
  it("runs finalizers LIFO", async () => {
    const order: number[] = [];
    await Scope.use(async (scope) => {
      scope.addFinalizer(() => {
        order.push(1);
      });
      scope.addFinalizer(() => {
        order.push(2);
      });
    });
    expect(order).toEqual([2, 1]);
  });
});

describe("Task", () => {
  it("runs a successful Task", async () => {
    const fiber = Task.run(Task.succeed(7));
    expect(await fiber.result()).toEqual(Result.ok(7));
    expect(fiber.isDone).toBe(true);
  });

  it("keeps a completed Fiber result stable after a late abort", async () => {
    const fiber = Task.run(Task.succeed(42));
    expect(await fiber.result()).toEqual(Result.ok(42));

    fiber.abort("too late");

    expect(await fiber.result()).toEqual(Result.ok(42));
  });

  it("fork is cancelled when its Scope closes", async () => {
    let outcome: Promise<Result<void, Cancelled>> | undefined;
    await Scope.use(async (scope) => {
      const fiber = Task.fork(scope, Time.sleep(1_000));
      outcome = fiber.result();
      await Task.execute(Time.sleep(5));
    });
    const result = await outcome!;
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(Cancelled.is(result.error)).toBe(true);
  });

  it("all preserves tuple order and typed failures", async () => {
    const success = await Task.execute(Task.all([Task.succeed(1), Task.succeed("two")] as const));
    expect(success).toEqual(Result.ok([1, "two"]));

    const failure = { _tag: "AllError" as const };
    const failed = await Task.execute(Task.all([Task.succeed(1), Task.fail(failure)] as const));
    expect(Result.isError(failed)).toBe(true);
    if (Result.isError(failed)) expect(failed.error).toBe(failure);
  });

  it("all aborts and settles siblings after a typed failure", async () => {
    const failure = { _tag: "AllError" as const };
    let siblingAborted = false;
    const sibling = Task.from<never>(async (signal) => {
      try {
        return await new Promise<never>((_, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
      } finally {
        siblingAborted = signal.aborted;
      }
    });
    const delayedFailure = Task.flatMap(Time.sleep(5), () => Task.fail(failure));
    const result = await Task.execute(Task.all([sibling, delayedFailure] as const));
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(result.error).toBe(failure);
    expect(siblingAborted).toBe(true);
  });

  it("race returns the first success and settles losers", async () => {
    const slow = Task.map(Time.sleep(50), () => "slow" as const);
    const result = await Task.execute(Task.race([slow, Task.succeed("fast" as const)]));
    expect(result).toEqual(Result.ok("fast"));
  });

  it("race ignores typed failures while a branch can succeed", async () => {
    const failure = { _tag: "RaceError" as const };
    const winner = Task.map(Time.sleep(5), () => "winner" as const);
    expect(await Task.execute(Task.race([Task.fail(failure), winner]))).toEqual(
      Result.ok("winner"),
    );
  });

  it("race returns the first observed typed failure when every branch fails", async () => {
    const first = { _tag: "First" as const };
    const second = { _tag: "Second" as const };
    const result = await Task.execute(Task.race([Task.fail(first), Task.fail(second)]));
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(result.error).toBe(first);
  });

  it("does not turn defects into expected failures", async () => {
    const defect = new Error("defect");
    const task = Task.from<never>(async () => {
      throw defect;
    });
    await expect(Task.execute(task)).rejects.toBe(defect);
  });

  it("select returns the correlated winning key", async () => {
    const result = await Task.execute(
      Task.select({
        slow: Task.map(Time.sleep(50), () => 1),
        winner: Task.succeed("ready"),
      }),
    );
    expect(result).toEqual(Result.ok({ key: "winner", value: "ready" }));
  });

  it("forEach bounds concurrency and preserves order", async () => {
    let active = 0;
    let maximum = 0;
    const task = Task.forEach(
      [1, 2, 3, 4, 5],
      (value) =>
        Task.from(async (signal) => {
          active += 1;
          maximum = Math.max(maximum, active);
          await Time.sleep(5)(signal);
          active -= 1;
          return Result.ok(value * 2);
        }),
      { concurrency: 2 },
    );
    expect(await Task.execute(task)).toEqual(Result.ok([2, 4, 6, 8, 10]));
    expect(maximum).toBeLessThanOrEqual(2);
  });

  it("forEach rejects invalid concurrency synchronously", () => {
    expect(() => Task.forEach([1], Task.succeed, { concurrency: 1.5 })).toThrow(RangeError);
  });

  it("forEach preserves undefined rejection defects", async () => {
    const task = Task.forEach([1], () => Task.from<never>(async () => Promise.reject(undefined)));
    let caught = false;
    let observed: unknown = "not undefined";
    try {
      await Task.execute(task);
    } catch (cause: unknown) {
      caught = true;
      observed = cause;
    }
    expect(caught).toBe(true);
    expect(observed).toBe(undefined);
  });

  it("forEach aborts and settles siblings after a typed failure", async () => {
    const failure = { _tag: "ForEachError" as const };
    let siblingAborted = false;
    const result = await Task.execute(
      Task.forEach(
        [1, 2],
        (value) =>
          value === 1
            ? Task.from<never>(async (signal) => {
                try {
                  return await new Promise<never>((_, reject) => {
                    signal.addEventListener("abort", () => reject(signal.reason), { once: true });
                  });
                } finally {
                  siblingAborted = signal.aborted;
                }
              })
            : Task.flatMap(Time.sleep(5), () => Task.fail(failure)),
        { concurrency: 2 },
      ),
    );
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(result.error).toBe(failure);
    expect(siblingAborted).toBe(true);
  });
});

describe("Resource", () => {
  it("releases after successful use", async () => {
    const released: string[] = [];
    const resource = Resource.make(Task.succeed("db"), (value) => {
      released.push(value);
    });
    const result = await Task.execute(
      Resource.use(resource, (database) => Task.succeed(database.toUpperCase())),
    );
    expect(result).toEqual(Result.ok("DB"));
    expect(released).toEqual(["db"]);
  });

  it("does not register release when acquisition fails", async () => {
    let released = false;
    const failure = { _tag: "AcquireError" as const };
    const resource = Resource.make(Task.fail(failure), () => {
      released = true;
    });
    const result = await Task.execute(Resource.use(resource, () => Task.succeed("unused")));
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(result.error).toBe(failure);
    expect(released).toBe(false);
  });

  it("releases a value acquired after cancellation wins", async () => {
    const controller = new AbortController();
    let notifyAcquireStarted: (() => void) | undefined;
    const acquireStarted = new Promise<void>((resolve) => {
      notifyAcquireStarted = resolve;
    });
    let finishAcquire: ((value: string) => void) | undefined;
    const acquired = new Promise<string>((resolve) => {
      finishAcquire = resolve;
    });
    const released: string[] = [];
    let used = false;
    const resource = Resource.make(
      Task.from(async () => {
        notifyAcquireStarted?.();
        return Result.ok(await acquired);
      }),
      (value) => {
        released.push(value);
      },
    );

    const running = Task.execute(
      Resource.use(resource, () => {
        used = true;
        return Task.succeed(undefined);
      }),
      { signal: controller.signal },
    );
    await acquireStarted;
    controller.abort("stop");
    finishAcquire?.("db");

    const result = await running;
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(Cancelled.is(result.error)).toBe(true);
    expect(released).toEqual(["db"]);
    expect(used).toBe(false);
  });
});
