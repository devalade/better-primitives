import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";
import { Cancelled } from "../errors/index";
import { Schedule } from "../schedule/index";
import { Task } from "./index";

describe("Task.retry", () => {
  it("retries expected failures and returns the first success", async () => {
    let attempts = 0;
    const task = Task.retry(
      Task.from(async () => {
        attempts += 1;
        return attempts < 3 ? Result.err("temporary") : Result.ok("ready");
      }),
      Schedule.immediate({ maxRetries: 3 }),
    );
    expect(await Task.execute(task)).toEqual(Result.ok("ready"));
    expect(attempts).toBe(3);
  });

  it("returns the final expected failure without retrying defects", async () => {
    let attempts = 0;
    const task = Task.retry(
      Task.from(async () => {
        attempts += 1;
        return Result.err("nope");
      }),
      Schedule.fixed(0, { maxRetries: 2 }),
    );
    const result = await Task.execute(task);
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(result.error).toBe("nope");
    expect(attempts).toBe(3);
  });

  it("cancels during backoff", async () => {
    const controller = new AbortController();
    const task = Task.retry(Task.fail("temporary"), Schedule.fixed(100, { maxRetries: 3 }));
    const pending = Task.execute(task, { signal: controller.signal });
    controller.abort("stop");
    const result = await pending;
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(Cancelled.is(result.error)).toBe(true);
  });

  it("rejects invalid delays from custom schedules", async () => {
    const task = Task.retry(Task.fail("temporary"), {
      next: () => Number.NaN,
    });
    await expect(Task.execute(task)).rejects.toThrow(RangeError);
  });
});
