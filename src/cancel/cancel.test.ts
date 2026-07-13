import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";
import { Cancelled, TimeoutError } from "../errors/index";
import { Task } from "../task/index";
import { Time } from "../time/index";
import { Cancel } from "./index";

describe("Cancel", () => {
  it("cancels children", async () => {
    const parent = Cancel.make();
    const child = parent.child();
    const waited = child.cancelled();
    parent.cancel("stop");
    expect(Result.isOk(await waited)).toBe(true);
    expect(child.isCancelled).toBe(true);
  });

  it("check returns Cancelled when aborted", () => {
    const controller = new AbortController();
    controller.abort("x");
    const checked = Cancel.check({ signal: controller.signal });
    expect(Result.isError(checked)).toBe(true);
    if (Result.isError(checked)) expect(Cancelled.is(checked.error)).toBe(true);
  });

  it("timeout cancels", async () => {
    const cancel = Cancel.timeout(20);
    await cancel.cancelled();
    expect(cancel.isCancelled).toBe(true);
  });
});

describe("Time", () => {
  it("sleep succeeds as a Task", async () => {
    expect(await Task.execute(Time.sleep(5))).toEqual(Result.ok(undefined));
  });

  it("classifies sleep cancellation at the execution boundary", async () => {
    const controller = new AbortController();
    const pending = Task.execute(Time.sleep(1_000), { signal: controller.signal });
    controller.abort("stop");
    const result = await pending;
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(Cancelled.is(result.error)).toBe(true);
  });

  it("returns a typed TimeoutError", async () => {
    const result = await Task.execute(Time.timeout(10, Time.sleep(200)));
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(TimeoutError.is(result.error)).toBe(true);
  });

  it("preserves a fast success", async () => {
    expect(await Task.execute(Time.timeout(200, Task.succeed(42)))).toEqual(Result.ok(42));
  });

  it("preserves a typed task failure", async () => {
    const failure = { _tag: "LookupError" as const };
    const result = await Task.execute(Time.timeout(200, Task.fail(failure)));
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(result.error).toBe(failure);
  });
});
