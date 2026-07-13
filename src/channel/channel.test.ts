import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";
import { Cancelled, QueueClosedError } from "../errors/index";
import { Channel } from "./index";
import { Barrier } from "../sync/index";

describe("Channel", () => {
  it("provides bounded MPSC backpressure and cancellation", async () => {
    const [sender, receiver] = Channel.mpsc<number>(1);
    expect(await sender.send(1)).toEqual(Result.ok(undefined));
    const controller = new AbortController();
    const pending = sender.send(2, { signal: controller.signal });
    controller.abort("stop");
    const cancelled = await pending;
    expect(Result.isError(cancelled)).toBe(true);
    if (Result.isError(cancelled)) expect(Cancelled.is(cancelled.error)).toBe(true);
    expect(await receiver.recv()).toEqual(Result.ok(1));
    sender.close();
    const closed = await receiver.recv();
    expect(Result.isError(closed)).toBe(true);
    if (Result.isError(closed)) expect(QueueClosedError.is(closed.error)).toBe(true);
  });

  it("delivers a oneshot exactly once", async () => {
    const [sender, receiver] = Channel.oneshot<number>();
    expect(sender.send(42)).toBe(true);
    expect(sender.send(7)).toBe(false);
    expect(await receiver.recv()).toEqual(Result.ok(42));
    const closed = await receiver.recv();
    expect(Result.isError(closed)).toBe(true);
    if (Result.isError(closed)) expect(QueueClosedError.is(closed.error)).toBe(true);
  });

  it("broadcasts to every receiver and applies backpressure atomically", async () => {
    const [sender, first] = Channel.broadcast<number>(1);
    const second = sender.subscribe();
    expect(sender.send(1)).toBe(true);
    expect(sender.send(2)).toBe(false);
    expect(await first.recv()).toEqual(Result.ok(1));
    expect(await second.recv()).toEqual(Result.ok(1));
    expect(sender.send(2)).toBe(true);
    expect(await first.recv()).toEqual(Result.ok(2));
    expect(await second.recv()).toEqual(Result.ok(2));
  });

  it("watches the latest value", async () => {
    const [sender, receiver] = Channel.watch("idle");
    expect(receiver.borrow()).toBe("idle");
    const changed = receiver.changed();
    sender.send("ready");
    expect(await changed).toEqual(Result.ok("ready"));
    sender.send("running");
    expect(await receiver.changed()).toEqual(Result.ok("running"));
    sender.close();
    const closed = await receiver.changed();
    expect(Result.isError(closed)).toBe(true);
    if (Result.isError(closed)) expect(QueueClosedError.is(closed.error)).toBe(true);
  });
});

describe("Barrier", () => {
  it("releases a generation after all parties arrive", async () => {
    const barrier = new Barrier(2);
    let released = false;
    const first = barrier.wait().then((result) => {
      released = true;
      return result;
    });
    await Promise.resolve();
    expect(released).toBe(false);
    expect(await barrier.wait()).toEqual(Result.ok(undefined));
    expect(await first).toEqual(Result.ok(undefined));
    expect(released).toBe(true);
  });
});
