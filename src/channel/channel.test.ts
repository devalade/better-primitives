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

  it("allows a cancelled oneshot receive to be retried", async () => {
    const [sender, receiver] = Channel.oneshot<number>();
    const controller = new AbortController();
    const pending = receiver.recv({ signal: controller.signal });
    controller.abort("stop waiting");
    const cancelled = await pending;
    expect(Result.isError(cancelled)).toBe(true);
    if (Result.isError(cancelled)) expect(Cancelled.is(cancelled.error)).toBe(true);
    expect(sender.send(7)).toBe(true);
    expect(await receiver.recv()).toEqual(Result.ok(7));
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

  it("removes closed broadcast receivers from future delivery", async () => {
    const [sender, first] = Channel.broadcast<number>(1);
    const second = sender.subscribe();
    second.close();
    expect(sender.send(1)).toBe(true);
    expect(await first.recv()).toEqual(Result.ok(1));
    const closed = await second.recv();
    expect(Result.isError(closed)).toBe(true);
    if (Result.isError(closed)) expect(QueueClosedError.is(closed.error)).toBe(true);
  });

  it("supports async iteration until the sender closes", async () => {
    const [sender, receiver] = Channel.mpsc<number>(2);
    const values = (async () => {
      const collected: number[] = [];
      for await (const value of receiver) collected.push(value);
      return collected;
    })();
    await sender.send(1);
    await sender.send(2);
    sender.close();
    expect(await values).toEqual([1, 2]);
  });

  it("supports broadcast async iteration", async () => {
    const [sender, receiver] = Channel.broadcast<number>(2);
    const values = (async () => {
      const collected: number[] = [];
      for await (const value of receiver) collected.push(value);
      return collected;
    })();
    expect(sender.send(1)).toBe(true);
    expect(sender.send(2)).toBe(true);
    sender.close();
    expect(await values).toEqual([1, 2]);
  });

  it("watches the latest value", async () => {
    const [sender, receiver] = Channel.watch("idle");
    expect(receiver.borrow()).toBe("idle");
    const changed = receiver.changed();
    sender.send("ready");
    sender.send("running");
    expect(await changed).toEqual(Result.ok("running"));
    const controller = new AbortController();
    const pending = receiver.changed({ signal: controller.signal });
    controller.abort("stop watching");
    const cancelled = await pending;
    expect(Result.isError(cancelled)).toBe(true);
    if (Result.isError(cancelled)) expect(Cancelled.is(cancelled.error)).toBe(true);
    sender.close();
    const closed = await receiver.changed();
    expect(Result.isError(closed)).toBe(true);
    if (Result.isError(closed)) expect(QueueClosedError.is(closed.error)).toBe(true);
  });

  it("does not lose a watch update when a waiter cancels before flush", async () => {
    const [sender, receiver] = Channel.watch("idle");
    const controller = new AbortController();
    const pending = receiver.changed({ signal: controller.signal });
    sender.send("ready");
    controller.abort("stop watching");
    const cancelled = await pending;
    expect(Result.isError(cancelled)).toBe(true);
    if (Result.isError(cancelled)) expect(Cancelled.is(cancelled.error)).toBe(true);
    await Promise.resolve();
    expect(await receiver.changed()).toEqual(Result.ok("ready"));
  });

  it("wakes overlapping watch waiters for the same update", async () => {
    const [sender, receiver] = Channel.watch("idle");
    const first = receiver.changed();
    sender.send("ready");
    const second = receiver.changed();
    expect(await first).toEqual(Result.ok("ready"));
    expect(await second).toEqual(Result.ok("ready"));
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
