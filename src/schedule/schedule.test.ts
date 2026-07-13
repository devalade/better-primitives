import { describe, expect, it } from "vite-plus/test";
import { Schedule } from "./index";

describe("Schedule", () => {
  it("limits immediate retries", () => {
    const schedule = Schedule.immediate({ maxRetries: 2 });
    expect(schedule.next(0, "error")).toBe(0);
    expect(schedule.next(1, "error")).toBe(0);
    expect(schedule.next(2, "error")).toBeUndefined();
    const limited = Schedule.limit(Schedule.immediate(), 1);
    expect(limited.next(0, "error")).toBe(0);
    expect(limited.next(1, "error")).toBeUndefined();
  });

  it("calculates capped exponential delays", () => {
    const schedule = Schedule.exponential(10, { factor: 2, maxDelay: 25, maxRetries: 4 });
    expect([0, 1, 2, 3].map((attempt) => schedule.next(attempt, "error"))).toEqual([
      10, 20, 25, 25,
    ]);
    expect(schedule.next(4, "error")).toBeUndefined();
    expect(Schedule.exponential(10).next(0, "error")).toBe(10);
  });

  it("applies deterministic multiplicative jitter", () => {
    const schedule = Schedule.jitter(Schedule.fixed(100, { maxRetries: 1 }), 0.2, () => 0);
    expect(schedule.next(0, "error")).toBe(80);
    expect(Schedule.jitter(Schedule.fixed(100), 0.2, () => 1).next(0, "error")).toBeCloseTo(120);
    const capped = Schedule.jitter(Schedule.exponential(80, { maxDelay: 100 }), 1, () => 1);
    expect(capped.next(0, "error")).toBe(100);
  });

  it("rejects invalid policy parameters", () => {
    expect(() => Schedule.fixed(-1)).toThrow(RangeError);
    expect(() => Schedule.exponential(1, { factor: 0 })).toThrow(RangeError);
    expect(() => Schedule.immediate({ maxRetries: -1 })).toThrow(RangeError);
    expect(() => Schedule.jitter(Schedule.immediate(), 1.1, () => 0)).toThrow(RangeError);
    expect(() => Schedule.jitter(Schedule.fixed(1), 0.2, () => 2).next(0, "error")).toThrow(
      RangeError,
    );
  });
});
