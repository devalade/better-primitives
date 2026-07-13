import { describe, expect, it } from "vite-plus/test";
import { Schedule } from "./index";

describe("Schedule", () => {
  it("limits immediate retries", () => {
    const schedule = Schedule.immediate({ maxRetries: 2 });
    expect(schedule.next(0, "error")).toBe(0);
    expect(schedule.next(1, "error")).toBe(0);
    expect(schedule.next(2, "error")).toBeUndefined();
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
  });

  it("rejects invalid policy parameters", () => {
    expect(() => Schedule.fixed(-1)).toThrow(RangeError);
    expect(() => Schedule.exponential(1, { factor: 0 })).toThrow(RangeError);
    expect(() => Schedule.immediate({ maxRetries: -1 })).toThrow(RangeError);
    expect(() => Schedule.jitter(Schedule.immediate(), 1.1, () => 0)).toThrow(RangeError);
  });
});
