import { describe, expect, test } from "bun:test";
import { AsyncChannel } from "../../src/concurrency/AsyncChannel.ts";

describe("AsyncChannel", () => {
  test("delivers buffered and awaited items in order, then ends on close", async () => {
    const channel = new AsyncChannel<number>();
    channel.push(1);
    const iterator = channel[Symbol.asyncIterator]();
    expect(await iterator.next()).toEqual({ value: 1, done: false });
    const waiting = iterator.next();
    channel.push(2);
    expect(await waiting).toEqual({ value: 2, done: false });
    const ending = iterator.next();
    channel.close();
    expect((await ending).done).toBe(true);
    expect((await iterator.next()).done).toBe(true);
  });

  test("fail rejects current and later waiters", async () => {
    const channel = new AsyncChannel<number>();
    const iterator = channel[Symbol.asyncIterator]();
    const waiting = iterator.next();
    channel.fail(new Error("boom"));
    await expect(waiting).rejects.toThrow("boom");
    await expect(iterator.next()).rejects.toThrow("boom");
  });
});
