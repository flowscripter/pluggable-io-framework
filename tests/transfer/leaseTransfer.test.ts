import { describe, expect, test } from "bun:test";
import {
  type BufferLease,
  type BufferProvider,
  type FillReadable,
  type Item,
  PayloadKind,
  type StreamHandle,
} from "@flowscripter/pluggable-io-framework-api";
import {
  DEFAULT_LEASE_DEPTH,
  leaseDepth,
  leaseTransfer,
} from "../../src/transfer/leaseTransfer.ts";
import type { TransferOptions } from "../../src/transfer/TransferOptions.ts";

interface LeaseLog {
  events: string[];
  outstanding: number;
  maxOutstanding: number;
}

function makeSink(
  log: LeaseLog,
  options: { failCommit?: number; maxOutstanding?: number } = {},
): StreamHandle & BufferProvider {
  let next = 0;
  return {
    kind: PayloadKind.Native,
    domain: "host",
    maxOutstanding: options.maxOutstanding,
    stream: new WritableStream<Item>({
      close: () => {
        log.events.push("close");
      },
      abort: () => {
        log.events.push("abort");
      },
    }),
    async acquire(): Promise<BufferLease> {
      const id = next;
      next += 1;
      log.outstanding += 1;
      log.maxOutstanding = Math.max(log.maxOutstanding, log.outstanding);
      let settled = false;
      const settle = () => {
        if (!settled) {
          settled = true;
          log.outstanding -= 1;
        }
      };
      return {
        ptr: id,
        length: 4,
        domain: "host",
        async commit(length) {
          await new Promise((resolve) => setTimeout(resolve, id % 2 === 0 ? 5 : 0));
          if (options.failCommit === id) throw new Error(`commit ${id} failed`);
          log.events.push(`commit:${id}:${length}`);
          settle();
        },
        release() {
          log.events.push(`release:${id}`);
          settle();
        },
      };
    },
  };
}

function makeSource(
  lengths: number[],
  onRead?: (index: number) => void,
): StreamHandle & FillReadable {
  let index = 0;
  return {
    kind: PayloadKind.Native,
    stream: new ReadableStream<Item>(),
    domains: ["host"],
    async readInto() {
      onRead?.(index);
      const length = lengths[index];
      index += 1;
      return length ?? null;
    },
  };
}

function context(options: TransferOptions = {}) {
  return { operationId: "op", hooks: {}, options, type: "copy" as const };
}

describe("leaseDepth", () => {
  test("defaults to 2, honours a request, and is capped by maxOutstanding", () => {
    const log: LeaseLog = { events: [], outstanding: 0, maxOutstanding: 0 };
    expect(leaseDepth(makeSink(log), undefined)).toBe(DEFAULT_LEASE_DEPTH);
    expect(leaseDepth(makeSink(log), 4)).toBe(4);
    expect(leaseDepth(makeSink(log, { maxOutstanding: 1 }), 4)).toBe(1);
    expect(leaseDepth(makeSink(log), 0)).toBe(1);
  });
});

describe("leaseTransfer", () => {
  test("commits in order, keeps at most depth leases outstanding, and closes the sink", async () => {
    const log: LeaseLog = { events: [], outstanding: 0, maxOutstanding: 0 };
    const progress: number[] = [];
    const result = await leaseTransfer(makeSource([4, 4, 4, 2]), makeSink(log), 2, {
      ...context(),
      hooks: { onProgress: (event) => progress.push(event.bytesProcessed) },
    });
    expect(result).toEqual({ bytes: 14, items: 4, stopped: false });
    expect(log.events.filter((event) => event.startsWith("commit"))).toEqual([
      "commit:0:4",
      "commit:1:4",
      "commit:2:4",
      "commit:3:2",
    ]);
    expect(log.events).toContain("release:4");
    expect(log.events.at(-1)).toBe("close");
    expect(log.maxOutstanding).toBeLessThanOrEqual(2);
    expect(progress.at(-1)).toBe(14);
  });

  test("releases every outstanding lease and aborts the sink when a commit fails", async () => {
    const log: LeaseLog = { events: [], outstanding: 0, maxOutstanding: 0 };
    await expect(
      leaseTransfer(makeSource([4, 4, 4]), makeSink(log, { failCommit: 0 }), 2, context()),
    ).rejects.toThrow("commit 0 failed");
    expect(log.outstanding).toBe(0);
    expect(log.events).toContain("abort");
  });

  test("signal aborts and releases outstanding leases", async () => {
    const log: LeaseLog = { events: [], outstanding: 0, maxOutstanding: 0 };
    const signal = new AbortController();
    const error = await leaseTransfer(
      makeSource([4, 4, 4], (index) => {
        if (index === 1) signal.abort();
      }),
      makeSink(log),
      2,
      context({ signal: signal.signal }),
    ).catch((e: unknown) => e);
    expect((error as DOMException).name).toBe("AbortError");
    expect(log.outstanding).toBe(0);
  });

  test("stop finishes pending commits and closes the sink", async () => {
    const log: LeaseLog = { events: [], outstanding: 0, maxOutstanding: 0 };
    const stop = new AbortController();
    const result = await leaseTransfer(
      makeSource([4, 4, 4], (index) => {
        if (index === 1) stop.abort();
      }),
      makeSink(log),
      2,
      context({ stop: stop.signal }),
    );
    expect(result.stopped).toBe(true);
    expect(result.items).toBe(2);
    expect(log.events.at(-1)).toBe("close");
  });
});
