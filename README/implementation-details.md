# Implementation Details

## Choosing a transfer strategy

For each entry, `copy`/`move` choose one strategy:

1. **Direct**: when `TransferOptions.directTransfer` is not `false` and
   `source.canDirectTransfer(sink)` is true, the provider's own
   `directCopy`/`directMove` is used.
2. **Multipart**: when the source is bounded, its readable handle is
   `RangeReadable`, the sink has `getMultipartWriter`, the entry size reaches
   `multipartThreshold` and a part size can be negotiated.
3. **Lease**: when no converter is needed, the readable handle is
   `FillReadable`, the writable handle is a `BufferProvider`, and the sink's
   domain is one the source can fill.
4. **Stream**: otherwise, items are piped from the readable to the writable
   handle.

Before streaming, the source's payload type must be one of
`TransferOptions.writePayloadTypes` (`["bytes"]` by default).

```mermaid
flowchart TD
    A[entry] --> B{direct eligible?}
    B -- yes --> C[directCopy / directMove]
    B -- no --> D{multipart eligible?}
    D -- yes --> E[parts via readRange -> getMultipartWriter]
    D -- no --> F{lease eligible?}
    F -- yes --> G[acquire -> readInto -> commit]
    F -- no --> H[stream items]
```

## Part-size negotiation

Both providers report `getPartSizeConstraints(totalSize)` (a missing
implementation is unconstrained). The part size is the largest of both
minimums and defaults, raised so the part count stays within both
`maxParts`, and capped at the smaller maximum. If the bounds cannot be met,
the transfer streams instead. Parts are read with `readRange(start, end)`,
where `end` is exclusive.

## Lease transfers

The engine acquires a buffer from the sink, has the source fill it, then
commits it. Up to `leaseDepth` leases (default 2, capped by the sink's
`maxOutstanding`) are outstanding, so reading the next lease overlaps
committing the previous one. Reads are serialised and commits happen in
order. Every lease not yet committed is released on abort, error or end of
stream.

## Retries and resume

Only `TransientIOError`s are retried, up to `retry.maxRetries` with
`retry.backoffMs` between attempts:

- Multipart: a part whose read fails is re-read with `readRange` on its own,
  within the same upload.
- Bounded stream: if the source is `RangeReadable` and the writable handle
  exposes `resumeToken()`, the sink is reopened with
  `getWritableStream(key, { resume })` and the source re-read from the
  returned `startOffset`. Otherwise the transfer restarts.
- Live source failure: the source is reopened while the sink stays open,
  the first new item gets `attributes.discontinuity = true`, and
  `TelemetryHooks.onGap` is called. `retry.onGap: "fail"` fails instead.
- Live sink failure: the sink is reopened with its resume token, or the
  transfer fails.

For stream transfers `maxRetries` counts consecutive failures and resets
once an item is written. Direct transfers are never retried by the
framework.

## Recursive and pattern transfers

A container transfer resolves its destination with `cp -r` rules: a
destination that doesn't exist becomes the copy; an existing container gets
the source nested inside it as `<dest>/<source basename>`; an existing entry
is rejected. If the source declares `supportsRecursiveDirectTransfer` and a
direct transfer is allowed, the whole container is handed to one
`directCopy`/`directMove`. Otherwise the container is listed recursively,
sub-containers are recreated with `createContainer`, and each entry is
transferred individually. A non-direct move deletes the source container
once, after every entry succeeds.

A pattern transfer lists the source container non-recursively with the glob
translated to a regular expression, and transfers each matching entry
(containers are skipped) into the destination container. A pattern move
deletes each entry after it is transferred.

Child keys are built with the provider's `joinKey`, defaulting to a
`/`-join.

## Concurrency

Multipart parts and recursive or pattern entries are bounded by a
`ConcurrencyLimiter` (`TransferOptions.concurrencyLimiter`), defaulting to
the shared `defaultConcurrencyLimiter`, so concurrent `copy()`/`move()`
calls share one cap unless a caller passes its own instance.

## Telemetry

Every operation reports progress through `TelemetryHooks.onProgress` with
its own `operationId`. Multipart parts and recursive or pattern entries
report under their own ids, tagged with the parent's id as
`parentOperationId`. Container operations report `entriesProcessed` and
`totalEntries`.

## Provider resolution

The registry passes a `ProviderResolver` to every provider it creates, so a
composite provider can obtain other installed providers. Inner providers
default to the outer provider's kind and domain, and resolution fails with
`PermanentIOError("provider resolution too deep")` beyond four nested
levels. The outer provider disposes the providers it resolved.
