# Implementation Details

## Choosing a Transfer Strategy

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
`TransferOptions.writePayloadTypes` (`["bytes"]` by default), which
`createProvidersForTransfer` sets from the destination factory.

```mermaid
flowchart TD
    CM["copy() / move()"] --> T{source target}
    T -- entry --> E[transferEntry]
    T -- container --> R[recursiveTransfer]
    T -- pattern --> P[patternTransfer]
    R -- "supportsRecursiveDirectTransfer" --> DC
    R -- "list(recursive) per entry" --> E
    P -- "list(regex) per matching entry" --> E
    E --> D{"directTransfer !== false and canDirectTransfer(sink)?"}
    D -- yes --> DC["direct: directCopy / directMove<br/>provider moves the data itself"]
    D -- no --> O["open readable, check payload type,<br/>move rejects bounded === false"]
    O --> M{"bounded, RangeReadable, sink has getMultipartWriter,<br/>size >= multipartThreshold, part size negotiated?"}
    M -- yes --> MP["multipart: readRange per part,<br/>parts buffered concurrently via ConcurrencyLimiter,<br/>handed to the sink's multipart writer"]
    M -- no --> W[open writable]
    W --> L{"no converter, FillReadable source,<br/>BufferProvider sink, shared domain?"}
    L -- yes --> LE["lease: acquire -> readInto -> commit,<br/>up to leaseDepth leases outstanding"]
    L -- no --> S["stream: read item -> convert -> write item,<br/>with resume / reconnect on TransientIOError"]
```

| Strategy  | Conditions                                                                | Behaviour                                                                           | Path suffix       |
| --------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ----------------- |
| direct    | `canDirectTransfer(sink)` and `directTransfer` is not `false`             | calls the provider's `directCopy`/`directMove`; no streams in the framework         | `direct`          |
| multipart | bounded `RangeReadable` source, sink multipart writer, size >= threshold  | reads parts with `readRange`, retries a part on its own, writes via the sink writer | `multipart`       |
| lease     | no converter, `FillReadable` source and `BufferProvider` sink in a domain | the source fills sink-provided buffers which are committed in order                 | `lease (depth N)` |
| stream    | otherwise                                                                 | pipes items, applying the converter, with resume and live-source reconnection       | `stream`          |

The path suffix is appended to the negotiated path description in
`TransferResult.path`, so the result records which strategy moved the data.
For example, a stream transfer between two `file` providers reports
`file/js -> file/js, stream`.

```mermaid
sequenceDiagram
    participant E as transferEntry
    participant S as source handle
    participant K as sink
    Note over E,K: multipart
    loop each part (bounded by ConcurrencyLimiter)
        E->>S: readRange(offset, end)
        S-->>E: items (re-read on TransientIOError)
    end
    E->>K: getMultipartWriter(key, partSize).write(parts)
    Note over E,K: lease
    loop until readInto returns null
        E->>K: acquire()
        K-->>E: BufferLease
        E->>S: readInto(lease)
        E-)K: lease.commit(length) (in order, up to leaseDepth pending)
    end
    E->>K: close writable
    Note over E,K: stream
    loop until end of stream or stop
        E->>S: read()
        S-->>E: item
        E->>K: write(item)
    end
    E->>K: close() (abort() on signal)
```

## Part-Size Negotiation

Both providers report `getPartSizeConstraints(totalSize)` (a missing
implementation is unconstrained). The part size is the largest of both
minimums and defaults, raised so the part count stays within both
`maxParts`, and capped at the smaller maximum. If the bounds cannot be met,
the transfer streams instead. Parts are read with `readRange(start, end)`,
where `end` is exclusive.

## Lease Transfers

The engine acquires a buffer from the sink, has the source fill it, then
commits it. Up to `leaseDepth` leases (default 2, capped by the sink's
`maxOutstanding`) are outstanding, so reading the next lease overlaps
committing the previous one. Reads are serialised and commits happen in
order. Every lease not yet committed is released on abort, error or end of
stream.

## Retries and Resume

Only `TransientIOError`s are retried, up to `retry.maxRetries` with
`retry.backoffMs` between attempts:

- Multipart: a part whose read fails is re-read with `readRange` on its own,
  within the same upload. When the upload itself fails and the multipart
  writer exposes `resumeToken()`, a new writer is created with
  `getMultipartWriter(key, partSize, { resume })` and only the parts from
  the token's `offset` on are sent again. Otherwise the upload restarts.
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
once an item is written. Direct transfers are not retried by the framework:
a provider's `directCopy`/`directMove` can retry internally, since it knows
which of its backend's failures are safe to repeat (the AWS SDK already
does this for S3 server-side copies).

## Recursive and Pattern Transfers

A container transfer resolves its destination with `cp -r` rules:

- a destination that doesn't exist becomes the copy;
- an existing container gets the source nested inside it as
  `<dest>/<source basename>`;
- an existing entry is rejected.

If the source declares `supportsRecursiveDirectTransfer` and a
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

`mapAsyncIterableConcurrently(source, fn, limiter)` starts
`limiter.maxConcurrency` worker loops. Each worker pulls the next source
item, runs `fn` inside `limiter.run` (which waits in a FIFO queue when the
limiter's active count is at its cap), and pushes the result onto an
`AsyncChannel` that the caller iterates. Results arrive in completion order.
Because each worker pulls only after finishing its previous item, at most
`maxConcurrency` items are pulled at once. The first error, from the source
or from `fn`, stops all workers from pulling more; in-flight items finish,
then the channel rethrows the error to the caller.

```mermaid
sequenceDiagram
    participant C as caller (for await)
    participant Ch as AsyncChannel
    participant W as worker 1..maxConcurrency
    participant Src as source iterable
    participant L as ConcurrencyLimiter
    participant F as fn
    loop until source done or stopped
        W->>Src: next()
        Src-->>W: item
        W->>L: run(() => fn(item))
        alt active < maxConcurrency
            L->>L: active += 1
        else at cap
            L->>L: queue until a slot frees
        end
        L->>F: fn(item)
        F-->>L: result
        L->>L: active -= 1, drain queue
        L-->>W: result
        W->>Ch: push(result)
        Ch-->>C: next() resolves with result
    end
    alt any error
        W->>Ch: fail(firstError) after all workers finish
        Ch-->>C: next() rejects
    else
        W->>Ch: close() after all workers finish
        Ch-->>C: done
    end
```

```mermaid
classDiagram
    class ConcurrencyLimiter {
      +maxConcurrency: number
      +setMaxConcurrency(n)
      +run(fn) Promise
    }
    class AsyncChannel~T~ {
      +push(item)
      +close()
      +fail(error)
    }
    class mapAsyncIterableConcurrently {
      <<function>>
    }
    mapAsyncIterableConcurrently --> ConcurrencyLimiter : bounds fn calls
    mapAsyncIterableConcurrently --> AsyncChannel : yields results through
    recursiveTransfer ..> mapAsyncIterableConcurrently : entries
    patternTransfer ..> mapAsyncIterableConcurrently : entries
    multipartTransfer ..> mapAsyncIterableConcurrently : parts
```

## Telemetry

Every operation reports progress through `TelemetryHooks.onProgress` with
its own `operationId`. Multipart parts and recursive or pattern entries
report under their own ids, tagged with the parent's id as
`parentOperationId`. Container operations report `entriesProcessed` and
`totalEntries`.

## Decorators

`seekable` turns a handle that can serve byte ranges into one logical
stream whose position can be moved: `seek(offset)` replaces the current
reader with `readRange(offset, ...)`.

`locallyCached` wraps the function that opens a handle rather than the
handle itself, because a handle's stream can only be read once: the first
open drains and caches the items, later opens replay them.

```mermaid
classDiagram
    class StreamHandle~K~ {
      <<interface>>
      +kind: K
      +stream
      +bounded?
      +payloadType?
    }
    class RangeReadable~K~ {
      <<interface>>
      +readRange(start, end) ReadableStream
    }
    class Seekable {
      <<interface>>
      +seek(offset)
    }
    class StreamDecorator~K, C~ {
      <<type>>
      handle => handle & C
    }
    class StreamOpenerDecorator~K, C~ {
      <<type>>
      open => () => handle & C
    }
    class seekable {
      <<function>>
      StreamHandle & RangeReadable => StreamHandle & Seekable
    }
    class locallyCached {
      <<function>>
      opener => caching opener
    }
    class IOProvider {
      <<interface>>
      +getReadableStream(path) StreamHandle
    }
    seekable ..|> StreamDecorator : shape of
    locallyCached ..|> StreamOpenerDecorator : typed as
    seekable ..> RangeReadable : requires
    seekable ..> Seekable : adds
    StreamHandle <|-- RangeReadable : capability of
    StreamHandle <|-- Seekable : capability of
    IOProvider ..> StreamHandle : returns
    locallyCached ..> IOProvider : wraps getReadableStream opener
```

## Provider Registry and Provider Resolution

The registry passes a `ProviderResolver` to every provider it creates, so a
composite provider can obtain other installed providers. Inner providers
default to the outer provider's kind and domain, and resolution fails with
`PermanentIOError("provider resolution too deep")` beyond four nested
levels. The outer provider disposes the providers it resolved.

```mermaid
classDiagram
    class ProviderRegistry {
      +discover()
      +getProtocols() string[]
      +getKinds(protocol) PayloadKind[]
      +getFactory(protocol, kind?) IOProviderFactory
      +getConverters() PayloadConverter[]
      +createProviderForLocation(location, options?) ResolvedProvider
      +createProvidersForTransfer(source, dest, options?) TransferProviders
    }
    class ProviderResolver {
      <<interface>>
      +createProviderForLocation(location, opts?)
    }
    class PluginManager {
      <<interface>>
      +registerExtensions(extensionPoint)
      +getRegisteredExtensions(extensionPoint)
      +instantiate(extensionHandle)
    }
    class IOProviderFactory {
      <<interface>>
      +protocol
      +kind
      +domains
      +readPayloadTypes
      +writePayloadTypes
      +locationSchema
      +parseLocationString(location)
      +toProviderInputs(location)
      +createProvider(config, context)
    }
    class PayloadConverter {
      <<interface>>
      +from
      +to
      +cost
      +convert(item)
    }
    class IOProvider {
      <<interface>>
    }
    class ProviderContext {
      <<interface>>
      +domain
      +resolver
    }
    class TransferProviders {
      +source: ResolvedProvider
      +dest: ResolvedProvider
      +options: TransferOptions
    }
    ProviderRegistry ..|> ProviderResolver
    ProviderRegistry --> PluginManager : discovers through
    ProviderRegistry "1" o-- "*" IOProviderFactory : by protocol and kind
    ProviderRegistry "1" o-- "*" PayloadConverter
    ProviderRegistry ..> detectProtocol : uses
    ProviderRegistry ..> negotiateTransfer : uses
    ProviderRegistry ..> TransferProviders : returns
    IOProviderFactory ..> IOProvider : creates
    IOProviderFactory ..> ProviderContext : receives
    ProviderContext --> ProviderResolver : scoped resolver
```

```mermaid
sequenceDiagram
    participant H as host
    participant R as ProviderRegistry
    participant N as negotiateTransfer
    participant SF as source factory
    participant DF as dest factory
    H->>R: createProvidersForTransfer(source, dest, { kind? })
    R->>R: protocol of source and dest (detectProtocol for strings)
    R->>N: factories for both protocols, converters, kind
    N-->>R: (factory, kind, domain) per side, converter?, path
    R->>SF: locationSchema.parse(parseLocationString(source) or source.location)
    R->>SF: toProviderInputs(location)
    R->>SF: createProvider(config, { domain, resolver })
    SF-->>R: source provider
    R->>DF: same steps for dest
    DF-->>R: dest provider (source disposed if this fails)
    R-->>H: { source, dest, options: { converter, path, writePayloadTypes } }
```
