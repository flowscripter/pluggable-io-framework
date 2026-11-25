# pluggable-io-framework

Orchestration layer of the Flowscripter pluggable IO framework (contracts live in `../pluggable-io-framework-api`).

## Design records

- `.claude/plans/` holds interviewed design plans. Current: `further-requirements-for-the-cozy-russell.md` (protocol-aware locations, s3/http plugins, CLI, native filesystem plugin, plus contracts for future media protocols and a Future/roadmap section).
- Earlier: `i-have-reviewed-the-eager-hellman.md` (multipart negotiation, recursive copy/move, concurrency, retries, progress).

## Key decisions (cozy-russell plan, 2026-10-01)

- Terminology follows `docs/design/domain-model-concepts.md`: stream unit `ChunkRef` -> `Item { attributes, payload: JsPayload | NativePayload }`, `ChunkKind` -> `PayloadKind`, `ChunkConverter` -> `PayloadConverter`; stored file/object -> "entry" (`EntryProperties`, `LocationTarget` `entry | container | pattern`, `isContainer`, `createContainer`); CLI `--payload-kind`. Breaking API changes are acceptable.
- `PayloadKind` stays `js | native`; `NativePayload.domain` = addressability ("host", later "cuda:N"); memory registration (libfabric) lives in `descriptor.registrations`, not as a domain.
- One kind per factory, `domains[]` in preference order; negotiation = common (kind, domain) -> cheapest single registered converter -> error.
- Payload types (`StreamHandle.payloadType`, factory `readPayloadTypes`/`writePayloadTypes`) are opaque IDs (`"bytes"` built in); meaning belongs to a future packet schema registry; framework only exact-matches IDs.
- Sink-provided buffers (leases) are native-only and engine-driven (pipelined, depth 2).
- `TransferOptions.signal` = cancel/abort sink, `stop` = graceful/close sink. `copy` stays named copy; `StreamHandle.bounded` marks live sources; `move` rejects unbounded.
- Resume via serializable `ResumeToken` from the writable handle; per-part multipart retry; live sources reconnect with `discontinuity` flag.
- Composite schemes: protocol = token before first `+` (`tams+https:` -> `tams`).
- Native providers use a Rust I/O thread + bounded ring + pluggable `NativeWaker`: default `CallbackWaker` (Bun `JSCallback` with `threadsafe: true`, documented but experimental), fallback `WorkerWaker` (shared Worker); no synchronous blocking FFI.
- S3 plugin uses `@aws-sdk/client-s3`, not Bun's `S3Client` (no exposed multipart UploadId/ListParts, server-side copy or tagging).

## Structured locations and multipart resume (2026-10-07, PR #21, framework 2.1.0)

- `ProviderRegistry.createProviderForLocation`/`createProvidersForTransfer` accept `string | StructuredLocation` (`{ protocol, location }`); structured skips `detectProtocol`/`parseLocationString`. CLI uses only structured; strings stay for `ProviderResolver` (composite providers like TAMS get URLs). Dropping strings = API major; Nick asked, recommendation was keep both.
- A file location string always becomes a `container` target (`parseFileLocationString` returns only `{ path }`), so single-entry/pattern needs structured locations.
- Multipart retry: if the failed `MultipartWriter` implements `ResumableWritable`, the retry calls `getMultipartWriter(key, partSize, { resume: token })` and sends parts from `floor(token.offset / partSize)` (API 3.1.0, api PR #23).
- Local dev against unreleased API: copy the API worktree's `index.ts`, `src` and built `dist` into `node_modules/@flowscripter/pluggable-io-framework-api` (tsc resolves `dist/index.d.ts`, bun resolves `index.ts`).
