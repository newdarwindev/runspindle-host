# Durable lifecycle library mapping

The reviewed `runspindle.integration.v1` contract at composition
`96299842505ce7662178b7c7b00fd31f6c2754f0` extends the unchanged Runtime v1
binding/observation semantics. `Adapter<V, S>` uses a `Storage` interface;
`Memory` retains previous process-local behavior and `Sqlite` persists state.
`Request.version` continues to require `runspindle.runtime.v1`; the storage schema
has explicit version 1 and rejects other versions. This is a native Rust mapping,
not fixture JSON or native app-server qualification.

A single bounded SQLite state row holds complete bindings, session, sequence/time,
cancellation reservations and pending receipts. A transaction atomically compares
and swaps its generation. Dispatch/cancel intent commits before invoking the peer;
only the successful CAS owner may invoke it. Effects execute outside transactions.
A stale owner cannot commit a reply over newer state. A fresh process reconciles
scheduled/unknown work; it never takes over an effect reservation or retries launch.
The conservative shared generation can return conflicts for unrelated concurrent
mutations; callers may refresh/reconcile, never blindly repeat effects. Bounded
liveness under sustained writer contention is not qualified.

Bounds: at most 1,024 operations, 4,096 receipts, a 16 MiB serialized state and
8,192 SQLite pages. Store configuration is immutable on reopen; there is no
retention eviction or power-loss/filesystem qualification. `try_drain` commits
receipt removal atomically; legacy `drain` fails closed with a panic on storage
errors. Shutdown is local to an adapter instance. Operational activation stays
blocked; no host service, credential resolver, real native transport or capture
capability is introduced.

The existing lifecycle negative suite covers authority, unsupported versions,
identity/epoch/session preservation, stale observations, ambiguity, capacity and
cancel/terminal races. Durable tests add fresh-process interruption during launch
and cancel, concurrent process owners, persistent receipts, pre-dispatch cancel,
reopened observation fences and stale CAS owners. The independent SQLite peer
ledger makes duplicate effects fail and records actual call order. Mutation runs
remove dispatch deduplication and the session fence in disposable source copies;
only behavioral assertion failures count. See `docs/durable-lifecycle-tests.json`.
