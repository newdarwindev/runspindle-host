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

Queued reconciliation is verified in a fresh process before dispatch. The
independent SQLite peer records zero calls while that operation is queued. A
later dispatch with fresh selected-host presence records one launch, and a
second fresh process reconciles the lost reply by discovery. A separate
component mutation drops the queued guard and adopts a session before launch.
Exact local results are in [queued reconciliation evidence](../docs/queued-reconciliation-evidence.json).

## Ordinary filesystem path requirement

Both Sqlite::open and NativeBridge::open reject an empty raw path, the exact
:memory: token and raw lowercase file: URI prefix before opening SQLite or invoking
a native factory. These entrypoints promise persistent reservation storage rather
than accepting arbitrary SQLite connection strings. Explicit Memory remains the
nondurable test API.

Pinned rusqlite 0.32.1 Connection::open uses OpenFlags::default including
SQLITE_OPEN_URI ([source](https://github.com/rusqlite/rusqlite/blob/v0.32.1/src/lib.rs),
Git blob c3d3228260aba3e3808f3115d4535ef428ec52c4).
SQLite documents that [empty/:memory: names](https://www.sqlite.org/inmemorydb.html)
create temporary/in-memory databases discarded on close. Its
[file URI interface](https://www.sqlite.org/uri.html) additionally permits memory
mode and connection options such as VFS/locking behavior. All raw file: forms are
unsupported by this ordinary-path API, including disk-backed URIs, instead of
implementing a partial URI/options interpreter. A caller needing a literal Unix
filename beginning file: can supply a directory-qualified ordinary path.

Validation examines raw OS-encoded bytes without canonicalization, case folding,
path-component normalization or UTF-8 conversion. Thus ./:memory:, a directory-qualified file: filename, uppercase File:
and non-UTF8 Unix names are not mistaken for SQLite special connection strings.
Their actual validity still depends on the OS and SQLite. Tests for colon and
non-UTF8 filenames are Unix-specific; no Windows native acceptance is claimed.
No filesystem, mount, permission or network settings are changed.

This is not proof that a path resides on permanent media. Temporary filesystems,
external deletion/replacement, symlink races, storage durability and power loss
remain unqualified. The API does not choose or create an approved persistent
location for the caller.

Authored tests independently check the pinned SQLite special-name behavior,
zero factory/database side effects for rejected aliases, file-backed state reopen,
fresh child-process reuse of a literal ./:memory: file with one recorded peer
effect, and ordinary filename compatibility. Two mutation checks remove the
individual durable-entrypoint gates. Scaffold/Python checks are separate from
pending Rust compilation, mutation execution and platform qualification.
