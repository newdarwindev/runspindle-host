# Owned-child shutdown observations

Source candidate for Host #9, stacked on metadata-only #8/PR #11. Rust compilation,
formatting, tests and mutations of this revision are not yet run. It does not
qualify another OS or expand the pinned macOS native transport.

`StdioSession::close_observed` closes the local protocol and returns a cached
`ShutdownReceipt`: the owned child PID, whether termination was requested,
whether the OS kill call succeeded, independently polled child exit status and
whether both local I/O workers finished. PID is descriptive only; no PID lookup,
process discovery, adoption or killing of unrelated processes occurs.

`Child::try_wait` supplies actual OS child status. A successful kill call alone is
not exit. The child-exit and worker-finish polling share a five-second deadline;
there is no blocking Child::wait or join of an unfinished worker. Operating-system
scheduling/system calls and filesystem cleanup are not hard real-time guarantees.
The cached receipt makes repeated close/Drop bounded without another termination
attempt. An unknown result is not silently retried or promoted later.

The existing `close` API delegates to this observation API and still rejects all
subsequent protocol activity. Drop removes the private directory only after an
observed child exit and completed, successfully joined local I/O workers. Unknown
child status, unfinished workers or worker panic retain the directory and existing
evidence for reconciliation. Failed directory deletion also leaves files as the
filesystem permits; no cleanup-success claim is emitted. Detached unfinished
workers and orphaned descendants are not claimed recovered. This owns one direct
child and its local I/O workers, not an OS process tree or cross-process lease.

These observations are never native task completion or turn cancellation. The
metadata bridge still does not authorize turn submission/interruption, and native
protocol identity/pin checks are unchanged. Capture, text and submission remain
unsupported; no replacement host or fallback is selected.

## Authored tests, pending qualified Rust execution

The close/idempotence test uses a request/response barrier before snapshotting the
child ledger. The unresponsive-child test waits for a closed-file marker written
inside its blocking branch, with a bounded deadline and independent alive-child
check, before requesting shutdown. These tests remain unexecuted.

Four added Rust tests use existing stdio scaffolding but observe real disposable
Python child processes: OS exit/readback, idempotent close and no later protocol
effect, unresponsive child termination, unknown observation retention and unfinished
I/O retention. Unknown-exit and unfinished-I/O receipts are deliberately injected
after independently reaping their child, so those fault tests leave no live child.
They are not claims of real kill failures or filesystem failure qualification.

A fourth native mutation removes cleanup eligibility; the independent retained
file assertions must fail behaviorally. The existing mutation runner rejects
successful test exits, compilation failure and missing behavioral failure evidence.
No actual pinned app-server, provider or user session runs in these tests.

Required commands before readiness: `cargo fmt --all -- --check`,
`cargo test --offline --locked`, `cargo clippy --offline --locked --all-targets -- -D warnings`,
`python3 tools/check_native_mutations.py`, and the full mandatory validation entrypoint.
Local Strix source checks with Rust 1.94.1 passed 54 Rust tests, formatting,
all-target Clippy and seven native/bridge behavioral mutations. Exact-image CI
and the full mandatory inventory remain required before readiness. No real pinned
app-server or provider was run. Historical native metadata does not cover this change.


## Fallible worker setup candidate

This follow-up to #9 explicitly depends on the reviewed, unmerged wire-validation
PR #15. It changes only owned-child setup, not the pending merge or CI gate.

Previously, fallible pipe extraction and infallible worker spawning happened
before StdioSession owned the already-started child. Missing pipes or OS worker
creation failure could unwind without an owning shutdown guard. Setup now creates
the owning session first, validates both pipes before starting any worker, and
uses fallible `thread::Builder::spawn`. Every successfully started worker is
immediately retained by that owner.

A setup error calls the same bounded `close_observed` path before returning.
Observed child exit plus joined workers yields the original I/O error and permits
normal Drop cleanup. Unconfirmed shutdown, including a worker panic detected on
join, returns explicit `WireError::ShutdownUnknown` and retains the directory.
Repeated close/drop uses the cached receipt; no retry, PID lookup, PID-based kill
or unrelated process termination is added. Only the original owned Child handle
is terminated. Process abort or unrecoverable allocation failure is not claimed
recoverable.

The binary/platform preparation stage remains separate: failures before any
child exists may remove its newly created directory. Once a child exists, only
the owning session's observed shutdown policy can remove that directory; the
outer spawn wrapper no longer deletes it unconditionally after setup errors.
Native pins, credentials, environment, RPCs and operational activation are unchanged.

Tests use actual disposable Python children and injected worker-creation failures
before the first worker and after the writer has started. They verify the actual
owned OS child handle reports exit, started workers finish/join, failed setup
cannot emit native protocol calls and successful cleanup removes the directory.
Missing stdin/stdout starts no worker. An injected actual worker panic is joined
and leaves a truthful unknown-cleanup error and retained directory; the fixture
then explicitly cleans only after independent exit/join checks. Removing the
setup shutdown call must fail these tests behaviorally. Native app-server startup,
real OS resource exhaustion and actual kill failures remain untested.
