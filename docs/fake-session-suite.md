# Offline fake native-session acceptance

After provisioning Rust, Cargo dependencies (`cargo fetch --locked`), Python 3,
and Linux `libseccomp.so.2`, run from the repository root:

```sh
python3 tools/check_fake_sessions.py
```

This is synthetic component evidence, not native protocol conformance or live
acceptance. No native executable, account, credential, fleet or provider is used.
The same command covers synthetic macOS and Linux capability profiles; it does
not assert that either operating system has passed native qualification.

## Isolation and failure behavior

Each workload check starts in its own process group with stdin disconnected and
no preconnected network descriptor. A disposable Linux supervisor installs an
inherited seccomp user-notification filter before exec. Socket creation,
connection, binding, listening, accepting and io_uring setup are intercepted.
The supervisor records each attempt and always responds EPERM; it never continues
a syscall. A caught exception or a parent swallowing a descendant failure cannot
turn a violation into a passing check. The guard returns 125 for violations or
notification errors. The supervisor is a process-local child subreaper and waits
for all descendants, including orphaned or detached children, before reporting.

Local anonymous socketpair IPC remains available for compiler job control and
the guard's descriptor handshake. Notification descriptors stay in the supervisor,
not the workload. A denial canary must pass first. Missing Linux/libseccomp API 6+
notification support blocks execution, without an unconfined workload fallback.
This is an accidental-network-use boundary for trusted tests, not a hostile-code
sandbox: malicious code installing replacement filters is outside its contract.

The guard-control tests are part of the trusted supervisor/control plane. They
run separately from the guarded Python workload partition because Linux rejects
stacked notification listeners. They launch only synthetic fixtures through the
guard and assert denial, swallowed-failure detection and process cleanup. The
suite accepts exactly the checked-in control driver command for this exception;
all other workload checks are guarded. Nested lifecycle-owner cleanup uses the
already inherited filter rather than stacking another listener.

Every check has a bounded timeout. Each owns a disposable temporary root exposed
through TMPDIR/TEMP/TMP. After its supervised descendants exit, that root is
removed on success, failure, timeout and cancellation; nested checks inherit
the outer root. A completed/reaped leader is never signalled by numeric process
group ID, so cleanup cannot target a later reused identity. Stable pidfds are
used for test-only fallback cleanup.

Every check has a bounded timeout. On cancellation, TERM propagates to nested
owners before a bounded KILL fallback. The supervisor separately identifies its
own descendants, verifies ancestry after opening stable pidfds, kills/reaps them
(including detached sessions), and never targets unrelated processes. This needs
Linux pidfd support and readable /proc process metadata. No global network,
service or security setting is changed.

## Versioned scenarios and evidence

`fixtures/fake-session-suite.json` is the versioned scenario manifest. Seed 0
identifies fixed deterministic scripts, not stochastic sampling. Every selector
must exactly match an enumerated Rust test and must appear as successful in the
Rust run; a rename, deletion, ignored test or zero-test run cannot satisfy it.
The full Rust and Python suites also run, including three deliberate lifecycle
mutations that must be detected. The manifest covers offline/stale pins, ambiguous
launch, exact-binding reconnect, distinct cancellation states, bounded queues,
shutdown, receipt correlation, frame validation and crash recovery.

The ignored local `.verification/fake-session-suite.json` records source revision,
dirty-tree flag, manifest and fixture SHA-256 hashes, fixed seed, profile names,
scenario/check results, command-output hashes, timestamps and evidence class.
Retain this report with the check log when reviewing a revision; a dirty-tree run
is development evidence, not exact-commit acceptance. The report is not uploaded.
The prior report is removed at startup. Early manifest, confinement or inventory
failures exit nonzero without publishing a success report.

## Separate native lane

`native_protocol_conformance` remains `blocked` and `live_acceptance` remains
`not_run` in every fake-suite report. Existing native protocol fixtures are
synthetic. A pinned supported native build and explicit host capability inventory
are still needed for a separate native conformance lane. Capture, text transfer
and destination submission are not inferred or authorized by this suite.
