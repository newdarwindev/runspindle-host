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

Each owned check starts in its own process group with stdin disconnected, only
stdout/stderr pipes inherited, and a Linux seccomp filter installed before exec.
The filter is inherited by child processes and denies socket creation, connection,
binding, listening and accepting, plus io_uring setup (which could otherwise
submit network operations). Unexpected use kills the calling process with SIGSYS.
A parent may ignore an offending descendant’s exit status: the boundary prevents
network access but does not centrally count every descendant violation. Therefore
the issue’s “fail on any unexpected outbound call” acceptance criterion remains
unmet. Reports explicitly mark `descendant_violation_accounting: blocked` and
`issue_acceptance: incomplete`; a passing test run does not close that gap.
Local anonymous socketpair IPC remains available for Cargo/compiler job control;
no preconnected network descriptor is passed. A socket-creation denial canary must
pass before checks run. Missing libseccomp or an unsupported platform blocks the
suite; there is no unconfined fallback. This is a boundary for these trusted tests,
not a general-purpose hostile-code sandbox.

Every check has a bounded timeout. On completion, failure or interruption, its
process group is killed and reaped so owned helpers cannot linger. A timeout is a
failed check. No global network, service or security setting is changed.

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
