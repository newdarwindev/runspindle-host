# Lifecycle evidence

This original Rust adapter executes process-local lifecycle coordination against a
scripted peer; it is component evidence, not native compatibility, durable recovery,
isolated integration or live acceptance. The old Python fixture checks remain
synthetic format evidence. No external/native service or paid provider was used.

Run `python3 tools/check_lifecycle.py`. Rust/Cargo 1.94.1 and Python 3.9.6 were
used. The suite runs offline with a repository-local Cargo home. Mutation checks
compile disposable copies and assert three selected tests fail for semantic
changes; temporary source/build directories are deleted on success and failure.
All tests create no background processes or services. No CI workflow was changed;
remote execution remains unverified.

The exact hashes, source base, contract pin and results are recorded in
[local-lifecycle-tests.json](local-lifecycle-tests.json). The final reviewed source
commit and merge are recorded by the composition ledger/PR.

Covered invariants include offline/unknown/stale pins, forbidden failover, duplicate
launch prevention, accepted launch/lost reply, absent/ambiguous discovery, exact
session/binding preservation, stale/reordered/unsupported/oversized observations,
independent cancellation stages/completion races, denied authority, queue/storage
bounds, explicit rejection, effectful peer panics, shutdown and fail-closed native
activation. Peer call count/order and independent accepted-effect counts are
asserted; three mutations break blind-retry, native-session and cancellation
terminal invariants and are detected.

Remaining #1/#2 criteria: actual public app-server transport/version pin and native
protocol conformance; session/turn/item/tool message negotiation; real macOS/Linux
capability qualification; durable storage and fresh-process cancellation/recovery;
complete wire sanitization and raw input parser; capture/text transfer/submission
selection. Shared native semantic mappings are agreed but JSON wire conformance
is unclaimed. These gaps keep the full issues and operational acceptance blocked.

The new native metadata transport candidate is specified in
[the stdio mapping](../specs/native-stdio.md), with evidence in
[native-transport-tests.json](native-transport-tests.json). Empty native thread
creation/readback is separate from task execution and durable continuity.

The native session preparation transport is now connected to the existing durable
lifecycle by [NativeBridge](../specs/native-bridge.md). Actual native metadata
evidence includes a real returned thread ID persisted under exact Binding in
SQLite and read by a separate process. Scripted turn/item/restart behavior remains
separate; task execution, task terminal propagation and bridge interrupts remain
unsupported.
