# Native session preparation and durable identity bridge

Relates to #8, the existing durable Peer contract, and desired-intent.md's native
identity/uncertainty obligations. `NativeBridge<I, C>` implements that existing
Rust Peer over a separately pinned `StdioSession` or explicitly scripted NativeIo.
It submits no prompt and has no turn/start port. It does not qualify task execution.

## Authority and identity

Construction compares the complete incoming Binding with separately trusted local
host/workspace configuration (`LocalPin`) before touching SQLite or invoking the
lazy transport factory. Reopened operation rows must match every Binding field;
changed task/revision/policy/host/workspace/lease cannot reuse the row. A different
operation may have its own bounded row but cannot discover another operation's
session. Existing Adapter Verifier and dispatch-time checks remain authoritative.
This comparison is not transport authentication or a production grant resolver.

For this explicitly partial mapping, Peer Accepted and Adapter Acknowledged mean
only that an empty native session was prepared and its observed ID durably bound.
They do not mean task running, task completion, validation, acceptance or delivery.
The existing generic lifecycle.md's `terminal=None`/`observed_running` terminology
is limited here to session observation; full Runtime v1 execution mapping remains
unqualified. Bridge observations always carry terminal=None. A native turn outcome
is retained separately as historical metadata and cannot promote the operation to
Terminal(Completed), Failed or Cancelled. Observed turn feedback also cannot grant
permission to interrupt that turn: bridge Peer.cancel returns LostReply with zero
native interrupt calls. The low-level transport's explicit thread+turn interrupt
API remains a separately tested boundary, not enabled by this bridge.

## Durable effects and recovery

The existing host_state table and schema are unchanged. A separate version-1
host_native_state table in the same actual SQLite database stores exact Binding,
returned native thread ID, one observed native turn ID, up to 128 observed item IDs
and their start/completion state, a native turn outcome, generation, local
observation counter/time, receive fence and pending sanitized observation.
Rows are limited to 1,024, serialized records to 65,536 bytes and the database to
8,192 pages. It uses WAL, synchronous=FULL and generation compare-and-swap; no
power-loss/filesystem or sustained contention qualification is claimed.

Adapter persists Scheduled before calling Peer. Bridge commits a launch reservation
before thread/start and commits the actual returned native ID before returning
Accepted. A reservation with no thread ID is uncertain, never proven absent.
Reopening or calling launch again cannot issue another thread/start. A saved ID can
be read using thread/read after restart; the ID must match exactly. If the native
start reply was lost before the ID was saved, there is no qualified operation-key
lookup in this protocol mapping. Discovery returns no observation, Adapter retains
Unknown, and the reservation forbids relaunch. Rejected/unavailable thread/read
(including an ephemeral thread lost with its server) likewise leaves Unknown and
preserves the prior ID. A fresh process does not invent a replacement session.

The observation sequence is a durable **local host observation counter**, supplied
by this Peer; it is not a native notification sequence. Clock values are injected
trusted host configuration, checked against retained time. The caller must retain
a comparable clock domain across processes. The metadata probe uses an explicit
synthetic trusted clock and makes no native freshness claim. No RPC field, authority
field or native ordering guarantee is manufactured from arrival order.

## Notifications and receipts

The pinned transport parses only supported thread/turn/item notifications into
sanitized typed metadata. Every notice carries its real thread/turn/item IDs and is
checked against the retained thread. Turn IDs cannot change within an operation;
items require that turn, completion requires a prior start, duplicates do not
advance state and reopened/completed identities cannot be reopened by stale events.
Only pinned inProgress/completed/failed/interrupted statuses are mapped. The single
observed turn is metadata, not a separately authorized task-turn binding.

Before consuming a native notice, the bridge commits a receive fence. It persists
the resulting metadata plus pending lifecycle observation before delivery. A
same-process SQLite save/CAS failure retains the typed notice for retry. A crash
between receive and commit leaves the durable fence: reopening returns Unknown,
without fabricating notification replay or a terminal result. This conservative
fence also applies if a crash happened before an empty receive could be cleared;
there is no automatic fence reset without qualified reconciliation.

Pending receipt delivery stops further notification consumption. Adapter receipt
backpressure leaves the native journal record pending across restart. If Adapter
commits an observation but the bridge acknowledgement is lost, exact stored
session/sequence/time/state readback reconciles that handoff without another native
effect. Only metadata Progress is delivered; no observed turn grants task terminal
or cancellation authority. A poisoned/closed stdio stream cannot drain queued
notices as newly accepted observations.

## Evidence limits

`tests/native_bridge.rs` exercises actual SQLite and fresh OS processes with a
separate scripted peer call/effect database. Crash after native start, saved reply
before Adapter acknowledgement, consumed notification before commit, and blocked
cancellation after unsolicited turn observation repeat twice. SQLite save failure,
receipt backpressure, exact pins/bindings, foreign IDs, duplicates and reordered
metadata have negative tests. Three bridge mutations must fail behaviorally.

The opt-in native_probe now exercises actual pinned stdio through NativeBridge and
Adapter into SQLite, then reads both journals from a separate child process. Actual
native evidence is empty-thread preparation/readback only, repeated twice under
outbound denial. Actual task/turn/item execution, authorized cancellation, native
restart recovery, native ordering/freshness, provider use, transport authentication,
additional platforms, live acceptance and installed-binary/source correspondence
remain blocked. Scripted native turn metadata is not actual native turn acceptance.
