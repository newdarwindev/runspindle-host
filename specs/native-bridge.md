# Native session preparation and durable identity bridge

Relates to #8, the existing durable Peer contract, and desired-intent.md's native
identity/uncertainty obligations. `NativeBridge<I, C>` implements that existing
Rust Peer over a separately pinned `StdioSession` or explicitly scripted NativeIo.
It submits no prompt and has no turn/start port. It does not qualify task execution.

## Authority and identity

Construction compares the complete incoming Binding with separately trusted local
host/workspace configuration (`LocalPin`) and requires an explicit
`Verifier::verify(binding, Action::TakeOwnership)` grant before touching SQLite or invoking the
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


## Durable native transport ownership

`open` requires explicit `TakeOwnership` authority even for an initial owner. The
verifier is checked before database/factory access and again after acquiring the
each SQLite writer lock, including immediately before factory invocation after
the separately committed ownership claim. Revocation while the second lock waits
returns Denied without invoking the factory or restoring the previous owner.
It is a trusted local verifier, not peer-provided authority.
A caller cannot supply an owner generation or use takeover to alter any Binding
field. The new `host_native_owner` table allocates monotonically increasing owner
generations for each exact operation binding, separately from immutable lease
identity and journal event generations. Owner rows are bounded to 1,024.

A new authorized open replaces the previous owner. Ownership is committed before
the lazy factory: a failed/crashed factory does not restore stale ownership.
Factory creation, thread/start, thread/read and notification consumption execute
only while an IMMEDIATE transaction verifies the owner and excludes competing
ownership replacement. State writes also check ownership in their transaction.
Existing live bridge objects fail closed after takeover; they cannot read, consume,
launch or save state. A stale object's cleanup may still terminate its own child.
It cannot terminate another owner's child or authorize a turn interrupt.

The native call is intentionally inside this **separate native-journal fence**,
not the generic Adapter storage transaction. Each callback must be bounded and
must not reenter or wait for another writer on this database. The pinned stdio
transport already supplies bounded RPC and shutdown deadlines. Arbitrary injected
ports are trusted code and are not made bounded by this API. SQLite contention
has a five-second busy timeout. This is a local single-database serialization
contract, not a distributed lease or cross-host authority service.

Launch and receive reservations are committed before the fenced external call.
A process crash, commit failure or takeover after I/O cannot roll back an external
effect or erase that uncertainty. The new owner reconciles a previously saved ID
and never repeats start; a missing ID/receive fence remains Unknown. A takeover
alone does not qualify recovery of lost replies or notifications. Task execution,
capture/text/submission, actual OS capability qualification and live acceptance
remain blocked. Deployments must stop old pre-fence processes before upgrading;
old binaries do not understand the ownership table.

Tests include independently recorded peer effects, rejected takeover before any
factory/database access, stale-owner calls after takeover, failed factory recovery,
and competing SQLite-writer probes inside factory/start/read/notice callbacks.
The existing fresh-process crash/recovery suite now exercises owner replacement.
A deliberate mutation removing owner comparison must fail the stale-owner test.
These are synthetic peer/component tests with real SQLite, not new native-peer
qualification.

The owner-fencing follow-up also tests revocation during an actual contended
factory-lock wait, full-open third-check denial with retained owner generation,
and a concurrent child process holding factory ownership while the old process
attempts launch. Independent effect counts must remain zero. Removing the final
factory authority check is a separate required behavioral mutation failure.

## Explicit authorized text-turn source candidate

A separate native-turn.v1 request can submit one bounded text input to the already
bound thread. It is not an Admission request or a grant inherited from session
preparation. The injected exact-request verifier defaults to denial and is
rechecked inside the existing ownership transaction after lock acquisition,
immediately before turn/start. Input resolvers are trusted bounded local callbacks,
not an installed input store: only the content address and byte length are retained.
The bridge reads at most 2,049 bytes, checks the exact nonempty UTF-8 bytes against
the reference and SHA-256, and never writes raw input to its SQLite journal.

The closed profile requires an explicit restricted-syntax model identifier and
maps only text input, approvalPolicy=never and readOnly/networkAccess=false.
These are requested protocol settings, not proof of OS or tool confinement.
The pinned upstream schema says these model/approval/sandbox overrides also apply
to subsequent turns. Inherited plugins/tools are not disabled or qualified by this
profile. The returned ID is an acknowledged native turn identity, not proof that
a new turn rather than an existing active turn was created or that work completed.

A durable request reservation commits before the external effect. Lost replies,
process crashes, failed saves and post-reservation denial preserve uncertainty;
repeated or reopened requests never repeat turn/start. An exact acknowledged
request returns its persisted identity without rereading input or invoking the
peer. Changed request data conflicts. The journal refuses submission with a prior
observed turn, terminal, pending receipt or receive fence; this cannot prove the
remote peer has no unobserved active turn. A successful response persists the exact
turn ID alongside the full authorized request. Task-terminal promotion and
cancellation remain unsupported in this slice; unsolicited notices do not grant
task authority.

The raw request decoder is canonical-only, with sorted object keys and compact
UTF-8 JSON, no duplicate or additional members at any depth. Version, seven-field
Binding, thread ID, input reference and profile are all explicit. Input length is
1..2048 bytes; SHA-256 is exactly 64 lowercase hexadecimal characters. The input
reference contains no URL, file path or credentials. Request envelopes and actual
encoded native requests each have independent 4096-byte bounds; JSON escaping can
make an otherwise valid input exceed the native bound, yielding no send and an
uncertain reserved operation. No arbitrary upstream override is passed through.

Old journal rows without task_turn remain readable. Pre-feature binaries reject
rows containing task_turn via their existing deny_unknown_fields behavior;
deployments must stop old writers before using this source candidate. No schema
migration, history upgrade, input-store availability, live/provider execution or
OS capability qualification is claimed. Synthetic tests use real SQLite, a
fresh-process crash and independent effect counts; exact-source Rust execution
and independent review remain required.

Public injected NativeIo implementations receive a non-cloneable TurnPermit by
value for task effects. Its fields/constructor are private outside the crate and
it is minted only after reservation and final exact authority verification; direct
external StdioSession trait calls cannot manufacture a task dispatch capability.
This is a safe-Rust API boundary, not containment of malicious trusted adapters or
unsafe code. Input access also rechecks exact authority after state/owner reads and
immediately before resolver opening, so a SQLite wait cannot preserve an expired
input grant. A regression observes an actual SQLite busy callback during a blocked
read, revokes authority before releasing the lock, and independently counts zero
resolver opens. A compile-fail public API test rejects external permit construction.

## Separately authorized durable turn cancellation candidate

This PR #18-dependent source slice defines runspindle.native-turn-cancel.v1:
version, the complete immutable native-turn request, and its acknowledged turn ID.
Canonical JSON remains bounded and closed. A distinct CancelVerifier defaults to
deny; neither Admission/session authority nor permission to submit authorizes
cancellation. Only an already persisted authorized task request and acknowledged
turn ID can be targeted. Unsolicited metadata or a lost start reply cannot supply
that binding. Changed content/profile/thread/turn/lease targets reject.

The bridge persists an uncertain cancel reservation before acquiring the existing
ownership fence for interrupt. It rereads the complete persisted target under the
writer lock and rechecks exact cancellation authority immediately before the
effect. Public NativeIo dispatch consumes a non-cloneable CancelPermit whose fields
and constructor are inaccessible externally. The former raw public StdioSession
interrupt is crate-internal, eliminating a direct ID-only API bypass. Public API
compile-fail tests cover both restrictions.

At most one interrupt is dispatched for the request. Timeout, process crash,
post-reservation denial and failed acknowledgement persistence never authorize
retry. Exact repetition/reopen returns stored acknowledgement or Unknown without
another interrupt. Acknowledged means protocol acknowledgement only; it changes
neither native terminal metadata nor generic Adapter execution.

Existing observed terminal metadata wins before cancellation and after a racing
acknowledgement; completed/failed is not rewritten as cancelled. ObservedTerminal
explicitly means the retained local native metadata, not independent task
completion/acceptance authority or current native freshness. The bridge does not
poll to invent terminal evidence and cannot exclude an unobserved remote race.
A receive fence stays Unknown. Generic Peer.cancel remains disabled, so a generic
Binding grant still cannot invoke this effect.

Old task reservations without cancellation deserialize unchanged. Older binaries
reject newly populated cancellation records; old writers must be stopped before
any future deployment. No actual migration, qualified native cancellation,
production grant provider or runtime activation is claimed. Synthetic tests use
independent SQLite effect counts, actual writer contention/revocation with observed
busy callback, and fresh-process post-effect crashes. Existing native transport
tests independently check object-shaped interrupt acknowledgements. New mutations
remove exact dispatch authority, reservation, no-retry and acknowledgement/terminal
separation. Exact-source Rust checks and independent review remain required.
