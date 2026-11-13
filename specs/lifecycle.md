# Process-local lifecycle adapter

`src/lifecycle.rs` implements a bounded Rust coordinator over injected `Peer`
launch/discover/cancel ports. `tests/lifecycle.rs` contains an independent scripted
peer with call-order and observed-effect ledgers. It opens no network connections,
processes or native sessions. Public activation and operational gates still fail
closed. The component has no dependencies and `Cargo.lock` locks its package.

## Runtime contract mapping

The semantic reference is composition `4b700269be9a4feab7b955faf0229fbdd77d8aa9`,
`specs/runtime-v1.md` / `contracts/runtime-v1.json`, version
`runspindle.runtime.v1`. This native API is not a JSON serializer and does not
claim wire parity with the fixture schema or native app-server protocol.

`Binding` maps the seven identically named contract fields. `Request` carries
version and binding; unknown versions, non-ASCII/malformed/oversized identifiers,
zero revision/epoch reject before dispatch. The injected `Verifier` validates the
complete binding and action; `Adapter::denied` defaults to denial. Synthetic tests
supply a verifier bound to exact policy, operation, host, workspace and epoch.
No credentials or production grant resolver exists. Inputs/presence/events must
come from the trusted adapter boundary; arbitrary callers are not authenticated.

`Observation.native_session_id`, sequence and observed time come from the peer;
the adapter never generates them. `terminal=None` maps `observed_running`, and
`Some(Completed|Failed|Cancelled)` maps the respective observed terminal outcomes.
`Execution::Acknowledged` is adapter acknowledgement, never terminal success.
`Queued`, `Rejected` and `CancelledBeforeDispatch` are additional local states;
pre-dispatch cancellation has no native terminal observation and retains
`Cancellation::Requested`. `Cancellation::None` maps `not_requested`.

Scheduling retains the binding before entering an effectful peer call, including
if a peer panics; a scheduled/unknown operation cannot launch again. A lost reply
requires discovery. Zero or multiple discoveries produce unknown and never
permission to retry. Exactly one complete matching observation may bind. No
`proven_absent` retry is implemented in this conservative subset. Reconnect cannot
change the observed session or any binding field. Future/stale observations and
non-increasing sequences reject. `max_age_ms` is explicit trusted configuration;
`now` is an injected trusted monotonic clock value, never peer time. Offline,
unknown, mismatched, future, expired or stale presence queues the selected pin.

Cancellation request, acknowledgement and observed terminal are separate. A lost
cancel response cannot retry; after an unknown launch is discovered, an unsent
cancel may be dispatched once. Completion/failure racing cancellation preserves
the actual terminal outcome. Identical terminal events are idempotent; conflicting
terminal or later running events reject without changing the accepted terminal.

## Bounds and lifecycle

Caller selects 1..1024 retained operation records and 1..4096 queued receipts.
Known duplicates reconcile at capacity; new identities reject without eviction.
Receipt capacity is checked before effectful peer calls and before observation
mutation. Backpressure rejects a terminal event without dropping its state:
transport must retain/retry the event after draining. Native structured events are
trusted size-accounted inputs, limited to 4096 encoded bytes; zero/oversized and
unsupported kinds reject. This is not raw network parsing or a byte-size proof.
Receipts contain metadata only, never peer content, hidden reasoning or raw
text. Capture, text transfer and submission remain explicitly unsupported in
both scripted macOS and Linux capability profiles. They are not native support
claims. `shutdown` stops new mutations/effects; snapshots/draining remain local.

All adapter state is process-local and single-owner (`&mut self`); no disk
persistence, multi-process uniqueness, crash recovery or restart during
cancellation is claimed. A peer panic retains intent within the surviving
adapter, but a fresh adapter has no history. Production restart safety, native
session/turn/item/tool conformance, actual capability negotiation and trusted
transport/sanitization remain blocked. Issues #1 and #2 remain open.
