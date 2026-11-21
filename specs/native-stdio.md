# Pinned app-server metadata transport candidate

Relates to #8 and desired-intent.md's native identity and bounded transport
obligations. This candidate is stacked on the hygiene head; no composition pin
changes are qualified by it. `native::StdioSession` speaks the non-experimental
stdio JSON-lines protocol for Codex 0.169.0, release source
`d32063f96c2dff1dc96af839b3952abb6acfeda4`. `docs/native-protocol-pin.json`
records selected generated-schema hashes and the observed local executable hash.
The source correspondence of the installed executable has not independently been
verified. Other binary/platform identities fail closed, including Linux native
acceptance. The Rust/scripted suite is portable to the isolated Linux lane.

The executable is copied into a newly created private directory and hashed before
launching those copied bytes. An existing directory is rejected. The process gets
an empty environment and fresh HOME/CODEX_HOME and working directory, with analytics
and feedback disabled. No real workspace, credentials, provider configuration or
history is read. Callers must use a separately controlled outbound-denied sandbox
for native qualification; configuration alone does not enforce network isolation.
The library adds no host service or operational activation.

Initialization requests experimentalApi=false and requires the exact pinned
version in the leading user-agent product token before emitting initialized.
Versions embedded in another build version or in trailing metadata fail the
handshake. The product format follows the [pinned Codex user-agent builder](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/login/src/auth/default_client.rs).
This is version negotiation, not
a claim that unsupported runtime capabilities are available. The only public
methods create an empty ephemeral thread, read its metadata, and interrupt an
explicit native thread/turn pair. There is no prompt/turn submission, arbitrary
RPC, capture, text transfer or human input capability. A successful interrupt is
an acknowledgement; only independently observed terminal lifecycle evidence could
establish completion. An empty thread is not execution of an authorized task.

Input lines including newline are bounded to 65,536 bytes, output to 4,096 bytes,
incoming frames and pending notifications to 32 each, with one request in flight.
Read and write responses have five-second deadlines. Unsupported server requests,
foreign/duplicate reply IDs, recursively duplicated JSON members,
malformed/oversized frames, overflow and uncertain
transport results close the connection; no method automatically retries effects.
Close/drop kills and reaps the child and removes the disposable directory.
Notification decoding returns only native thread/turn/item IDs and rejects a
foreign thread. Raw content is not returned. The app-server lacks the trusted
sequence/freshness fields expected by the lifecycle Event contract; local arrival
order is not promoted to native freshness or an authoritative event sequence.

The [durable identity bridge](native-bridge.md) now connects this transport to the
existing Adapter as a session-preparation-only Peer. It persists actual native
thread IDs under exact Binding and configured local host/workspace pins, retains
bounded observed turn/item metadata, and fences uncertain launch/receive state.
It cannot submit a task turn, promote a task terminal result or authorize native
interrupts from observed metadata. A missing native start ID or unavailable
thread/read remains Unknown across restart with no relaunch. The bridge's local
observation counter is not a native event sequence. Native execution and broader
acceptance remain blocked.

Reproduce scripted tests and mutations with `python3 tools/validate.py` in the
prepared isolated lane. For the opt-in macOS metadata probe, build with
`cargo build --offline --locked --example native_probe`, then invoke
`target/debug/examples/native_probe <pinned-codex-path> <fresh-private-directory>`
inside an outbound-denied sandbox. This creates no turn and sends no prompt.
Do not run it against a real home or workspace. The sanitized probe exercises NativeBridge, actual SQLite and a separate child
journal reader; it reports only ID length and equality. It never prints IDs,
content or process logs.


## Reply and notification validation

The wire-validation follow-up to #8 extends this same transport, without adding
RPCs or broadening authority. For an expected numeric response ID, exactly one
of `result` and `error` must be present. An explicitly present null result remains
a generic RPC result; it is distinct from a missing result. The interrupt method
requires an object result as specified by the pinned `TurnInterruptResponse`.
Its successful acknowledgement still does not establish task completion.

Error responses require the pinned signed-64-bit integer code and string message.
Error text/data is never returned or logged. Malformed/ambiguous errors close the
connection rather than becoming an authoritative rejection. Contradictory method,
params, result or error envelope combinations are rejected by the host profile;
unrelated additional object fields remain allowed. This is a deliberately stricter
unambiguous-envelope profile, not a claim that generated JSON Schema forbids all
additional fields.

The immutable reviewed upstream schemas at source
`d32063f96c2dff1dc96af839b3952abb6acfeda4` are:
- [JSONRPCResponse](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/app-server-protocol/schema/json/JSONRPCResponse.json), Git blob `9f1ec2954852de4787547f1c96ebb532a911a149`.
- [JSONRPCError](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/app-server-protocol/schema/json/JSONRPCError.json), Git blob `6db5d1a7fa557a813332561218b00224209ec553`.
- [TurnInterruptResponse](https://github.com/openai/codex/blob/d32063f96c2dff1dc96af839b3952abb6acfeda4/codex-rs/app-server-protocol/schema/json/v2/TurnInterruptResponse.json), Git blob `5d8a0f9ce22e0387218fd390c362de6a61247b8c`.

Both notification consumers now use the same typed parser and stream-poisoning
path. `take_notification` returns only validated identities and can drain an
already received unsolicited frame just like `next_notice`. Unsupported events,
foreign identities, malformed status or hybrid reply/notification envelopes cannot
leave the legacy identity-only consumer usable for another effect. The standalone
`notification_ids` helper remains an identity extractor, not a lifecycle-status
validator; live consumers use the stricter typed parser.

Synthetic subprocess tests separately record received method calls and verify
there is no second effect after malformed input. Positive tests preserve null
versus missing generic results and exact valid thread/turn/item IDs. Deliberate
mutations remove ambiguity and interrupt-shape checks. Native peer execution,
actual cancellation, OS capability qualification and broad issue acceptance remain
unqualified; historical native evidence is not rerun by these changes.

## Observed reader failure before reply acceptance

This follow-up depends on PR #16 and retains the existing fail-closed stream
contract. A reader failure (including malformed/oversized frames, overflow or EOF)
may occur after a valid response has been queued. RPC consumption now checks the
reader's poison flag after receiving a frame and before interpreting it as a
response or notification. A success queued before an already observed failure
cannot become a fresh accepted identity or interrupt acknowledgment. The call
closes with uncertainty and never retries an effect. Even a normal EOF ends this
persistent session; a buffered reply is not an exception to observed closure.

This is an observed-state check, not an atomic continuous-failure guarantee.
A failure that occurs after the acceptance check can still follow an accepted
reply. No native freshness, lost-reply recovery or terminal authority is added.

A test-only completion relay holds the RPC consumer until the real reader has
observed a synthetic child's malformed frame, oversized frame, queue overflow or
EOF after a valid reply. The actual child ledger independently records one
thread/start and no retry. A healthy reply remains accepted. A deliberate mutation
removes the consumption check. These source tests require exact-source Rust
verification; no native/provider or CI qualification is claimed.

## Thread identity observed before the start reply

For Host #8, the `thread/start` caller validates every `thread/started` frame
it buffered while waiting for that reply. Each must carry an ID that exactly
matches the reply's thread ID. A malformed or different identity closes the
transport before the caller accepts a thread ID. The bridge treats the start as
uncertain and its durable reservation prevents another start. A valid reply
can still identify a thread when no such notification was observed. Matching
notifications remain available to consumers, as do notices already buffered
before the request. Later arrivals remain subject to the ordinary notice checks.

The regression uses a real subprocess with scripted JSON and an independent
method ledger. A contradictory or malformed ID must produce one start and zero
read or retry effects. A second test checks that two successful exchanges keep
their previously buffered notices. The new mutation bypasses this validation.
These tests cover the window before the matching reply. Recovery from a lost
native reply and conformance of the actual pinned peer require separate evidence.

The local verification uses base `0ce30ad3dee6b51bc35eec9c716d93f310faa9bf`,
Rust and Cargo 1.94.1, Python 3.13.18, and a macOS arm64 host. Run the Rust
suite and native mutation check with cached lockfile dependencies:

```sh
cargo test --locked --offline --all-features
python3 -B tools/check_native_mutations.py
```

The regression failed on the base transport by accepting the contradictory
ID. With the change, 110 default-feature and 111 all-feature Rust tests passed,
and all 11 native mutations failed behaviorally. Rust formatting, all-target
all-feature Clippy, Ruff and scaffold validation passed. Python unittest passed
94 tests and skipped 15 on macOS. This is scripted component evidence.

```text
1d73174df98c69aceeccaf70fd6e71d337ff83acdc7f32b44ad415bb88ba225d  Cargo.lock
0f7f9a2f81b9da9e7f5de688588287193020589facd5565092f2baa0f91d32ac  src/native.rs
10add14e8d59e4c85d903c576968ddd114953c8c2c3aeb6cea26645debdf884f  tools/check_native_mutations.py
```

## Thread readback identity

`thread/read` checks supported lifecycle notices that arrived before its reply.
A notice naming another thread or carrying invalid lifecycle metadata rejects
the read and closes the session even if the reply names the requested thread.
Earlier queued notices and matching notices stay available in queue order. A
successful read returns thread identity only, with turn and item observations
still available through the notice consumer.

Scripted subprocess tests check the independently recorded `thread/read` count,
closed-session rejection, and the order of retained notices. The mutation suite
removes the pre-reply notice check to exercise the negative regression.
