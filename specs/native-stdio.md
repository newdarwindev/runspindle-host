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

Initialization requests experimentalApi=false and requires the pinned version in
the returned user-agent, then emits initialized. This is version negotiation, not
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

The existing durable Adapter remains authoritative for task/binding/host/workspace/
lease admission and uncertainty. This transport is deliberately not yet connected
as its Peer: empty thread creation cannot stand for task execution, and app-server
thread/start supplies no qualified operation-id discovery key. Losing the native
launch reply before persisting the ID remains unresolved; creating another
connection does not authorize relaunch. Native durable binding, lost-ack recovery,
fresh-process native reconciliation and turn/item/terminal/cancel races remain
blocked. Existing SQLite scripted-peer tests do not satisfy those native gates.

Reproduce scripted tests and mutations with `python3 tools/validate.py` in the
prepared isolated lane. For the opt-in macOS metadata probe, build with
`cargo build --offline --locked --example native_probe`, then invoke
`target/debug/examples/native_probe <pinned-codex-path> <fresh-private-directory>`
inside an outbound-denied sandbox. This creates no turn and sends no prompt.
Do not run it against a real home or workspace. The sanitized probe reports only
ID length and equality; it never prints IDs, content or process logs.
