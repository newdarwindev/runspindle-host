# Host #1 bounded acceptance audit

This audits the original offline fake-session issue, not Host #2/#8/#9 or a
native, composition or production acceptance claim. Implementation source and
fixture/dependency hashes are in [the exact receipt](fake-session-qualification.json).

## Reproduction

Provision the locked Cargo dependencies, Rust 1.94.1 and Python 3.12, then run:

```sh
python3 tools/check_fake_sessions.py
```

The command enforces inherited network denial, deterministic synthetic peers,
actual disposable SQLite, bounded process groups and owned scratch cleanup.
A missing Linux seccomp/pidfd prerequisite fails closed. macOS/Linux profiles
are synthetic data; the executable qualification host was Linux x86_64.

## Complete criterion mapping

| Original requirement | Implementation and independently observable evidence |
|---|---|
| Versioned fake transport, IDs, event sequences, capabilities | `src/native.rs` scripted stdio peer tests cover request ID, session/turn/item correlation, malformed/oversized frames and poisoned connections; `src/lifecycle.rs` versioned envelopes and `tests/lifecycle.rs` independent call/effect ledgers. No native executable is started. |
| macOS/Linux profiles, offline/unknown/freshness | `Capabilities::scripted` for both profiles; H01 stale/future/expired/wrong workspace/host observations queue without a launch; H09 explicitly unsupported capture/text transfer/submission. |
| launch, rejection, lost response, reconnect, absent/ambiguous discovery | H02-H04 and explicit rejection/duplicate/conflict/panic tests retain original binding and independently count launches. Fresh-process `native_bridge` tests use actual SQLite and scripted wire peers. |
| cancellation requested/acknowledged/terminal and races | H05-H07 plus `tests/native_cancel.rs`: no acknowledgement is mistaken for terminal, no repeated interrupt after a fresh-process crash, out-of-order/mismatched observations rejected. |
| malformed/oversized/unsupported/reordered events, backpressure, shutdown, sanitization | H08-H13, native frame/notification tests, exact receipt binding/canary rejection and terminal queue recovery. |
| offline acceptance, process and temporary cleanup | 15 required selectors must exist and actually pass; independent effects/calls prove queue/reconcile behavior. 21 guard controls verify denied descendant calls, timeout, success/failure cleanup, cancellation including nested/detached descendants, and no signal after a process identity is reaped. |
| deliberate invariant breaks | Three lifecycle mutations (blind retry, session rebinding, cancellation-as-terminal) and network-accounting mutation are detected. The two new no-signal tests also independently fail against the prior cleanup implementation. |
| separate native protocol lane | Remains explicitly **blocked**. The issue requests it separately when available; fake parity does not qualify native behavior. No account, fleet, native user session or paid provider is used. |

## Results and interpretation

All seven checks and fifteen required scenarios passed on the clean frozen source.
98 Rust tests and three compile-fail doctests, 85 Python workload tests and 21
trusted guard controls passed. The reviewer separately ran guard controls normally
and with Python optimization. The full receipt preserves its legacy automatic
`issue_acceptance: incomplete` field: the runner deliberately does not close an
issue; completion is a separate human-readable criterion audit and review.
No automatic runtime or issue qualification is inferred from that field.

Later documentation changes do not relabel historical results as a fresh run.
Exact implementation/test hashes allow the reviewed publication tree to be checked.
Native/live, protocol source-binary correspondence and whole-system composition
remain unqualified and outside this bounded closure.

## Final test-only annotation revision

The final annotation/import-only test revision also passed all seven checks and
15 scenarios. Its report truthfully marks a dirty tree: only this audit, evidence
and STATUS documentation was pending; runtime/tests were frozen. The original
clean-source receipt is retained separately. Static quality has the **same 41
above-baseline diagnostics** and 157 historical baseline entries as unchanged
main; this is a failure, not a green aggregate or a new baseline.
