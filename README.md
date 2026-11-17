# RUNSPINDLE HOST

## Same workstation. Existing session. No silent failover.

**HOST — SESSION CONTINUITY ON THE CHOSEN MACHINE** coordinates launch, reconnect,
cancellation and shutdown while retaining the exact task, host, workspace and
native-session binding. The durable library uses SQLite and fresh-process tests;
a lost launch reply is reconciled rather than automatically becoming another launch.

[Codex](https://github.com/openai/codex) supplies the native agent runtime.
**HOST** wraps that existing runtime rather than implementing a new agent. Its
pinned stdio transport and durable identity bridge distinguish session preparation,
observed metadata and independently established outcomes. Preparing an empty thread
is not proof that an authorized task ran or completed.

The [fake-session suite](docs/fake-session-suite.md) exercises the actual lifecycle
and recovery code against scripted native peers with independently observed effects.
Its synthetic macOS/Linux profiles do not establish native support on either OS.
The [Core host lane](https://github.com/newdarwindev/runspindle-core/blob/main/docs/HOST_RUNTIME.md)
connects the durable adapter to real Temporal activities while retaining that boundary.

*UPCOMING — complete qualified native execution, public app-server protocol coverage
and real macOS/Linux capability acceptance. Continuous text capture, cross-machine
insertion and submission need their own selected mechanisms, permissions and
verified outcomes; they are not supplied merely by a session registry.*

[![Conceptual selected coding workstation recovery, gallery page 12](https://newdarwindev.github.io/runspindle_for_bloomberg/gallery/12.webp)](https://newdarwindev.github.io/runspindle_for_bloomberg/assets/Runspindle_Gallery_Index.html#page-12)

**Concept illustration, not a live host dashboard.** The pictured workflow waits
for the selected workstation and reconciles its existing session.
[Slide/source index](https://newdarwindev.github.io/runspindle_for_bloomberg/assets/Runspindle_Gallery_Index.html#page-12)
· [Full conceptual UI gallery](https://github.com/newdarwindev/runspindle_for_bloomberg/blob/main/assets/Runspindle_Revised_Gallery.pdf)
· [Research proposal](https://github.com/newdarwindev/runspindle_for_bloomberg/blob/main/assets/runspindle-paper.pdf).
The [source asset guide](https://github.com/newdarwindev/runspindle_for_bloomberg/blob/main/ASSETS.md)
identifies fictional/synthetic material; the linked galleries do not prove a deployed
integration, measured savings or Bloomberg endorsement.

### Follow the implementation

- [Lifecycle contract](specs/lifecycle.md) and [durable mapping](specs/durable-lifecycle.md).
- [Pinned stdio transport](specs/native-stdio.md) and [native identity bridge](specs/native-bridge.md).
- [Observed child shutdown](specs/observed-child-shutdown.md) and [fake-session isolation](docs/fake-session-suite.md).
- [Current source](src/), [tests](tests/) and [machine-readable status](STATUS.json).

## Development and qualification boundaries

Native Codex session adapter, host pinning and reconnect receipts.

**Status: bounded process-local lifecycle adapter; native protocol and deployment unqualified.**

This repository is one boundary of [Runspindle](https://github.com/newdarwindev/runspindle). Repository boundaries do not require one microservice each.

- Read [specifications](specs/README.md), [agent guidance](AGENTS.md), [provenance](provenance.json), [status](STATUS.json) and [legal status](LEGAL.md).
- Run `python3 tools/check_scaffold.py` for structural validation only.
- Operational entrypoints and acceptance gates deliberately fail closed until implemented.
- GitHub Actions use isolated **self-hosted local runners only**. No GitHub-hosted fallback. Runner capacity is not established by publishing workflows.
- No credentials, real sessions, messages, datasets, browser profiles or live host inventory belong here.

No old repository is retired by this scaffold. Migration and archival are separate verified gates.

Run `python3 tools/check_lifecycle.py` for the offline synthetic Rust/Python suite,
formatting, Clippy, three deliberate invariant mutations and structural checks.
Requires Rust/Cargo 1.94.1 and Python 3.9+; no dependency download, native account,
network transport, background service or provider is used. See
[behavior and boundaries](specs/lifecycle.md) and
[implementation evidence](docs/IMPLEMENTATION_EVIDENCE.md).

## Implemented validation

Run `python3 tools/validate.py` for all implemented structural, language, fixture, artifact and mutation checks in this repository. The same entrypoint runs on trusted `main` and `ci/*` pushes and manual qualification using disposable Strix x64 guests. Logs include the checkout SHA, image/tool identities, command results and applicable artifact checksums. Unimplemented builds and product acceptance remain explicit in `STATUS.json`; blocked acceptance workflows retain their failing conclusions. ARM64 and native macOS qualification remain pending.

## Desired technical direction

Native session continuity: this component contributes to turning authorized one-off work into
repeatable, maintained cross-application workflows. Scoped access, managed
sessions, explicit human exceptions and evaluated DSPy candidates are intended
to compose into that capability; no automatic improvement or live integration
is implied.

Read [the component-specific technical intent](specs/desired-intent.md) for its
interfaces, privacy boundaries, failure cases and acceptance obligations. Current
implementation and evidence limits above remain unchanged.

The additional [durable library mapping](specs/durable-lifecycle.md) now adds bounded SQLite storage and fresh-process
scripted-peer tests. Earlier process-local evidence remains separate; actual native
protocol, production durability and live acceptance remain unqualified.

Offline fake acceptance: see [fake-session suite](docs/fake-session-suite.md).
