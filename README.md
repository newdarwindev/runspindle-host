# runspindle-host

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
