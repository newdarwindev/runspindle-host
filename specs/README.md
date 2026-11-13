# host specification

Status: process-local lifecycle candidate; native protocol, durable recovery and live acceptance not verified.

## Responsibility

Native Codex session adapter, host pinning and reconnect receipts.

## Shared identifiers

Correlate task, intent revision, selected host, native session, browser instance, lease epoch, trace/span, artifact digest and Slack destination without placing credentials or raw private content in public receipts.

## Required behavior

Preserve explicit authority and unknown outcomes. Distinguish request admission, execution, validation, acceptance and delivery. Exact state transitions belong in component-specific specs added beside this file.

## Acceptance

1. Unit/contract tests with synthetic data.
2. Isolated integration tests against pinned upstream dependencies.
3. Bounded live acceptance in an authorized target.
4. Failure, restart, uncertain-effect reconciliation and rollback evidence.

Synthetic component behavior is exercised separately; integration and live gates remain not run.

A pinned [shared synthetic fixture subset](../docs/SHARED_FIXTURES.md) now has
standalone Python consumer checks. Rust lifecycle behavior is separately exercised in [lifecycle.md](lifecycle.md); native upstream
compatibility remains blocked. Fixture-format checks remain separate evidence.
