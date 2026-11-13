# Desired intent: Native session continuity

Status: engineering direction and acceptance obligations, not a new implementation
claim, grant of authority, or replacement for existing versioned contracts.

## System intent and evidence boundary

Runspindle is intended to turn an authorized one-off task into a repeatable,
maintained workflow by composing scoped access, managed native/browser sessions,
explicit human takeover, and evaluated workflow candidates. The useful capability
is the composition: cross-application execution can retain identity and evidence
across interruptions, while approved examples can inform later DSPy experiments.
This is a design objective, not evidence of emergent intelligence, autonomous
self-improvement, deployed integration, or measured gains.

A workflow revision must identify its inputs, permitted effects, target bindings,
exception policy, acceptance criteria and resource budget. Repetition does not
create a recurring schedule or expand permission. Human intervention is first an
execution event; only a separately reviewed, consented and sanitized example may
enter a dataset. A successful evaluation is evidence for review, not authority to
promote, serve, spend, publish or act in another account.

## Component responsibility

The host boundary should let authorized work continue on the selected real
computer and native session without confusing a logical task with an actual
runtime session. It supplies trusted lifecycle observations and explicit
capabilities; it does not widen the task's account permissions or substitute a
new host/session when recovery is difficult.

## Interfaces and acceptance obligations

- Bind launch/discover/cancel and observations to the complete operation, host,
  workspace and lease. Preserve the native session ID observed from the peer.
  A fabricated session ID or a successful process start is not native acceptance.
- Reconnect by observation and reconciliation. A missing launch response does not
  permit another launch; zero or multiple discovery matches remain unresolved
  unless the protocol supplies authoritative absence evidence.
- Report capability availability explicitly for each platform and adapter version.
  Native text transfer, screenshots, input submission and human control require
  separately qualified seams; a scripted capability profile is not OS support.
- Maintain monotonic event sequencing, trusted freshness checks and bounded queues.
  Backpressure must not erase terminal observations or silently discard cancel
  intent. Distinguish offline, stale, unknown and observed terminal states.
- Human takeover across desktop/browser surfaces needs coordinated fencing;
  reconnect is not permission to restore agent input. Emit only sanitized metadata
  suitable for core/console/tether, never hidden reasoning or raw private content.
- Qualify restart, uncertain launch/cancel, concurrent ownership and transport
  authentication against a real native protocol before advertising continuity.

## Present scope

The bounded process-local lifecycle adapter exercises injected scripted peers.
It has no durable recovery or qualified native app-server transport, and capture,
text transfer and submission remain unsupported. See [lifecycle semantics](lifecycle.md)
and [implementation evidence](../docs/IMPLEMENTATION_EVIDENCE.md).
