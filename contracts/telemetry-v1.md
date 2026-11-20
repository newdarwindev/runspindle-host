# runspindle.telemetry.v1

Shared producer contract, version 1. This first implementation covers Host native
RPC observations and independently observed owned-child cleanup only. Other
components, context propagation, replay links, metrics and live exporters remain
planned. Nothing here grants authority or changes operational activation.

Standard service resource: service.name=runspindle-<component>, service.version
is the source release. Caller sets these on its provider; no implicit global
provider or environment discovery. Random trace/span IDs come from the upstream
SDK. Current Host adapter creates independent root spans, never copying ambient
baggage. Cross-component propagation is NOT yet implemented.

Allowed attributes: runspindle.telemetry.version, runspindle.component,
runspindle.outcome. Operation names and outcomes are closed enums. Outcomes:
acknowledged (wire reply only, not validated task completion), error (no detail),
observed (independent local cleanup observation), unknown. Future additions need
contract review; requested/dispatched/accepted must not be conflated with observed.
No task/session IDs, prompts, outputs, model names, paths, URLs, credentials,
exception messages or baggage. No content hashes as proxy private identifiers.

Tracing is opt-in and lossy, not an audit journal. Producer buffer capacity is
128 records; try_lock contention, poison or capacity drops increment a counter.
No callbacks, waiting, exporter flush or network on the native path or Drop.
Callers drain and invoke optional actual OpenTelemetry API conversion outside
execution. Provider/exporter configuration, authorization, sampling, total time
bounds and shutdown belong to a separately qualified supervisor. This library
installs no exporter, background worker, credential or collector by default.

SDK conversion can block/fail in caller-supplied exporter code; it must not be
invoked as part of an effect or shutdown. Buffer overflow loses observations,
not authority records. A crash loses undrained events; recovery must use durable
journals, never absence of a span. Wall-clock end is clamped to start on export.

Use this same vocabulary for later Core, Tether, Browser, Refinery, Console,
Platform and Next slices. Telemetry never substitutes for source provenance,
consent, accepted artifacts, effect receipts or backend persistence evidence.
