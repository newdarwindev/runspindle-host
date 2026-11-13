# Native host session contract

Use the supported native Codex app-server session/turn/item/tool protocol, preserving actual session identity. Owner-selected hosts stay pinned while offline. Reconnect must reconcile a lost launch acknowledgement before launching again. Cancellation requested, acknowledged and observed terminal state are distinct.

Only exposed progress/reasoning summaries may be mirrored; hidden chain-of-thought is not a source. Capture and cross-device text-transfer methods and destination submission behavior remain unresolved. Do not silently choose them.

Required tests: offline pin, duplicate delivery, lost launch response, reconnect, stale session, unsupported event, cancellation and native macOS/Linux capability differences. None has run here.
