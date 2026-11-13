#!/usr/bin/env python3
"""Compile and detect deliberate invariant breaks in disposable source copies."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
MUTATIONS = [
    ("blind_retry", "if value.execution != Execution::Queued", "if false", "accepted_lost_reply_reconciles_once_without_second_launch"),
    ("session_rebinding", ".is_some_and(|old| old != &observation.native_session_id)", ".is_some_and(|_old| false)", "reconnect_preserves_observed_session_and_every_binding_field"),
    ("cancel_ack_as_terminal", "value.cancellation = Cancellation::Acknowledged;", "value.cancellation = Cancellation::Acknowledged; value.execution = Execution::Terminal(Terminal::Cancelled);", "cancellation_ack_is_not_terminal_and_completion_race_keeps_actual_outcome"),
]
observed = []
for name, before, after, test in MUTATIONS:
    with tempfile.TemporaryDirectory(prefix="runspindle-host-mutation-") as temporary:
        target = Path(temporary)
        shutil.copy2(ROOT / "Cargo.toml", target)
        shutil.copy2(ROOT / "Cargo.lock", target)
        shutil.copytree(ROOT / "src", target / "src")
        (target / "tests").mkdir()
        shutil.copy2(ROOT / "tests/lifecycle.rs", target / "tests/lifecycle.rs")
        source = target / "src/lifecycle.rs"
        text = source.read_text()
        assert text.count(before) == 1, (name, "mutation anchor changed")
        source.write_text(text.replace(before, after))
        result = subprocess.run(["cargo", "test", "--offline", "--test", "lifecycle", test, "--", "--exact"], cwd=target, capture_output=True, text=True, timeout=60, env=os.environ)
        assert result.returncode != 0 and "test result: FAILED" in result.stdout and "could not compile" not in result.stderr, (name, result.stdout, result.stderr)
        observed.append({"mutation": name, "test": test, "detected": True})
print(json.dumps({"evidence_class": "component", "mutations": observed}, sort_keys=True))
