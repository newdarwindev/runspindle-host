#!/usr/bin/env python3
"""Require behavioral failures for native bridge authority and durability mutations."""

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    source = (ROOT / "src/native_bridge.rs").read_text()
    mutations = {
        "native_bridge_factory_authority": (
            "if !verifier.verify(binding, Action::TakeOwnership) {",
            "if false && !verifier.verify(binding, Action::TakeOwnership) {",
        ),
        "native_bridge_stale_owner": (
            "if record.generation != owner {",
            "if false && record.generation != owner {",
        ),
        "native_bridge_reservation_loss_allows_factory": (
            "if self.launch_reserved != u64::from(self.has_state) {",
            "if false && self.launch_reserved != u64::from(self.has_state) {",
        ),
        "native_bridge_relaunch": (
            "if self.reserve().is_err() {\n            return LaunchReply::LostReply;\n        }",
            "let _ = self.reserve();",
        ),
        "native_bridge_task_terminal_promotion": (
            "observed_at_ms: now,\n            terminal: None,",
            "observed_at_ms: now,\n            terminal: state.terminal,",
        ),
        "native_bridge_receive_fence_removed": (
            "state.receiving = true;",
            "state.receiving = false;",
        ),
    }
    rows = []
    for name, (old, new) in mutations.items():
        if source.count(old) != 1:
            raise AssertionError(name)
        with tempfile.TemporaryDirectory(prefix="host-native-mutation-") as directory:
            root = Path(directory)
            for path in ("Cargo.toml", "Cargo.lock", "src", "tests"):
                item = ROOT / path
                if item.is_dir():
                    shutil.copytree(item, root / path)
                else:
                    shutil.copy2(item, root / path)
            (root / "src/native_bridge.rs").write_text(source.replace(old, new))
            focus = []
            if name == "native_bridge_reservation_loss_allows_factory":
                focus = [
                    "native_bridge_missing_reservation_never_replays_launch",
                    "--",
                    "--exact",
                ]
            run = subprocess.run(
                [
                    "cargo",
                    "test",
                    "--locked",
                    "--offline",
                    "--test",
                    "native_bridge",
                    "--target-dir",
                    str(ROOT / "target/native-bridge-mutations"),
                    *focus,
                ],
                cwd=root,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=180,
            )
            if run.returncode == 0:
                raise AssertionError(name)
            if "test result: FAILED" not in run.stdout:
                raise AssertionError(run.stdout)
            if "could not compile" in run.stdout:
                raise AssertionError(run.stdout)
            if focus and f"test {focus[0]} ... FAILED" not in run.stdout:
                raise AssertionError(run.stdout)
            rows.append({"mutation": name, "behavioral_failure_observed": True})
    print(
        json.dumps({"evidence_class": "scripted_component_mutation", "results": rows})
    )


if __name__ == "__main__":
    main()
