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
        assert source.count(old) == 1, name
        with tempfile.TemporaryDirectory(prefix="host-native-mutation-") as directory:
            root = Path(directory)
            for path in ("Cargo.toml", "Cargo.lock", "src", "tests"):
                item = ROOT / path
                if item.is_dir():
                    shutil.copytree(item, root / path)
                else:
                    shutil.copy2(item, root / path)
            (root / "src/native_bridge.rs").write_text(source.replace(old, new))
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
                ],
                cwd=root,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=180,
            )
            assert run.returncode != 0, name
            assert "test result: FAILED" in run.stdout, run.stdout
            assert "could not compile" not in run.stdout, run.stdout
            rows.append({"mutation": name, "behavioral_failure_observed": True})
    print(
        json.dumps({"evidence_class": "scripted_component_mutation", "results": rows})
    )


if __name__ == "__main__":
    main()
