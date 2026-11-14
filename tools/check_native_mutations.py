#!/usr/bin/env python3
"""Require behavioral failures for native frame and identity mutations."""

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    source = (ROOT / "src/native.rs").read_text()
    mutations = {
        "native_duplicate_json_members": (
            "if values.contains_key(&key) {",
            "if false && values.contains_key(&key) {",
        ),
        "native_thread_rebinding": (
            "if thread != expected_thread {",
            "if false && thread != expected_thread {",
        ),
        "native_unbounded_frame": (
            "if bytes.len() > MAX_FRAME {",
            "if false && bytes.len() > MAX_FRAME {",
        ),
    }
    rows = []
    for name, (old, new) in mutations.items():
        assert source.count(old) == 1, name
        with tempfile.TemporaryDirectory(prefix="host-native-mutation-") as directory:
            root = Path(directory)
            for path in ("Cargo.toml", "Cargo.lock", "src"):
                item = ROOT / path
                if item.is_dir():
                    shutil.copytree(item, root / path)
                else:
                    shutil.copy2(item, root / path)
            (root / "src/native.rs").write_text(source.replace(old, new))
            run = subprocess.run(
                [
                    "cargo",
                    "test",
                    "--locked",
                    "--offline",
                    "--lib",
                    "native_",
                    "--target-dir",
                    str(ROOT / "target/native-mutations"),
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
