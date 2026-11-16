#!/usr/bin/env python3
"""Require real behavioral failures when either durable path boundary is removed."""

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    mutations = {
        "sqlite_path_gate": (
            "src/storage.rs",
            "        require_durable_path(path)?;",
            "        // path gate removed",
        ),
        "native_journal_path_gate": (
            "src/native_bridge.rs",
            "        crate::storage::require_durable_path(path)?;",
            "        // path gate removed",
        ),
    }
    rows = []
    for name, (path, old, new) in mutations.items():
        source = (ROOT / path).read_text()
        if source.count(old) != 1:
            raise AssertionError(name)
        with tempfile.TemporaryDirectory(prefix="host-path-mutation-") as directory:
            root = Path(directory)
            for item in ("Cargo.toml", "Cargo.lock", "src", "tests"):
                src = ROOT / item
                if src.is_dir():
                    shutil.copytree(src, root / item)
                else:
                    shutil.copy2(src, root / item)
            (root / path).write_text(source.replace(old, new))
            run = subprocess.run(
                [
                    "cargo",
                    "test",
                    "--offline",
                    "--locked",
                    "--jobs",
                    "1",
                    "--test",
                    "durable_paths",
                    "--target-dir",
                    str(ROOT / "target/durable-path-mutations"),
                ],
                cwd=root,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=90,
            )
            if (
                run.returncode == 0
                or "test result: FAILED" not in run.stdout
                or "could not compile" in run.stdout
            ):
                raise AssertionError(run.stdout)
            rows.append({"mutation": name, "behavioral_failure_observed": True})
    print(json.dumps({"evidence_class": "synthetic_component_mutation", "results": rows}))


if __name__ == "__main__":
    main()
