#!/usr/bin/env python3
"""Require behavioral failures for native task-turn authority and durability mutations."""

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    source = (ROOT / "src/native_bridge.rs").read_text()
    mutations = {
        "native_turn_input_authority": (
            "if !verifier.verify_turn(request) {\n            return Err(Error::Denied);\n        } // input-time authority",
            "if false && !verifier.verify_turn(request) {\n            return Err(Error::Denied);\n        } // input-time authority",
        ),
        "native_turn_dispatch_authority": (
            "if !verifier.verify_turn(request) {\n            return Err(Error::Denied);\n        } // dispatch-time authority",
            "if false && !verifier.verify_turn(request) {\n            return Err(Error::Denied);\n        } // dispatch-time authority",
        ),
        "native_turn_reservation": (
            "state.task_turn = Some(Reservation {\n            request: request.clone(),\n            turn_id: None,\n        });",
            "state.task_turn = None;",
        ),
        "native_turn_no_retry": (
            "if let Some(saved) = &state.task_turn {",
            "if let Some(saved) = None::<&crate::native_turn::Reservation> {",
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
            run = subprocess.run(
                [
                    "cargo",
                    "test",
                    "--locked",
                    "--offline",
                    "native_turn_",
                    "--target-dir",
                    str(ROOT / "target/native-turn-mutations"),
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
            rows.append({"mutation": name, "behavioral_failure_observed": True})
    print(
        json.dumps({"evidence_class": "scripted_component_mutation", "results": rows})
    )


if __name__ == "__main__":
    main()
