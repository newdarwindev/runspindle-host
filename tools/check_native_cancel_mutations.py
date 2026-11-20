#!/usr/bin/env python3
"""Require behavioral failures for native cancellation authority and durability mutations."""

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    source = (ROOT / "src/native_bridge.rs").read_text()
    mutations = {
        "native_cancel_dispatch_authority": (
            "if !verifier.verify_cancel(request) {\n            return Err(Error::Denied);\n        } // cancel dispatch authority",
            "if false && !verifier.verify_cancel(request) {\n            return Err(Error::Denied);\n        } // cancel dispatch authority",
        ),
        "native_cancel_reservation": (
            "task.cancellation = Some(CancelReservation {\n            request: request.clone(),\n            acknowledged: false,\n        });",
            "task.cancellation = None;",
        ),
        "native_cancel_no_retry": (
            "if let Some(saved) = &task.cancellation {",
            "if let Some(saved) = None::<&crate::native_turn::CancelReservation> {",
        ),
        "native_cancel_ack_not_terminal": (
            "saved.acknowledged = true;",
            "saved.acknowledged = true; state.terminal = Some(Terminal::Cancelled);",
        ),
        "native_cancel_acknowledgement_authority": (
            "if !verifier.verify_cancel(request) {\n            return Err(Error::Denied);\n        }\n        Ok(CancelOutcome::Acknowledged)",
            "Ok(CancelOutcome::Acknowledged)",
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
                    "native_cancel_",
                    "--target-dir",
                    str(ROOT / "target/native-cancel-mutations"),
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
