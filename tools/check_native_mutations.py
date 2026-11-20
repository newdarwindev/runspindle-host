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
        "native_turn_start_conflicting_notice": (
            "if observed.turn_id.as_deref().is_some_and(|seen| seen != turn) {",
            "if false && observed.turn_id.as_deref().is_some_and(|seen| seen != turn) {",
        ),
        "native_binary_digest": (
            'if format!("{:x}", hash.finalize()) != expected {',
            'if false && format!("{:x}", hash.finalize()) != expected {',
        ),
        "native_binary_growth": (
            "if input.read(&mut extra).map_err(|_| WireError::Io)? != 0 {",
            "if false && input.read(&mut extra).map_err(|_| WireError::Io)? != 0 {",
        ),
        "native_observed_poison": (
            "if self.reader_failed.load(Ordering::Acquire) {",
            "if false && self.reader_failed.load(Ordering::Acquire) {",
        ),
        "native_setup_observed_cleanup": (
            "let result = if result.is_err() {",
            "let result = if false && result.is_err() {",
        ),
        "native_ambiguous_reply": (
            'if result.is_some() == error.is_some() || frame.get("params").is_some() {',
            'if false && (result.is_some() == error.is_some() || frame.get("params").is_some()) {',
        ),
        "native_scalar_interrupt_ack": (
            "if !reply.is_object() {",
            "if false && !reply.is_object() {",
        ),
        "native_unknown_cleanup": (
            "if cleanup_permitted(receipt) {\n            let _ = fs::remove_dir_all",
            "if true {\n            let _ = fs::remove_dir_all",
        ),
        "native_duplicate_json_members": (
            "if values.contains_key(&key) {",
            "if false && values.contains_key(&key) {",
        ),
        "native_thread_rebinding": (
            "if thread != expected_thread {",
            "if false && thread != expected_thread {",
        ),
        "native_contradictory_start_identity": (
            'if frame["method"] == "thread/started" {',
            'if false && frame["method"] == "thread/started" {',
        ),
        "native_unbounded_frame": (
            "if bytes.len() > MAX_FRAME {",
            "if false && bytes.len() > MAX_FRAME {",
        ),
    }
    rows = []
    for name, (old, new) in mutations.items():
        if source.count(old) != 1:
            raise AssertionError(name)
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
