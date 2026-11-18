#!/usr/bin/env python3
"""Run fake-session acceptance under an inherited Linux no-network filter.

This removes capabilities from child processes only. It never changes host
network settings, starts a real native session, or relaxes an operational gate.
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "fixtures/fake-session-suite.json"


def run(command: list[str], timeout: int, *, guard: bool = True) -> tuple[int, bytes]:
    def cancelled(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)

    grace = float(os.environ.get("RUNSPINDLE_FAKE_CLEANUP_GRACE", "8"))
    scratch = tempfile.TemporaryDirectory(prefix="runspindle-fake-check-")
    environment = {
        **os.environ,
        "RUNSPINDLE_FAKE_CLEANUP_GRACE": str(grace / 2),
        "TMPDIR": scratch.name,
        "TEMP": scratch.name,
        "TMP": scratch.name,
    }
    previous = signal.signal(signal.SIGTERM, cancelled)
    process = None
    try:
        # Defer termination until the owned process handle is available, so a
        # signal during spawn cannot strand an untracked new-session child.
        blocked = signal.pthread_sigmask(
            signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT}
        )
        try:
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--exec" if guard else "--exec-inherited",
                    *command,
                ],
                cwd=ROOT,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
                env=environment,
            )
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
        output, _ = process.communicate(timeout=timeout)
        return process.returncode, output
    except subprocess.TimeoutExpired as error:
        return 124, (error.output or b"") + b"\ncheck timed out\n"
    finally:
        # Complete cleanup even if a second cancellation arrives.
        blocked = signal.pthread_sigmask(
            signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT}
        )
        try:
            if process is not None:
                # Give nested owners a chance to cancel their own new-session
                # groups before forcibly terminating this group.
                # An unreaped leader pins its process-group identity. Never
                # signal its numeric PGID after communicate/wait has reaped it.
                if process.returncode is None:
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=grace)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        process.wait(timeout=10)
                if process.stdout is not None:
                    process.stdout.close()
            scratch.cleanup()
        finally:
            signal.signal(signal.SIGTERM, previous)
            signal.pthread_sigmask(signal.SIG_SETMASK, blocked)


def validate_manifest(manifest: dict[str, Any]) -> None:
    if manifest.get("schema") != "runspindle.host.fake-suite.v1":
        raise ValueError("unsupported manifest schema")
    if not manifest.get("scenarios") or not manifest.get("checks"):
        raise ValueError("empty manifest")
    for section in ("scenarios", "checks"):
        identifiers = [entry["id"] for entry in manifest[section]]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError(f"duplicate {section} ID")
    for check in manifest["checks"]:
        if not check["command"] or not 0 < check["timeout_seconds"] <= 600:
            raise ValueError("invalid check command or timeout")
    if sum(check["id"] == "rust" for check in manifest["checks"]) != 1:
        raise ValueError("required rust check missing")


def missing_scenarios(manifest: dict[str, Any], listing: str) -> list[str]:
    names = {
        line.removesuffix(": test")
        for line in listing.splitlines()
        if line.endswith(": test")
    }
    return [s["test"] for s in manifest["scenarios"] if s["test"] not in names]


def main() -> int:
    if sys.argv[1:2] in (["--exec"], ["--exec-inherited"]):
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGTERM, signal.SIGINT})
        if sys.argv[1] == "--exec-inherited":
            os.execvp(sys.argv[2], sys.argv[2:])
        os.execv(
            sys.executable,
            [sys.executable, str(ROOT / "tools/network_guard.py"), *sys.argv[2:]],
        )
    target = ROOT / ".verification/fake-session-suite.json"
    target.unlink(missing_ok=True)
    manifest = json.loads(MANIFEST.read_text())
    validate_manifest(manifest)
    evidence: dict[str, object] = {
        "schema": "runspindle.host.fake-suite.v1",
        "evidence_class": "synthetic",
        "seed": manifest["seed"],
        "started_at_unix": int(time.time()),
        "source_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "working_tree_dirty": bool(
            subprocess.check_output(
                ["git", "status", "--porcelain", "--untracked-files=normal"], cwd=ROOT
            )
        ),
        "manifest_sha256": hashlib.sha256(MANIFEST.read_bytes()).hexdigest(),
        "fixture_sha256": {
            str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted((ROOT / "fixtures").rglob("*"))
            if p.is_file()
        },
        "profiles": manifest["profiles"],
        "native_protocol_conformance": "blocked",
        "descendant_violation_accounting": "seccomp_notifications",
        "issue_acceptance": "incomplete",
        "live_acceptance": "not_run",
        "checks": [],
    }
    checks = []
    # Every scenario selector must resolve to an existing test; test renames
    # cannot silently turn a required scenario into a zero-test success.
    code, listing = run(["cargo", "test", "--offline", "--locked", "--", "--list"], 180)
    available = listing.decode(errors="replace")
    missing = missing_scenarios(manifest, available)
    if code or missing:
        raise RuntimeError(f"blocked: scenario inventory ({code}, missing={missing})")
    for call in ["socket.socket()"]:
        code, output = run([sys.executable, "-c", f"import socket; {call}"], 10)
        if code != 125 or b"network guard: denied=1 errors=[]" not in output:
            raise RuntimeError(f"blocked: deny canary {call}: {code}: {output!r}")
    for check in manifest["checks"]:
        print(f"running check: {check['id']}", flush=True)
        control = check["id"] == "guard-controls"
        if control and check["command"] != [
            "python3",
            "tools/check_python_tests.py",
            "--guard-controls",
        ]:
            raise RuntimeError("blocked: unexpected control-plane command")
        code, output = run(
            check["command"], check["timeout_seconds"], guard=not control
        )
        checks.append(
            {
                "id": check["id"],
                "status": "passed" if code == 0 else "failed",
                "returncode": code,
                "output_sha256": hashlib.sha256(output).hexdigest(),
            }
        )
        if check["id"] == "rust":
            completed = output.decode(errors="replace").splitlines()
            scenarios = [
                {
                    **scenario,
                    "status": "passed"
                    if f"test {scenario['test']} ... ok" in completed
                    else "failed",
                }
                for scenario in manifest["scenarios"]
            ]
            evidence["scenarios"] = scenarios
            if any(s["status"] != "passed" for s in scenarios):
                checks[-1]["status"] = "failed"
        sys.stdout.buffer.write(output)
        sys.stdout.buffer.flush()
    evidence["checks"] = checks
    evidence["finished_at_unix"] = int(time.time())
    evidence["network_denial"] = "passed_socket_notification_accounting"
    evidence["status"] = (
        "passed" if all(c["status"] == "passed" for c in checks) else "failed"
    )
    target.parent.mkdir(exist_ok=True)
    target.write_text(json.dumps(evidence, indent=2) + "\n")
    return 0 if evidence["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
