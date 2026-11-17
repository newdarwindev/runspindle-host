#!/usr/bin/env python3
"""Run fake-session acceptance under an inherited Linux no-network filter.

This removes capabilities from child processes only. It never changes host
network settings, starts a real native session, or relaxes an operational gate.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "fixtures/fake-session-suite.json"
NETWORK_CALLS = (
    "socket",
    "connect",
    "bind",
    "listen",
    "accept",
    "accept4",
    "io_uring_setup",
)


def deny_network() -> None:
    if sys.platform != "linux":
        raise RuntimeError("blocked: Linux libseccomp required")
    library = ctypes.CDLL("libseccomp.so.2", use_errno=True)
    library.seccomp_init.argtypes = [ctypes.c_uint32]
    library.seccomp_init.restype = ctypes.c_void_p
    library.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    library.seccomp_syscall_resolve_name.restype = ctypes.c_int
    library.seccomp_rule_add.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_int,
        ctypes.c_uint,
    ]
    library.seccomp_rule_add.restype = ctypes.c_int
    library.seccomp_load.argtypes = [ctypes.c_void_p]
    library.seccomp_load.restype = ctypes.c_int
    library.seccomp_release.argtypes = [ctypes.c_void_p]
    context = library.seccomp_init(0x7FFF0000)  # SCMP_ACT_ALLOW
    if not context:
        raise RuntimeError("blocked: seccomp_init")
    try:
        for name in NETWORK_CALLS:
            number = library.seccomp_syscall_resolve_name(name.encode("ascii"))
            if (
                number < 0
                or library.seccomp_rule_add(context, 0x80000000, number, 0) != 0
            ):
                raise RuntimeError(f"blocked: syscall rule {name}")
        # KILL_PROCESS: unexpected network use cannot be caught and silently
        # converted into passing fallback behavior by a test or its descendants.
        if library.seccomp_load(context) != 0:
            raise RuntimeError("blocked: seccomp_load")
    finally:
        library.seccomp_release(context)


def run(command: list[str], timeout: int) -> tuple[int, bytes]:
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--exec", *command],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        close_fds=True,
    )
    try:
        output, _ = process.communicate(timeout=timeout)
        return process.returncode, output
    except subprocess.TimeoutExpired as error:
        return 124, (error.output or b"") + b"\ncheck timed out\n"
    finally:
        # Clean the entire owned process group on success, failure or cancel,
        # including descendants that outlive the direct child.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=10)
        if process.stdout is not None:
            process.stdout.close()


def validate_manifest(manifest: dict) -> None:
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


def missing_scenarios(manifest: dict, listing: str) -> list[str]:
    names = {
        line.removesuffix(": test")
        for line in listing.splitlines()
        if line.endswith(": test")
    }
    return [s["test"] for s in manifest["scenarios"] if s["test"] not in names]


def main() -> int:
    if sys.argv[1:2] == ["--exec"]:
        deny_network()
        os.execvp(sys.argv[2], sys.argv[2:])
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
        if code != -signal.SIGSYS:
            raise RuntimeError(f"blocked: deny canary {call}: {code}: {output!r}")
    for check in manifest["checks"]:
        code, output = run(check["command"], check["timeout_seconds"])
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
    evidence["checks"] = checks
    evidence["finished_at_unix"] = int(time.time())
    evidence["network_denial"] = "passed_socket_SIGSYS"
    evidence["status"] = (
        "passed" if all(c["status"] == "passed" for c in checks) else "failed"
    )
    target.parent.mkdir(exist_ok=True)
    target.write_text(json.dumps(evidence, indent=2) + "\n")
    return 0 if evidence["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
