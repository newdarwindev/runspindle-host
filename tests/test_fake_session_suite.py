"""Manifest integrity and bounded, network-denied process execution."""

import importlib.util
import json
import signal
import sys
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "fake_suite", Path(__file__).resolve().parents[1] / "tools/check_fake_sessions.py"
)
assert SPEC and SPEC.loader
suite = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(suite)


class FakeSuiteTests(unittest.TestCase):
    def test_manifest_valid(self):
        suite.validate_manifest(json.loads(suite.MANIFEST.read_text()))

    def test_empty_manifest_rejected(self):
        with self.assertRaises(ValueError):
            suite.validate_manifest({"schema": "runspindle.host.fake-suite.v1"})

    def test_duplicate_ids_rejected(self):
        manifest = json.loads(suite.MANIFEST.read_text())
        manifest["scenarios"].append(manifest["scenarios"][0])
        with self.assertRaises(ValueError):
            suite.validate_manifest(manifest)

    def test_inventory_exact_not_suffix_match(self):
        manifest = {"scenarios": [{"test": "required"}]}
        self.assertEqual(
            suite.missing_scenarios(manifest, "not_required: test"), ["required"]
        )
        self.assertEqual(suite.missing_scenarios(manifest, "required: test"), [])

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_network_default_denied(self):
        code, _ = suite.run(
            [sys.executable, "-c", "import socket; socket.socket()"], 10
        )
        self.assertEqual(code, 125)

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_timeout_is_failure(self):
        code, output = suite.run(
            [sys.executable, "-c", "import time; time.sleep(30)"], 1
        )
        self.assertEqual(code, 124)
        self.assertIn(b"timed out", output)

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_sigterm_cleans_owned_child_and_descendant(self):
        self.check_cancellation(nested=False)

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_sigterm_cleans_nested_owner_session(self):
        self.check_cancellation(nested=True)

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_sigterm_cleans_detached_descendant(self):
        self.check_cancellation(nested=False, detached=True)

    def check_cancellation(self, nested, detached=False):
        import os
        import subprocess
        import tempfile
        import time

        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "pids.json"
            child = (
                "import subprocess,sys,os,json,time; "
                f"p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],start_new_session={detached}); "
                f"open({str(marker) + '.tmp'!r},'w').write(json.dumps([os.getpid(),p.pid])); "
                f"os.replace({str(marker) + '.tmp'!r},{str(marker)!r}); "
                "time.sleep(60)"
            )
            if nested:
                child = (
                    "import sys; sys.path.insert(0,'tools'); import check_fake_sessions as s; "
                    f"s.run({[sys.executable, '-c', child]!r},120,guard=False)"
                )
            owner = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    "import sys; sys.path.insert(0,'tools'); "
                    f"import check_fake_sessions as s; s.run({[sys.executable, '-c', child]!r},120)",
                ],
                cwd=suite.ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            pids = []
            try:
                deadline = time.monotonic() + 10
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(marker.exists(), "owned child did not start")
                pids = json.loads(marker.read_text())
                owner.terminate()
                _, owner_error = owner.communicate(timeout=10)
                self.assertNotEqual(owner.returncode, 0)
                for pid in pids:
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        stat = Path(f"/proc/{pid}/stat")
                        if not stat.exists() or stat.read_text().split()[2] == "Z":
                            break
                        time.sleep(0.02)
                    else:
                        self.fail(
                            f"owned process {pid} survived cancellation: {owner_error!r}"
                        )
            finally:
                if owner.poll() is None:
                    owner.kill()
                owner.communicate(timeout=10)
                for pid in pids:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
