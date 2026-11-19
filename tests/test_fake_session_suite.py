"""Manifest integrity and bounded, network-denied process execution."""

import importlib.util
import json
import signal
import sys
import unittest
from pathlib import Path
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    "fake_suite", Path(__file__).resolve().parents[1] / "tools/check_fake_sessions.py"
)
assert SPEC and SPEC.loader
suite = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(suite)


class FakeSuiteTests(unittest.TestCase):
    def test_manifest_valid(self) -> None:
        suite.validate_manifest(json.loads(suite.MANIFEST.read_text()))

    def test_empty_manifest_rejected(self) -> None:
        with self.assertRaises(ValueError):
            suite.validate_manifest({"schema": "runspindle.host.fake-suite.v1"})

    def test_duplicate_ids_rejected(self) -> None:
        manifest = json.loads(suite.MANIFEST.read_text())
        manifest["scenarios"].append(manifest["scenarios"][0])
        with self.assertRaises(ValueError):
            suite.validate_manifest(manifest)

    def test_inventory_exact_not_suffix_match(self) -> None:
        manifest = {"scenarios": [{"test": "required"}]}
        self.assertEqual(
            suite.missing_scenarios(manifest, "not_required: test"), ["required"]
        )
        self.assertEqual(suite.missing_scenarios(manifest, "required: test"), [])

    def test_reaped_child_is_never_signalled(self) -> None:
        process = mock.Mock(returncode=0, stdout=mock.Mock())
        process.communicate.return_value = (b"done", None)
        with (
            mock.patch.object(suite.subprocess, "Popen", return_value=process),
            mock.patch.object(suite.os, "killpg") as signal_group,
        ):
            self.assertEqual(suite.run(["synthetic"], 1), (0, b"done"))
        signal_group.assert_not_called()
        process.wait.assert_not_called()

    def test_completed_wait_prevents_later_group_signal(self) -> None:
        process = mock.Mock(returncode=None, pid=1234, stdout=mock.Mock())
        process.communicate.side_effect = suite.subprocess.TimeoutExpired(
            "synthetic", 1
        )

        def reap(**_kwargs: object) -> None:
            process.returncode = 0

        process.wait.side_effect = reap
        with (
            mock.patch.object(suite.subprocess, "Popen", return_value=process),
            mock.patch.object(suite.os, "killpg") as signal_group,
        ):
            self.assertEqual(suite.run(["synthetic"], 1)[0], 124)
        signal_group.assert_called_once_with(1234, signal.SIGTERM)

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_success_and_failure_remove_owned_temporary_state(self) -> None:
        for status in (0, 3):
            code, output = suite.run(
                [
                    sys.executable,
                    "-c",
                    "import tempfile,pathlib,sys; "
                    "p=pathlib.Path(tempfile.mkdtemp()); (p/'retained').write_text('fixture'); "
                    f"print(str(p),flush=True); sys.exit({status})",
                ],
                10,
            )
            self.assertEqual(code, status)
            self.assertFalse(Path(output.decode().strip()).exists())

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_network_default_denied(self) -> None:
        code, _ = suite.run(
            [sys.executable, "-c", "import socket; socket.socket()"], 10
        )
        self.assertEqual(code, 125)

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_timeout_is_failure(self) -> None:
        code, output = suite.run(
            [sys.executable, "-c", "import time; time.sleep(30)"], 1
        )
        self.assertEqual(code, 124)
        self.assertIn(b"timed out", output)

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_sigterm_cleans_owned_child_and_descendant(self) -> None:
        self.check_cancellation(nested=False)

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_sigterm_cleans_nested_owner_session(self) -> None:
        self.check_cancellation(nested=True)

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_sigterm_cleans_detached_descendant(self) -> None:
        self.check_cancellation(nested=False, detached=True)

    def check_cancellation(self, nested: bool, detached: bool = False) -> None:
        import os
        import subprocess
        import tempfile
        import time

        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "pids.json"
            temp_marker = Path(directory) / "temp-path"
            child = (
                "import subprocess,sys,os,json,time,tempfile,pathlib; "
                "scratch=pathlib.Path(tempfile.mkdtemp()); (scratch/'retained').write_text('fixture'); "
                f"pathlib.Path({str(temp_marker)!r}).write_text(str(scratch)); "
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
            handles = []
            try:
                deadline = time.monotonic() + 10
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(marker.exists(), "owned child did not start")
                pids = json.loads(marker.read_text())
                handles = [os.pidfd_open(pid) for pid in pids]
                owner.terminate()
                _, owner_error = owner.communicate(timeout=10)
                self.assertNotEqual(owner.returncode, 0)
                self.assertFalse(Path(temp_marker.read_text()).exists())
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
                for handle in handles:
                    try:
                        signal.pidfd_send_signal(handle, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    finally:
                        os.close(handle)
