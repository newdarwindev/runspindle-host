"""Kernel-observed denial cannot be hidden by successful parent/child exits."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

GUARD = Path(__file__).resolve().parents[1] / "tools/network_guard.py"


@unittest.skipUnless(sys.platform == "linux", "Linux seccomp notification lane")
class NetworkGuardTests(unittest.TestCase):
    def guard(self, script):
        return subprocess.run(
            [sys.executable, str(GUARD), sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )

    def test_offline_passes(self):
        result = self.guard("print('offline')")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_original_failure_preserved(self):
        self.assertEqual(self.guard("raise SystemExit(7)").returncode, 7)

    def test_caught_permission_error_still_fails(self):
        result = self.guard(
            "import socket\ntry: socket.socket()\nexcept PermissionError: pass"
        )
        self.assertEqual(result.returncode, 125, result.stderr)
        self.assertIn(b"denied=1 errors=[]", result.stderr)

    def test_parent_ignoring_descendant_failure_still_fails(self):
        result = self.guard(
            "import subprocess,sys; subprocess.run([sys.executable,'-c','import socket;socket.socket()']);sys.exit(0)"
        )
        self.assertEqual(result.returncode, 125, result.stderr)
        self.assertIn(b"denied=1 errors=[]", result.stderr)

    def test_late_orphan_descendant_still_fails(self):
        result = self.guard(
            "import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time,socket;time.sleep(.1);socket.socket()']);sys.exit(0)"
        )
        self.assertEqual(result.returncode, 125, result.stderr)
        self.assertIn(b"denied=1 errors=[]", result.stderr)

    def test_detached_descendant_still_fails(self):
        result = self.guard(
            "import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time,socket;time.sleep(.1);socket.socket()'],start_new_session=True);sys.exit(0)"
        )
        self.assertEqual(result.returncode, 125, result.stderr)
        self.assertIn(b"denied=1 errors=[]", result.stderr)

    def test_multiple_attempts_counted(self):
        result = self.guard(
            "import socket\nfor i in range(3):\n try: socket.socket()\n except PermissionError: pass"
        )
        self.assertEqual(result.returncode, 125, result.stderr)
        self.assertIn(b"denied=3 errors=[]", result.stderr)

    def test_missing_accounting_mutation_is_detected(self):
        script = "import socket\ntry: socket.socket()\nexcept PermissionError: pass"
        self.assertEqual(self.guard(script).returncode, 125)
        with tempfile.TemporaryDirectory(prefix="host-guard-mutation-") as directory:
            source = GUARD.read_text()
            self.assertEqual(source.count("attempts[0] += 1"), 1)
            mutated = Path(directory) / "guard.py"
            mutated.write_text(
                source.replace(
                    "attempts[0] += 1", "pass  # deliberate broken accounting"
                )
            )
            result = subprocess.run(
                [sys.executable, str(mutated), sys.executable, "-c", script],
                capture_output=True,
                timeout=10,
            )
            # The actual invariant (denial must force 125) fails under mutation;
            # a setup/runtime error cannot masquerade as successful detection.
            self.assertEqual(result.returncode, 0, result.stderr)
