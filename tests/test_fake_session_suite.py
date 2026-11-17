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
        self.assertEqual(code, -signal.SIGSYS)

    @unittest.skipUnless(sys.platform == "linux", "Linux confinement")
    def test_timeout_is_failure(self):
        code, output = suite.run(
            [sys.executable, "-c", "import time; time.sleep(30)"], 1
        )
        self.assertEqual(code, 124)
        self.assertIn(b"timed out", output)
