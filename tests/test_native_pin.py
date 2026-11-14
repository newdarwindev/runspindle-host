"""Offline consistency of reviewed native identities; no native execution."""

import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class NativePin(unittest.TestCase):
    def test_native_source_binary_and_schema_pins_are_explicit(self) -> None:
        pin = json.loads((ROOT / "docs/native-protocol-pin.json").read_text())
        source = (ROOT / "src/native.rs").read_text()
        self.assertEqual(pin["version"], "0.169.0")
        self.assertIn(f'pub const SOURCE: &str = "{pin["source_commit"]}";', source)
        self.assertIn(
            f'pub const BINARY_SHA256: &str = "{pin["binary"]["sha256"]}";', source
        )
        self.assertEqual(pin["binary"]["platform"], "aarch64-apple-darwin")
        self.assertGreaterEqual(len(pin["schemas"]), 12)
        self.assertTrue(
            all(
                re.fullmatch("[0-9a-f]{64}", value) for value in pin["schemas"].values()
            )
        )
        self.assertIn("not independently verified", pin["binary"]["qualification"])
