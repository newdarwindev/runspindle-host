#!/usr/bin/env python3
"""Separate trusted guard-control tests from guarded Python workload tests."""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTROLS = {"test_fake_session_suite", "test_network_guard"}


def main() -> int:
    if sys.argv[1:] not in ([], ["--guard-controls"]):
        raise ValueError("unsupported test partition")
    control = bool(sys.argv[1:])
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "tests"))
    modules = sorted(
        p.stem
        for p in (ROOT / "tests").glob("test_*.py")
        if (p.stem in CONTROLS) == control
    )
    if not modules:
        raise ValueError("empty test partition")
    suite = unittest.TestSuite(
        unittest.defaultTestLoader.loadTestsFromName(name) for name in modules
    )
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
