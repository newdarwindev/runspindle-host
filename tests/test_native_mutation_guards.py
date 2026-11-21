"""Mutation evidence gates remain fail-closed under optimized Python."""

import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = """
import importlib.util
import sys
from types import SimpleNamespace
from unittest.mock import patch
spec = importlib.util.spec_from_file_location('checker', sys.argv[1])
if spec is None or spec.loader is None:
    raise RuntimeError('checker loader unavailable')
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)
result = SimpleNamespace(returncode=int(sys.argv[2]), stdout=sys.argv[3])
with patch.object(checker.subprocess, 'run', return_value=result):
    checker.main()
"""


class NativeMutationGuards(unittest.TestCase):
    def test_optimized_mode_requires_actual_behavioral_failure(self) -> None:
        for checker in (
            "check_native_mutations.py",
            "check_native_bridge_mutations.py",
            "check_durable_path_mutations.py",
        ):
            focused = checker == "check_native_bridge_mutations.py"
            for code, output, accepted in (
                (0, "test result: ok", False),
                (1, "could not compile; test result: FAILED", False),
                (1, "unrelated tool failure", False),
                (1, "test result: FAILED", not focused),
                (
                    1,
                    "test native_bridge_missing_reservation_never_replays_launch ... FAILED\n"
                    "test result: FAILED",
                    True,
                ),
            ):
                with self.subTest(checker=checker, code=code, output=output):
                    result = subprocess.run(
                        [
                            sys.executable,
                            "-O",
                            "-c",
                            SCRIPT,
                            str(ROOT / "tools" / checker),
                            str(code),
                            output,
                        ],
                        text=True,
                        capture_output=True,
                        timeout=30,
                    )
                    self.assertEqual(result.returncode == 0, accepted, result.stderr)
                    if not accepted:
                        self.assertNotIn(
                            '"behavioral_failure_observed": true', result.stdout
                        )


if __name__ == "__main__":
    unittest.main()
