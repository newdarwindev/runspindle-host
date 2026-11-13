#!/usr/bin/env python3
"""Offline, synthetic component checks. Does not activate a native host."""
import os
from pathlib import Path
import subprocess
import sys
ROOT = Path(__file__).resolve().parents[1]
env = dict(os.environ, CARGO_HOME=str(ROOT / 'local/cargo'))
commands = [
    ['cargo', 'fmt', '--all', '--', '--check'],
    ['cargo', 'test', '--offline'],
    ['cargo', 'clippy', '--offline', '--all-targets', '--', '-D', 'warnings'],
    [sys.executable, '-m', 'unittest', 'discover', '-s', 'tests', '-p', 'test_*.py'],
    [sys.executable, 'tools/check_lifecycle_mutations.py'],
    [sys.executable, 'tools/check_scaffold.py'],
]
for command in commands:
    subprocess.run(command, cwd=ROOT, env=env, check=True, timeout=60)
result = subprocess.run([sys.executable, 'tools/acceptance_gate.py', 'native'], cwd=ROOT, env=env, timeout=10)
assert result.returncode == 78, 'Operational acceptance must remain blocked'
