"""Conformance vectors from the bare-rpc-test npm package, shared with the JS,
Kotlin and Swift ports."""

import json
import shutil
import subprocess
import tarfile
from pathlib import Path

VERSION = "0.1.0"

_root = Path(__file__).parent / ".bare-rpc-test" / VERSION


def _fixtures_dir():
    package = _root / "package"
    if not package.is_dir():
        _root.mkdir(parents=True, exist_ok=True)
        npm = shutil.which("npm")
        out = subprocess.run(
            [npm, "pack", f"bare-rpc-test@{VERSION}", "--loglevel=error"],
            cwd=_root,
            check=True,
            capture_output=True,
            text=True,
        )
        with tarfile.open(_root / out.stdout.strip().splitlines()[-1]) as tar:
            tar.extractall(_root, filter="data")
    return package / "fixtures"


FIXTURES_DIR = _fixtures_dir()

FAMILIES = json.loads((FIXTURES_DIR / "families.json").read_text())


def _read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_family(name):
    """Families with {messages, frames} (index-aligned)."""
    return {
        "messages": _read_json(FIXTURES_DIR / name / "messages.json"),
        "frames": _read_json(FIXTURES_DIR / name / "frames.json"),
    }


def load_negative():
    """{hex, reason}[] a conformant decoder must reject."""
    return _read_json(FIXTURES_DIR / "negative" / "frames.json")


def load_sequence():
    """{concatenated, count} - one byte stream of several frames."""
    return _read_json(FIXTURES_DIR / "sequence" / "frames.json")
