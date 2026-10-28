"""Conformance vectors from the bare-rpc-test npm package, shared with the JS,
Kotlin and Swift ports."""

import json
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

VERSION = "0.1.0"

_root = Path(__file__).parent / ".bare-rpc-test" / VERSION


def _read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _fixtures_dir():
    package = _root / "package"
    if not package.is_dir():
        _root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=_root) as tmp:
            subprocess.run(
                [
                    shutil.which("npm"),
                    "pack",
                    f"bare-rpc-test@{VERSION}",
                    "--loglevel=error",
                ],
                cwd=tmp,
                check=True,
                capture_output=True,
            )
            tarball = Path(tmp) / f"bare-rpc-test-{VERSION}.tgz"
            with tarfile.open(tarball) as tar:
                tar.extractall(tmp, filter="data")
            (Path(tmp) / "package").rename(package)
    return package / "fixtures"


FIXTURES_DIR = _fixtures_dir()

FAMILIES = _read_json(FIXTURES_DIR / "families.json")


def load_family(name):
    """Families with {messages, frames} (index-aligned)."""
    return {
        "messages": _read_json(FIXTURES_DIR / name / "messages.json"),
        "frames": _read_json(FIXTURES_DIR / name / "frames.json"),
    }


def load_negative():
    """{hex, reason}[] a conformant decoder must reject."""
    return _read_json(FIXTURES_DIR / "negative" / "frames.json")


def load_decode_only():
    """{hex, descriptor, reason}[] a conformant decoder must decode to descriptor;
    the frames do not re-encode to the same bytes."""
    return _read_json(FIXTURES_DIR / "decode-only" / "frames.json")


def load_sequence():
    """{concatenated, count} - one byte stream of several frames."""
    return _read_json(FIXTURES_DIR / "sequence" / "frames.json")
