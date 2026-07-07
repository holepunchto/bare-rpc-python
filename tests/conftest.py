import hashlib
import io
import json
import tarfile
import urllib.request
from base64 import b64decode
from pathlib import Path

import pytest

HRPC_TEST_VERSION = "0.0.2"
_TARBALL = f"https://registry.npmjs.org/hrpc-test/-/hrpc-test-{HRPC_TEST_VERSION}.tgz"
# npm dist.integrity (sha512, base64) for hrpc-test@0.0.2
_INTEGRITY_SHA512 = "IAjY9G9OwpJVsph6lexUYj2C0ElyrXlUk28B8dLEEDz7CHupG+aoD/DodBV/QyjxgDzoLjxsGuPinKXsTGK+hQ=="  # noqa: E501
_PREFIX = "package/fixtures/"
_CACHE = Path(__file__).parent / ".hrpc-fixtures"
DATA_FAMILIES = ["envelope", "error", "boundary", "dispatch"]


def _download_and_extract():
    try:
        with urllib.request.urlopen(_TARBALL, timeout=30) as resp:
            raw = resp.read()
    except Exception as exc:
        raise RuntimeError(
            f"could not fetch hrpc-test tarball from {_TARBALL}: {exc}"
        ) from exc
    if hashlib.sha512(raw).digest() != b64decode(_INTEGRITY_SHA512):
        raise RuntimeError("hrpc-test tarball failed sha512 integrity check")
    _CACHE.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
        for member in tar.getmembers():
            name = member.name
            if not (name.startswith(_PREFIX) and name.endswith(".json")):
                continue
            rel = name[len(_PREFIX) :]
            if ".." in Path(rel).parts:
                continue
            src = tar.extractfile(member)
            if src is None:
                continue
            dest = _CACHE / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(src.read())


def _load(rel):
    if not (_CACHE / rel).exists():
        _download_and_extract()
    return json.loads((_CACHE / rel).read_text())


@pytest.fixture(scope="session")
def fixtures():
    out = {}
    for fam in DATA_FAMILIES:
        out[fam] = {
            "frames": _load(f"{fam}/frames.json"),
            "messages": _load(f"{fam}/messages.json"),
        }
    out["negative"] = {"frames": _load("negative/frames.json")}
    return out
