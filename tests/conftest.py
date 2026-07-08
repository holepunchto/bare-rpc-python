import io
import json
import tarfile
import urllib.request
from pathlib import Path

import pytest

HRPC_TEST_SHA = "a0fef8ed32d22f48e476505223ccfddd8f481c89"
_TARBALL = f"https://codeload.github.com/holepunchto/hrpc-test/tar.gz/{HRPC_TEST_SHA}"
_PREFIX = f"hrpc-test-{HRPC_TEST_SHA}/fixtures/"
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
