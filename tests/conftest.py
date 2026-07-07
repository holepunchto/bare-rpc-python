import json
import urllib.request
from pathlib import Path

import pytest

HRPC_TEST_SHA = "a0fef8ed32d22f48e476505223ccfddd8f481c89"
_BASE = (
    f"https://raw.githubusercontent.com/holepunchto/hrpc-test/{HRPC_TEST_SHA}/fixtures"
)
_CACHE = Path(__file__).parent / ".hrpc-fixtures"
_LOCAL_FALLBACK = Path(__file__).parent.parent.parent / "hrpc-test" / "fixtures"
DATA_FAMILIES = ["envelope", "error", "boundary", "dispatch"]


def _fetch(rel):
    cached = _CACHE / rel
    if cached.exists():
        return cached.read_bytes()
    url = f"{_BASE}/{rel}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            content = resp.read()
    except Exception as exc:
        # Try local fallback
        local_path = _LOCAL_FALLBACK / rel
        if local_path.exists():
            content = local_path.read_bytes()
        else:
            raise RuntimeError(
                f"could not fetch hrpc-test fixture {rel} from {url}: {exc}"
            ) from exc
    cached.parent.mkdir(parents=True, exist_ok=True)
    cached.write_bytes(content)
    return content


@pytest.fixture(scope="session")
def fixtures():
    out = {}
    for fam in DATA_FAMILIES:
        out[fam] = {
            "frames": json.loads(_fetch(f"{fam}/frames.json")),
            "messages": json.loads(_fetch(f"{fam}/messages.json")),
        }
    out["negative"] = {"frames": json.loads(_fetch("negative/frames.json"))}
    return out
