import json
from pathlib import Path

import pytest

# Conformance vectors vendored from holepunchto/hrpc-test @
# a0fef8ed32d22f48e476505223ccfddd8f481c89 (see tests/fixtures/). Re-vendor from
# that repo's fixtures/ when updating - keeps the suite hermetic (no network).
_FIXTURES = Path(__file__).parent / "fixtures"
DATA_FAMILIES = ["envelope", "error", "boundary", "dispatch"]


def _load(rel):
    return json.loads((_FIXTURES / rel).read_text())


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
