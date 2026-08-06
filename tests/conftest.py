import pytest
from hrpc_test import FAMILIES, load_family, load_negative

# Conformance vectors come from the hrpc-test package, pinned in the dev extra,
# rather than a copy in this repo - a vendored set silently falls behind, which
# is what happened before. Wire changes are still made in hrpc-test first; here
# you bump the pin.
DATA_FAMILIES = FAMILIES


@pytest.fixture(scope="session")
def fixtures():
    out = {family: load_family(family) for family in DATA_FAMILIES}
    out["negative"] = {"frames": load_negative()}
    return out
