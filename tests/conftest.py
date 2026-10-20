import pytest

from wire_vectors import FAMILIES, load_family, load_negative, load_sequence

# Conformance vectors come from the bare-rpc-test package, pinned in
# wire_vectors.py rather than copied into this repo. Wire changes are made in
# bare-rpc-test first; here you bump the pin.
DATA_FAMILIES = FAMILIES


@pytest.fixture(scope="session")
def fixtures():
    out = {family: load_family(family) for family in DATA_FAMILIES}
    out["negative"] = {"frames": load_negative()}
    out["sequence"] = load_sequence()
    return out
