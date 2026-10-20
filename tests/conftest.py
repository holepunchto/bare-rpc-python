import pytest

from wire_vectors import (
    FAMILIES,
    load_decode_only,
    load_family,
    load_negative,
    load_sequence,
)

DATA_FAMILIES = FAMILIES


@pytest.fixture(scope="session")
def fixtures():
    out = {family: load_family(family) for family in DATA_FAMILIES}
    out["negative"] = {"frames": load_negative()}
    out["decode-only"] = {"frames": load_decode_only()}
    out["sequence"] = load_sequence()
    return out
