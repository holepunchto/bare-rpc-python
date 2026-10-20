import pytest

from wire_vectors import FAMILIES, load_family, load_negative, load_sequence

DATA_FAMILIES = FAMILIES


@pytest.fixture(scope="session")
def fixtures():
    out = {family: load_family(family) for family in DATA_FAMILIES}
    out["negative"] = {"frames": load_negative()}
    out["sequence"] = load_sequence()
    return out
