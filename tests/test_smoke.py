import compact_encoding

import bare_rpc


def test_packages_import():
    assert bare_rpc is not None
    assert compact_encoding is not None
