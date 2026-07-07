from bare_rpc import RequestMessage, RPCRemoteError, StreamFlag, Type


def test_type_values():
    assert (Type.REQUEST, Type.RESPONSE, Type.STREAM) == (1, 2, 3)


def test_stream_flag_values():
    assert StreamFlag.OPEN == 0x1
    assert StreamFlag.DATA == 0x10
    assert StreamFlag.ERROR == 0x80
    assert StreamFlag.REQUEST == 0x100
    assert StreamFlag.RESPONSE == 0x200


def test_message_dataclass_is_frozen_and_eq():
    a = RequestMessage(id=1, command=2, stream=0, data=b"x")
    b = RequestMessage(id=1, command=2, stream=0, data=b"x")
    assert a == b
    import dataclasses

    import pytest

    with pytest.raises(dataclasses.FrozenInstanceError):
        a.id = 9


def test_remote_error_fields():
    e = RPCRemoteError(message="boom", code="BAD", errno=400)
    assert (e.message, e.code, e.errno) == ("boom", "BAD", 400)
