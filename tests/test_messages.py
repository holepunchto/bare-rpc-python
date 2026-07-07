import compact_encoding as cenc
import pytest

import bare_rpc as rpc
from bare_rpc import RequestMessage, ResponseMessage, RPCRemoteError


def test_encode_request_matches_fixture():
    # envelope[0]: id 1, command 0, stream 0, data "hi"
    assert rpc.encode_request(1, 0, data=b"hi").hex() == "0700000001010000026869"


def test_encode_request_empty_data():
    # envelope-style: id 2, command 7, empty payload
    assert rpc.encode_request(2, 7, data=b"").hex() == "050000000102070000"


def test_encode_event_matches_fixture():
    # event: id 0, command 99, data deadbeef
    assert (
        rpc.encode_event(99, b"\xde\xad\xbe\xef").hex() == "090000000100630004deadbeef"
    )


def test_decode_request_roundtrip():
    frame = rpc.encode_request(1, 0, data=b"hi")
    msg = rpc.decode_frame(frame)
    assert msg == RequestMessage(id=1, command=0, stream=0, data=b"hi")


def test_decode_request_empty_data_is_empty_bytes():
    msg = rpc.decode_frame(bytes.fromhex("050000000102070000"))
    assert isinstance(msg, RequestMessage)
    assert msg.data == b""  # present-but-empty, not None


def test_decode_request_stream_open_has_no_data():
    # stream != 0 request carries no data field
    frame = rpc.encode_request(1, 0, stream=0x101)
    msg = rpc.decode_frame(frame)
    assert msg.stream == 0x101
    assert msg.data is None


def test_decode_unknown_type_returns_none():
    # length 2, type 99
    assert rpc.decode_frame(bytes.fromhex("020000006300")) is None


def test_decode_truncated_frame_raises():
    # length 4 declared, no body bytes
    with pytest.raises(cenc.OutOfBounds):
        rpc.decode_frame(bytes.fromhex("04000000"))


def test_encode_response_success_matches_fixture():
    # envelope[6]: response id 1, stream 0, data "hi"
    assert rpc.encode_response(1, data=b"hi").hex() == "0700000002010000026869"


def test_encode_error_response_matches_fixture():
    # error[0]: id 1, message "boom", code "BAD", errno 400
    assert (
        rpc.encode_error_response(1, "boom", "BAD", 400).hex()
        == "100000000201010004626f6f6d03424144fd2003"
    )


def test_decode_response_success_roundtrip():
    msg = rpc.decode_frame(rpc.encode_response(1, data=b"hi"))
    assert msg == ResponseMessage(id=1, stream=0, data=b"hi", error=None)


def test_decode_error_response_roundtrip():
    frame = rpc.encode_error_response(1, "boom", "BAD", 400)
    msg = rpc.decode_frame(frame)
    assert msg == ResponseMessage(
        id=1, stream=0, data=None, error=RPCRemoteError("boom", "BAD", 400)
    )


def test_decode_response_empty_data_is_empty_bytes():
    msg = rpc.decode_frame(rpc.encode_response(1, data=None))
    assert isinstance(msg, ResponseMessage)
    assert msg.data == b""
    assert msg.error is None
