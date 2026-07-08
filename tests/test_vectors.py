import compact_encoding as cenc
import pytest

import bare_rpc as rpc
from bare_rpc import RequestMessage, ResponseMessage, RPCRemoteError, StreamMessage
from bare_rpc.constants import StreamFlag, Type

DATA_FAMILIES = ["envelope", "error", "boundary", "dispatch"]


def _descriptor_data(descriptor):
    """Encode-input bytes for a descriptor's data field (None stays None)."""
    d = descriptor.get("data")
    return None if d is None else bytes.fromhex(d)


def _expected_decoded_data(descriptor):
    """Decoded value for a data-bearing frame: null/'' -> b'', hex -> bytes."""
    d = descriptor.get("data")
    if d is None or d == "":
        return b""
    return bytes.fromhex(d)


def _encode_from_descriptor(descriptor):
    t = descriptor["type"]
    if t == Type.REQUEST:
        return rpc.encode_request(
            descriptor["id"],
            descriptor["command"],
            stream=descriptor["stream"],
            data=_descriptor_data(descriptor),
        )
    if t == Type.RESPONSE:
        err = descriptor.get("error")
        if err is not None:
            return rpc.encode_error_response(
                descriptor["id"], err["message"], err["code"], err["errno"]
            )
        return rpc.encode_response(
            descriptor["id"],
            stream=descriptor["stream"],
            data=_descriptor_data(descriptor),
        )
    if t == Type.STREAM:
        flags = descriptor["stream"]
        err = descriptor.get("error")
        if err is not None:
            return rpc.encode_stream(
                descriptor["id"],
                flags,
                error=RPCRemoteError(err["message"], err["code"], err["errno"]),
            )
        return rpc.encode_stream(
            descriptor["id"], flags, data=_descriptor_data(descriptor)
        )
    raise AssertionError(f"unknown descriptor type {t}")


def _assert_decoded_matches(msg, descriptor):
    t = descriptor["type"]
    if t == Type.REQUEST:
        assert isinstance(msg, RequestMessage)
        assert msg.id == descriptor["id"]
        assert msg.command == descriptor["command"]
        assert msg.stream == descriptor["stream"]
        expected = _expected_decoded_data(descriptor) if msg.stream == 0 else None
        assert msg.data == expected
    elif t == Type.RESPONSE:
        assert isinstance(msg, ResponseMessage)
        assert msg.id == descriptor["id"]
        assert msg.stream == descriptor["stream"]
        err = descriptor.get("error")
        if err is not None:
            assert msg.error == RPCRemoteError(
                err["message"], err["code"], err["errno"]
            )
        else:
            assert msg.error is None
            expected = _expected_decoded_data(descriptor) if msg.stream == 0 else None
            assert msg.data == expected
    elif t == Type.STREAM:
        assert isinstance(msg, StreamMessage)
        assert msg.id == descriptor["id"]
        flags = descriptor["stream"]
        assert msg.flags == flags
        err = descriptor.get("error")
        if flags & StreamFlag.ERROR:
            assert msg.error == RPCRemoteError(
                err["message"], err["code"], err["errno"]
            )
        elif flags & StreamFlag.DATA:
            assert msg.data == _expected_decoded_data(descriptor)
        else:
            assert msg.data is None and msg.error is None
    else:
        raise AssertionError(f"unknown descriptor type {t}")


@pytest.mark.parametrize("family", DATA_FAMILIES)
def test_decode_all(fixtures, family):
    frames = fixtures[family]["frames"]
    messages = fixtures[family]["messages"]
    assert len(frames) == len(messages)
    for frame_hex, entry in zip(frames, messages, strict=True):
        msg = rpc.decode_frame(bytes.fromhex(frame_hex))
        _assert_decoded_matches(msg, entry["descriptor"])


@pytest.mark.parametrize("family", DATA_FAMILIES)
def test_encode_all(fixtures, family):
    frames = fixtures[family]["frames"]
    messages = fixtures[family]["messages"]
    for frame_hex, entry in zip(frames, messages, strict=True):
        assert _encode_from_descriptor(entry["descriptor"]).hex() == frame_hex


def test_negative_frames(fixtures):
    for entry in fixtures["negative"]["frames"]:
        frame = bytes.fromhex(entry["hex"])
        try:
            result = rpc.decode_frame(frame)
        except cenc.OutOfBounds:
            continue  # truncated / under-length: acceptable failure
        assert result is None, f"negative frame decoded to a message: {entry['reason']}"
