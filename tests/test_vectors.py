import compact_encoding as cenc
import pytest
from hrpc_test import FAMILIES

import bare_rpc as rpc
from bare_rpc import RequestMessage, ResponseMessage, RPCRemoteError, StreamMessage
from bare_rpc.constants import Type


def _descriptor_data(descriptor):
    """Encode-input bytes for a descriptor's data field (None stays None)."""
    d = descriptor.get("data")
    return None if d is None else bytes.fromhex(d)


def _expected_decoded_data(descriptor):
    """The descriptor pins the payload exactly, so compare against it rather than
    re-deriving the rule: null where the wire carries no dataLen at all, and an
    empty string where it carries a zero-length one, which decodes to empty
    bytes rather than None."""
    d = descriptor.get("data")
    if d is None:
        return None
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
        assert msg.data == _expected_decoded_data(descriptor)
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
            assert msg.data == _expected_decoded_data(descriptor)
    elif t == Type.STREAM:
        assert isinstance(msg, StreamMessage)
        assert msg.id == descriptor["id"]
        flags = descriptor["stream"]
        assert msg.flags == flags
        err = descriptor.get("error")
        if err is not None:
            assert msg.error == RPCRemoteError(
                err["message"], err["code"], err["errno"]
            )
            assert msg.data is None
        else:
            assert msg.error is None
            assert msg.data == _expected_decoded_data(descriptor)
    else:
        raise AssertionError(f"unknown descriptor type {t}")


@pytest.mark.parametrize("family", FAMILIES)
def test_decode_all(fixtures, family):
    frames = fixtures[family]["frames"]
    messages = fixtures[family]["messages"]
    assert len(frames) == len(messages)
    for frame_hex, entry in zip(frames, messages, strict=True):
        msg = rpc.decode_frame(bytes.fromhex(frame_hex))
        _assert_decoded_matches(msg, entry["descriptor"])


@pytest.mark.parametrize("family", FAMILIES)
def test_encode_all(fixtures, family):
    frames = fixtures[family]["frames"]
    messages = fixtures[family]["messages"]
    for frame_hex, entry in zip(frames, messages, strict=True):
        assert _encode_from_descriptor(entry["descriptor"]).hex() == frame_hex


def test_negative_frames(fixtures):
    """Both classes WIRE.md says must be rejected - malformed, and well-formed
    with an unrecognized type - are signalled rather than skipped."""
    for entry in fixtures["negative"]["frames"]:
        frame = bytes.fromhex(entry["hex"])
        with pytest.raises((cenc.OutOfBounds, rpc.UnknownMessageTypeError)):
            rpc.decode_frame(frame)


def test_sequence_resplits_by_length_prefix(fixtures):
    """The sequence family is one byte stream of several frames - a transport has
    to find the boundaries from the uint32 length prefix alone."""
    sequence = fixtures["sequence"]
    buf = bytes.fromhex(sequence["concatenated"])

    seen = 0
    offset = 0
    while offset < len(buf):
        body = int.from_bytes(buf[offset : offset + 4], "little")
        end = offset + 4 + body
        assert rpc.decode_frame(buf[offset:end]) is not None, f"frame {seen}"
        offset = end
        seen += 1

    assert seen == sequence["count"]
