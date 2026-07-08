"""bare-rpc message and frame codec, wire-compatible with the JS reference."""

from .constants import StreamFlag, Type
from .incoming import IncomingEvent, IncomingRequest
from .messages import (
    DecodedMessage,
    RequestMessage,
    ResponseMessage,
    RPCRemoteError,
    StreamMessage,
    decode_frame,
    encode_error_response,
    encode_event,
    encode_request,
    encode_response,
    encode_stream,
)

__all__ = [
    "DecodedMessage",
    "IncomingEvent",
    "IncomingRequest",
    "RPCRemoteError",
    "RequestMessage",
    "ResponseMessage",
    "StreamFlag",
    "StreamMessage",
    "Type",
    "decode_frame",
    "encode_error_response",
    "encode_event",
    "encode_request",
    "encode_response",
    "encode_stream",
]
