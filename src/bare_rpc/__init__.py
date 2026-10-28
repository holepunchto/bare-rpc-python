"""RPC over any byte transport, wire compatible with bare-rpc."""

from .command_router import CommandRouter
from .constants import StreamFlag, Type
from .incoming import IncomingEvent, IncomingRequest
from .incoming_stream import IncomingStream
from .messages import (
    DecodedMessage,
    RequestMessage,
    ResponseMessage,
    RPCRemoteError,
    StreamMessage,
    UnknownMessageTypeError,
    decode_frame,
    encode_error_response,
    encode_event,
    encode_request,
    encode_response,
    encode_stream,
)
from .outgoing_stream import OutgoingStream
from .rpc import RPC
from .transport import StreamTransport, Transport

__all__ = [
    "CommandRouter",
    "DecodedMessage",
    "IncomingEvent",
    "IncomingRequest",
    "IncomingStream",
    "OutgoingStream",
    "RPC",
    "RPCRemoteError",
    "RequestMessage",
    "ResponseMessage",
    "StreamFlag",
    "StreamMessage",
    "StreamTransport",
    "Transport",
    "Type",
    "UnknownMessageTypeError",
    "decode_frame",
    "encode_error_response",
    "encode_event",
    "encode_request",
    "encode_response",
    "encode_stream",
]
