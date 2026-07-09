import asyncio

import bare_rpc as rpc
from bare_rpc import ResponseMessage, RPCRemoteError
from bare_rpc.incoming import IncomingEvent, IncomingRequest


class FakeRPC:
    def __init__(self):
        self.sent = []

    def _send(self, frame):
        self.sent.append(frame)


def test_incoming_event_has_no_reply():
    ev = IncomingEvent(9, b"data")
    assert ev.command == 9
    assert ev.data == b"data"
    assert not hasattr(ev, "reply")


def test_incoming_request_reply():
    fake = FakeRPC()

    async def body():
        req = IncomingRequest(fake, 1, 5, b"hi")
        await req.reply(b"pong")

    asyncio.run(body())
    msg = rpc.decode_frame(fake.sent[0])
    assert isinstance(msg, ResponseMessage)
    assert msg.id == 1
    assert msg.data == b"pong"
    assert msg.error is None


def test_incoming_request_reject_from_exception():
    fake = FakeRPC()

    class Boom(Exception):
        code = "BAD"
        errno = 400

    async def body():
        req = IncomingRequest(fake, 2, 5, None)
        await req.reject(Boom("boom"))

    asyncio.run(body())
    msg = rpc.decode_frame(fake.sent[0])
    assert msg.error == RPCRemoteError("boom", "BAD", 400)


def test_incoming_request_reject_from_rpc_remote_error():
    fake = FakeRPC()

    async def body():
        req = IncomingRequest(fake, 3, 5, None)
        await req.reject(RPCRemoteError("nope", "NO", 7))

    asyncio.run(body())
    assert rpc.decode_frame(fake.sent[0]).error == RPCRemoteError("nope", "NO", 7)


def test_incoming_request_replies_once():
    fake = FakeRPC()

    async def body():
        req = IncomingRequest(fake, 4, 0, None)
        await req.reply(b"a")
        await req.reply(b"b")  # no-op
        await req.reject(Exception("x"))  # no-op

    asyncio.run(body())
    assert len(fake.sent) == 1
