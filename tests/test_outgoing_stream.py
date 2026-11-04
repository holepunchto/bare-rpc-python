import asyncio

import bare_rpc as rpc
from bare_rpc import RPCRemoteError, StreamFlag
from bare_rpc.outgoing_stream import OutgoingStream


class FakeRPC:
    def __init__(self):
        self.sent = []
        self._outgoing_streams = {}

    def _send(self, frame):
        self.sent.append(frame)

    async def _send_and_wait(self, frame):
        self._send(frame)


def flags_of(frame):
    return rpc.decode_frame(frame).flags


def test_write_sends_data_frame():
    host = FakeRPC()

    async def body():
        s = OutgoingStream(host, 7, StreamFlag.REQUEST, opened=True)
        host._outgoing_streams[(7, StreamFlag.REQUEST)] = s
        await s.write(b"hi")

    asyncio.run(asyncio.wait_for(body(), 2))
    msg = rpc.decode_frame(host.sent[0])
    assert msg.flags == (StreamFlag.REQUEST | StreamFlag.DATA)
    assert msg.data == b"hi"


def test_cork_suspends_write_until_uncork():
    host = FakeRPC()

    async def body():
        s = OutgoingStream(host, 7, StreamFlag.REQUEST, opened=True)
        s._cork()
        task = asyncio.create_task(s.write(b"x"))
        await asyncio.sleep(0)
        before = len(host.sent)
        s._uncork()
        await task
        return before, len(host.sent)

    before, after = asyncio.run(asyncio.wait_for(body(), 2))
    assert before == 0
    assert after == 1


def test_end_sends_only_end_and_removes():
    host = FakeRPC()

    async def body():
        s = OutgoingStream(host, 7, StreamFlag.RESPONSE, opened=True)
        host._outgoing_streams[(7, StreamFlag.RESPONSE)] = s
        await s.end()

    asyncio.run(asyncio.wait_for(body(), 2))
    assert flags_of(host.sent[0]) == (StreamFlag.RESPONSE | StreamFlag.END)
    assert len(host.sent) == 1
    assert (7, StreamFlag.RESPONSE) not in host._outgoing_streams


def test_destroy_with_error_sends_close_error():
    host = FakeRPC()

    async def body():
        s = OutgoingStream(host, 7, StreamFlag.REQUEST, opened=True)
        host._outgoing_streams[(7, StreamFlag.REQUEST)] = s
        s.destroy(RPCRemoteError("boom", "BAD", 1))

    asyncio.run(asyncio.wait_for(body(), 2))
    msg = rpc.decode_frame(host.sent[0])
    assert msg.flags == (StreamFlag.REQUEST | StreamFlag.CLOSE | StreamFlag.ERROR)
    assert msg.error == RPCRemoteError("boom", "BAD", 1)


def test_write_after_end_is_dropped_and_teardown_is_idempotent():
    host = FakeRPC()

    async def body():
        s = OutgoingStream(host, 7, StreamFlag.REQUEST, opened=True)
        host._outgoing_streams[(7, StreamFlag.REQUEST)] = s
        await s.end()
        await s.end()  # idempotent no-op
        await s.write(b"x")
        return len(host.sent)

    assert asyncio.run(asyncio.wait_for(body(), 2)) == 1
