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


def flags_of(frame):
    return rpc.decode_frame(frame).flags


def test_write_sends_data_frame():
    host = FakeRPC()

    async def body():
        s = OutgoingStream(host, 7, StreamFlag.REQUEST)
        host._outgoing_streams[7] = s
        await s.write(b"hi")

    asyncio.run(asyncio.wait_for(body(), 2))
    msg = rpc.decode_frame(host.sent[0])
    assert msg.flags == (StreamFlag.REQUEST | StreamFlag.DATA)
    assert msg.data == b"hi"


def test_cork_suspends_write_until_uncork():
    host = FakeRPC()

    async def body():
        s = OutgoingStream(host, 7, StreamFlag.REQUEST)
        s.cork()
        task = asyncio.ensure_future(s.write(b"x"))
        await asyncio.sleep(0)
        before = len(host.sent)
        s.uncork()
        await task
        return before, len(host.sent)

    before, after = asyncio.run(asyncio.wait_for(body(), 2))
    assert before == 0
    assert after == 1


def test_end_sends_end_then_close_and_removes():
    host = FakeRPC()

    async def body():
        s = OutgoingStream(host, 7, StreamFlag.RESPONSE)
        host._outgoing_streams[7] = s
        await s.end()

    asyncio.run(asyncio.wait_for(body(), 2))
    assert flags_of(host.sent[0]) == (StreamFlag.RESPONSE | StreamFlag.END)
    assert flags_of(host.sent[1]) == (StreamFlag.RESPONSE | StreamFlag.CLOSE)
    assert 7 not in host._outgoing_streams


def test_destroy_with_error_sends_close_error():
    host = FakeRPC()

    async def body():
        s = OutgoingStream(host, 7, StreamFlag.REQUEST)
        host._outgoing_streams[7] = s
        await s.destroy(RPCRemoteError("boom", "BAD", 1))

    asyncio.run(asyncio.wait_for(body(), 2))
    msg = rpc.decode_frame(host.sent[0])
    assert msg.flags == (StreamFlag.REQUEST | StreamFlag.CLOSE | StreamFlag.ERROR)
    assert msg.error == RPCRemoteError("boom", "BAD", 1)


def test_write_after_end_raises_and_teardown_is_idempotent():
    host = FakeRPC()

    async def body():
        s = OutgoingStream(host, 7, StreamFlag.REQUEST)
        host._outgoing_streams[7] = s
        await s.end()
        await s.end()  # idempotent no-op
        raised = False
        try:
            await s.write(b"x")
        except Exception:
            raised = True
        return raised, len(host.sent)

    raised, sent = asyncio.run(asyncio.wait_for(body(), 2))
    assert raised
    assert sent == 2  # only END + CLOSE from the single end()
