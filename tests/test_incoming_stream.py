import asyncio

import bare_rpc as rpc
from bare_rpc import RPCRemoteError, StreamFlag
from bare_rpc.incoming_stream import HIGH_WATER_MARK, IncomingStream


class FakeRPC:
    def __init__(self):
        self.sent = []
        self._incoming_streams = {}
        self._max_buffered_bytes = 2**24

    def _send(self, frame):
        self.sent.append(frame)


def test_iterates_buffered_then_ends():
    host = FakeRPC()

    async def body():
        s = IncomingStream(host, 7, StreamFlag.RESPONSE)
        host._incoming_streams[(7, StreamFlag.RESPONSE)] = s
        s._push(b"a")
        s._push(b"b")
        s._end()
        return [chunk async for chunk in s]

    assert asyncio.run(asyncio.wait_for(body(), 2)) == [b"a", b"b"]
    assert (7, StreamFlag.RESPONSE) not in host._incoming_streams


def test_parked_consumer_gets_direct_handoff():
    host = FakeRPC()

    async def body():
        s = IncomingStream(host, 7, StreamFlag.RESPONSE)
        got = []

        async def consume():
            async for chunk in s:
                got.append(chunk)

        task = asyncio.create_task(consume())
        await asyncio.sleep(0)  # consumer parks on an empty buffer
        s._push(b"x")
        await asyncio.sleep(0)
        s._end()
        await task
        return got

    assert asyncio.run(asyncio.wait_for(body(), 2)) == [b"x"]


def test_pause_at_high_water_bytes_and_resume_below_it():
    host = FakeRPC()
    chunk = bytes(HIGH_WATER_MARK // 4)

    def flags():
        return [rpc.decode_frame(f).flags for f in host.sent]

    async def body():
        s = IncomingStream(host, 7, StreamFlag.RESPONSE)
        for _ in range(3):
            s._push(chunk)
        below = flags()
        s._push(chunk)
        paused = flags()
        await s.read()
        return below, paused, flags()

    below, paused, resumed = asyncio.run(asyncio.wait_for(body(), 2))
    assert below == []
    assert paused == [StreamFlag.RESPONSE | StreamFlag.PAUSE]
    assert resumed == [
        StreamFlag.RESPONSE | StreamFlag.PAUSE,
        StreamFlag.RESPONSE | StreamFlag.RESUME,
    ]


def test_destroy_with_error_raises_from_iterator():
    host = FakeRPC()

    async def body():
        s = IncomingStream(host, 8, StreamFlag.RESPONSE)

        async def consume():
            async for _ in s:
                pass

        task = asyncio.create_task(consume())
        await asyncio.sleep(0)  # consumer parks on the empty buffer
        s.destroy(RPCRemoteError("gone", "E", 2))
        raised = None
        try:
            await task
        except RPCRemoteError as exc:
            raised = exc
        return raised

    raised = asyncio.run(asyncio.wait_for(body(), 2))
    assert raised == RPCRemoteError("gone", "E", 2)
