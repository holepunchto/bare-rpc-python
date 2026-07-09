import asyncio

import bare_rpc as rpc
from bare_rpc import RPCRemoteError, StreamFlag
from bare_rpc.incoming_stream import HIGH_WATER_MARK, IncomingStream


class FakeRPC:
    def __init__(self):
        self.sent = []
        self._incoming_streams = {}

    def _send(self, frame):
        self.sent.append(frame)


def test_iterates_buffered_then_ends():
    host = FakeRPC()

    async def body():
        s = IncomingStream(host, 7, StreamFlag.RESPONSE)
        host._incoming_streams[7] = s
        s.push(b"a")
        s.push(b"b")
        s.end()
        return [chunk async for chunk in s]

    assert asyncio.run(asyncio.wait_for(body(), 2)) == [b"a", b"b"]
    assert 7 not in host._incoming_streams


def test_parked_consumer_gets_direct_handoff():
    host = FakeRPC()

    async def body():
        s = IncomingStream(host, 7, StreamFlag.RESPONSE)
        got = []

        async def consume():
            async for chunk in s:
                got.append(chunk)

        task = asyncio.ensure_future(consume())
        await asyncio.sleep(0)  # consumer parks on an empty buffer
        s.push(b"x")
        await asyncio.sleep(0)
        s.end()
        await task
        return got

    assert asyncio.run(asyncio.wait_for(body(), 2)) == [b"x"]


def test_pause_at_high_water_and_resume_on_drain():
    host = FakeRPC()

    async def body():
        s = IncomingStream(host, 7, StreamFlag.RESPONSE)
        for i in range(HIGH_WATER_MARK):
            s.push(bytes([i]))
        paused = [rpc.decode_frame(f).flags for f in host.sent]
        # drain below the low-water mark
        out = []
        async for chunk in s:
            out.append(chunk)
            if len(out) == HIGH_WATER_MARK:
                s.end()
        resumed = [rpc.decode_frame(f).flags for f in host.sent]
        return paused, resumed

    paused, resumed = asyncio.run(asyncio.wait_for(body(), 2))
    assert (StreamFlag.RESPONSE | StreamFlag.PAUSE) in paused
    assert (StreamFlag.RESPONSE | StreamFlag.RESUME) in resumed


def test_destroy_with_error_raises_from_iterator():
    host = FakeRPC()

    async def body():
        s = IncomingStream(host, 8, StreamFlag.RESPONSE)

        async def consume():
            async for _ in s:
                pass

        task = asyncio.ensure_future(consume())
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
