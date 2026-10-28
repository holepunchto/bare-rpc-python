import asyncio
import socket

import bare_rpc as rpc
from bare_rpc import RPC, RPCRemoteError, StreamTransport
from pipe import End, pair


def make_pair(
    *, on_request_a=None, on_event_a=None, on_request_b=None, on_event_b=None
):
    left, right = pair()

    a = RPC(left, on_request=on_request_a, on_event=on_event_a)
    b = RPC(right, on_request=on_request_b, on_event=on_event_b)
    return a, b


class Broken:
    def __init__(self):
        self._closed = asyncio.Event()

    async def read(self):
        await self._closed.wait()
        return b""

    async def write(self, data):
        raise OSError("transport down")

    def close(self):
        self._closed.set()


class Slow(End):
    def __init__(self, order):
        super().__init__()
        self.order = order

    async def write(self, data):
        self.order.append("write-start")
        await asyncio.sleep(0)
        await super().write(data)
        self.order.append("write-done")


def test_request_response_roundtrip():
    async def on_request(req):
        assert req.command == 5
        assert req.data == b"ping"
        await req.reply(b"pong")

    async def body():
        a, _b = make_pair(on_request_b=on_request)
        return await a.request(5, b"ping")

    assert asyncio.run(body()) == b"pong"


def test_empty_payload_roundtrips_to_empty_bytes():
    async def on_request(req):
        assert req.data == b""  # None/empty request payload decodes to b""
        await req.reply()  # empty response

    async def body():
        a, _b = make_pair(on_request_b=on_request)
        return await a.request(7, None)

    assert asyncio.run(body()) == b""


def test_event_delivery():
    async def body():
        got = asyncio.get_running_loop().create_future()

        def on_event(ev):
            got.set_result((ev.command, ev.data))

        a, _b = make_pair(on_event_b=on_event)
        await a.event(3, b"note")
        return await asyncio.wait_for(got, 1.0)

    assert asyncio.run(body()) == (3, b"note")


def test_concurrent_requests_resolve_independently():
    async def on_request(req):
        await req.reply(bytes([req.data[0] * 2]))

    async def body():
        a, _b = make_pair(on_request_b=on_request)
        results = await asyncio.gather(
            a.request(1, bytes([10])),
            a.request(1, bytes([20])),
            a.request(1, bytes([30])),
        )
        return results

    assert asyncio.run(body()) == [bytes([20]), bytes([40]), bytes([60])]


def test_reassembles_split_and_coalesced_frames():
    async def body():
        seen = []
        done = asyncio.Event()

        async def on_request(req):
            seen.append((req.id, req.command, req.data))
            if len(seen) == 2:
                done.set()

        left, right = pair()
        RPC(left, on_request=on_request)
        f1 = rpc.encode_request(1, 5, data=b"hi")
        f2 = rpc.encode_request(2, 6, data=b"yo")
        await right.write(f1[:3])
        await right.write(f1[3:] + f2)
        await asyncio.wait_for(done.wait(), 1.0)
        return seen

    assert asyncio.run(body()) == [(1, 5, b"hi"), (2, 6, b"yo")]


def test_handler_raise_becomes_error_response():
    class Boom(Exception):
        code = "BAD"
        errno = 42

    async def on_request(_req):
        raise Boom("kaboom")

    async def body():
        a, _b = make_pair(on_request_b=on_request)
        try:
            await a.request(5, b"x")
        except RPCRemoteError as err:
            return err
        raise AssertionError("expected RPCRemoteError")

    err = asyncio.run(body())
    assert err.message == "kaboom"
    assert err.code == "BAD"
    assert err.errno == 42


def test_decode_failure_poisons_and_rejects_pending():
    async def body():
        errors = []

        left, right = pair()
        r = RPC(left, on_error=errors.append)
        task = asyncio.ensure_future(r.request(5, b"x"))
        await asyncio.sleep(0)
        # A complete frame whose body ends after the type, so decoding the
        # request fields runs out of bytes.
        await right.write((1).to_bytes(4, "little") + bytes([1]))
        try:
            await task
        except Exception as exc:  # noqa: BLE001
            first = exc
        assert errors
        raised = None
        try:
            await r.request(1)
        except Exception as exc:  # noqa: BLE001
            raised = exc
        await r.event(1)
        await r.close()
        return first, raised, left.closed

    first, raised, closed = asyncio.run(body())
    assert first is raised
    assert closed


def test_oversize_frame_poisons():
    async def body():
        errors = []

        left, right = pair()
        r = RPC(left, on_error=errors.append, max_frame_size=8)
        await right.write((100).to_bytes(4, "little"))
        await r.close()
        return errors

    errors = asyncio.run(body())
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
    assert "max_frame_size" in str(errors[0])


def test_write_raise_poisons_and_propagates():
    async def body():
        errors = []

        r = RPC(Broken(), on_error=errors.append)
        raised = None
        try:
            await r.request(5, b"x")
        except OSError as exc:
            raised = exc
        await r.close()
        return errors, raised

    errors, raised = asyncio.run(body())
    assert isinstance(raised, OSError)
    assert errors and errors[0] is raised


def test_request_with_no_handler_is_unanswered():
    async def body():
        a, _b = make_pair()  # no on_request on b
        try:
            await asyncio.wait_for(a.request(5, b"x"), 0.2)
        except asyncio.TimeoutError:
            return "unanswered"
        return "answered"

    assert asyncio.run(body()) == "unanswered"


def test_event_returns_before_its_write_completes():
    order = []

    async def body():
        left, right = Slow(order), End()
        left.peer, right.peer = right, left
        r = RPC(left)
        await r.event(5, b"x")
        order.append("event-returned")
        await r.close()
        return order

    result = asyncio.run(body())
    assert "write-done" in result
    assert result.index("event-returned") < result.index("write-done")


def test_frames_are_sent_in_call_order():
    async def body():
        left, right = pair()
        r = RPC(left)
        await r.event(1, b"a")
        task = asyncio.ensure_future(r.request(2, b"b"))
        await asyncio.sleep(0)
        await r.close()
        task.cancel()
        return left.written

    sent = asyncio.run(body())
    assert rpc.decode_frame(sent[0]).command == 1
    assert rpc.decode_frame(sent[1]).command == 2


def test_close_writes_earlier_frames_and_drops_later_ones():
    async def body():
        left, right = pair()
        r = RPC(left)
        await r.event(1, b"a")
        await r.close()
        await r.event(2, b"b")
        return left.written, left.closed

    sent, closed = asyncio.run(body())
    assert [rpc.decode_frame(frame).command for frame in sent] == [1]
    assert closed


def test_request_after_close_raises_not_hangs():
    async def body():
        r = RPC(pair()[0])
        await r.close()
        raised = None
        try:
            await asyncio.wait_for(r.request(1, b"x"), 0.5)
        except RuntimeError as exc:
            raised = exc
        except asyncio.TimeoutError:
            raised = "hung"
        return raised

    raised = asyncio.run(body())
    assert isinstance(raised, RuntimeError)


def test_close_rejects_in_flight_request():
    async def body():
        r = RPC(pair()[0])
        task = asyncio.ensure_future(r.request(1, b"x"))
        await asyncio.sleep(0)
        await r.close()
        raised = None
        try:
            await asyncio.wait_for(task, 0.5)
        except RuntimeError as exc:
            raised = exc
        except asyncio.TimeoutError:
            raised = "hung"
        return raised

    raised = asyncio.run(body())
    assert isinstance(raised, RuntimeError)


def test_event_handler_error_poisons_the_connection():
    async def body():
        errors = []
        done = asyncio.Event()

        def on_event(_ev):
            raise ValueError("bad event")

        def on_error(err):
            errors.append(err)
            done.set()

        left, right = pair()
        r = RPC(left, on_event=on_event, on_error=on_error)
        await right.write(rpc.encode_event(3, b"note"))
        await asyncio.wait_for(done.wait(), 1.0)
        raised = None
        try:
            await r.request(1)
        except Exception as exc:  # noqa: BLE001
            raised = exc
        await r.close()
        return errors, raised

    errors, raised = asyncio.run(body())
    assert len(errors) == 1
    assert isinstance(raised, ValueError)
    assert str(raised) == "bad event"


def test_transport_end_closes_the_session():
    async def body():
        left, right = pair()
        r = RPC(left)
        task = asyncio.ensure_future(r.request(1, b"x"))
        await right.read()
        await right.close()
        raised = None
        try:
            await asyncio.wait_for(task, 0.5)
        except RuntimeError as exc:
            raised = exc
        except asyncio.TimeoutError:
            raised = "hung"
        return raised

    assert isinstance(asyncio.run(body()), RuntimeError)


def test_leaving_async_with_closes_the_transport():
    async def body():
        left, right = pair()
        async with RPC(left) as r:
            await r.event(1)
        return left.closed, await right.read(), await right.read()

    closed, frame, end = asyncio.run(body())
    assert closed
    assert rpc.decode_frame(frame).command == 1
    assert end == b""


def test_async_on_error_is_awaited():
    ran = []

    async def on_error(err):
        ran.append(err)

    async def body():
        left, right = pair()
        r = RPC(left, on_error=on_error, max_frame_size=8)
        await right.write((9).to_bytes(4, "little") + b"123456789")
        await r.close()
        for _ in range(3):
            await asyncio.sleep(0)
        return ran

    assert len(asyncio.run(body())) == 1


def test_runs_over_asyncio_streams():
    async def on_request(req):
        await req.reply(req.data + b" pong")

    async def body():
        left, right = socket.socketpair()
        a = RPC(StreamTransport(*await asyncio.open_connection(sock=left)))
        b = RPC(
            StreamTransport(*await asyncio.open_connection(sock=right)),
            on_request=on_request,
        )
        reply = await a.request(1, b"ping")
        await a.close()
        await b.close()
        return reply

    assert asyncio.run(asyncio.wait_for(body(), 2)) == b"ping pong"
