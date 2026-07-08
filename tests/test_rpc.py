import asyncio

import bare_rpc as rpc
from bare_rpc import RPC, RPCRemoteError


def make_pair(
    *, on_request_a=None, on_event_a=None, on_request_b=None, on_event_b=None
):
    holder = {}

    async def send_a(frame):
        await holder["b"].receive(frame)

    async def send_b(frame):
        await holder["a"].receive(frame)

    holder["a"] = RPC(send_a, on_request=on_request_a, on_event=on_event_a)
    holder["b"] = RPC(send_b, on_request=on_request_b, on_event=on_event_b)
    return holder["a"], holder["b"]


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


def test_receive_reassembles_split_and_coalesced_frames():
    async def body():
        seen = []
        done = asyncio.Event()

        async def on_request(req):
            seen.append((req.id, req.command, req.data))
            if len(seen) == 2:
                done.set()

        async def send(_frame):
            pass

        r = RPC(send, on_request=on_request)
        f1 = rpc.encode_request(1, 5, data=b"hi")
        f2 = rpc.encode_request(2, 6, data=b"yo")
        # f1 split across two receives, then f2 coalesced with nothing extra
        await r.receive(f1[:3])
        await r.receive(f1[3:] + f2)
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
        sent = []

        async def send(frame):
            sent.append(frame)

        r = RPC(send, on_error=errors.append)
        # start a request so there is a pending future to reject
        task = asyncio.ensure_future(r.request(5, b"x"))
        await asyncio.sleep(0)  # let the request register + send
        # feed a complete frame whose body overruns during decode: length=1,
        # body is just the REQUEST type byte, so decoding the request fields
        # runs out of bytes -> OutOfBounds -> poison. (A frame that is merely
        # short of its declared length is buffered, not a decode failure.)
        await r.receive((1).to_bytes(4, "little") + bytes([1]))
        try:
            await task
        except Exception as exc:  # noqa: BLE001
            first = exc
        assert errors  # on_error was called
        # subsequent request raises immediately; event is a no-op; receive is a no-op
        raised = None
        try:
            await r.request(1)
        except Exception as exc:  # noqa: BLE001
            raised = exc
        await r.event(1)  # no raise
        await r.receive(b"whatever")  # no raise
        return first, raised

    first, raised = asyncio.run(body())
    assert first is raised  # same stored failure error


def test_oversize_frame_poisons():
    async def body():
        errors = []

        async def send(_frame):
            pass

        r = RPC(send, on_error=errors.append, max_frame_size=8)
        # length prefix claims 100 bytes -> 4 + 100 > 8
        await r.receive((100).to_bytes(4, "little"))
        return errors

    assert len(asyncio.run(body())) == 1


def test_send_raise_poisons_and_propagates():
    async def body():
        errors = []

        async def send(_frame):
            raise OSError("transport down")

        r = RPC(send, on_error=errors.append)
        raised = None
        try:
            await r.request(5, b"x")
        except OSError as exc:
            raised = exc
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
