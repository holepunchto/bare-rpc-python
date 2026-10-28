import asyncio

import pytest

from bare_rpc import RPC, CommandRouter, RPCRemoteError
from pipe import pair


def make_pair(router):
    left, right = pair()

    RPC(right, on_request=router.dispatch_request, on_event=router.dispatch_event)
    return RPC(left)


def test_replies_with_the_handler_result():
    router = CommandRouter()

    async def ping(req):
        assert req.data == b"ping"
        return b"pong"

    router.on_request(1, ping)

    async def body():
        return await make_pair(router).request(1, b"ping")

    assert asyncio.run(body()) == b"pong"


def test_accepts_synchronous_handlers():
    router = CommandRouter()
    router.on_request(1, lambda req: req.data.upper())

    async def body():
        return await make_pair(router).request(1, b"ping")

    assert asyncio.run(body()) == b"PING"


def test_routes_by_command():
    router = CommandRouter()
    router.on_request(1, lambda req: b"one")
    router.on_request(2, lambda req: b"two")

    async def body():
        client = make_pair(router)
        return await client.request(2), await client.request(1)

    assert asyncio.run(body()) == (b"two", b"one")


def test_replaces_a_registered_handler():
    router = CommandRouter()
    router.on_request(1, lambda req: b"old")
    router.on_request(1, lambda req: b"new")

    async def body():
        return await make_pair(router).request(1)

    assert asyncio.run(body()) == b"new"


def test_rejects_when_the_handler_raises():
    router = CommandRouter()

    def fail(req):
        raise RPCRemoteError("nope", "ERR_NOPE", 7)

    router.on_request(1, fail)

    async def body():
        await make_pair(router).request(1)

    with pytest.raises(RPCRemoteError) as err:
        asyncio.run(body())

    assert (err.value.message, err.value.code, err.value.errno) == (
        "nope",
        "ERR_NOPE",
        7,
    )


def test_rejects_unknown_commands():
    router = CommandRouter()

    async def body():
        await make_pair(router).request(42)

    with pytest.raises(RPCRemoteError) as err:
        asyncio.run(body())

    assert err.value.code == "ERR_UNKNOWN_COMMAND"


def test_delivers_events_to_the_handler():
    router = CommandRouter()

    async def body():
        received = asyncio.get_running_loop().create_future()
        router.on_event(3, lambda event: received.set_result(event.data))

        await make_pair(router).event(3, b"note")

        return await asyncio.wait_for(received, 1)

    assert asyncio.run(body()) == b"note"


def test_drops_unknown_events():
    router = CommandRouter()
    seen = []
    router.on_event(3, lambda event: seen.append(event.command))

    async def body():
        delivered = asyncio.get_running_loop().create_future()
        router.on_event(5, lambda event: delivered.set_result(None))
        client = make_pair(router)
        await client.event(4, b"ignored")
        await client.event(3, b"note")
        await client.event(5)
        await asyncio.wait_for(delivered, 1)

    asyncio.run(body())

    assert seen == [3]
