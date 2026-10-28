from collections.abc import Awaitable
from contextlib import suppress
from typing import Protocol


class Transport(Protocol):
    """A byte transport that ``RPC`` runs a session over, such as a
    ``Sidecar`` from bare-sidecar-python, an ``IPC`` from bare-stow-host-python,
    or a ``StreamTransport``."""

    async def read(self) -> bytes:
        """Read the next chunk of data, or ``b""`` once the transport has
        ended."""

    async def write(self, data: bytes) -> None:
        """Write ``data`` and wait for it to drain."""

    def close(self) -> Awaitable[None] | None:
        """Close the transport, optionally returning an awaitable that
        completes once it is closed."""


class StreamTransport:
    """A ``Transport`` over an asyncio ``StreamReader`` and ``StreamWriter``,
    such as those returned by ``asyncio.open_connection()``."""

    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer

    async def read(self, n=65536):
        return await self._reader.read(n)

    async def write(self, data):
        self._writer.write(data)
        await self._writer.drain()

    async def close(self):
        self._writer.close()

        # The connection may already have been reset by the peer.
        with suppress(OSError):
            await self._writer.wait_closed()
