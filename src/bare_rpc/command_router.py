from .messages import RPCRemoteError
from .rpc import _maybe_await


class CommandRouter:
    """Sends each request and event to the handler registered for its command.

    Pass ``dispatch_request`` and ``dispatch_event`` to ``RPC`` as
    ``on_request`` and ``on_event``.
    """

    def __init__(self):
        self._request_handlers = {}
        self._event_handlers = {}

    def on_request(self, command, handler):
        """Answer requests for ``command`` with ``handler``, replacing any
        handler registered for it.

        ``handler`` is called with the ``IncomingRequest`` and returns the
        payload to reply with, unless it already answered the request. It may
        be a coroutine function.
        """
        self._request_handlers[command] = handler

    def on_event(self, command, handler):
        """Deliver events for ``command`` to ``handler``, replacing any handler
        registered for it.

        ``handler`` is called with the ``IncomingEvent`` and may be a coroutine
        function.
        """
        self._event_handlers[command] = handler

    async def dispatch_request(self, request):
        """Answer ``request`` with the handler for its command.

        Raises ``RPCRemoteError`` with code ``ERR_UNKNOWN_COMMAND`` if no
        handler is registered for it.
        """
        handler = self._request_handlers.get(request.command)

        if handler is None:
            raise RPCRemoteError(
                f"Unknown command {request.command}", "ERR_UNKNOWN_COMMAND", 0
            )

        await request.reply(await _maybe_await(handler(request)))

    async def dispatch_event(self, event):
        """Deliver ``event`` to the handler for its command, or drop it if
        there is none."""
        handler = self._event_handlers.get(event.command)

        if handler is not None:
            await _maybe_await(handler(event))
