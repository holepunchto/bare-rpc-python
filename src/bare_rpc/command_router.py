from .messages import RPCRemoteError
from .rpc import _maybe_await


class CommandRouter:
    def __init__(self):
        self._request_handlers = {}
        self._event_handlers = {}

    def on_request(self, command, handler):
        self._request_handlers[command] = handler

    def on_event(self, command, handler):
        self._event_handlers[command] = handler

    async def dispatch_request(self, request):
        handler = self._request_handlers.get(request.command)

        if handler is None:
            raise RPCRemoteError(
                f"Unknown command {request.command}", "ERR_UNKNOWN_COMMAND", 0
            )

        await request.reply(await _maybe_await(handler(request)))

    async def dispatch_event(self, event):
        handler = self._event_handlers.get(event.command)

        if handler is not None:
            await _maybe_await(handler(event))
