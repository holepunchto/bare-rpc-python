# Changelog

Entries land under Unreleased and move to a version heading when that version is tagged.

## Unreleased

- Add `CommandRouter`, which sends each request and event to the handler registered for its command. Request handlers return the data to reply with, and requests for a command without a handler are rejected with `ERR_UNKNOWN_COMMAND`.
- Opening a response stream with `create_response_stream()` now counts as answering the request, so a later `reply()` or `reject()` is ignored instead of sending a second response, matching the JS implementation.
- `decode_frame` now raises `UnknownMessageTypeError` for a type tag this format does not define, where it returned `None`. A peer sending an unknown type fails the session rather than being ignored, matching the wire spec and the JS and C implementations.

## 0.0.1

Initial release.

- Message/frame wire codec, wire-compatible with the JavaScript `bare-rpc` reference (shared `hrpc-test` conformance vectors).
- Async `RPC` runtime: unary request/response, fire-and-forget events, and all three streaming shapes (response, request, duplex) with cork/uncork backpressure.
- `RPCRemoteError` carries a code/message/errno across the wire.
- Built on `compact-encoding`.
