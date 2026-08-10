# Changelog

Entries land under Unreleased and move to a version heading when that version is tagged.

## Unreleased

- `decode_frame` now raises `UnknownMessageTypeError` for a type tag this format does not define, where it returned `None`. A peer sending an unknown type fails the session rather than being ignored, matching the wire spec and the JS and C implementations.

## 0.0.1

Initial release.

- Message/frame wire codec, wire-compatible with the JavaScript `bare-rpc` reference (shared `hrpc-test` conformance vectors).
- Async `RPC` runtime: unary request/response, fire-and-forget events, and all three streaming shapes (response, request, duplex) with cork/uncork backpressure.
- `RPCRemoteError` carries a code/message/errno across the wire.
- Built on `compact-encoding`.
