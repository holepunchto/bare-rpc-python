# Changelog

## 1.0.0

First stable release.

- Message/frame wire codec, wire-compatible with the JavaScript `bare-rpc` reference (shared `hrpc-test` conformance vectors).
- Async `RPC` runtime: unary request/response, fire-and-forget events, and all three streaming shapes (response, request, duplex) with cork/uncork backpressure.
- `RPCRemoteError` carries a code/message/errno across the wire.
- Built on `compact-encoding-python`.
