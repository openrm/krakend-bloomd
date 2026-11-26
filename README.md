# krakend-bloomd
Bloomd integration for the KrakenD framework

## Configuration

Under the `github_com/openrm/krakend-bloomd` key of the service `extra_config`:

| Key           | Description                                                        |
|---------------|--------------------------------------------------------------------|
| `name`        | Filter to check (required)                                         |
| `server_addr` | `host:port` of bloomd                                              |
| `token_keys`  | Claims whose `<claim>-<value>` is looked up in the filter          |
| `pool_size`   | Connections to bloomd per gateway process (default 4)              |

## Behaviour

- Every request uses a connection of its own for one round trip, so concurrent requests never share
  a socket. (A single shared connection mixed up the replies under load: `slice bounds out of range`,
  `i/o timeout`, `use of closed network connection`.)
- Dialing, a round trip and waiting for a free connection are each bounded by one second.
- Fails open: if bloomd cannot be reached, times out or answers something other than one answer per
  key, the error is logged and the token is not rejected. The connection is then closed and dialed
  again on the next request.
- bloomd being down when the gateway starts is logged, not fatal; connections are made on demand.

## Tests

`go test -race ./...` runs against a fake bloomd server in the test file (concurrent requests,
stalled replies, dropped connections, bloomd down at start).
