# TeeRex

`tee-rex` duplicates TCP traffic received on a port to multiple destinations.
One destination is the primary server which responds to the incoming TCP traffic.
Other destinations are mirrors whose responses are discarded.

Mirror failures are isolated and do not affect the primary traffic path, making this suitable for production use.

## Why

This is helpful in the following scenarios:

- Test a Dev/QA/secondary server with the same requests, traffic
  and load that the primary production server handles.

- Performance testing of candidate servers against the existing
  production server.

- Rewrite a server in another language and verify the new server
  responds the same as the existing server for identical requests.

## Install

### Download from releases

Download a pre-built binary from the release page.

### Using `go install`

```
$ go install github.com/oldwired/tee-rex@latest
```
The `tee-rex` binary is now available in your `$GOPATH/bin` directory.

### Compile from source

```
$ git clone https://github.com/oldwired/tee-rex.git
$ cd tee-rex
$ go build .
```

### Build with version info

A plain `go build` reports version `dev`. Release binaries have the version stamped from the Git tag by the release workflow (tag `v1.2.3` → `1.2.3`), which also runs the binary and fails the release if the reported version doesn't match the tag. To stamp a local build the same way:

```
$ make build-version          # version from `git describe --tags`, plus commit and build time
$ ./tee-rex -version
1.1.0 (abc1234 2024-01-15T10:30:00Z)
```

or by hand:

```
$ go build -ldflags "-X main.Version=1.2.3 -X main.BuildCommit=$(git rev-parse --short HEAD) -X main.BuildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" .
```

## Usage

```
tee-rex -l <listen_addr> -p <primary_addr> [-m <mirror_addrs>] [-line]
```

Flags:
- `-version` - Print version and exit
- `-l` - Listen address for incoming connections (default: `localhost:8080`)
- `-p` - Primary server address; responses are relayed to clients (default: `localhost:9090`)
- `-m` - Mirror addresses (comma-separated); receive traffic but responses are discarded
- `-loglevel` - Log level: `error`, `info` (default), `debug` (anything else is rejected at startup)
- `-logformat` - Log format: `text` (default) or `json`
- `-logfile` - Append logs to a file instead of stderr; `SIGHUP` reopens it (logrotate-friendly)
- `-line` - Enable line-oriented mode for text protocols (SMTP, FTP, IRC, etc.)
- `-delim` - Line delimiter: `crlf` (default), `lf`, `cr`, `etx`, `eot`, hex like `0x03`/`0x0d0a`, or custom string
- `-esc` - Escape character for framed protocols: `esc` (0x1B), `0x1b`, or custom byte (must differ from the delimiter; `0x00` is rejected)
- `-maxframe` - Maximum frame size in bytes for line mode (default: 65536)
- `-partial` - Partial frame policy at clean client EOF: `drop` (discard), `forward` (send anyway), `error` (log and close) (default: drop). Fragments interrupted by errors/idle/teardown are always dropped
- `-timeout` - Connection timeout for dialing primary and mirrors; must be positive (default: 10s)
- `-writetimeout` - Write timeout for sending data, minimum 5s enforced with a warning (default: 30s)
- `-idletimeout` - Session idle timeout: a session ends as idle only when *neither* direction has seen data for this long; 0 to disable (default: 2m)
- `-halfclosetimeout` - After one peer closes its side (TCP half-close), keep relaying the other direction until it too closes or carries no data for this long; `0` disables half-close support (default: 30s)
- `-mirrorbuf` - Buffer size for mirror write channels; must be at least 1 (default: 100)
- `-mirrordrain` - How long to wait at session end for queued mirror data to flush; 0 drops it immediately (default: 5s)
- `-maxdrains` - Maximum sessions concurrently flushing mirror data after their client/primary traffic has ended; `0` disables the limit (default: 1024)
- `-shutdowntimeout` - Graceful shutdown timeout before force-closing connections (default: 30s)
- `-statsinterval` - Interval for the aggregate `stats` log line; 0 disables it (default: 1m)
- `-maxconns` - Maximum concurrent client connections; `0` disables the limit (default: 1024)

Setting `-delim`, `-esc`, `-partial`, or `-maxframe` without `-line` logs a warning — those flags have no effect in raw mode.

## Sample use case

Mirror production traffic to a staging server:

```
$ tee-rex -l 0.0.0.0:8080 -p prod-server:3000 -m staging-server:3000
```

Mirror to multiple destinations:

```
$ tee-rex -l 0.0.0.0:8080 -p prod:3000 -m staging:3000,dev:3000,metrics:3000
```

Use as a simple TCP proxy (no mirrors):

```
$ tee-rex -l 0.0.0.0:8080 -p backend:3000
```

### Line-oriented mode

For text-based protocols (SMTP, FTP, IRC, POP3, etc.), use `-line` to ensure mirrors always receive complete lines:

```
$ tee-rex -l :25 -p mail-server:25 -m mirror:25 -line
```

With a different delimiter (LF only for IRC):

```
$ tee-rex -l :6667 -p irc-server:6667 -m logger:6667 -line -delim lf
```

### Escape-framed protocols

Some protocols use escape sequences where the delimiter can appear in data by escaping it. For example, a protocol using ETX (0x03) as frame end and ESC (0x1B) as escape:

- `1B xx` = literal byte `xx` in data (escape causes next byte to be treated as data)
- `03` = end of frame (unescaped delimiter)

```
$ tee-rex -l :5000 -p as-server:5000 -m mirror:5000 -line -delim etx -esc esc
```

### Custom multi-byte delimiters

For protocols using multi-byte delimiters (e.g., XML-over-TCP with closing tags):

```
$ tee-rex -l :5000 -p server:5000 -m mirror:5000 -line -delim '</data>'
```

## Behavior

- Each client connection is handled independently
- If the primary server is unreachable, only that client connection fails
- At most `-maxconns` connections are served concurrently (default 1024); over-limit connections are accepted and closed immediately with a logged warning. Use `-maxconns 0` to disable the limit
- Mirror writes are asynchronous and non-blocking; a slow mirror won't affect primary traffic. When a mirror's queue is full, the chunk is dropped before it is even copied, so an overloaded mirror costs the primary path neither allocations nor locks
- Whatever a mirror sends back is read and discarded continuously, so a mirror that answers every request (even with responses larger than the socket buffers) keeps receiving later traffic instead of filling its send buffer and stalling
- Mirrors automatically reconnect on failure with exponential backoff (100ms to 5s)
- If a mirror's buffer fills up (slow consumer), data is dropped for that mirror only
- Every dropped chunk is counted in `mirror_drops` regardless of cause (buffer full, write failure, mirror unreachable, teardown) and attributed per mirror in the session summary
- A mirror that has never connected logs a `mirror unreachable` warning once per session, so a dead or misconfigured mirror address is visible at the default log level
- If a mirror write fails mid-stream, the in-flight chunk is dropped (counted in `mirror_drops`) and the mirror reconnects for later data — partial writes are never replayed, so a reconnected mirror resumes on a clean boundary
- At session end, data still queued for mirrors is flushed for up to `-mirrordrain` (default 5s) before the mirror connection closes; anything left undelivered after that is counted in `mirror_drops`. The session's `-maxconns` slot is released as soon as its client/primary traffic is over, *before* the mirror flush, so mirror trouble never blocks new clients. Mirror flushing has its own budget (`-maxdrains`, default 1024 concurrent sessions); when it is exhausted a finishing session drops its queued mirror data (counted) instead of holding resources. A force-stopped mirror aborts promptly, including an in-flight connect/DNS lookup, so a session's mirror cleanup never takes longer than `-mirrordrain` plus one interrupted write
- TCP half-close is relayed transparently: when one peer closes its write side (clean EOF), the other peer sees EOF, and the opposite direction keeps flowing — a primary may close its side and still read a client upload, and a client may half-close and still receive the primary's response. The session ends when the other peer also closes, or when the remaining direction carries no data for `-halfclosetimeout` (default 30s; reason `half_close_timeout`). Resets and errors on either side still tear down both directions promptly. `-halfclosetimeout 0` disables this: a primary close ends the whole session at once, and after a client close the primary may still respond until the session idle timeout
- TCP keepalive (30s) is enabled on all connections to detect dead peers; a keepalive failure surfaces as a read error rather than being mistaken for idleness
- Idle detection is session-wide: a session ends as idle (`session_idle`) only when *neither* direction has seen data for `-idletimeout` (default 2m). One-way traffic — e.g. a client heartbeating into a server that never responds, as in alarm-receiver protocols like FrontelGI — keeps the session alive. Idle time is measured on the monotonic clock, so NTP corrections don't disturb it
- On SIGINT/SIGTERM, waits for active sessions to finish gracefully before force-closing; force-closed sessions end with reason `shutdown`. A second SIGINT/SIGTERM exits immediately

### Line mode vs raw mode

| Feature | Raw mode (default) | Line mode (`-line`) |
|---------|-------------------|---------------------|
| Data unit | Arbitrary byte chunks | Complete lines with delimiter |
| Reconnected mirrors | May receive partial data mid-stream | Always start at line boundary |
| Max frame size | Unlimited | 64KB (configurable via `-maxframe`) |
| Best for | Binary protocols, HTTP | SMTP, FTP, IRC, POP3, Redis |

**Note:** In line mode, frames exceeding the max size will cause the connection to be closed. The `-esc` option requires a single-byte delimiter.

## Logging

Logs are written to stderr in structured format (key=value pairs by default, or JSON with `-logformat=json`). Use `-logfile` to append them to a file instead; `SIGHUP` reopens the file so standard logrotate setups work.

Every `-statsinterval` (default 1m) an aggregate `stats` line reports active/total sessions, total bytes in/out, and mirror drops (overall and per mirror address) since process start. The byte counters are live: they count bytes received from clients (`bytes_in`) and bytes delivered to clients (`bytes_out`) as they flow, so a long-lived session shows up while it is still open rather than only when it ends.

### Log levels

- **error** - Only errors (primary failures, protocol violations)
- **info** (default) - Startup config, session start/end with stats, primary connections, mirror state changes and unreachable warnings, periodic aggregate stats
- **debug** - Data received on either end (per chunk, both directions), frame-level forwarding, mirror write confirmations, mirror reconnect attempts, dropped data

### Example output

```
# Default (info level)
time=2024-01-15T10:30:00.000Z level=INFO msg="session started" sid=1 client=127.0.0.1:52341 primary=localhost:9090
time=2024-01-15T10:30:00.001Z level=INFO msg="primary connected" sid=1 primary=localhost:9090
time=2024-01-15T10:30:00.002Z level=INFO msg="mirror up" sid=1 mirror=localhost:9091
time=2024-01-15T10:30:05.123Z level=INFO msg="session ended" sid=1 client=127.0.0.1:52341 reason=client_eof duration=5.122s bytes_in=1024 bytes_out=2048 mirror_drops=0
time=2024-01-15T10:31:00.000Z level=INFO msg=stats active_sessions=3 total_sessions=17 bytes_in=480394 bytes_out=112388 mirror_drops=0 drops_localhost:9091=0

# With -loglevel=debug (shows receive and frame-level details)
time=2024-01-15T10:30:00.010Z level=DEBUG msg="data received" sid=1 from=client bytes=128
time=2024-01-15T10:30:00.010Z level=DEBUG msg="frame forwarded" sid=1 size=128
time=2024-01-15T10:30:00.011Z level=DEBUG msg="mirror write ok" sid=1 mirror=localhost:9091 bytes=128

# JSON format (-logformat=json)
{"time":"2024-01-15T10:30:00.000Z","level":"INFO","msg":"session started","sid":1,"client":"127.0.0.1:52341","primary":"localhost:9090"}
```

### Session summary

Each session logs a summary on close with:
- `sid` - Unique session ID
- `client` - Client address
- `reason` - Why the session ended (see below)
- `duration` - Session duration
- `bytes_in` - Bytes received from client
- `bytes_out` - Bytes sent to client (from primary)
- `frames` - Frame count (line mode only)
- `mirror_drops` - Data dropped across all mirrors (unreachable, slow, failed writes, teardown); a per-mirror `mirror session drops` line precedes the summary when a specific mirror dropped data

The `reason` field is one of:
- `client_eof`, `client_reset`, `client_read_error` - client side closed, reset, or errored
- `client_slow` - client too slow to drain responses (write timeout)
- `client_write_error` - other failure writing back to the client
- `primary_eof`, `primary_reset` - primary closed or reset
- `primary_dial_failed`, `primary_write_error`, `primary_read_error` - primary connection problems
- `session_idle` - no data in either direction for `-idletimeout`
- `half_close_timeout` - one peer closed its side, the other stayed open but sent nothing for `-halfclosetimeout` (`client_eof`/`primary_eof` name the peer that closed *first* when the other then finished cleanly)
- `shutdown` - force-closed because the proxy was shutting down
- `frame_too_large`, `partial_frame` - line-mode framing violations

## License

`tee-rex` is licensed under the **GNU Affero General Public License v3.0 or later**
(AGPL-3.0-or-later). See [`LICENSE`](LICENSE) for the full text. This is not a
dual-licensed project; see [`NOTICE`](NOTICE) for a summary of the licensing and
upstream attribution.

### Attribution

This project is a fork of [`tcpmirror`](https://github.com/codeexpress/tcpmirror)
and includes code originally licensed under the MIT License. The original MIT
notice is preserved in [`NOTICE`](NOTICE) and reproduced below:

```
MIT License

Copyright (c) 2020 Code Express

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

Modifications and additions are copyright (c) 2026 oldwired and licensed under
AGPL-3.0-or-later.
