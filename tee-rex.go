// SPDX-License-Identifier: AGPL-3.0-or-later
//
// tee-rex — a TCP traffic-mirroring proxy.
//
// Portions of this file are derived from tcpmirror
// (https://github.com/codeexpress/tcpmirror), licensed under the MIT License.
// Original copyright (c) 2020 Code Express.
//
// Modifications copyright (c) 2026 oldwired.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License as published by the
// Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
// details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Build info (set via -ldflags)
var (
	Version     = "1.0.0"
	BuildCommit = ""
	BuildTime   = ""
)

// versionString returns the version with optional build info
func versionString() string {
	v := Version
	if BuildCommit != "" {
		v += " (" + BuildCommit
		if BuildTime != "" {
			v += " " + BuildTime
		}
		v += ")"
	}
	return v
}

const (
	// Mirror writer settings
	initialBackoff    = 100 * time.Millisecond
	maxBackoff        = 5 * time.Second
	backoffMultiplier = 2
	minWriteTimeout   = 5 * time.Second // minimum write timeout to prevent indefinite hangs

	// Accept-loop backoff on transient errors (e.g. fd exhaustion) to avoid a
	// busy spin while the listener itself is still healthy.
	acceptInitialBackoff = 5 * time.Millisecond
	acceptMaxBackoff     = 1 * time.Second

	// Line mode settings
	defaultMaxLineLen = 64 * 1024 // 64KB max line length
	maxBufioSize      = 64 * 1024 // Cap bufio buffer size (frame limit enforced separately)

	// I/O buffer size for copying data
	copyBufSize = 32 * 1024
)

// Session end reasons for structured logging
const (
	reasonClientEOF       = "client_eof"
	reasonClientReset     = "client_reset"
	reasonClientReadErr   = "client_read_error"
	reasonClientSlow      = "client_slow"        // client too slow to drain (write timeout)
	reasonClientWriteErr  = "client_write_error" // non-timeout failure writing to client
	reasonPrimaryDialFail = "primary_dial_failed"
	reasonPrimaryWriteErr = "primary_write_error"
	reasonPrimaryReadErr  = "primary_read_error"
	reasonPrimaryReset    = "primary_reset"
	reasonPrimaryEOF      = "primary_eof"  // primary closed its side (no error)
	reasonSessionIdle     = "session_idle" // no data in either direction for -idletimeout
	reasonFrameTooLarge   = "frame_too_large"
	reasonPartialFrame    = "partial_frame"
)

// sessionStats tracks per-session metrics for summary logging
type sessionStats struct {
	bytesIn     atomic.Uint64 // client → primary
	bytesOut    atomic.Uint64 // primary → client
	framesIn    atomic.Uint64 // line mode only
	mirrorDrops atomic.Uint64 // total drops across mirrors
	endReason   atomic.Value  // stores string reason for session end
}

// setEndReason sets the end reason (only first reason wins)
func (s *sessionStats) setEndReason(reason string) {
	s.endReason.CompareAndSwap(nil, reason)
}

// getEndReason returns the end reason or "unknown" if not set
func (s *sessionStats) getEndReason() string {
	if r := s.endReason.Load(); r != nil {
		return r.(string)
	}
	return "unknown"
}

// tracked holds both ends of a session for shutdown
type tracked struct {
	client  net.Conn
	primary net.Conn
}

// registerConn tracks a client connection for graceful shutdown.
// Only increments count if sid wasn't already present (prevents drift).
func registerConn(sid uint64, client net.Conn) {
	activeConnsMu.Lock()
	_, existed := activeConns[sid]
	activeConns[sid] = &tracked{client: client}
	activeConnsMu.Unlock()
	if !existed {
		activeCount.Add(1)
	}
}

// registerPrimary adds the primary connection to an existing tracked session.
func registerPrimary(sid uint64, primary net.Conn) {
	activeConnsMu.Lock()
	if t, ok := activeConns[sid]; ok {
		t.primary = primary
	}
	activeConnsMu.Unlock()
}

// unregisterConn removes a connection from tracking.
// Only decrements count if sid was present (prevents drift).
func unregisterConn(sid uint64) {
	activeConnsMu.Lock()
	_, existed := activeConns[sid]
	delete(activeConns, sid)
	activeConnsMu.Unlock()
	if existed {
		activeCount.Add(-1)
	}
}

// closeAllConns closes all tracked connections to force shutdown.
// Copies connections under lock, then closes without holding the lock.
func closeAllConns() {
	activeConnsMu.Lock()
	conns := make([]*tracked, 0, len(activeConns))
	sids := make([]uint64, 0, len(activeConns))
	for sid, t := range activeConns {
		conns = append(conns, t)
		sids = append(sids, sid)
	}
	activeConnsMu.Unlock()

	for i, t := range conns {
		// Set deadline to now to unblock any pending I/O, then close
		if t.client != nil {
			_ = t.client.SetDeadline(time.Now())
			_ = t.client.Close()
		}
		if t.primary != nil {
			_ = t.primary.SetDeadline(time.Now())
			_ = t.primary.Close()
		}
		slog.Debug("force-closed connection", "sid", sids[i])
	}
}

// mirrorWriter manages async writes to a mirror with reconnection support.
// The run() goroutine owns the connection lifecycle (dial/write/close) via the
// mw.conn field. connPtr mirrors the current connection as an atomic pointer so
// that interrupt() can force-close an in-flight write from another goroutine
// (used by stop() during teardown); no other external access to the connection
// is allowed.
type mirrorWriter struct {
	addr         string
	conn         net.Conn                 // connection lifecycle owned by run()
	connPtr      atomic.Pointer[net.Conn] // atomic view of conn for interrupt()
	ch           chan []byte
	done         chan struct{}
	drainOnce    sync.Once
	stopOnce     sync.Once
	connTimeout  time.Duration
	writeTimeout time.Duration
	wasConnected bool          // for state transition logging (only accessed from run goroutine)
	sid          uint64        // session ID for logging
	stats        *sessionStats // shared session stats for drop counting
}

var (
	versionPtr = flag.Bool("version", false, "Print version and exit")

	listenPtr = flag.String("l", "localhost:8080",
		"Listen on `host:port` for incoming traffic to be duplicated")

	primaryPtr = flag.String("p", "localhost:9090",
		"Relay traffic to primary `host:port` and establish a two way TCP connection")

	mirrorPtr = flag.String("m", "",
		"Mirror incoming traffic to `host:port[,host:port]...`. Can specify multiple addresses separated by a comma. Eg. localhost:9091,localhost:9092")

	logLevelPtr = flag.String("loglevel", "info",
		"Log level: error, info, debug")

	logFormatPtr = flag.String("logformat", "text",
		"Log format: text (key=value) or json")

	logFilePtr = flag.String("logfile", "",
		"Append logs to `file` instead of stderr")

	lineModePtr = flag.Bool("line", false, "Enable line-oriented mode for text protocols (SMTP, FTP, IRC, etc.)")

	delimPtr = flag.String("delim", "crlf",
		"Line delimiter: 'crlf', 'lf', 'cr', 'etx', 'eot', or custom string")

	escPtr = flag.String("esc", "",
		"Escape character for byte stuffing: 'esc' (0x1B), '0x1b', or single character. ESC+any = literal byte (not just delimiter).")

	maxFramePtr = flag.Int("maxframe", defaultMaxLineLen,
		"Maximum frame size in bytes for line mode (default 64KB)")

	partialPtr = flag.String("partial", "drop",
		"Partial frame policy at EOF: 'drop' (discard), 'forward' (send anyway), 'error' (log and close)")

	connTimeoutPtr = flag.Duration("timeout", 10*time.Second,
		"Connection timeout for dialing primary and mirrors")

	writeTimeoutPtr = flag.Duration("writetimeout", 30*time.Second,
		"Write timeout for sending data (minimum 5s enforced)")

	mirrorBufPtr = flag.Int("mirrorbuf", 100,
		"Buffer size for mirror write channels (per mirror)")

	mirrorDrainPtr = flag.Duration("mirrordrain", 5*time.Second,
		"How long to wait at session end for queued mirror data to flush (0 = drop immediately)")

	idleTimeoutPtr = flag.Duration("idletimeout", 2*time.Minute,
		"Idle timeout for client and primary connections (0 to disable)")

	shutdownTimeoutPtr = flag.Duration("shutdowntimeout", 30*time.Second,
		"Graceful shutdown timeout for active sessions")

	maxConnsPtr = flag.Int("maxconns", 1024,
		"Maximum concurrent client connections (0 = unlimited). Over-limit connections are closed immediately.")

	sessionCounter atomic.Uint64
	activeCount    atomic.Int64
	activeSessions sync.WaitGroup
	activeConnsMu  sync.Mutex
	activeConns    = make(map[uint64]*tracked) // for shutdown: sid -> client+primary conns
)

// writeAll writes all bytes to conn, handling partial writes.
// Sets write deadline if timeout > 0.
// Returns error if not all bytes could be written.
func writeAll(conn net.Conn, data []byte, timeout time.Duration) error {
	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		defer func() { _ = conn.SetWriteDeadline(time.Time{}) }() // clear deadline (error ignored - conn will be closed on failure)
	}
	for len(data) > 0 {
		n, err := conn.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// clientWriteErr wraps an error that occurred while writing to the destination
// (the client) in copyWithDeadlines, so the caller can attribute it to a
// slow/dead client rather than a primary read failure.
type clientWriteErr struct{ err error }

func (e *clientWriteErr) Error() string { return e.err.Error() }
func (e *clientWriteErr) Unwrap() error { return e.err }

// activityMonitor tracks when a session last saw data in either direction so
// idle detection is session-wide: traffic client→primary keeps the
// primary→client read alive and vice versa. Asymmetric protocols (e.g. a
// client that heartbeats into a server that never responds, like FrontelGI)
// would otherwise be torn down by the silent direction's own timer.
type activityMonitor struct {
	last atomic.Int64 // unix nanos of the last byte seen in either direction
}

func newActivityMonitor() *activityMonitor {
	m := &activityMonitor{}
	m.touch()
	return m
}

func (m *activityMonitor) touch() { m.last.Store(time.Now().UnixNano()) }

// idleDeadline returns the instant the session becomes idle: timeout after the
// last activity in either direction.
func (m *activityMonitor) idleDeadline(timeout time.Duration) time.Time {
	return time.Unix(0, m.last.Load()).Add(timeout)
}

// errReadUnblocked marks a read that was deliberately unblocked for session
// teardown (idleConn.unblock); isClosedErr treats it as a self-inflicted close.
var errReadUnblocked = errors.New("read unblocked for session teardown")

// idleConn wraps one side of a session and owns that side's read deadlines.
//
//   - Idle detection: before each read the deadline is armed to the shared
//     activity monitor's idle instant. A deadline that fires while the *other*
//     direction was recently active is stale — it is re-armed and the read
//     retried, so only a genuinely session-wide idle surfaces as a timeout.
//   - Receive logging: every chunk read is logged at debug level.
//   - unblock(): teardown helper that makes a blocked read return
//     errReadUnblocked instead of being retried (used when the opposite
//     direction has ended and further data has nowhere to go).
//
// With idleTimeout 0 no deadlines are armed, and a timeout (necessarily from an
// external SetReadDeadline: unblock() or shutdown's force-close) is never
// retried.
type idleConn struct {
	net.Conn
	idleTimeout time.Duration
	activity    *activityMonitor
	sid         uint64
	label       string // "client" or "primary", for logs

	mu        sync.Mutex // guards unblocked and serializes deadline arming against unblock()
	unblocked bool
}

func newIdleConn(conn net.Conn, idleTimeout time.Duration, activity *activityMonitor, sid uint64, label string) *idleConn {
	return &idleConn{Conn: conn, idleTimeout: idleTimeout, activity: activity, sid: sid, label: label}
}

// arm sets the read deadline to the session-wide idle instant. Skipped when
// unblocked so the "now" deadline set by unblock() is never overwritten.
func (c *idleConn) arm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.unblocked && c.idleTimeout > 0 {
		_ = c.Conn.SetReadDeadline(c.activity.idleDeadline(c.idleTimeout))
	}
}

func (c *idleConn) isUnblocked() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unblocked
}

// unblock makes any current or future Read return errReadUnblocked promptly.
// Safe to call from any goroutine.
func (c *idleConn) unblock() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unblocked = true
	_ = c.Conn.SetReadDeadline(time.Now())
}

func (c *idleConn) Read(p []byte) (int, error) {
	for {
		c.arm()
		n, err := c.Conn.Read(p)
		if n > 0 {
			c.activity.touch()
			slog.Debug("data received", "sid", c.sid, "from", c.label, "bytes", n)
		}
		if err == nil || !isTimeoutErr(err) {
			return n, err
		}
		// A read deadline fired: a teardown unblock, a genuine session idle, or
		// a stale deadline outdated by activity on the other direction.
		if c.isUnblocked() {
			if n > 0 {
				return n, nil
			}
			return 0, errReadUnblocked
		}
		if c.idleTimeout <= 0 || !time.Now().Before(c.activity.idleDeadline(c.idleTimeout)) {
			return n, err // genuine idle (or an external deadline with idle disabled)
		}
		if n > 0 {
			return n, nil // deliver data; the retry happens on the caller's next Read
		}
	}
}

// copyWithDeadlines copies from src to dst until EOF or error. Read-side idle
// detection is owned by src (an *idleConn arms a session-wide deadline; a bare
// conn blocks until data or close); writeTimeout bounds each write to dst. A
// write failure is wrapped in *clientWriteErr so the caller can tell it apart
// from a src read failure. Returns bytes copied and any error (io.EOF
// normalized to nil).
func copyWithDeadlines(dst net.Conn, src net.Conn, writeTimeout time.Duration) (int64, error) {
	buf := make([]byte, copyBufSize)
	var total int64

	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			// writeAll handles partial writes and sets/clears write deadline
			if err := writeAll(dst, buf[:n], writeTimeout); err != nil {
				return total, &clientWriteErr{err: err}
			}
			total += int64(n)
		}

		if readErr != nil {
			if readErr == io.EOF {
				return total, nil
			}
			return total, readErr
		}
	}
}

// parseDelimiter converts delimiter flag to actual bytes
func parseDelimiter(s string) []byte {
	switch strings.ToLower(s) {
	case "crlf":
		return []byte("\r\n")
	case "lf":
		return []byte("\n")
	case "cr":
		return []byte("\r")
	case "etx":
		return []byte{0x03}
	case "eot":
		return []byte{0x04}
	default:
		return []byte(s)
	}
}

// parseEscape converts escape flag to a single byte (0 means no escape).
// Returns error for invalid hex format.
func parseEscape(s string) (byte, error) {
	if s == "" {
		return 0, nil
	}
	switch strings.ToLower(s) {
	case "esc":
		return 0x1B, nil
	default:
		// Handle 0x1b or 0x1B format
		ss := strings.ToLower(s)
		if strings.HasPrefix(ss, "0x") {
			u, err := strconv.ParseUint(ss[2:], 16, 8)
			if err != nil {
				return 0, fmt.Errorf("invalid escape hex value %q: %w", s, err)
			}
			return byte(u), nil
		}
		// Use first byte of string (single character escape)
		if len(s) != 1 {
			return 0, fmt.Errorf("escape must be single character, 'esc', or 0xNN hex (got %q)", s)
		}
		return s[0], nil
	}
}

// validateConfig validates resolved configuration and returns a descriptive
// error if anything is invalid. It performs no I/O and never exits the process,
// so it is unit-testable; main() prints the error and exits on failure.
// It must run before any code that dereferences delim (e.g. the startup banner
// reads delim[len(delim)-1]).
func validateConfig(lineMode bool, delim []byte, esc byte, partial, listen, primary string, mirrors []string, maxFrame int) error {
	// Line mode dereferences delim (delim[len(delim)-1], delim[0]); an empty
	// delimiter would panic, so reject it up front.
	if lineMode && len(delim) == 0 {
		return fmt.Errorf("-delim must not be empty in line mode")
	}
	// Escape-based byte stuffing only makes sense with a single-byte delimiter.
	if esc != 0 && len(delim) != 1 {
		return fmt.Errorf("-esc requires a single-byte delimiter (got %d bytes: %q)", len(delim), delim)
	}
	switch partial {
	case "drop", "forward", "error":
		// valid
	default:
		return fmt.Errorf("-partial must be 'drop', 'forward', or 'error' (got %q)", partial)
	}
	if _, _, err := net.SplitHostPort(listen); err != nil {
		return fmt.Errorf("invalid listen address %q: %v", listen, err)
	}
	if _, _, err := net.SplitHostPort(primary); err != nil {
		return fmt.Errorf("invalid primary address %q: %v", primary, err)
	}
	for _, m := range mirrors {
		if _, _, err := net.SplitHostPort(m); err != nil {
			return fmt.Errorf("invalid mirror address %q: %v", m, err)
		}
	}
	if maxFrame <= 0 {
		return fmt.Errorf("-maxframe must be positive (got %d)", maxFrame)
	}
	return nil
}

// memWarnThreshold is the per-connection mirror-buffer footprint above which
// configWarnings emits a heads-up.
const memWarnThreshold = 100 * 1024 * 1024 // 100MB

// configWarnings returns non-fatal startup warnings about the resolved config.
// In line mode each mirror buffers up to mirrorbuf frames of up to maxframe
// bytes per connection, so a large -maxframe combined with -mirrorbuf and many
// mirrors can use a lot of memory (multiplied by the number of connections).
func configWarnings(lineMode bool, mirrorbuf, maxframe, nMirrors int) []string {
	var warns []string
	if lineMode && nMirrors > 0 {
		// int64 to avoid overflow with large maxframe values.
		perConn := int64(mirrorbuf) * int64(maxframe) * int64(nMirrors)
		if perConn > memWarnThreshold {
			warns = append(warns, fmt.Sprintf(
				"mirror buffers may use up to %d MB per connection (mirrorbuf=%d * maxframe=%d * mirrors=%d); consider lowering -mirrorbuf or -maxframe",
				perConn/(1024*1024), mirrorbuf, maxframe, nMirrors))
		}
	}
	return warns
}

// ErrFrameTooLarge is returned when a frame exceeds the maximum allowed size
var ErrFrameTooLarge = errors.New("frame too large")

// isClosedErr returns true for errors indicating we closed the connection (or
// deliberately ended reads on it) ourselves
func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, errReadUnblocked) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

// isResetErr returns true for connection reset errors
func isResetErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "connection reset by peer")
}

// isTimeoutErr returns true for timeout errors
func isTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

// errFrameTooLarge returns an error for frames exceeding the max size
func errFrameTooLarge(maxSize int) error {
	return fmt.Errorf("%w: max %d bytes", ErrFrameTooLarge, maxSize)
}

// readUntilDelim reads until the complete delimiter sequence is found.
// Returns (frame, complete, err) where complete indicates if delimiter was found.
// Returns error if frame exceeds maxFrameSize.
func readUntilDelim(reader *bufio.Reader, delim []byte, maxFrameSize int) ([]byte, bool, error) {
	// Preallocate to reduce allocations during append
	initCap := 4096
	if maxFrameSize < initCap {
		initCap = maxFrameSize
	}
	frame := make([]byte, 0, initCap)
	lastDelimByte := delim[len(delim)-1]

	for {
		b, err := reader.ReadByte()
		if err != nil {
			return frame, false, err
		}

		frame = append(frame, b)

		// Enforce maximum frame size to prevent OOM
		if len(frame) > maxFrameSize {
			return frame, false, errFrameTooLarge(maxFrameSize)
		}

		// Optimization: only check full delimiter when last byte matches
		if b == lastDelimByte && len(frame) >= len(delim) {
			if bytes.HasSuffix(frame, delim) {
				return frame, true, nil
			}
		}
	}
}

// readFrameEscaped reads a complete frame handling escape sequences.
// A frame ends with an unescaped delimiter byte (single-byte delimiter only).
// The escape character causes the next byte to be treated as literal data,
// regardless of what it is (ESC+anything = literal byte).
// Returns (frame, complete, err) where complete indicates if delimiter was found.
// Returns error if frame exceeds maxFrameSize.
func readFrameEscaped(reader *bufio.Reader, delim byte, esc byte, maxFrameSize int) ([]byte, bool, error) {
	// Preallocate to reduce allocations during append
	initCap := 4096
	if maxFrameSize < initCap {
		initCap = maxFrameSize
	}
	frame := make([]byte, 0, initCap)
	escaped := false

	for {
		b, err := reader.ReadByte()
		if err != nil {
			return frame, false, err
		}

		frame = append(frame, b)

		// Enforce maximum frame size to prevent OOM
		if len(frame) > maxFrameSize {
			return frame, false, errFrameTooLarge(maxFrameSize)
		}

		if escaped {
			// This byte is escaped, don't interpret it as delimiter
			escaped = false
			continue
		}

		if b == esc {
			// Next byte is escaped
			escaped = true
			continue
		}

		if b == delim {
			// Found unescaped delimiter - frame complete
			return frame, true, nil
		}
	}
}

// newMirrorWriter creates a mirror writer and starts its goroutine.
// Caller must ensure writeTimeout >= minWriteTimeout (enforced in main).
func newMirrorWriter(addr string, wg *sync.WaitGroup, connTimeout, writeTimeout time.Duration, chanSize int, sid uint64, stats *sessionStats) *mirrorWriter {
	mw := &mirrorWriter{
		addr:         addr,
		ch:           make(chan []byte, chanSize),
		done:         make(chan struct{}),
		connTimeout:  connTimeout,
		writeTimeout: writeTimeout,
		wasConnected: false,
		sid:          sid,
		stats:        stats,
	}
	wg.Add(1)
	go mw.run(wg)
	return mw
}

// closeConn closes and clears the connection.
// Only called from run() goroutine which owns the connection.
func (mw *mirrorWriter) closeConn() {
	if mw.conn != nil {
		_ = mw.conn.Close()
		mw.conn = nil
		mw.connPtr.Store(nil)
	}
}

// interrupt force-closes the current connection (if any) so an in-flight
// writeAll in run() returns immediately. Safe to call from any goroutine.
// Close (not a write deadline) is used deliberately: writeAll re-arms the write
// deadline on every call, so a past deadline could be overwritten and the
// interrupt lost, whereas a closed conn fails the in-flight and all later writes.
func (mw *mirrorWriter) interrupt() {
	if p := mw.connPtr.Load(); p != nil {
		_ = (*p).Close()
	}
}

// run is the mirror writer goroutine that handles writes and reconnection.
// This goroutine is the sole owner of the connection. It exits when the send
// channel is closed and fully drained (graceful end via beginDrain) or as soon
// as done is closed (force-stop via stop) — in the latter case whatever is
// still queued is counted as mirror drops instead of vanishing silently.
func (mw *mirrorWriter) run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer mw.closeConn()

	backoff := initialBackoff

	for {
		select {
		case <-mw.done:
			mw.dropRemaining(0)
			return
		case data, ok := <-mw.ch:
			if !ok {
				// Channel closed and drained: graceful end
				return
			}

			// Re-check done so a force-stop isn't outraced by buffered chunks
			select {
			case <-mw.done:
				mw.dropRemaining(1) // current chunk plus whatever is queued
				return
			default:
			}

			mw.writeChunk(data, &backoff)
		}
	}
}

// dropRemaining counts n plus all still-queued chunks as mirror drops after a
// force-stop. Only called once done is closed; stop() closes ch before done,
// so draining the channel here terminates.
func (mw *mirrorWriter) dropRemaining(n uint64) {
	for range mw.ch {
		n++
	}
	if n == 0 {
		return
	}
	if mw.stats != nil {
		mw.stats.mirrorDrops.Add(n)
	}
	slog.Debug("mirror chunks dropped at stop", "sid", mw.sid, "mirror", mw.addr, "dropped", n)
}

// writeChunk delivers one chunk to the mirror, connecting first if needed.
// On any write error it tears down the connection and DROPS the chunk (counting
// it as a mirror drop) instead of replaying it: a partial write may already
// have reached the mirror, so replaying the whole chunk would corrupt the
// stream. The next chunk reconnects lazily via the mw.conn==nil path.
// Must only be called from the run goroutine which owns the connection.
func (mw *mirrorWriter) writeChunk(data []byte, backoff *time.Duration) {
	if mw.conn == nil {
		if !mw.tryConnect(backoff) {
			return // could not connect; drop this chunk and try the next one
		}
	}
	if mw.conn == nil {
		return
	}

	if err := writeAll(mw.conn, data, mw.writeTimeout); err != nil {
		slog.Debug("mirror write failed", "sid", mw.sid, "mirror", mw.addr, "err", err)
		mw.markDown()
		if mw.stats != nil {
			mw.stats.mirrorDrops.Add(1)
		}
		return
	}
	slog.Debug("mirror write ok", "sid", mw.sid, "mirror", mw.addr, "bytes", len(data))
	*backoff = initialBackoff // reset on success
}

// markDown handles mirror going down - logs state transition once.
// Must only be called from the run goroutine.
func (mw *mirrorWriter) markDown() {
	if mw.wasConnected {
		slog.Info("mirror down", "sid", mw.sid, "mirror", mw.addr)
		mw.wasConnected = false
	}
	mw.closeConn()
}

// tryConnect attempts to connect with backoff, returns true on success.
// Must only be called from the run goroutine which owns the connection.
func (mw *mirrorWriter) tryConnect(backoff *time.Duration) bool {
	// Use Dialer with TCP keepalive to detect dead peers
	dialer := &net.Dialer{
		Timeout:   mw.connTimeout,
		KeepAlive: 30 * time.Second,
	}
	conn, err := dialer.Dial("tcp", mw.addr)
	if err != nil {
		slog.Debug("mirror reconnect attempt", "sid", mw.sid, "mirror", mw.addr, "backoff", *backoff, "err", err)

		// Wait with backoff, but check for done signal
		select {
		case <-mw.done:
			return false
		case <-time.After(*backoff):
		}

		// Increase backoff for next attempt
		*backoff *= backoffMultiplier
		if *backoff > maxBackoff {
			*backoff = maxBackoff
		}
		return false
	}

	// Log state transition: down → up (or initial connection)
	if !mw.wasConnected {
		slog.Info("mirror up", "sid", mw.sid, "mirror", mw.addr)
	}
	mw.wasConnected = true
	mw.conn = conn
	mw.connPtr.Store(&conn) // publish for interrupt()
	*backoff = initialBackoff
	return true
}

// send attempts to send data to the mirror (non-blocking)
func (mw *mirrorWriter) send(data []byte) {
	// Make a copy since the buffer will be reused
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	select {
	case mw.ch <- dataCopy:
	default:
		slog.Debug("mirror channel full", "sid", mw.sid, "mirror", mw.addr)
		if mw.stats != nil {
			mw.stats.mirrorDrops.Add(1)
		}
	}
}

// beginDrain closes the send channel: run() delivers whatever is already
// queued and then exits. Callers must not send() afterwards. Idempotent.
func (mw *mirrorWriter) beginDrain() {
	mw.drainOnce.Do(func() { close(mw.ch) })
}

// stop force-stops the mirror writer (idempotent): still-queued chunks are
// counted as drops and an in-flight write is interrupted by force-closing the
// connection, after which run() exits and its deferred closeConn() cleans up.
func (mw *mirrorWriter) stop() {
	mw.beginDrain() // ensure ch is closed so run()'s drop-drain loop terminates
	mw.stopOnce.Do(func() {
		close(mw.done) // unblock run() from any waiting state
		mw.interrupt() // unblock run() if blocked in an in-flight write
	})
}

// buildLogHandler returns the slog handler for the configured level and
// format, writing to w.
func buildLogHandler(w io.Writer, level, format string) slog.Handler {
	var lv slog.Level
	switch level {
	case "error":
		lv = slog.LevelError
	case "debug":
		lv = slog.LevelDebug
	default:
		lv = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lv}
	if format == "json" {
		return slog.NewJSONHandler(w, opts)
	}
	return slog.NewTextHandler(w, opts)
}

// acceptLoop accepts connections and dispatches each to handle in its own
// goroutine, returning only when accept reports net.ErrClosed (listener closed
// for shutdown). accept and sleep are injectable for testing.
//
//   - M1: on any non-ErrClosed accept error it backs off (capped) before
//     retrying, so a persistent error like fd exhaustion can't spin the CPU.
//   - M5: when maxConns > 0 a counting semaphore caps concurrent sessions; an
//     over-limit connection is closed immediately with a warning rather than
//     blocking the accept loop.
//   - M2: activeSessions is incremented before the goroutine is spawned and
//     decremented (with the semaphore slot released) when handle returns, so
//     graceful shutdown's Wait() can never race a just-accepted session.
func acceptLoop(accept func() (net.Conn, error), handle func(net.Conn), sleep func(time.Duration), maxConns int) {
	var sem chan struct{}
	if maxConns > 0 {
		sem = make(chan struct{}, maxConns)
	}

	backoff := acceptInitialBackoff
	for {
		conn, err := accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return // listener closed: shutdown
			}
			slog.Error("accept failed", "err", err, "backoff", backoff)
			sleep(backoff)
			backoff *= 2
			if backoff > acceptMaxBackoff {
				backoff = acceptMaxBackoff
			}
			continue
		}
		backoff = acceptInitialBackoff // reset on success

		// Enforce the concurrency cap (non-blocking): reject when full so the
		// accept loop is never blocked by a slow drain.
		if sem != nil {
			select {
			case sem <- struct{}{}:
			default:
				slog.Warn("connection rejected: max connections reached", "max", maxConns, "client", conn.RemoteAddr())
				_ = conn.Close()
				continue
			}
		}

		// Count the session before spawning; release the slot and decrement
		// when handle returns.
		activeSessions.Add(1)
		go func(c net.Conn) {
			defer activeSessions.Done()
			if sem != nil {
				defer func() { <-sem }()
			}
			handle(c)
		}(conn)
	}
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if *versionPtr {
		fmt.Println(versionString())
		return
	}

	// Set up slog logger, optionally appending to a file instead of stderr
	logDst := io.Writer(os.Stderr)
	if *logFilePtr != "" {
		f, err := os.OpenFile(*logFilePtr, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening log file: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = f.Close() }()
		logDst = f
	}
	slog.SetDefault(slog.New(buildLogHandler(logDst, *logLevelPtr, *logFormatPtr)))

	var mirrorAddrs []string
	if *mirrorPtr != "" {
		for _, addr := range strings.Split(*mirrorPtr, ",") {
			addr = strings.TrimSpace(addr)
			if addr == "" {
				continue
			}
			mirrorAddrs = append(mirrorAddrs, addr)
		}
	}

	// Parse delimiter and escape for line mode
	delim := parseDelimiter(*delimPtr)
	esc, err := parseEscape(*escPtr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Validate resolved configuration before anything dereferences it (the
	// startup banner below reads delim[len(delim)-1]).
	if err := validateConfig(*lineModePtr, delim, esc, *partialPtr, *listenPtr, *primaryPtr, mirrorAddrs, *maxFramePtr); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	for _, w := range configWarnings(*lineModePtr, *mirrorBufPtr, *maxFramePtr, len(mirrorAddrs)) {
		slog.Warn(w)
	}

	// Enforce minimum write timeout to prevent indefinite hangs
	writeTimeout := *writeTimeoutPtr
	if writeTimeout < minWriteTimeout {
		writeTimeout = minWriteTimeout
	}

	// A negative drain makes no sense; clamp to 0 (drop queued mirror data
	// immediately at session end)
	mirrorDrain := *mirrorDrainPtr
	if mirrorDrain < 0 {
		mirrorDrain = 0
	}

	fmt.Printf("tee-rex %s\n", versionString())
	fmt.Printf("Listening on                    %s\n", *listenPtr)
	fmt.Printf("Connecting in primary mode to   %s\n", *primaryPtr)
	if len(mirrorAddrs) > 0 {
		fmt.Printf("Duplicating incoming traffic to %s\n", *mirrorPtr)
	}
	if *lineModePtr {
		if esc != 0 {
			fmt.Printf("Mode: line-oriented (delimiter: 0x%02X, escape: 0x%02X)\n", delim[len(delim)-1], esc)
		} else {
			fmt.Printf("Mode: line-oriented (delimiter: %q)\n", delim)
		}
	} else {
		fmt.Printf("Mode: raw TCP\n")
	}
	if *maxConnsPtr > 0 {
		fmt.Printf("Max concurrent connections      %d\n", *maxConnsPtr)
	} else {
		fmt.Printf("Max concurrent connections      unlimited\n")
	}

	l, err := net.Listen("tcp", *listenPtr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listening: %s\n", err.Error())
		os.Exit(1)
	}

	// Set up signal handling for graceful shutdown
	// Use os.Interrupt for Windows compatibility; SIGTERM for Unix
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// Accept loop in goroutine so we can select on shutdown.
	handle := func(c net.Conn) {
		handleConnection(c, *primaryPtr, mirrorAddrs, *lineModePtr, delim, esc, *maxFramePtr, *partialPtr, *connTimeoutPtr, writeTimeout, *mirrorBufPtr, *idleTimeoutPtr, mirrorDrain)
	}
	accepting := make(chan struct{})
	go func() {
		defer close(accepting)
		acceptLoop(l.Accept, handle, time.Sleep, *maxConnsPtr)
	}()

	// Wait for shutdown signal
	sig := <-sigCh
	slog.Info("shutdown initiated", "signal", sig.String(), "active_sessions", activeCount.Load())

	// Stop accepting new connections
	_ = l.Close()
	<-accepting // wait for accept loop to exit

	// Wait for active sessions with timeout
	done := make(chan struct{})
	go func() {
		activeSessions.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("graceful shutdown complete", "active_sessions", activeCount.Load())
	case <-time.After(*shutdownTimeoutPtr):
		remaining := activeCount.Load()
		slog.Warn("shutdown timeout, force-closing connections", "timeout", *shutdownTimeoutPtr, "remaining_sessions", remaining)
		closeAllConns()
		// Wait a bit more for goroutines to exit after force-close
		select {
		case <-done:
			slog.Info("shutdown complete after force-close")
		case <-time.After(5 * time.Second):
			slog.Error("shutdown incomplete, some goroutines may be stuck", "remaining_sessions", activeCount.Load())
		}
	}
}

// handleConnection serves one client session. Session accounting
// (activeSessions.Add/Done) is owned by acceptLoop, which brackets this call.
func handleConnection(in net.Conn, primaryAddr string, mirrorAddrs []string, lineMode bool, delim []byte, esc byte, maxFrameSize int, partialPolicy string, connTimeout, writeTimeout time.Duration, mirrorBufSize int, idleTimeout, mirrorDrain time.Duration) {
	sid := sessionCounter.Add(1)
	registerConn(sid, in)
	defer unregisterConn(sid)

	startTime := time.Now()
	clientAddr := in.RemoteAddr().String()
	stats := &sessionStats{}
	var primary net.Conn

	slog.Info("session started", "sid", sid, "client", clientAddr, "primary", primaryAddr)

	// Ensure session summary is always logged
	defer func() {
		_ = in.Close()
		if primary != nil {
			_ = primary.Close()
		}
		attrs := []any{
			"sid", sid,
			"client", clientAddr,
			"reason", stats.getEndReason(),
			"duration", time.Since(startTime),
			"bytes_in", stats.bytesIn.Load(),
			"bytes_out", stats.bytesOut.Load(),
		}
		if lineMode {
			attrs = append(attrs, "frames", stats.framesIn.Load())
		}
		if len(mirrorAddrs) > 0 {
			attrs = append(attrs, "mirror_drops", stats.mirrorDrops.Load())
		}
		slog.Info("session ended", attrs...)
	}()

	// Connect to primary - this must succeed
	// Use Dialer with TCP keepalive to detect dead peers (consistent with mirrors)
	var err error
	dialer := &net.Dialer{Timeout: connTimeout, KeepAlive: 30 * time.Second}
	primary, err = dialer.Dial("tcp", primaryAddr)
	if err != nil {
		stats.setEndReason(reasonPrimaryDialFail)
		slog.Error("primary dial failed", "sid", sid, "primary", primaryAddr, "err", err)
		return
	}
	registerPrimary(sid, primary)
	slog.Info("primary connected", "sid", sid, "primary", primaryAddr)

	// Create async mirror writers (connections happen in goroutines)
	var mirrorWg sync.WaitGroup
	var mirrors []*mirrorWriter
	for _, addr := range mirrorAddrs {
		mw := newMirrorWriter(addr, &mirrorWg, connTimeout, writeTimeout, mirrorBufSize, sid, stats)
		mirrors = append(mirrors, mw)
	}

	// Session-wide idle detection: both directions share one activity monitor,
	// so traffic either way keeps the whole session alive. Asymmetric protocols
	// (e.g. a heartbeating client with a silent server) would otherwise be torn
	// down by the silent direction's own timer.
	activity := newActivityMonitor()
	client := newIdleConn(in, idleTimeout, activity, sid, "client")
	primarySrc := newIdleConn(primary, idleTimeout, activity, sid, "primary")

	var wg sync.WaitGroup

	// Copy incoming data to primary and mirrors
	wg.Add(1)
	go func() {
		defer wg.Done()
		if lineMode {
			fanOutLines(client, primary, mirrors, delim, esc, maxFrameSize, writeTimeout, partialPolicy, sid, stats)
		} else {
			fanOut(client, primary, mirrors, writeTimeout, sid, stats)
		}
		// Mirrors: no more data is coming; let queued frames flush (bounded
		// below by mirrorDrain)
		for _, mw := range mirrors {
			mw.beginDrain()
		}
		// Half-close primary: signal EOF to server while keeping read side open
		// This allows server to finish sending any response data
		if tc, ok := primary.(interface{ CloseWrite() error }); ok {
			_ = tc.CloseWrite()
		} else {
			// Fallback for non-TCP connections: close entirely
			_ = primary.Close()
		}
	}()

	// Copy primary response back to client with deadline support
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := copyWithDeadlines(in, primarySrc, writeTimeout)
		stats.bytesOut.Add(uint64(n))
		var cwErr *clientWriteErr
		switch {
		case err == nil:
			// Clean primary EOF. First-wins (setEndReason) makes this lose to
			// client_eof when the client closed first, and win only on a
			// primary-initiated close — preventing the spurious client_idle that
			// fanOut would otherwise set when we unblock its read below.
			stats.setEndReason(reasonPrimaryEOF)
		case errors.As(err, &cwErr):
			// Failure writing to the client, not a primary problem.
			if isTimeoutErr(err) {
				stats.setEndReason(reasonClientSlow)
				slog.Info("client write timeout (slow client)", "sid", sid, "err", err)
			} else if isClosedErr(err) {
				stats.setEndReason(reasonClientWriteErr)
				slog.Debug("client write stopped", "sid", sid, "err", err)
			} else {
				stats.setEndReason(reasonClientWriteErr)
				slog.Info("client write failed", "sid", sid, "err", err)
			}
		case isClosedErr(err):
			slog.Debug("primary read stopped", "sid", sid, "err", err)
		case isResetErr(err):
			stats.setEndReason(reasonPrimaryReset)
			slog.Info("primary connection reset", "sid", sid, "err", err)
		case isTimeoutErr(err):
			stats.setEndReason(reasonSessionIdle)
			slog.Info("session idle timeout", "sid", sid, "err", err)
		default:
			stats.setEndReason(reasonPrimaryReadErr)
			slog.Error("primary read failed", "sid", sid, "op", "read", "err", err)
		}
		// Primary is done sending. Unblock fanOut's client read (any further
		// client data has nowhere to go now) so the session tears down promptly
		// instead of lingering until the session idle timeout — or forever when
		// idle timeout is disabled.
		client.unblock()
		// Half-close the client write side: signal no more responses are coming.
		if tc, ok := in.(interface{ CloseWrite() error }); ok {
			_ = tc.CloseWrite()
		}
	}()

	wg.Wait()
	// Wait for mirror goroutines to flush queued data, bounded by mirrorDrain;
	// then force-stop. The wait after a force-stop is short: stop() interrupts
	// an in-flight write, and a dial attempt is bounded by connTimeout.
	if len(mirrors) > 0 {
		drained := make(chan struct{})
		go func() {
			mirrorWg.Wait()
			close(drained)
		}()
		timer := time.NewTimer(mirrorDrain)
		select {
		case <-drained:
			timer.Stop()
		case <-timer.C:
			slog.Debug("mirror drain timed out, force-stopping", "sid", sid, "drain", mirrorDrain)
			for _, mw := range mirrors {
				mw.stop()
			}
			<-drained
		}
	}
}

// fanOut reads from src and writes to primary (required) and mirrors (best-effort async).
// Primary write errors stop the operation. Mirror writes are non-blocking via channels.
// Read-side idle detection is owned by src (see idleConn).
func fanOut(src net.Conn, primary net.Conn, mirrors []*mirrorWriter, writeTimeout time.Duration, sid uint64, stats *sessionStats) {
	buf := make([]byte, copyBufSize)

	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			data := buf[:n]
			stats.bytesIn.Add(uint64(n))

			// Write to primary - must succeed
			if err := writeAll(primary, data, writeTimeout); err != nil {
				stats.setEndReason(reasonPrimaryWriteErr)
				slog.Error("primary write failed", "sid", sid, "op", "write", "err", err)
				return
			}

			// Send to mirrors asynchronously (non-blocking)
			for _, mw := range mirrors {
				mw.send(data)
			}
		}

		if readErr != nil {
			if readErr == io.EOF || isClosedErr(readErr) {
				stats.setEndReason(reasonClientEOF)
			} else if isResetErr(readErr) {
				stats.setEndReason(reasonClientReset)
				slog.Info("client connection reset", "sid", sid, "err", readErr)
			} else if isTimeoutErr(readErr) {
				stats.setEndReason(reasonSessionIdle)
				slog.Info("session idle timeout", "sid", sid, "err", readErr)
			} else {
				stats.setEndReason(reasonClientReadErr)
				slog.Error("client read failed", "sid", sid, "op", "read", "err", readErr)
			}
			return
		}
	}
}

// fanOutLines reads complete lines/frames from src and writes to primary and mirrors.
// Lines are delimited by the specified delimiter and always include the delimiter.
// When esc is non-zero, escape sequences are handled (ESC+DELIM = literal DELIM).
// partialPolicy controls handling of incomplete frames at EOF: "drop", "forward", or "error".
// Primary write errors stop the operation. Mirror writes are non-blocking via channels.
// Read-side idle detection is owned by src (see idleConn).
func fanOutLines(src net.Conn, primary net.Conn, mirrors []*mirrorWriter, delim []byte, esc byte, maxFrameSize int, writeTimeout time.Duration, partialPolicy string, sid uint64, stats *sessionStats) {
	// Cap bufio buffer size - frame limit is enforced separately in read functions
	bufSize := maxFrameSize
	if bufSize > maxBufioSize {
		bufSize = maxBufioSize
	}
	reader := bufio.NewReaderSize(src, bufSize)

	for {
		var frame []byte
		var complete bool
		var err error

		if esc != 0 {
			// Use escape-aware frame reader (single-byte delimiter only)
			frame, complete, err = readFrameEscaped(reader, delim[0], esc, maxFrameSize)
		} else {
			// Read until complete delimiter sequence
			frame, complete, err = readUntilDelim(reader, delim, maxFrameSize)
		}

		// Handle oversized frames - don't forward, just close
		if errors.Is(err, ErrFrameTooLarge) {
			stats.setEndReason(reasonFrameTooLarge)
			slog.Error("frame too large", "sid", sid, "size", len(frame), "max", maxFrameSize)
			return
		}

		if len(frame) > 0 {
			stats.bytesIn.Add(uint64(len(frame)))

			// Determine if we should forward this frame
			shouldForward := complete
			if !complete && err != nil {
				// Partial frame at EOF/error - apply policy
				switch partialPolicy {
				case "forward":
					shouldForward = true
					slog.Debug("partial frame forwarded", "sid", sid, "size", len(frame), "policy", partialPolicy, "err", err)
				case "error":
					stats.setEndReason(reasonPartialFrame)
					slog.Error("partial frame", "sid", sid, "size", len(frame), "err", err)
					return
				default: // "drop"
					slog.Debug("partial frame dropped", "sid", sid, "size", len(frame), "policy", partialPolicy, "err", err)
					shouldForward = false
				}
			}

			if shouldForward {
				// Write to primary - must succeed
				if err := writeAll(primary, frame, writeTimeout); err != nil {
					stats.setEndReason(reasonPrimaryWriteErr)
					slog.Error("primary write failed", "sid", sid, "op", "write", "err", err)
					return
				}
				stats.framesIn.Add(1)
				slog.Debug("frame forwarded", "sid", sid, "size", len(frame))

				// Send to mirrors asynchronously (non-blocking)
				for _, mw := range mirrors {
					mw.send(frame)
				}
			}
		}

		if err != nil {
			if err == io.EOF || isClosedErr(err) {
				stats.setEndReason(reasonClientEOF)
			} else if isResetErr(err) {
				stats.setEndReason(reasonClientReset)
				slog.Info("client connection reset", "sid", sid, "err", err)
			} else if isTimeoutErr(err) {
				stats.setEndReason(reasonSessionIdle)
				slog.Info("session idle timeout", "sid", sid, "err", err)
			} else {
				stats.setEndReason(reasonClientReadErr)
				slog.Error("client read failed", "sid", sid, "op", "read", "err", err)
			}
			return
		}
	}
}

func Usage() {
	fmt.Fprintf(os.Stderr, "tee-rex version %s\n", versionString())
	fmt.Fprintf(os.Stderr, "Usage:   $ tee-rex -l <listen_addr> -p <primary_addr> [-m <mirror_addrs>] [-line]\n")
	fmt.Fprintf(os.Stderr, "Example: $ tee-rex -l localhost:8080 -p localhost:9090 -m localhost:9091,localhost:9092\n")
	fmt.Fprintf(os.Stderr, "         $ tee-rex -l :25 -p mail:25 -m mirror:25 -line  # SMTP mirroring\n")
	fmt.Fprintf(os.Stderr, "         $ tee-rex -l :5000 -p as:5000 -line -delim etx -esc esc  # Framed protocol\n")
	fmt.Fprintf(os.Stderr, "         $ tee-rex -l :8080 -p :9090 -loglevel=debug  # Debug logging\n")
	fmt.Fprintf(os.Stderr, "-----------------------\nFlags:\n")
	flag.PrintDefaults()
}
