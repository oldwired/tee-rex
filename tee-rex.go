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
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Build info (set via -ldflags). Version defaults to "dev" so an unstamped
// local build can never masquerade as a release; the release workflow stamps
// it from the Git tag (-X main.Version=1.2.3) and verifies the result.
var (
	Version     = "dev"
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
	reasonPrimaryEOF      = "primary_eof"        // primary closed its side (no error)
	reasonHalfCloseIdle   = "half_close_timeout" // one peer closed; the other stayed open but silent for -halfclosetimeout
	reasonSessionIdle     = "session_idle"       // no data in either direction for -idletimeout
	reasonShutdown        = "shutdown"           // force-closed by proxy shutdown
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

// countIn records n bytes received from the client, in the session counter
// and the process aggregate at the same time, so the periodic stats line
// reflects traffic on sessions that are still open.
func (s *sessionStats) countIn(n int) {
	s.bytesIn.Add(uint64(n))
	aggBytesIn.Add(uint64(n))
}

// countOut records n bytes successfully delivered to the client (see countIn).
func (s *sessionStats) countOut(n int) {
	s.bytesOut.Add(uint64(n))
	aggBytesOut.Add(uint64(n))
}

// setEndReason sets the end reason (only first reason wins) and reports
// whether this call won; callers can gate their log line on it so a lost race
// (e.g. shutdown already attributed the session) doesn't emit a misleading line.
func (s *sessionStats) setEndReason(reason string) bool {
	return s.endReason.CompareAndSwap(nil, reason)
}

// setHalfCloseTimeout upgrades a provisional clean-EOF reason (client_eof or
// primary_eof — "this peer closed first") to half_close_timeout: the other
// peer never finished and the remaining direction went silent for
// -halfclosetimeout. Error reasons and shutdown are never overridden. Reports
// whether the upgrade happened so the caller can gate its log line.
func (s *sessionStats) setHalfCloseTimeout() bool {
	return s.endReason.CompareAndSwap(reasonClientEOF, reasonHalfCloseIdle) ||
		s.endReason.CompareAndSwap(reasonPrimaryEOF, reasonHalfCloseIdle)
}

// getEndReason returns the end reason or "unknown" if not set
func (s *sessionStats) getEndReason() string {
	if r := s.endReason.Load(); r != nil {
		return r.(string)
	}
	return "unknown"
}

// Aggregate counters for the periodic stats line (totals since process start).
// Bytes are counted at the I/O accounting points (sessionStats.countIn /
// countOut), so open long-lived sessions show up live; mirror drops are counted
// as they happen. Per-address drop counters are stable *atomic.Uint64 values:
// a mirrorWriter resolves its counter once (mirrorDropCounter) and then bumps
// it lock-free, so a stalled mirror dropping every chunk never contends on
// mirrorDropsMu from the primary forwarding path. The mutex only guards the
// map structure.
var (
	aggBytesIn        atomic.Uint64
	aggBytesOut       atomic.Uint64
	aggMirrorDrops    atomic.Uint64
	mirrorDropsMu     sync.Mutex
	mirrorDropsByAddr = make(map[string]*atomic.Uint64)
)

// mirrorDropCounter returns the process-wide drop counter for a mirror
// address, creating it on first use.
func mirrorDropCounter(addr string) *atomic.Uint64 {
	mirrorDropsMu.Lock()
	defer mirrorDropsMu.Unlock()
	c, ok := mirrorDropsByAddr[addr]
	if !ok {
		c = new(atomic.Uint64)
		mirrorDropsByAddr[addr] = c
	}
	return c
}

// statsAttrs returns the attribute list for the periodic aggregate stats line.
func statsAttrs() []any {
	attrs := []any{
		"active_sessions", activeCount.Load(),
		"total_sessions", sessionCounter.Load(),
		"bytes_in", aggBytesIn.Load(),
		"bytes_out", aggBytesOut.Load(),
		"mirror_drops", aggMirrorDrops.Load(),
	}
	mirrorDropsMu.Lock()
	addrs := make([]string, 0, len(mirrorDropsByAddr))
	for a := range mirrorDropsByAddr {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	for _, a := range addrs {
		attrs = append(attrs, "drops_"+a, mirrorDropsByAddr[a].Load())
	}
	mirrorDropsMu.Unlock()
	return attrs
}

// Mirror drain budget: after a session's client/primary traffic has finished,
// its -maxconns admission slot is released and mirror cleanup continues under
// this separate, bounded budget (set from -maxdrains; nil = unlimited). When it
// is exhausted the finishing session force-stops its mirrors immediately
// (queued data counted as drops) instead of holding a goroutine — mirror
// trouble sheds mirror work, never primary traffic. Set in main before the
// accept loop starts; held atomically so tests can swap it between sessions.
var drainSlots atomic.Pointer[chan struct{}]

func setDrainBudget(n int) {
	if n > 0 {
		ch := make(chan struct{}, n)
		drainSlots.Store(&ch)
	} else {
		drainSlots.Store(nil)
	}
}

// acquireDrainSlot takes a drain slot without blocking. ok=false means the
// budget is exhausted. The returned release func gives back the slot to the
// same channel it was taken from, so a budget swap in between is harmless.
func acquireDrainSlot() (release func(), ok bool) {
	p := drainSlots.Load()
	if p == nil {
		return func() {}, true
	}
	ch := *p
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, true
	default:
		return nil, false
	}
}

// debugEnabled reports whether debug logging is active. Hot paths cache the
// result so per-chunk log calls don't pay argument boxing for records that
// would be discarded anyway (the level never changes after startup).
func debugEnabled() bool {
	return slog.Default().Enabled(context.Background(), slog.LevelDebug)
}

// tracked holds the live resources of a session so shutdown can force-close it
type tracked struct {
	client  net.Conn
	primary net.Conn
	stats   *sessionStats
	mirrors []*mirrorWriter
}

// registerConn tracks a client connection for graceful shutdown.
// Only increments count if sid wasn't already present (prevents drift).
func registerConn(sid uint64, client net.Conn, stats *sessionStats) {
	activeConnsMu.Lock()
	_, existed := activeConns[sid]
	activeConns[sid] = &tracked{client: client, stats: stats}
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

// registerMirrors records a session's mirror writers so shutdown's force-close
// can stop them too, aborting an in-progress drain instead of waiting it out.
func registerMirrors(sid uint64, mirrors []*mirrorWriter) {
	activeConnsMu.Lock()
	if t, ok := activeConns[sid]; ok {
		t.mirrors = mirrors
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

// closeAllConns force-closes all tracked sessions for shutdown: marks their
// end reason, closes both conns to unblock pending I/O, and stops mirror
// writers so drain waits abort promptly. All fields are copied by value while
// holding the lock — registerPrimary/registerMirrors may be storing into the
// same tracked structs concurrently, so dereferencing them after Unlock would
// be a data race.
func closeAllConns() {
	type snapshot struct {
		sid             uint64
		client, primary net.Conn
		stats           *sessionStats
		mirrors         []*mirrorWriter
	}
	activeConnsMu.Lock()
	snaps := make([]snapshot, 0, len(activeConns))
	for sid, t := range activeConns {
		snaps = append(snaps, snapshot{sid, t.client, t.primary, t.stats, t.mirrors})
	}
	activeConnsMu.Unlock()

	for _, s := range snaps {
		if s.stats != nil {
			s.stats.setEndReason(reasonShutdown)
		}
		// Set deadline to now to unblock any pending I/O, then close
		if s.client != nil {
			_ = s.client.SetDeadline(time.Now())
			_ = s.client.Close()
		}
		if s.primary != nil {
			_ = s.primary.SetDeadline(time.Now())
			_ = s.primary.Close()
		}
		for _, mw := range s.mirrors {
			mw.stop()
		}
		slog.Debug("force-closed connection", "sid", s.sid)
	}
}

// mirrorWriter manages async writes to a mirror with reconnection support.
// The run() goroutine owns the connection lifecycle (dial/write/close) via the
// mw.conn field. connPtr mirrors the current connection as an atomic pointer so
// that interrupt() can force-close an in-flight write from another goroutine
// (used by stop() during teardown); no other external access to the connection
// is allowed. Every established connection also gets a discard reader
// (startDiscardReader) bound to that exact conn, so a mirror that answers is
// never able to fill its send buffer and stop reading our writes.
type mirrorWriter struct {
	addr         string
	conn         net.Conn                 // connection lifecycle owned by run()
	connPtr      atomic.Pointer[net.Conn] // atomic view of conn for interrupt()
	ch           chan []byte
	done         chan struct{}
	drainOnce    sync.Once
	stopOnce     sync.Once
	ctx          context.Context    // cancelled by stop() so an in-flight dial aborts
	cancel       context.CancelFunc // nil when constructed without newMirrorWriter (tests)
	dial         mirrorDialFunc     // nil = real TCP dial (injectable for tests)
	readers      sync.WaitGroup     // discard readers; run() waits for them after closing the conn
	connTimeout  time.Duration
	writeTimeout time.Duration
	wasConnected bool           // for state transition logging (only accessed from run goroutine)
	failWarned   bool           // never-connected warning emitted (only accessed from run goroutine)
	sid          uint64         // session ID for logging
	stats        *sessionStats  // shared session stats for drop counting
	drops        atomic.Uint64  // drops for THIS mirror (per-mirror attribution in the summary)
	addrDrops    *atomic.Uint64 // process-wide per-address counter (nil = not attributed)
	debug        bool           // cached debug-level check for hot-path logging
}

// mirrorDialFunc dials a mirror; ctx is cancelled by mirrorWriter.stop().
type mirrorDialFunc func(ctx context.Context, addr string) (net.Conn, error)

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
		"Append logs to `file` instead of stderr (SIGHUP reopens it for log rotation)")

	lineModePtr = flag.Bool("line", false, "Enable line-oriented mode for text protocols (SMTP, FTP, IRC, etc.)")

	delimPtr = flag.String("delim", "crlf",
		"Line delimiter: 'crlf', 'lf', 'cr', 'etx', 'eot', hex like '0x03' or '0x0d0a', or custom string")

	escPtr = flag.String("esc", "",
		"Escape character for byte stuffing: 'esc' (0x1B), '0xNN' hex, or single character; must differ from the delimiter. ESC+any = literal byte (not just delimiter).")

	maxFramePtr = flag.Int("maxframe", defaultMaxLineLen,
		"Maximum frame size in bytes for line mode")

	partialPtr = flag.String("partial", "drop",
		"Partial frame policy at clean client EOF: 'drop' (discard), 'forward' (send anyway), 'error' (log and close). Fragments cut off by errors/idle/teardown are always dropped.")

	connTimeoutPtr = flag.Duration("timeout", 10*time.Second,
		"Connection timeout for dialing primary and mirrors (must be positive)")

	writeTimeoutPtr = flag.Duration("writetimeout", 30*time.Second,
		"Write timeout for sending data (minimum 5s enforced)")

	mirrorBufPtr = flag.Int("mirrorbuf", 100,
		"Buffer size for mirror write channels (per mirror, minimum 1)")

	mirrorDrainPtr = flag.Duration("mirrordrain", 5*time.Second,
		"How long to wait at session end for queued mirror data to flush (0 = drop immediately)")

	idleTimeoutPtr = flag.Duration("idletimeout", 2*time.Minute,
		"Session idle timeout: ends a session only when NEITHER direction has seen data for this long; 0 to disable")

	halfCloseTimeoutPtr = flag.Duration("halfclosetimeout", 30*time.Second,
		"After one peer closes its side (TCP half-close), keep relaying the other direction until it too closes or carries no data for this long; 0 disables half-close support (a primary close ends the session immediately)")

	maxDrainsPtr = flag.Int("maxdrains", 1024,
		"Maximum sessions concurrently flushing mirror data after their client/primary traffic has ended (0 = unlimited). When exhausted, a finishing session drops its queued mirror data instead of holding resources.")

	shutdownTimeoutPtr = flag.Duration("shutdowntimeout", 30*time.Second,
		"Graceful shutdown timeout for active sessions")

	statsIntervalPtr = flag.Duration("statsinterval", time.Minute,
		"Interval for the aggregate stats log line (0 to disable)")

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
// Returns the bytes actually written and an error if not all could be written,
// so callers can account for partial deliveries.
func writeAll(conn net.Conn, data []byte, timeout time.Duration) (int, error) {
	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return 0, err
		}
		defer func() { _ = conn.SetWriteDeadline(time.Time{}) }() // clear deadline (error ignored - conn will be closed on failure)
	}
	written := 0
	for written < len(data) {
		n, err := conn.Write(data[written:])
		if n > 0 {
			written += n
		}
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

// clientWriteErr wraps an error that occurred while writing to the destination
// (the client) in copyWithDeadlines, so the caller can attribute it to a
// slow/dead client rather than a primary read failure.
type clientWriteErr struct{ err error }

func (e *clientWriteErr) Error() string { return e.err.Error() }
func (e *clientWriteErr) Unwrap() error { return e.err }

// processStart anchors all idle-detection arithmetic to the monotonic clock:
// storing wall-clock UnixNano would strip Go's monotonic reading, so an NTP
// step or VM resume would spuriously expire (or extend) idle deadlines.
var processStart = time.Now()

// activityMonitor tracks when a session last saw data in either direction so
// idle detection is session-wide: traffic client→primary keeps the
// primary→client read alive and vice versa. Asymmetric protocols (e.g. a
// client that heartbeats into a server that never responds, like FrontelGI)
// would otherwise be torn down by the silent direction's own timer.
type activityMonitor struct {
	last atomic.Int64 // monotonic nanos since processStart of the last byte seen
}

func newActivityMonitor() *activityMonitor {
	m := &activityMonitor{}
	m.touch()
	return m
}

func (m *activityMonitor) touch() { m.last.Store(int64(time.Since(processStart))) }

// idleDeadline returns the instant the session becomes idle as a time.Time
// that carries a monotonic reading (suitable for SetReadDeadline).
func (m *activityMonitor) idleDeadline(timeout time.Duration) time.Time {
	return processStart.Add(time.Duration(m.last.Load()) + timeout)
}

// idleExceeded reports whether the session has been idle for at least timeout,
// measured on the monotonic clock.
func (m *activityMonitor) idleExceeded(timeout time.Duration) bool {
	return time.Since(processStart) >= time.Duration(m.last.Load())+timeout
}

// errReadUnblocked marks a read that was deliberately unblocked for session
// teardown (idleConn.unblock); isClosedErr treats it as a self-inflicted close.
var errReadUnblocked = errors.New("read unblocked for session teardown")

// errHalfCloseTimeout is returned by idleConn.Read when the session is
// half-closed (the other peer sent a clean EOF) and this, the only remaining
// direction, carried no data for -halfclosetimeout. It is deliberately not a
// deadline error: the classifiers attribute it as half_close_timeout, not idle.
var errHalfCloseTimeout = errors.New("half-close timeout: remaining direction idle")

// idleConn wraps one side of a session and owns that side's read deadlines.
//
//   - Idle detection: before each read the deadline is armed to the shared
//     activity monitor's idle instant. A deadline that fires while the *other*
//     direction was recently active is stale — it is re-armed and the read
//     retried, so only a genuinely session-wide idle surfaces as a timeout.
//   - Half-close: enterHalfClose() tightens the idle bound once the opposite
//     direction has ended with a clean EOF (TCP half-close). The remaining
//     direction keeps flowing, but if it goes silent for the half-close
//     timeout the read returns errHalfCloseTimeout.
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
	activity *activityMonitor
	sid      uint64
	label    string // "client" or "primary", for logs
	debug    bool   // cached debug-level check for the per-chunk receive log

	mu          sync.Mutex    // guards the fields below and serializes deadline arming against unblock()
	idleTimeout time.Duration // effective read-idle bound (tightened by enterHalfClose)
	halfClosed  bool          // the other direction ended with a clean EOF
	unblocked   bool
}

func newIdleConn(conn net.Conn, idleTimeout time.Duration, activity *activityMonitor, sid uint64, label string) *idleConn {
	return &idleConn{Conn: conn, idleTimeout: idleTimeout, activity: activity, sid: sid, label: label, debug: debugEnabled()}
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

// idleState returns the effective idle bound and whether the session is
// half-closed, consistently under the lock.
func (c *idleConn) idleState() (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.idleTimeout, c.halfClosed
}

// enterHalfClose marks this side as the only remaining open direction after
// the peer's clean EOF and bounds its silence by timeout (must be > 0): the
// effective idle bound becomes the tighter of -idletimeout and timeout, and a
// Read already blocked is re-armed so it wakes at the new instant. Safe to
// call from any goroutine.
func (c *idleConn) enterHalfClose(timeout time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.halfClosed = true
	if c.idleTimeout <= 0 || timeout < c.idleTimeout {
		c.idleTimeout = timeout
	}
	if !c.unblocked && c.idleTimeout > 0 {
		_ = c.Conn.SetReadDeadline(c.activity.idleDeadline(c.idleTimeout))
	}
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
			if c.debug {
				slog.Debug("data received", "sid", c.sid, "from", c.label, "bytes", n)
			}
		}
		// Only deadline expiry is ever ours to interpret. Other timeout-flavored
		// errors (e.g. ETIMEDOUT from TCP keepalive detecting a dead peer) are
		// real failures and must surface to the caller, not be retried.
		if err == nil || !isDeadlineErr(err) {
			return n, err
		}
		// A read deadline fired: a teardown unblock, a genuine session idle, a
		// half-close bound, or a stale deadline outdated by activity on the
		// other direction.
		if c.isUnblocked() {
			if n > 0 {
				return n, nil
			}
			return 0, errReadUnblocked
		}
		timeout, halfClosed := c.idleState()
		if timeout <= 0 || c.activity.idleExceeded(timeout) {
			if halfClosed && timeout > 0 {
				if n > 0 {
					return n, nil // deliver data; the next Read re-arms and times out
				}
				return 0, errHalfCloseTimeout
			}
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
// from a src read failure. onWrite (optional) is called with the byte count of
// every successful delivery to dst — including the delivered part of a failed
// partial write — so callers can keep live counters. Returns bytes copied and
// any error (io.EOF normalized to nil).
func copyWithDeadlines(dst net.Conn, src net.Conn, writeTimeout time.Duration, onWrite func(int)) (int64, error) {
	buf := make([]byte, copyBufSize)
	var total int64

	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			// writeAll handles partial writes and sets/clears write deadline;
			// count what was actually delivered even when the write fails partway.
			w, werr := writeAll(dst, buf[:n], writeTimeout)
			total += int64(w)
			if w > 0 && onWrite != nil {
				onWrite(w)
			}
			if werr != nil {
				return total, &clientWriteErr{err: werr}
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				return total, nil
			}
			return total, readErr
		}
	}
}

// parseDelimiter converts the delimiter flag to bytes. Named values and 0xNN..
// hex (an even number of hex digits, e.g. 0x03 or 0x0d0a) are recognized —
// consistent with -esc's hex syntax; anything else is taken literally. A
// malformed 0x value is an error rather than a silent literal string.
func parseDelimiter(s string) ([]byte, error) {
	switch strings.ToLower(s) {
	case "crlf":
		return []byte("\r\n"), nil
	case "lf":
		return []byte("\n"), nil
	case "cr":
		return []byte("\r"), nil
	case "etx":
		return []byte{0x03}, nil
	case "eot":
		return []byte{0x04}, nil
	}
	if ls := strings.ToLower(s); strings.HasPrefix(ls, "0x") {
		b, err := hex.DecodeString(ls[2:])
		if err != nil || len(b) == 0 {
			return nil, fmt.Errorf("invalid hex delimiter %q: use an even number of hex digits, e.g. 0x03 or 0x0d0a", s)
		}
		return b, nil
	}
	return []byte(s), nil
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
			if u == 0 {
				// 0 is the internal "no escape" sentinel; a NUL escape byte
				// would silently disable escaping.
				return 0, fmt.Errorf("escape byte 0x00 is not supported")
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
func validateConfig(lineMode bool, delim []byte, esc byte, partial, listen, primary string, mirrors []string, maxFrame, mirrorBuf int, connTimeout time.Duration) error {
	// Line mode dereferences delim (delim[len(delim)-1], delim[0]); an empty
	// delimiter would panic, so reject it up front.
	if lineMode && len(delim) == 0 {
		return fmt.Errorf("-delim must not be empty in line mode")
	}
	// Escape-based byte stuffing only makes sense with a single-byte delimiter.
	if esc != 0 && len(delim) != 1 {
		return fmt.Errorf("-esc requires a single-byte delimiter (got %d bytes: %q)", len(delim), delim)
	}
	// An escape equal to the delimiter makes the delimiter unrecognizable
	// (every occurrence reads as "escape the next byte"), so no frame could
	// ever terminate.
	if esc != 0 && len(delim) == 1 && esc == delim[0] {
		return fmt.Errorf("-esc must differ from the delimiter byte (both are 0x%02X)", esc)
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
	// A non-positive buffer would panic in make(chan) (negative) or silently
	// drop most mirror traffic (zero: the non-blocking send only succeeds when
	// the writer is already parked on the channel).
	if mirrorBuf < 1 {
		return fmt.Errorf("-mirrorbuf must be at least 1 (got %d)", mirrorBuf)
	}
	// Dialer.Timeout <= 0 means no Go-level bound at all (only the OS connect
	// timeout, typically minutes), which breaks every teardown bound that
	// assumes dials finish within -timeout.
	if connTimeout <= 0 {
		return fmt.Errorf("-timeout must be positive (got %v)", connTimeout)
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

// isResetErr returns true for connection reset errors. The errno check works
// on every platform (on Windows syscall.ECONNRESET is WSAECONNRESET); the
// string fallback covers errors that don't wrap an errno.
func isResetErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return true
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

// isDeadlineErr reports whether err stems from an expired read/write deadline
// (os.ErrDeadlineExceeded). This is deliberately narrower than isTimeoutErr:
// other timeout-flavored errors, such as ETIMEDOUT from TCP keepalive
// detecting a dead peer, are real failures rather than idle/slow conditions.
func isDeadlineErr(err error) bool {
	return err != nil && errors.Is(err, os.ErrDeadlineExceeded)
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
	ctx, cancel := context.WithCancel(context.Background())
	mw := &mirrorWriter{
		addr:         addr,
		ch:           make(chan []byte, chanSize),
		done:         make(chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
		connTimeout:  connTimeout,
		writeTimeout: writeTimeout,
		wasConnected: false,
		sid:          sid,
		stats:        stats,
		addrDrops:    mirrorDropCounter(addr),
		debug:        debugEnabled(),
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
// Deferred order matters: the conn is closed first (which ends any discard
// reader still blocked on it), then the readers are waited for, then wg is
// released — so no reader goroutine outlives the writer.
func (mw *mirrorWriter) run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer mw.readers.Wait()
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

// dropRemaining counts n plus all currently-queued chunks as mirror drops
// after a force-stop. The sweep is non-blocking because the channel may still
// be open (shutdown can force-stop a session whose fanOut is mid-send); a
// chunk enqueued concurrently with the sweep can slip past uncounted, which is
// acceptable for stats.
func (mw *mirrorWriter) dropRemaining(n uint64) {
	for {
		select {
		case _, ok := <-mw.ch:
			if !ok {
				mw.recordDrop(n, "force-stop")
				return
			}
			n++
		default:
			mw.recordDrop(n, "force-stop")
			return
		}
	}
}

// recordDrop counts n dropped chunks/frames for this mirror in the session
// stats, the per-writer counter (for per-mirror attribution in the session
// summary), and the process aggregates (for the periodic stats line).
func (mw *mirrorWriter) recordDrop(n uint64, why string) {
	if n == 0 {
		return
	}
	mw.drops.Add(n)
	if mw.stats != nil {
		mw.stats.mirrorDrops.Add(n)
	}
	aggMirrorDrops.Add(n)
	if mw.addrDrops != nil {
		mw.addrDrops.Add(n) // lock-free: resolved once in newMirrorWriter
	}
	if mw.debug {
		slog.Debug("mirror data dropped", "sid", mw.sid, "mirror", mw.addr, "chunks", n, "why", why)
	}
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
			// Could not connect; drop this chunk (counted) and try the next one.
			mw.recordDrop(1, "not connected")
			return
		}
	}

	if _, err := writeAll(mw.conn, data, mw.writeTimeout); err != nil {
		slog.Debug("mirror write failed", "sid", mw.sid, "mirror", mw.addr, "err", err)
		mw.markDown()
		mw.recordDrop(1, "write failed")
		return
	}
	if mw.debug {
		slog.Debug("mirror write ok", "sid", mw.sid, "mirror", mw.addr, "bytes", len(data))
	}
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

// dialContext returns the context that bounds mirror dials: cancelled by
// stop(), so a force-stop aborts an in-flight DNS lookup or connect instead of
// waiting out -timeout. Background when constructed without newMirrorWriter.
func (mw *mirrorWriter) dialContext() context.Context {
	if mw.ctx != nil {
		return mw.ctx
	}
	return context.Background()
}

// dialMirror dials the mirror through the injected dial func or a real TCP
// dialer with keepalive (to detect dead peers) bounded by connTimeout.
func (mw *mirrorWriter) dialMirror(ctx context.Context) (net.Conn, error) {
	if mw.dial != nil {
		return mw.dial(ctx, mw.addr)
	}
	dialer := &net.Dialer{
		Timeout:   mw.connTimeout,
		KeepAlive: 30 * time.Second,
	}
	return dialer.DialContext(ctx, "tcp", mw.addr)
}

// startDiscardReader consumes and discards everything the mirror sends on
// conn. Without it a request/response mirror that writes before reading its
// next request would eventually fill its send buffer (nobody reads our end),
// block in its write, stop reading, and stall our mirrored stream. The reader
// is bound to this exact conn instance and never touches mw.conn or connPtr:
// it exits when conn is closed (closeConn/interrupt) or the mirror stops
// sending, and a stale reader from a previous connection cannot affect a
// reconnected one. run() waits for all readers after closing the conn.
func (mw *mirrorWriter) startDiscardReader(conn net.Conn) {
	mw.readers.Add(1)
	go func() {
		defer mw.readers.Done()
		n, err := io.Copy(io.Discard, conn)
		if mw.debug {
			slog.Debug("mirror response reader exited", "sid", mw.sid, "mirror", mw.addr, "discarded_bytes", n, "err", err)
		}
	}()
}

// tryConnect attempts to connect with backoff, returns true on success.
// Must only be called from the run goroutine which owns the connection.
func (mw *mirrorWriter) tryConnect(backoff *time.Duration) bool {
	ctx := mw.dialContext()
	conn, err := mw.dialMirror(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false // force-stopped mid-dial: not a mirror failure, don't warn or back off
		}
		// A mirror that has never connected would otherwise only leave debug
		// traces; warn once per session so a dead or misconfigured mirror is
		// visible at the default log level.
		if !mw.wasConnected && !mw.failWarned {
			slog.Warn("mirror unreachable", "sid", mw.sid, "mirror", mw.addr, "err", err)
			mw.failWarned = true
		}
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

	// Publish before re-checking done: stop()'s interrupt() runs after done is
	// closed, so it either sees this pointer (and closes the conn out from
	// under a subsequent write) or we observe done below — either way a
	// force-stop can never be outrun by a dial that was in flight when it fired.
	mw.conn = conn
	mw.connPtr.Store(&conn)
	select {
	case <-mw.done:
		mw.closeConn()
		return false
	default:
	}
	mw.startDiscardReader(conn)

	// Log state transition: down → up (or initial connection)
	if !mw.wasConnected {
		slog.Info("mirror up", "sid", mw.sid, "mirror", mw.addr)
	}
	mw.wasConnected = true
	mw.failWarned = false
	*backoff = initialBackoff
	return true
}

// send attempts to send data to the mirror (non-blocking). It runs on the
// client→primary forwarding goroutine, so the full-queue drop path must stay
// cheap: no payload allocation, no lock (recordDrop is atomics only).
func (mw *mirrorWriter) send(data []byte) {
	// After a force-stop nothing will read the channel; count instead of queue.
	select {
	case <-mw.done:
		mw.recordDrop(1, "stopped")
		return
	default:
	}

	// Single producer: this goroutine is the only sender, and run() only ever
	// frees space, so a queue seen full stays full for the rest of this call —
	// drop before copying. (A slot freed concurrently would have been raced
	// for by the non-blocking send below anyway.) Conversely, a non-full
	// observation guarantees the send succeeds without blocking.
	if len(mw.ch) == cap(mw.ch) {
		mw.recordDrop(1, "buffer full")
		return
	}

	// Make a copy since the caller's buffer will be reused
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	select {
	case mw.ch <- dataCopy:
	default:
		mw.recordDrop(1, "buffer full")
	}
}

// beginDrain closes the send channel: run() delivers whatever is already
// queued and then exits. Only the sending goroutine may call it, after its
// last send() — it is the sole closer of ch. Idempotent.
func (mw *mirrorWriter) beginDrain() {
	mw.drainOnce.Do(func() { close(mw.ch) })
}

// stop force-stops the mirror writer (idempotent): an in-flight dial is
// cancelled, an in-flight write is interrupted by force-closing the
// connection, and run()'s exit path counts whatever is still queued as drops.
// The channel is deliberately NOT closed here — shutdown can force-stop a
// session whose fanOut is still send()ing, and only the sender (via
// beginDrain) may close ch without risking a panic.
func (mw *mirrorWriter) stop() {
	mw.stopOnce.Do(func() {
		close(mw.done) // unblock run() from any waiting state
		if mw.cancel != nil {
			mw.cancel() // abort an in-flight DNS lookup / connect
		}
		mw.interrupt() // unblock run() if blocked in an in-flight write
	})
}

// reopenableWriter is an append-mode log file whose handle can be swapped on
// SIGHUP, so external log rotation works without restarting the proxy.
type reopenableWriter struct {
	path string
	mu   sync.Mutex
	f    *os.File
}

func newReopenableWriter(path string) (*reopenableWriter, error) {
	f, err := openLogFile(path)
	if err != nil {
		return nil, err
	}
	return &reopenableWriter{path: path, f: f}, nil
}

func openLogFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

func (w *reopenableWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Write(p)
}

// Reopen swaps in a fresh handle to path (after rotation moved the old file
// aside) and closes the previous one.
func (w *reopenableWriter) Reopen() error {
	f, err := openLogFile(w.path)
	if err != nil {
		return err
	}
	w.mu.Lock()
	old := w.f
	w.f = f
	w.mu.Unlock()
	return old.Close()
}

func (w *reopenableWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
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
//     decremented when handle returns, so graceful shutdown's Wait() can never
//     race a just-accepted session.
//   - The admission slot is handed to handle as a release func so a session
//     can give it back as soon as its client/primary traffic is over, before
//     mirror cleanup (which is bounded separately by the drain budget). The
//     slot is released at most once, and always by the time handle returns.
func acceptLoop(accept func() (net.Conn, error), handle func(net.Conn, func()), sleep func(time.Duration), maxConns int) {
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

		// Count the session before spawning; the slot is released by handle
		// (early) or here (at the latest), and the count decremented when
		// handle returns.
		activeSessions.Add(1)
		go func(c net.Conn) {
			defer activeSessions.Done()
			var once sync.Once
			release := func() {
				if sem != nil {
					once.Do(func() { <-sem })
				}
			}
			defer release()
			handle(c, release)
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

	// Reject typo'd log flags up front instead of silently falling back to
	// info/text.
	switch *logLevelPtr {
	case "error", "info", "debug":
	default:
		fmt.Fprintf(os.Stderr, "Error: -loglevel must be 'error', 'info', or 'debug' (got %q)\n", *logLevelPtr)
		os.Exit(1)
	}
	switch *logFormatPtr {
	case "text", "json":
	default:
		fmt.Fprintf(os.Stderr, "Error: -logformat must be 'text' or 'json' (got %q)\n", *logFormatPtr)
		os.Exit(1)
	}

	// Set up slog logger, optionally appending to a reopenable file
	logDst := io.Writer(os.Stderr)
	var logFile *reopenableWriter
	if *logFilePtr != "" {
		w, err := newReopenableWriter(*logFilePtr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening log file: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = w.Close() }()
		logFile = w
		logDst = w
	}
	slog.SetDefault(slog.New(buildLogHandler(logDst, *logLevelPtr, *logFormatPtr)))

	// SIGHUP reopens the log file (log rotation support). Registering the
	// handler also prevents the default SIGHUP disposition — process
	// termination — from killing the proxy when e.g. logrotate sends HUP.
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	go func() {
		for range hupCh {
			if logFile == nil {
				continue
			}
			if err := logFile.Reopen(); err != nil {
				slog.Error("log file reopen failed", "file", *logFilePtr, "err", err)
			} else {
				slog.Info("log file reopened", "file", *logFilePtr)
			}
		}
	}()

	// Framing flags are silently inert without -line; warn so a forgotten
	// -line doesn't quietly disable framing.
	if !*lineModePtr {
		flag.Visit(func(f *flag.Flag) {
			switch f.Name {
			case "delim", "esc", "partial", "maxframe":
				slog.Warn("flag has no effect without -line", "flag", "-"+f.Name)
			}
		})
	}

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
	delim, err := parseDelimiter(*delimPtr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	esc, err := parseEscape(*escPtr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Validate resolved configuration before anything dereferences it (the
	// startup banner below reads delim[len(delim)-1]).
	if err := validateConfig(*lineModePtr, delim, esc, *partialPtr, *listenPtr, *primaryPtr, mirrorAddrs, *maxFramePtr, *mirrorBufPtr, *connTimeoutPtr); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	for _, w := range configWarnings(*lineModePtr, *mirrorBufPtr, *maxFramePtr, len(mirrorAddrs)) {
		slog.Warn(w)
	}

	// Seed per-mirror drop counters so the stats line always lists every
	// configured mirror, even before any drop occurs.
	for _, a := range mirrorAddrs {
		mirrorDropCounter(a)
	}

	// Enforce minimum write timeout to prevent indefinite hangs
	writeTimeout := *writeTimeoutPtr
	if writeTimeout < minWriteTimeout {
		writeTimeout = minWriteTimeout
		slog.Warn("-writetimeout below the enforced minimum, using minimum",
			"requested", *writeTimeoutPtr, "minimum", minWriteTimeout)
	}

	// A negative drain makes no sense; clamp to 0 (drop queued mirror data
	// immediately at session end)
	mirrorDrain := *mirrorDrainPtr
	if mirrorDrain < 0 {
		mirrorDrain = 0
		slog.Warn("-mirrordrain is negative, using 0 (queued mirror data is dropped at session end)",
			"requested", *mirrorDrainPtr)
	}
	halfCloseTimeout := *halfCloseTimeoutPtr
	if halfCloseTimeout < 0 {
		halfCloseTimeout = 0
		slog.Warn("-halfclosetimeout is negative, using 0 (half-close support disabled)",
			"requested", *halfCloseTimeoutPtr)
	}
	setDrainBudget(*maxDrainsPtr)

	l, err := net.Listen("tcp", *listenPtr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listening: %s\n", err.Error())
		os.Exit(1)
	}

	// Banner (stdout) after the listener is bound, so "Listening on" is true
	// when printed; the same startup config also goes to the log so a -logfile
	// records what configuration this run used.
	fmt.Printf("tee-rex %s\n", versionString())
	fmt.Printf("Listening on                    %s\n", *listenPtr)
	fmt.Printf("Connecting in primary mode to   %s\n", *primaryPtr)
	if len(mirrorAddrs) > 0 {
		fmt.Printf("Duplicating incoming traffic to %s\n", strings.Join(mirrorAddrs, ", "))
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
	slog.Info("started",
		"version", versionString(), "listen", *listenPtr, "primary", *primaryPtr,
		"mirrors", strings.Join(mirrorAddrs, ","), "line_mode", *lineModePtr,
		"idle_timeout", *idleTimeoutPtr, "half_close_timeout", halfCloseTimeout,
		"mirror_drain", mirrorDrain, "max_conns", *maxConnsPtr, "max_drains", *maxDrainsPtr)

	// Set up signal handling for graceful shutdown
	// Use os.Interrupt for Windows compatibility; SIGTERM for Unix.
	// Buffer 2 so a second signal during shutdown is observable (force exit).
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// Periodic aggregate stats so unattended deployments have ongoing
	// process-level evidence beyond per-session logs.
	if *statsIntervalPtr > 0 {
		statsTicker := time.NewTicker(*statsIntervalPtr)
		defer statsTicker.Stop()
		go func() {
			for range statsTicker.C {
				slog.Info("stats", statsAttrs()...)
			}
		}()
	}

	// Accept loop in goroutine so we can select on shutdown.
	cfg := &sessionConfig{
		primaryAddr:      *primaryPtr,
		mirrorAddrs:      mirrorAddrs,
		lineMode:         *lineModePtr,
		delim:            delim,
		esc:              esc,
		maxFrameSize:     *maxFramePtr,
		partialPolicy:    *partialPtr,
		connTimeout:      *connTimeoutPtr,
		writeTimeout:     writeTimeout,
		mirrorBufSize:    *mirrorBufPtr,
		idleTimeout:      *idleTimeoutPtr,
		halfCloseTimeout: halfCloseTimeout,
		mirrorDrain:      mirrorDrain,
	}
	handle := func(c net.Conn, release func()) {
		handleConnection(c, cfg, release)
	}
	accepting := make(chan struct{})
	go func() {
		defer close(accepting)
		acceptLoop(l.Accept, handle, time.Sleep, *maxConnsPtr)
	}()

	// Wait for shutdown signal
	sig := <-sigCh
	slog.Info("shutdown initiated", "signal", sig.String(), "active_sessions", activeCount.Load())

	// A second signal is the operator's escape hatch from a hung shutdown.
	go func() {
		s := <-sigCh
		slog.Warn("second signal received, exiting immediately", "signal", s.String())
		os.Exit(1)
	}()

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
		// Wait for goroutines to exit after force-close. Mirror dials are
		// cancelled by the force-stop, but a primary dial in flight is not
		// (it is not tracked until it completes), so the grace must still
		// cover -timeout plus teardown.
		grace := *connTimeoutPtr + 5*time.Second
		select {
		case <-done:
			slog.Info("shutdown complete after force-close")
		case <-time.After(grace):
			slog.Error("shutdown incomplete, some goroutines may be stuck", "grace", grace, "remaining_sessions", activeCount.Load())
		}
	}
}

// sessionConfig is the resolved per-process configuration every session runs
// with. Built once in main (and by tests), shared read-only across sessions.
type sessionConfig struct {
	primaryAddr   string
	mirrorAddrs   []string
	lineMode      bool
	delim         []byte
	esc           byte
	maxFrameSize  int
	partialPolicy string
	connTimeout   time.Duration
	writeTimeout  time.Duration
	mirrorBufSize int
	idleTimeout   time.Duration
	// halfCloseTimeout bounds the silence of the remaining direction after one
	// peer's clean EOF (TCP half-close). 0 disables half-close support and
	// restores the pre-#3 behavior: a primary close ends the session at once,
	// and after a client close the primary may still answer until the session
	// idle timeout.
	halfCloseTimeout time.Duration
	// mirrorDrain is how long queued mirror data may flush at session end.
	mirrorDrain time.Duration
}

// halfCloseWrite signals EOF to the peer on conn while keeping its read side
// open (TCP CloseWrite); connections without half-close support are closed.
func halfCloseWrite(conn net.Conn) {
	if tc, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = tc.CloseWrite()
	} else {
		_ = conn.Close()
	}
}

// handleConnection serves one client session. Session accounting
// (activeSessions.Add/Done) is owned by acceptLoop, which brackets this call.
// release (may be nil) gives back the -maxconns admission slot; it is called
// as soon as the client/primary traffic is over so that mirror cleanup — which
// is bounded separately by the drain budget — never counts against -maxconns.
//
// TCP half-close is relayed transparently: a clean EOF from one peer is passed
// on as a CloseWrite to the other, and the opposite direction keeps flowing
// until that peer also closes, or until it carries no data for
// cfg.halfCloseTimeout (reason half_close_timeout). Errors and resets on either
// side still tear down both directions promptly.
func handleConnection(in net.Conn, cfg *sessionConfig, release func()) {
	sid := sessionCounter.Add(1)
	stats := &sessionStats{}
	registerConn(sid, in, stats)
	defer unregisterConn(sid)

	startTime := time.Now()
	clientAddr := in.RemoteAddr().String()
	var primary net.Conn
	var mirrors []*mirrorWriter // declared before the summary defer so it can attribute per-mirror drops

	slog.Info("session started", "sid", sid, "client", clientAddr, "primary", cfg.primaryAddr)

	// Ensure session summary is always logged. Byte totals were already folded
	// into the process aggregates as they happened (countIn/countOut).
	defer func() {
		_ = in.Close()
		if primary != nil {
			_ = primary.Close()
		}
		for _, mw := range mirrors {
			if d := mw.drops.Load(); d > 0 {
				slog.Info("mirror session drops", "sid", sid, "mirror", mw.addr, "drops", d)
			}
		}
		attrs := []any{
			"sid", sid,
			"client", clientAddr,
			"reason", stats.getEndReason(),
			"duration", time.Since(startTime),
			"bytes_in", stats.bytesIn.Load(),
			"bytes_out", stats.bytesOut.Load(),
		}
		if cfg.lineMode {
			attrs = append(attrs, "frames", stats.framesIn.Load())
		}
		if len(cfg.mirrorAddrs) > 0 {
			attrs = append(attrs, "mirror_drops", stats.mirrorDrops.Load())
		}
		slog.Info("session ended", attrs...)
	}()

	// Connect to primary - this must succeed
	// Use Dialer with TCP keepalive to detect dead peers (consistent with mirrors)
	var err error
	dialer := &net.Dialer{Timeout: cfg.connTimeout, KeepAlive: 30 * time.Second}
	primary, err = dialer.Dial("tcp", cfg.primaryAddr)
	if err != nil {
		stats.setEndReason(reasonPrimaryDialFail)
		slog.Error("primary dial failed", "sid", sid, "primary", cfg.primaryAddr, "err", err)
		return
	}
	registerPrimary(sid, primary)
	slog.Info("primary connected", "sid", sid, "primary", cfg.primaryAddr)

	// Create async mirror writers (connections happen in goroutines)
	var mirrorWg sync.WaitGroup
	for _, addr := range cfg.mirrorAddrs {
		mw := newMirrorWriter(addr, &mirrorWg, cfg.connTimeout, cfg.writeTimeout, cfg.mirrorBufSize, sid, stats)
		mirrors = append(mirrors, mw)
	}
	registerMirrors(sid, mirrors)

	// Session-wide idle detection: both directions share one activity monitor,
	// so traffic either way keeps the whole session alive. Asymmetric protocols
	// (e.g. a heartbeating client with a silent server) would otherwise be torn
	// down by the silent direction's own timer.
	activity := newActivityMonitor()
	client := newIdleConn(in, cfg.idleTimeout, activity, sid, "client")
	primarySrc := newIdleConn(primary, cfg.idleTimeout, activity, sid, "primary")

	var wg sync.WaitGroup

	// Copy incoming data to primary and mirrors
	wg.Add(1)
	go func() {
		defer wg.Done()
		var clean bool
		if cfg.lineMode {
			clean = fanOutLines(client, primary, mirrors, cfg.delim, cfg.esc, cfg.maxFrameSize, cfg.writeTimeout, cfg.partialPolicy, sid, stats)
		} else {
			clean = fanOut(client, primary, mirrors, cfg.writeTimeout, sid, stats)
		}
		// Mirrors: no more data is coming; let queued frames flush (bounded
		// below by mirrorDrain)
		for _, mw := range mirrors {
			mw.beginDrain()
		}
		// Relay the client's FIN: half-close the primary so it sees EOF while
		// its responses can still flow back.
		halfCloseWrite(primary)
		// After a clean client EOF the primary read stays open — the
		// half-closed primary may still deliver late responses — bounded by the
		// half-close timeout so a silent primary can't hold the session forever
		// (even with idle detection disabled). With half-close support off (0)
		// the wait is bounded by the session idle timeout alone, as it always
		// was. For every other reason (reset, read error, framing violation,
		// primary write failure, shutdown) no deliverable responses remain —
		// unblock the primary read so the session tears down promptly.
		switch {
		case clean && cfg.halfCloseTimeout > 0:
			primarySrc.enterHalfClose(cfg.halfCloseTimeout)
		case clean:
			// idle-bounded only
		default:
			primarySrc.unblock()
		}
	}()

	// Copy primary response back to client with deadline support
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := copyWithDeadlines(in, primarySrc, cfg.writeTimeout, stats.countOut)
		if err == nil {
			// Clean primary EOF (TCP half-close from the primary). First-wins
			// makes this lose to client_eof when the client closed first, and
			// win only on a primary-initiated close — so an errReadUnblocked
			// teardown of the client read isn't misattributed as client_eof.
			stats.setEndReason(reasonPrimaryEOF)
			// Relay the primary's FIN to the client, then keep forwarding
			// client → primary: TCP EOF is directional and the primary may
			// well still be reading (e.g. a server that announces readiness,
			// closes its write side and then consumes an upload). The client
			// read is bounded by the half-close timeout instead of being
			// unblocked, unless half-close support is disabled (0).
			halfCloseWrite(in)
			if cfg.halfCloseTimeout > 0 {
				client.enterHalfClose(cfg.halfCloseTimeout)
			} else {
				client.unblock()
			}
			return
		}
		var cwErr *clientWriteErr
		switch {
		case errors.As(err, &cwErr):
			// Failure writing to the client, not a primary problem. Only a
			// deadline expiry means "slow client"; other timeout-flavored
			// errors (e.g. ETIMEDOUT) are a dead client connection.
			if isDeadlineErr(err) {
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
		case errors.Is(err, errHalfCloseTimeout):
			// The client finished long ago; the primary neither answered nor
			// closed within the half-close window.
			if stats.setHalfCloseTimeout() {
				slog.Info("half-close timeout: primary stayed open but silent after client EOF", "sid", sid, "timeout", cfg.halfCloseTimeout)
			}
		case isResetErr(err):
			if stats.setEndReason(reasonPrimaryReset) {
				slog.Info("primary connection reset", "sid", sid, "err", err)
			}
		case isDeadlineErr(err):
			if stats.setEndReason(reasonSessionIdle) {
				slog.Info("session idle timeout", "sid", sid, "err", err)
			}
		default:
			stats.setEndReason(reasonPrimaryReadErr)
			slog.Error("primary read failed", "sid", sid, "op", "read", "err", err)
		}
		// The primary can no longer deliver responses. Unblock fanOut's client
		// read (any further client data has nowhere to go now) so the session
		// tears down promptly instead of lingering until the session idle
		// timeout — or forever when idle timeout is disabled.
		client.unblock()
		// Half-close the client write side: signal no more responses are coming.
		halfCloseWrite(in)
	}()

	wg.Wait()
	// The session is over for both peers: close the conns now so the client
	// isn't left holding a half-open connection while mirrors flush. (The
	// deferred Closes remain as no-op safety.)
	_ = in.Close()
	_ = primary.Close()
	// Primary-path work is finished: give the -maxconns slot back before any
	// mirror cleanup so a slow or unhealthy mirror can never hold admission
	// capacity hostage.
	if release != nil {
		release()
	}
	drainMirrors(sid, mirrors, &mirrorWg, cfg.mirrorDrain)
}

// drainMirrors lets a finished session's mirror writers flush their queued
// data, bounded by mirrorDrain, then force-stops them. The drain happens under
// the process-wide drain budget (-maxdrains): when no slot is available the
// writers are force-stopped at once and whatever is queued is counted in
// mirror_drops — mirror trouble sheds mirror work, never primary traffic.
// Shutdown's closeAllConns force-stops the writers too, aborting the drain
// early. After a force-stop the wait is short and bounded: the in-flight dial
// is cancelled and the in-flight write interrupted.
func drainMirrors(sid uint64, mirrors []*mirrorWriter, mirrorWg *sync.WaitGroup, mirrorDrain time.Duration) {
	if len(mirrors) == 0 {
		return
	}
	drained := make(chan struct{})
	go func() {
		mirrorWg.Wait()
		close(drained)
	}()
	releaseSlot, ok := acquireDrainSlot()
	if !ok {
		slog.Debug("mirror drain budget exhausted, force-stopping", "sid", sid)
		for _, mw := range mirrors {
			mw.stop()
		}
		<-drained
		return
	}
	defer releaseSlot()
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

// classifyClientRead records the session end reason for a client-read error
// and logs it, and reports whether the read ended with a clean EOF (a TCP
// half-close from the client, after which the primary may still respond). The
// reset/idle logs are gated on winning the reason so a lost teardown race
// (e.g. shutdown already attributed the session) doesn't emit a misleading
// line.
func classifyClientRead(err error, sid uint64, stats *sessionStats) (clean bool) {
	switch {
	case err == io.EOF:
		stats.setEndReason(reasonClientEOF)
		return true
	case isClosedErr(err):
		// Self-inflicted (unblock after the primary finished, or shutdown's
		// force-close): whoever ended the session already set the reason, so
		// this first-wins set is only a fallback against "unknown".
		stats.setEndReason(reasonClientEOF)
	case errors.Is(err, errHalfCloseTimeout):
		// The primary closed long ago; the client neither sent more nor closed
		// within the half-close window.
		if stats.setHalfCloseTimeout() {
			slog.Info("half-close timeout: client stayed open but silent after primary EOF", "sid", sid)
		}
	case isResetErr(err):
		if stats.setEndReason(reasonClientReset) {
			slog.Info("client connection reset", "sid", sid, "err", err)
		}
	case isDeadlineErr(err):
		if stats.setEndReason(reasonSessionIdle) {
			slog.Info("session idle timeout", "sid", sid, "err", err)
		}
	default:
		stats.setEndReason(reasonClientReadErr)
		slog.Error("client read failed", "sid", sid, "op", "read", "err", err)
	}
	return false
}

// fanOut reads from src and writes to primary (required) and mirrors (best-effort async).
// Primary write errors stop the operation. Mirror writes are non-blocking via channels.
// Read-side idle detection is owned by src (see idleConn). Reports whether src
// ended with a clean EOF (see classifyClientRead).
func fanOut(src net.Conn, primary net.Conn, mirrors []*mirrorWriter, writeTimeout time.Duration, sid uint64, stats *sessionStats) bool {
	buf := make([]byte, copyBufSize)

	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			data := buf[:n]
			stats.countIn(n)

			// Write to primary - must succeed
			if _, err := writeAll(primary, data, writeTimeout); err != nil {
				stats.setEndReason(reasonPrimaryWriteErr)
				slog.Error("primary write failed", "sid", sid, "op", "write", "err", err)
				return false
			}

			// Send to mirrors asynchronously (non-blocking)
			for _, mw := range mirrors {
				mw.send(data)
			}
		}

		if readErr != nil {
			return classifyClientRead(readErr, sid, stats)
		}
	}
}

// fanOutLines reads complete lines/frames from src and writes to primary and mirrors.
// Lines are delimited by the specified delimiter and always include the delimiter.
// When esc is non-zero, escape sequences are handled (ESC+DELIM = literal DELIM).
// partialPolicy controls handling of incomplete frames at EOF: "drop", "forward", or "error".
// Primary write errors stop the operation. Mirror writes are non-blocking via channels.
// Read-side idle detection is owned by src (see idleConn). Reports whether src
// ended with a clean EOF (see classifyClientRead).
func fanOutLines(src net.Conn, primary net.Conn, mirrors []*mirrorWriter, delim []byte, esc byte, maxFrameSize int, writeTimeout time.Duration, partialPolicy string, sid uint64, stats *sessionStats) bool {
	// Cap bufio buffer size - frame limit is enforced separately in read functions
	bufSize := maxFrameSize
	if bufSize > maxBufioSize {
		bufSize = maxBufioSize
	}
	reader := bufio.NewReaderSize(src, bufSize)
	debug := debugEnabled()

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
			stats.countIn(len(frame)) // the bytes were read off the wire
			stats.setEndReason(reasonFrameTooLarge)
			slog.Error("frame too large", "sid", sid, "size", len(frame), "max", maxFrameSize)
			return false
		}

		if len(frame) > 0 {
			stats.countIn(len(frame))

			// Determine if we should forward this frame
			shouldForward := complete
			if !complete && err != nil {
				if err != io.EOF {
					// Mid-frame teardown (idle, reset, unblock, shutdown): the
					// -partial policy is for a clean EOF only — never forward a
					// fragment on an error path, it would corrupt downstream
					// framing for a session that is being torn down anyway.
					slog.Debug("partial frame dropped (read error)", "sid", sid, "size", len(frame), "err", err)
					shouldForward = false
				} else {
					// Partial frame at EOF - apply policy
					switch partialPolicy {
					case "forward":
						shouldForward = true
						slog.Debug("partial frame forwarded", "sid", sid, "size", len(frame), "policy", partialPolicy, "err", err)
					case "error":
						stats.setEndReason(reasonPartialFrame)
						slog.Error("partial frame", "sid", sid, "size", len(frame), "err", err)
						return false
					default: // "drop"
						slog.Debug("partial frame dropped", "sid", sid, "size", len(frame), "policy", partialPolicy, "err", err)
						shouldForward = false
					}
				}
			}

			if shouldForward {
				// Write to primary - must succeed
				if _, err := writeAll(primary, frame, writeTimeout); err != nil {
					stats.setEndReason(reasonPrimaryWriteErr)
					slog.Error("primary write failed", "sid", sid, "op", "write", "err", err)
					return false
				}
				stats.framesIn.Add(1)
				if debug {
					slog.Debug("frame forwarded", "sid", sid, "size", len(frame))
				}

				// Send to mirrors asynchronously (non-blocking)
				for _, mw := range mirrors {
					mw.send(frame)
				}
			}
		}

		if err != nil {
			return classifyClientRead(err, sid, stats)
		}
	}
}

func Usage() {
	fmt.Fprintf(os.Stderr, "tee-rex version %s\n", versionString())
	fmt.Fprintf(os.Stderr, "Usage:   $ tee-rex -l <listen_addr> -p <primary_addr> [-m <mirror_addrs>] [-line]\n")
	fmt.Fprintf(os.Stderr, "Example: $ tee-rex -l localhost:8080 -p localhost:9090 -m localhost:9091,localhost:9092\n")
	fmt.Fprintf(os.Stderr, "         $ tee-rex -l :25 -p mail:25 -m mirror:25 -line  # SMTP mirroring\n")
	fmt.Fprintf(os.Stderr, "         $ tee-rex -l :5000 -p as:5000 -line -delim etx -esc esc  # Framed protocol (e.g. FrontelGI)\n")
	fmt.Fprintf(os.Stderr, "         $ tee-rex -l :8080 -p :9090 -loglevel=debug -logfile /var/log/tee-rex.log  # Debug logging to a file\n")
	fmt.Fprintf(os.Stderr, "-----------------------\nFlags:\n")
	flag.PrintDefaults()
}
