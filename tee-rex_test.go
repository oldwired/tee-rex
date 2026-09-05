// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 oldwired

package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/iotest"
	"time"
)

// eventually polls cond until it is true or the deadline passes.
func eventually(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(msg)
}

// captureLogs installs a buffer-backed slog default logger for the duration of
// fn and returns everything logged. slog serializes writes to the buffer, so it
// is safe across the goroutines fn may spawn.
func captureLogs(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	fn()
	return buf.String()
}

// tcpConnPair returns a connected TCP pair via a throwaway loopback listener:
// `client` is the dialing side, `server` the accepted side. Both are cleaned up.
func tcpConnPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	srvCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		srvCh <- c
	}()
	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	select {
	case server = <-srvCh:
	case err := <-errCh:
		t.Fatalf("accept: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("accept timed out")
	}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return client, server
}

// ---------------------------------------------------------------------------
// Test scaffolding: mockConn, fake net.Error, loopback/pipe helpers.
// These tests are white-box (package main) so they can exercise unexported
// functions. Run with:  go test -race tee-rex.go tee-rex_test.go
// ---------------------------------------------------------------------------

// mockAddr is a trivial net.Addr for mockConn.
type mockAddr struct{}

func (mockAddr) Network() string { return "mock" }
func (mockAddr) String() string  { return "mock" }

// timeoutError implements net.Error with Timeout()==true, for simulating
// deadline-exceeded writes/reads without real sockets.
type timeoutError struct{ msg string }

func (e timeoutError) Error() string { return e.msg }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// mockConn is a configurable in-memory net.Conn used for unit tests.
// onRead/onWrite override the default behavior; all Write inputs and all
// deadlines are recorded for assertions. It is safe for concurrent use.
type mockConn struct {
	mu sync.Mutex

	onRead  func(b []byte) (int, error)
	onWrite func(b []byte) (int, error)

	writes         [][]byte    // copy of every Write input
	accepted       []byte      // logical stream actually accepted (b[:n] of each Write)
	closed         bool        // Close() called
	closeWrites    int         // CloseWrite() called count
	readDeadlines  []time.Time // every SetReadDeadline arg
	writeDeadlines []time.Time // every SetWriteDeadline arg
}

func (m *mockConn) Read(b []byte) (int, error) {
	if m.onRead != nil {
		return m.onRead(b)
	}
	return 0, net.ErrClosed
}

func (m *mockConn) Write(b []byte) (int, error) {
	n, err := len(b), error(nil)
	if m.onWrite != nil {
		n, err = m.onWrite(b)
	}
	m.mu.Lock()
	m.writes = append(m.writes, append([]byte(nil), b...))
	if n > 0 {
		m.accepted = append(m.accepted, b[:n]...)
	}
	m.mu.Unlock()
	return n, err
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return nil
}

// CloseWrite lets mockConn satisfy the interface{ CloseWrite() error } used by
// handleConnection for half-close.
func (m *mockConn) CloseWrite() error {
	m.mu.Lock()
	m.closeWrites++
	m.mu.Unlock()
	return nil
}

func (m *mockConn) LocalAddr() net.Addr  { return mockAddr{} }
func (m *mockConn) RemoteAddr() net.Addr { return mockAddr{} }

func (m *mockConn) SetDeadline(t time.Time) error {
	_ = m.SetReadDeadline(t)
	return m.SetWriteDeadline(t)
}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	m.mu.Lock()
	m.readDeadlines = append(m.readDeadlines, t)
	m.mu.Unlock()
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	m.mu.Lock()
	m.writeDeadlines = append(m.writeDeadlines, t)
	m.mu.Unlock()
	return nil
}

// writeCount returns how many times Write was called.
func (m *mockConn) writeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.writes)
}

// bytesWritten returns the logical stream the conn accepted (concatenation of
// b[:n] across every Write), i.e. what a real peer would have received.
func (m *mockConn) bytesWritten() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.accepted...)
}

// firstNonZeroWriteDeadline returns the first SetWriteDeadline arg that was not
// the zero time (i.e. an actual deadline, not the clearing call).
func (m *mockConn) firstNonZeroWriteDeadline() (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, d := range m.writeDeadlines {
		if !d.IsZero() {
			return d, true
		}
	}
	return time.Time{}, false
}

func (m *mockConn) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// startLoopback starts a TCP listener on 127.0.0.1:0 and serves each accepted
// connection with handler in its own goroutine. The listener is closed on test
// cleanup. Returns the listener (use .Addr().String() to dial).
func startLoopback(t *testing.T, handler func(net.Conn)) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go handler(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// dialLoopback dials a TCP address and registers the conn for cleanup.
func dialLoopback(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// testSessionConfig returns a raw-mode session config with the timings most
// handleConnection tests use: idle detection off, half-close support off (the
// pre-#3 behavior: a primary close tears the session down at once, so tests
// that don't care about half-close keep their expectations), 1s mirror drain.
// Tests override what they exercise.
func testSessionConfig(primaryAddr string) *sessionConfig {
	return &sessionConfig{
		primaryAddr:   primaryAddr,
		maxFrameSize:  defaultMaxLineLen,
		partialPolicy: "drop",
		connTimeout:   time.Second,
		writeTimeout:  5 * time.Second,
		mirrorBufSize: 10,
		mirrorDrain:   time.Second,
	}
}

// waitGroupDone reports whether wg.Wait() returns within d.
func waitGroupDone(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// TestScaffolding sanity-checks the helpers and that the package compiles as a
// white-box test binary.
func TestScaffolding(t *testing.T) {
	var mc net.Conn = &mockConn{}
	if _, ok := mc.(interface{ CloseWrite() error }); !ok {
		t.Fatal("mockConn must satisfy CloseWrite for half-close tests")
	}

	// net.Pipe ends must support deadlines (relied on by later tests).
	p1, p2 := net.Pipe()
	defer func() { _ = p1.Close() }()
	defer func() { _ = p2.Close() }()
	if err := p1.SetReadDeadline(time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("net.Pipe SetReadDeadline: %v", err)
	}

	// Loopback round-trip.
	ln := startLoopback(t, func(c net.Conn) {
		buf := make([]byte, 4)
		n, _ := c.Read(buf)
		_, _ = c.Write(buf[:n])
		_ = c.Close()
	})
	c := dialLoopback(t, ln.Addr().String())
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4)
	if _, err := c.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("echo mismatch: %q", buf)
	}
}

// TestValidateConfig covers C1 (empty delimiter in line mode must error, not
// panic) and L5 (mirror addresses are validated), plus the pre-existing checks.
func TestValidateConfig(t *testing.T) {
	crlf := []byte("\r\n")
	tests := []struct {
		name      string
		line      bool
		delim     []byte
		esc       byte
		partial   string
		listen    string
		primary   string
		mirrors   []string
		maxF      int
		mirrorBuf int           // 0 means "use a valid default" (100)
		timeout   time.Duration // 0 means "use a valid default" (1s)
		wantErr   bool
	}{
		{name: "valid raw", partial: "drop", listen: "localhost:8080", primary: "localhost:9090", maxF: 1024},
		{name: "valid line crlf", line: true, delim: crlf, partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024},
		{name: "C1 empty delim line mode", line: true, delim: []byte{}, partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024, wantErr: true},
		{name: "empty delim raw mode ok", line: false, delim: []byte{}, partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024},
		{name: "esc needs single-byte delim", line: true, delim: crlf, esc: 0x1B, partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024, wantErr: true},
		{name: "esc with single-byte delim ok", line: true, delim: []byte{0x03}, esc: 0x1B, partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024},
		{name: "esc equal to delim rejected", line: true, delim: []byte{0x03}, esc: 0x03, partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024, wantErr: true},
		{name: "bad partial", partial: "nope", listen: ":8080", primary: ":9090", maxF: 1024, wantErr: true},
		{name: "bad listen addr", partial: "drop", listen: "no-port", primary: ":9090", maxF: 1024, wantErr: true},
		{name: "bad primary addr", partial: "drop", listen: ":8080", primary: "no-port", maxF: 1024, wantErr: true},
		{name: "L5 bad mirror addr", partial: "drop", listen: ":8080", primary: ":9090", mirrors: []string{"ok:1", "bad"}, maxF: 1024, wantErr: true},
		{name: "L5 good mirrors", partial: "drop", listen: ":8080", primary: ":9090", mirrors: []string{"a:1", "b:2"}, maxF: 1024},
		{name: "non-positive maxframe", partial: "drop", listen: ":8080", primary: ":9090", maxF: 0, wantErr: true},
		{name: "negative mirrorbuf rejected", partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024, mirrorBuf: -1, wantErr: true},
		{name: "negative timeout rejected", partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024, timeout: -time.Second, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mb := tc.mirrorBuf
			if mb == 0 {
				mb = 100
			}
			to := tc.timeout
			if to == 0 {
				to = time.Second
			}
			err := validateConfig(tc.line, tc.delim, tc.esc, tc.partial, tc.listen, tc.primary, tc.mirrors, tc.maxF, mb, to)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateConfig err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}

	// Zero values can't be expressed through the defaulting table above.
	if err := validateConfig(false, nil, 0, "drop", ":8080", ":9090", nil, 1024, 0, time.Second); err == nil {
		t.Error("mirrorbuf 0 must be rejected (unbuffered channel drops most mirror traffic)")
	}
	if err := validateConfig(false, nil, 0, "drop", ":8080", ":9090", nil, 1024, 100, 0); err == nil {
		t.Error("timeout 0 must be rejected (unbounded dials)")
	}
}

// TestConfigWarnings covers L6: the mirror-buffer memory heads-up.
func TestConfigWarnings(t *testing.T) {
	if w := configWarnings(false, 100, 1<<30, 10); len(w) != 0 {
		t.Fatalf("raw mode must not warn, got %v", w)
	}
	if w := configWarnings(true, 100, 64*1024, 1); len(w) != 0 {
		t.Fatalf("below threshold must not warn, got %v", w)
	}
	if w := configWarnings(true, 100, 1<<30, 0); len(w) != 0 {
		t.Fatalf("no mirrors must not warn, got %v", w)
	}
	if w := configWarnings(true, 100, 64*1024, 200); len(w) != 1 {
		t.Fatalf("above threshold must warn once, got %v", w)
	}
}

// TestFanOutLinesBytesIn locks in L4: bytes_in counts bytes *read* from the
// client (including a dropped partial frame), while frames counts only complete
// forwarded frames. This is the documented, intentional semantics.
func TestFanOutLinesBytesIn(t *testing.T) {
	srcR, srcW := net.Pipe()
	primary := &mockConn{}
	stats := &sessionStats{}

	go func() {
		_, _ = srcW.Write([]byte("a\r\nbb\r\nccc")) // two complete frames + partial
		_ = srcW.Close()
	}()

	fanOutLines(srcR, primary, nil, []byte("\r\n"), 0, defaultMaxLineLen, 5*time.Second, "drop", 1, stats)

	if got := stats.framesIn.Load(); got != 2 {
		t.Fatalf("framesIn=%d want 2", got)
	}
	if got := stats.bytesIn.Load(); got != 10 {
		t.Fatalf("bytesIn=%d want 10 (3+4+3, includes dropped partial)", got)
	}
	if got := string(primary.bytesWritten()); got != "a\r\nbb\r\n" {
		t.Fatalf("primary received %q want %q", got, "a\r\nbb\r\n")
	}
	if got := stats.getEndReason(); got != reasonClientEOF {
		t.Fatalf("endReason=%q want %q", got, reasonClientEOF)
	}
}

// ---------------------------------------------------------------------------
// A-suite: regression tests for pure logic.
// ---------------------------------------------------------------------------

func TestParseDelimiter(t *testing.T) {
	cases := map[string][]byte{
		"crlf":    {'\r', '\n'},
		"CRLF":    {'\r', '\n'}, // case-insensitive
		"lf":      {'\n'},
		"cr":      {'\r'},
		"etx":     {0x03},
		"eot":     {0x04},
		"0x03":    {0x03},            // hex, consistent with -esc
		"0X03":    {0x03},            // case-insensitive hex
		"0x0d0a":  {'\r', '\n'},      // multi-byte hex
		"</data>": []byte("</data>"), // custom passthrough
		"":        {},                // empty passthrough (rejected later by validateConfig in line mode)
	}
	for in, want := range cases {
		got, err := parseDelimiter(in)
		if err != nil {
			t.Errorf("parseDelimiter(%q) unexpected error: %v", in, err)
			continue
		}
		if string(got) != string(want) {
			t.Errorf("parseDelimiter(%q)=%v want %v", in, got, want)
		}
	}

	// Malformed hex must error instead of silently becoming a literal string.
	for _, in := range []string{"0x", "0xzz", "0x123"} {
		if _, err := parseDelimiter(in); err == nil {
			t.Errorf("parseDelimiter(%q) should error on malformed hex", in)
		}
	}
}

func TestParseEscape(t *testing.T) {
	cases := []struct {
		in      string
		want    byte
		wantErr bool
	}{
		{in: "", want: 0},
		{in: "esc", want: 0x1B},
		{in: "ESC", want: 0x1B},
		{in: "0x1b", want: 0x1B},
		{in: "0x1B", want: 0x1B},
		{in: "0xff", want: 0xFF},
		{in: "A", want: 'A'},
		{in: "0xzz", wantErr: true},  // bad hex
		{in: "0x1ff", wantErr: true}, // out of byte range
		{in: "0x00", wantErr: true},  // collides with the "no escape" sentinel
		{in: "ab", wantErr: true},    // multi-char, not esc/hex
	}
	for _, tc := range cases {
		got, err := parseEscape(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseEscape(%q) err=%v wantErr=%v", tc.in, err, tc.wantErr)
			continue
		}
		if err == nil && got != tc.want {
			t.Errorf("parseEscape(%q)=0x%02X want 0x%02X", tc.in, got, tc.want)
		}
	}
}

func TestWriteAll(t *testing.T) {
	t.Run("full write", func(t *testing.T) {
		mc := &mockConn{}
		n, err := writeAll(mc, []byte("hello"), 0)
		if err != nil || n != 5 {
			t.Fatalf("n=%d err=%v", n, err)
		}
		if got := string(mc.bytesWritten()); got != "hello" {
			t.Fatalf("written %q", got)
		}
	})

	t.Run("partial writes loop until done", func(t *testing.T) {
		mc := &mockConn{onWrite: func(b []byte) (int, error) {
			n := 2
			if n > len(b) {
				n = len(b)
			}
			return n, nil
		}}
		n, err := writeAll(mc, []byte("hello"), 0)
		if err != nil || n != 5 {
			t.Fatalf("n=%d err=%v", n, err)
		}
		if got := string(mc.bytesWritten()); got != "hello" {
			t.Fatalf("written %q want hello", got)
		}
		if c := mc.writeCount(); c != 3 { // 2+2+1
			t.Fatalf("writeCount=%d want 3", c)
		}
	})

	t.Run("zero-write returns ErrShortWrite", func(t *testing.T) {
		mc := &mockConn{onWrite: func(b []byte) (int, error) { return 0, nil }}
		if _, err := writeAll(mc, []byte("x"), 0); err != io.ErrShortWrite {
			t.Fatalf("err=%v want ErrShortWrite", err)
		}
	})

	t.Run("error reports bytes already written", func(t *testing.T) {
		boom := errors.New("boom")
		calls := 0
		mc := &mockConn{onWrite: func(b []byte) (int, error) {
			calls++
			if calls == 1 {
				return 2, nil
			}
			return 0, boom
		}}
		n, err := writeAll(mc, []byte("hello"), 0)
		if err != boom {
			t.Fatalf("err=%v want boom", err)
		}
		if n != 2 {
			t.Fatalf("n=%d want 2 (partially delivered bytes must be reported)", n)
		}
	})

	t.Run("sets write deadline when timeout>0", func(t *testing.T) {
		mc := &mockConn{}
		before := time.Now()
		if _, err := writeAll(mc, []byte("x"), 50*time.Millisecond); err != nil {
			t.Fatalf("err=%v", err)
		}
		d, ok := mc.firstNonZeroWriteDeadline()
		if !ok {
			t.Fatal("expected a write deadline to be set")
		}
		if d.Before(before.Add(40*time.Millisecond)) || d.After(before.Add(5*time.Second)) {
			t.Fatalf("deadline %v not ~now+50ms", d)
		}
	})

	t.Run("no deadline when timeout==0", func(t *testing.T) {
		mc := &mockConn{}
		_, _ = writeAll(mc, []byte("x"), 0)
		if _, ok := mc.firstNonZeroWriteDeadline(); ok {
			t.Fatal("did not expect a write deadline")
		}
	})
}

func TestReadUntilDelim(t *testing.T) {
	t.Run("crlf frames", func(t *testing.T) {
		r := bufio.NewReader(strings.NewReader("hello\r\nworld\r\n"))
		f, complete, err := readUntilDelim(r, []byte("\r\n"), 1024)
		if err != nil || !complete || string(f) != "hello\r\n" {
			t.Fatalf("got %q complete=%v err=%v", f, complete, err)
		}
		f, complete, err = readUntilDelim(r, []byte("\r\n"), 1024)
		if err != nil || !complete || string(f) != "world\r\n" {
			t.Fatalf("got %q complete=%v err=%v", f, complete, err)
		}
		_, complete, err = readUntilDelim(r, []byte("\r\n"), 1024)
		if complete || err != io.EOF {
			t.Fatalf("want EOF/incomplete, complete=%v err=%v", complete, err)
		}
	})

	t.Run("multi-byte delim split across reads", func(t *testing.T) {
		// OneByteReader forces the underlying reader to return one byte per Read,
		// exercising delimiter assembly across reads.
		r := bufio.NewReader(iotest.OneByteReader(strings.NewReader("ab</x>cd</x>")))
		f, complete, err := readUntilDelim(r, []byte("</x>"), 1024)
		if err != nil || !complete || string(f) != "ab</x>" {
			t.Fatalf("got %q complete=%v err=%v", f, complete, err)
		}
	})

	t.Run("frame exactly at max is allowed", func(t *testing.T) {
		f, complete, err := readUntilDelim(bufio.NewReader(strings.NewReader("abc\r\n")), []byte("\r\n"), 5)
		if err != nil || !complete || string(f) != "abc\r\n" {
			t.Fatalf("got %q complete=%v err=%v", f, complete, err)
		}
	})

	t.Run("frame over max errors", func(t *testing.T) {
		_, complete, err := readUntilDelim(bufio.NewReader(strings.NewReader("abc\r\n")), []byte("\r\n"), 4)
		if !errors.Is(err, ErrFrameTooLarge) || complete {
			t.Fatalf("want ErrFrameTooLarge, complete=%v err=%v", complete, err)
		}
	})

	t.Run("partial at EOF", func(t *testing.T) {
		f, complete, err := readUntilDelim(bufio.NewReader(strings.NewReader("abc")), []byte("\r\n"), 1024)
		if complete || err != io.EOF || string(f) != "abc" {
			t.Fatalf("got %q complete=%v err=%v", f, complete, err)
		}
	})
}

func TestReadFrameEscaped(t *testing.T) {
	const etx, esc = byte(0x03), byte(0x1B)

	t.Run("escaped delimiter is literal", func(t *testing.T) {
		in := []byte{'a', 'b', esc, etx, 'c', etx} // ESC+ETX literal, trailing ETX ends frame
		f, complete, err := readFrameEscaped(bufio.NewReader(bytesReader(in)), etx, esc, 1024)
		if err != nil || !complete {
			t.Fatalf("complete=%v err=%v", complete, err)
		}
		if string(f) != string(in) {
			t.Fatalf("frame=%v want %v", f, in)
		}
	})

	t.Run("escape makes any next byte literal", func(t *testing.T) {
		in := []byte{esc, 'X', etx}
		f, complete, err := readFrameEscaped(bufio.NewReader(bytesReader(in)), etx, esc, 1024)
		if err != nil || !complete || len(f) != 3 {
			t.Fatalf("frame=%v complete=%v err=%v", f, complete, err)
		}
	})

	t.Run("trailing escape at EOF is incomplete", func(t *testing.T) {
		in := []byte{'a', 'b', esc}
		f, complete, err := readFrameEscaped(bufio.NewReader(bytesReader(in)), etx, esc, 1024)
		if complete || err != io.EOF || len(f) != 3 {
			t.Fatalf("frame=%v complete=%v err=%v", f, complete, err)
		}
	})

	t.Run("over max errors", func(t *testing.T) {
		in := []byte{'a', 'b', 'c', 'd', etx}
		_, _, err := readFrameEscaped(bufio.NewReader(bytesReader(in)), etx, esc, 3)
		if !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("want ErrFrameTooLarge err=%v", err)
		}
	})
}

func TestErrorClassifiers(t *testing.T) {
	if isClosedErr(nil) || isResetErr(nil) || isTimeoutErr(nil) {
		t.Fatal("nil must classify false everywhere")
	}
	if !isClosedErr(net.ErrClosed) {
		t.Error("net.ErrClosed should be closed")
	}
	if !isClosedErr(errors.New("use of closed network connection")) {
		t.Error("closed-string should be closed")
	}
	if !isResetErr(errors.New("read: connection reset by peer")) {
		t.Error("reset-string should be reset")
	}
	if !isTimeoutErr(timeoutError{msg: "i/o timeout"}) {
		t.Error("net.Error timeout should be timeout")
	}
	if !isTimeoutErr(fmt.Errorf("wrapped: %w", timeoutError{msg: "i/o timeout"})) {
		t.Error("wrapped timeout should unwrap to timeout")
	}
	if isTimeoutErr(errors.New("nope")) {
		t.Error("plain error is not a timeout")
	}
	if !isDeadlineErr(fmt.Errorf("read: %w", os.ErrDeadlineExceeded)) {
		t.Error("wrapped os.ErrDeadlineExceeded should be a deadline error")
	}
	if isDeadlineErr(timeoutError{msg: "i/o timeout"}) {
		t.Error("a generic net.Error timeout (e.g. ETIMEDOUT) is NOT a deadline error")
	}
	if !isResetErr(fmt.Errorf("read: %w", syscall.ECONNRESET)) {
		t.Error("errno-wrapped ECONNRESET should classify as reset (Windows compatibility)")
	}
}

// bytesReader returns an io.Reader over b without importing bytes in many spots.
func bytesReader(b []byte) io.Reader { return strings.NewReader(string(b)) }

func TestCopyWithDeadlines(t *testing.T) {
	t.Run("basic copy", func(t *testing.T) {
		srcR, srcW := net.Pipe()
		dst := &mockConn{}
		go func() {
			_, _ = srcW.Write([]byte("hello"))
			_ = srcW.Close()
		}()
		n, err := copyWithDeadlines(dst, srcR, 5*time.Second, nil)
		if err != nil || n != 5 || string(dst.bytesWritten()) != "hello" {
			t.Fatalf("n=%d err=%v written=%q", n, err, dst.bytesWritten())
		}
	})

	t.Run("idle read timeout (src) is not wrapped", func(t *testing.T) {
		p1, srcR := net.Pipe()            // nothing is ever written to p1
		defer func() { _ = p1.Close() }() // closed only after copy returns
		dst := &mockConn{}
		// Idle detection lives in idleConn now; the monitor sees no activity in
		// either direction, so the read times out as a genuine session idle.
		src := newIdleConn(srcR, 30*time.Millisecond, newActivityMonitor(), 1, "primary")
		n, err := copyWithDeadlines(dst, src, 5*time.Second, nil)
		if n != 0 || !isTimeoutErr(err) {
			t.Fatalf("n=%d err=%v want read timeout", n, err)
		}
		var cwErr *clientWriteErr
		if errors.As(err, &cwErr) {
			t.Fatal("a src read timeout must NOT be wrapped as a client write error")
		}
	})

	// L2: a write failure is attributed to the client via *clientWriteErr.
	t.Run("write failure attributed to client", func(t *testing.T) {
		srcR, srcW := net.Pipe()
		defer func() { _ = srcW.Close() }()
		go func() { _, _ = srcW.Write([]byte("data")) }()
		dst := &mockConn{onWrite: func(b []byte) (int, error) {
			return 0, timeoutError{msg: "i/o timeout"}
		}}
		_, err := copyWithDeadlines(dst, srcR, 50*time.Millisecond, nil)
		var cwErr *clientWriteErr
		if !errors.As(err, &cwErr) {
			t.Fatalf("err=%v want *clientWriteErr", err)
		}
		if !isTimeoutErr(err) {
			t.Fatalf("err=%v should unwrap to a timeout", err)
		}
	})

	// Bytes delivered before a write failure must still be counted in the total
	// (bytes_out reconciliation against the client's own accounting).
	t.Run("partial delivery counted on write failure", func(t *testing.T) {
		srcR, srcW := net.Pipe()
		defer func() { _ = srcW.Close() }()
		go func() { _, _ = srcW.Write([]byte("data")) }()
		calls := 0
		dst := &mockConn{onWrite: func(b []byte) (int, error) {
			calls++
			if calls == 1 {
				return 2, nil
			}
			return 0, errors.New("client stalled")
		}}
		total, err := copyWithDeadlines(dst, srcR, time.Second, nil)
		var cwErr *clientWriteErr
		if !errors.As(err, &cwErr) {
			t.Fatalf("err=%v want *clientWriteErr", err)
		}
		if total != 2 {
			t.Fatalf("total=%d want 2 (partially delivered bytes must count)", total)
		}
	})

	// L3: the write deadline uses writeTimeout, not idleTimeout — so the write
	// side has a deadline even when idle detection is disabled (idleTimeout=0).
	t.Run("write deadline uses writeTimeout when idle disabled", func(t *testing.T) {
		srcR, srcW := net.Pipe()
		go func() {
			_, _ = srcW.Write([]byte("x"))
			_ = srcW.Close()
		}()
		dst := &mockConn{}
		before := time.Now()
		if _, err := copyWithDeadlines(dst, srcR, 200*time.Millisecond, nil); err != nil {
			t.Fatalf("err=%v", err)
		}
		d, ok := dst.firstNonZeroWriteDeadline()
		if !ok {
			t.Fatal("expected a write deadline even with idleTimeout=0")
		}
		if d.Before(before.Add(150*time.Millisecond)) || d.After(before.Add(5*time.Second)) {
			t.Fatalf("write deadline %v not ~now+200ms (must use writeTimeout)", d)
		}
	})
}

// TestMirrorWriteChunkDropsOnError covers L1: a partial write followed by an
// error must NOT be replayed (which would corrupt the mirror stream); the chunk
// is dropped, counted, and the connection torn down for a clean reconnect.
func TestMirrorWriteChunkDropsOnError(t *testing.T) {
	stats := &sessionStats{}
	mc := &mockConn{onWrite: func(b []byte) (int, error) {
		return 2, errors.New("broken pipe") // partial write then failure
	}}
	mw := &mirrorWriter{addr: "test:0", writeTimeout: time.Second, stats: stats, conn: mc}

	backoff := initialBackoff
	mw.writeChunk([]byte("hello world"), &backoff)

	if c := mc.writeCount(); c != 1 {
		t.Fatalf("writeCount=%d want 1 (no replay)", c)
	}
	if got := stats.mirrorDrops.Load(); got != 1 {
		t.Fatalf("mirrorDrops=%d want 1", got)
	}
	if mw.conn != nil {
		t.Fatal("conn should be torn down after write error")
	}
	if !mc.isClosed() {
		t.Fatal("conn should be Close()d by markDown")
	}
}

func TestMirrorWriteChunkSuccess(t *testing.T) {
	mc := &mockConn{}
	mw := &mirrorWriter{addr: "test:0", writeTimeout: time.Second, stats: &sessionStats{}, conn: mc}

	backoff := 2 * time.Second // pretend we had backed off
	mw.writeChunk([]byte("data"), &backoff)

	if got := string(mc.bytesWritten()); got != "data" {
		t.Fatalf("written %q want data", got)
	}
	if backoff != initialBackoff {
		t.Fatalf("backoff=%v want reset to %v", backoff, initialBackoff)
	}
	if mw.stats.mirrorDrops.Load() != 0 {
		t.Fatal("no drops expected on success")
	}
}

// TestMirrorInterruptUnblocksWrite covers M3 mechanics: interrupt() force-closes
// the conn so a blocked writeAll returns immediately.
func TestMirrorInterruptUnblocksWrite(t *testing.T) {
	server, client := net.Pipe() // synchronous: Write with no reader blocks
	defer func() { _ = server.Close() }()
	mw := &mirrorWriter{addr: "test:0", writeTimeout: 10 * time.Second, conn: client}
	mw.connPtr.Store(&client)

	writeErr := make(chan error, 1)
	go func() {
		_, err := writeAll(client, []byte("blocked"), 10*time.Second)
		writeErr <- err
	}()

	time.Sleep(20 * time.Millisecond) // let the write block
	mw.interrupt()

	select {
	case err := <-writeErr:
		if err == nil {
			t.Fatal("expected blocked write to fail after interrupt")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("interrupt did not unblock the write")
	}
}

// TestMirrorStopInterruptsStuckWrite covers M3 end-to-end: stop() must unblock a
// mirror goroutine stuck in an in-flight write well before writeTimeout (30s
// here), so session/shutdown teardown isn't delayed.
func TestMirrorStopInterruptsStuckWrite(t *testing.T) {
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	ln := startLoopback(t, func(c net.Conn) {
		defer func() { _ = c.Close() }()
		<-done // accept but never read -> sender's writes eventually block
	})

	var wg sync.WaitGroup
	mw := newMirrorWriter(ln.Addr().String(), &wg, time.Second, 30*time.Second, 4, 1, &sessionStats{})

	big := make([]byte, 16<<20) // 16MB exceeds socket buffers -> writeAll blocks
	mw.send(big)
	mw.send(big)
	time.Sleep(150 * time.Millisecond) // let run() dial and block in writeAll

	start := time.Now()
	mw.stop()
	if !waitGroupDone(&wg, 3*time.Second) {
		t.Fatal("mirror goroutine did not exit after stop (interrupt failed)")
	}
	if d := time.Since(start); d > 1*time.Second {
		t.Fatalf("stop took %v; expected near-immediate despite 30s writeTimeout", d)
	}
}

// TestHandleConnPrimaryGoneClientSilent covers M4 under the half-close
// contract (issue #3): when the primary closes and the client is silent, the
// session must still tear down promptly even with idle timeout disabled — with
// half-close support the bound is -halfclosetimeout (reason
// half_close_timeout); with it disabled (0) teardown is immediate and
// attributed to the primary (primary_eof). Never session_idle.
func TestHandleConnPrimaryGoneClientSilent(t *testing.T) {
	cases := []struct {
		name       string
		halfClose  time.Duration
		wantReason string
	}{
		{"half-close bound", 100 * time.Millisecond, reasonHalfCloseIdle},
		{"half-close disabled", 0, reasonPrimaryEOF},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Primary accepts then immediately closes -> proxy sees primary EOF.
			primaryLn := startLoopback(t, func(c net.Conn) { _ = c.Close() })

			// Real client/proxy conn pair; `client` stays silent (never writes/closes).
			client, in := tcpConnPair(t)
			_ = client

			cfg := testSessionConfig(primaryLn.Addr().String())
			cfg.halfCloseTimeout = tc.halfClose // idleTimeout stays 0
			logs := captureLogs(t, func() {
				done := make(chan struct{})
				go func() {
					defer close(done)
					handleConnection(in, cfg, nil)
				}()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("handleConnection did not return: fanOut still blocked on a silent client (M4 regression)")
				}
			})

			if strings.Contains(logs, "reason="+reasonSessionIdle) {
				t.Errorf("end reason must not be session_idle when the primary closed:\n%s", logs)
			}
			if !strings.Contains(logs, "reason="+tc.wantReason) {
				t.Errorf("expected reason=%s:\n%s", tc.wantReason, logs)
			}
		})
	}
}

// TestHandleConnPrimaryHalfCloseThenUpload is the issue #3 integration test:
// the primary announces READY, closes its write side and keeps reading; the
// client sees READY + EOF and only then uploads. A transparent TCP proxy must
// deliver that upload — TCP EOF is directional.
func TestHandleConnPrimaryHalfCloseThenUpload(t *testing.T) {
	upload := bytes.Repeat([]byte("upload-"), 64*1024) // ~448 KiB: spans many chunks
	gotUpload := make(chan []byte, 1)
	primaryLn := startLoopback(t, func(c net.Conn) {
		defer func() { _ = c.Close() }()
		if _, err := c.Write([]byte("READY")); err != nil {
			return
		}
		_ = c.(*net.TCPConn).CloseWrite() // done talking, still listening
		data, _ := io.ReadAll(c)
		gotUpload <- data
	})
	client, in := tcpConnPair(t)

	cfg := testSessionConfig(primaryLn.Addr().String())
	cfg.halfCloseTimeout = 5 * time.Second
	done := make(chan struct{})
	logs := captureLogs(t, func() {
		go func() {
			defer close(done)
			handleConnection(in, cfg, nil)
		}()

		// Client: read READY and the relayed EOF first.
		_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
		banner, err := io.ReadAll(client)
		if err != nil || string(banner) != "READY" {
			t.Fatalf("client banner: %q err=%v (primary half-close not relayed?)", banner, err)
		}
		// Then upload — through the proxy this used to arrive empty.
		if _, err := client.Write(upload); err != nil {
			t.Fatalf("upload write after primary EOF failed: %v (client read was unblocked)", err)
		}
		_ = client.Close()

		select {
		case data := <-gotUpload:
			if !bytes.Equal(data, upload) {
				t.Fatalf("primary received %d upload bytes, want %d", len(data), len(upload))
			}
		case <-time.After(5 * time.Second):
			t.Fatal("primary never received the upload")
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("session did not end after both peers closed")
		}
	})
	// The primary closed first, then the client finished cleanly: attributed
	// to the primary, not to a timeout.
	if !strings.Contains(logs, "reason="+reasonPrimaryEOF) {
		t.Errorf("want reason=primary_eof:\n%s", logs)
	}
}

// TestHandleConnClientEOFPrimarySilent: the mirror-image half-close bound.
// After a clean client EOF the primary may still answer, but a primary that
// neither answers nor closes must not hold the session open forever when idle
// detection is disabled (pre-#3 this hung until the idle timeout, or forever
// with -idletimeout 0).
func TestHandleConnClientEOFPrimarySilent(t *testing.T) {
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	primaryLn := startLoopback(t, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c) // consume until client EOF ...
		<-hold                        // ... then stay open and silent
		_ = c.Close()
	})
	client, in := tcpConnPair(t)

	cfg := testSessionConfig(primaryLn.Addr().String())
	cfg.halfCloseTimeout = 100 * time.Millisecond
	logs := captureLogs(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			handleConnection(in, cfg, nil)
		}()
		if _, err := client.Write([]byte("REQ")); err != nil {
			t.Fatal(err)
		}
		_ = client.(*net.TCPConn).CloseWrite()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("session did not end: silent half-closed primary held it open")
		}
	})
	if !strings.Contains(logs, "reason="+reasonHalfCloseIdle) {
		t.Errorf("want reason=half_close_timeout:\n%s", logs)
	}
}

// TestHandleConnClientEOFLateResponse: a late primary response after the
// client's half-close is still delivered within the half-close window (the
// timeout is an idle bound, not an absolute one).
func TestHandleConnClientEOFLateResponse(t *testing.T) {
	primaryLn := startLoopback(t, func(c net.Conn) {
		defer func() { _ = c.Close() }()
		_, _ = io.Copy(io.Discard, c) // wait for client EOF
		time.Sleep(150 * time.Millisecond)
		_, _ = c.Write([]byte("LATE"))
	})
	client, in := tcpConnPair(t)

	cfg := testSessionConfig(primaryLn.Addr().String())
	cfg.halfCloseTimeout = 2 * time.Second
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConnection(in, cfg, nil)
	}()
	if _, err := client.Write([]byte("REQ")); err != nil {
		t.Fatal(err)
	}
	_ = client.(*net.TCPConn).CloseWrite()
	_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := io.ReadAll(client)
	if err != nil || string(resp) != "LATE" {
		t.Fatalf("late response: %q err=%v", resp, err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("session did not end")
	}
}

// TestAcceptLoopBackoff covers M1: a persistent accept error backs off (capped,
// non-decreasing) instead of busy-spinning, and never invokes handle.
func TestAcceptLoopBackoff(t *testing.T) {
	errFake := errors.New("temporary accept failure")
	var acceptCalls int
	accept := func() (net.Conn, error) {
		acceptCalls++
		if acceptCalls <= 3 {
			return nil, errFake
		}
		return nil, net.ErrClosed // shutdown after 3 failures
	}
	var handled int
	handle := func(net.Conn, func()) { handled++ }
	var sleeps []time.Duration
	sleep := func(d time.Duration) { sleeps = append(sleeps, d) }

	acceptLoop(accept, handle, sleep, 0) // returns when accept yields ErrClosed

	if handled != 0 {
		t.Fatalf("handle called %d times, want 0", handled)
	}
	if len(sleeps) != 3 {
		t.Fatalf("got %d sleeps (%v), want 3", len(sleeps), sleeps)
	}
	if sleeps[0] != acceptInitialBackoff {
		t.Fatalf("first backoff=%v want %v", sleeps[0], acceptInitialBackoff)
	}
	for i := 1; i < len(sleeps); i++ {
		if sleeps[i] < sleeps[i-1] {
			t.Fatalf("backoff decreased: %v", sleeps)
		}
		if sleeps[i] > acceptMaxBackoff {
			t.Fatalf("backoff %v exceeded cap %v", sleeps[i], acceptMaxBackoff)
		}
	}
}

// TestAcceptLoopSessionAccounting covers M2: activeSessions is incremented
// before the handler is spawned (so a Wait() after the loop exits sees the
// in-flight session) and is not released until the handler returns.
func TestAcceptLoopSessionAccounting(t *testing.T) {
	var i int
	accept := func() (net.Conn, error) {
		if i == 0 {
			i++
			return &mockConn{}, nil
		}
		return nil, net.ErrClosed
	}
	release := make(chan struct{})
	handleStarted := make(chan struct{})
	handle := func(net.Conn, func()) {
		close(handleStarted)
		<-release
	}

	loopDone := make(chan struct{})
	go func() { acceptLoop(accept, handle, func(time.Duration) {}, 0); close(loopDone) }()

	<-handleStarted // session in flight
	<-loopDone      // accept loop has exited (mirrors main's <-accepting)

	waitReturned := make(chan struct{})
	go func() { activeSessions.Wait(); close(waitReturned) }()
	select {
	case <-waitReturned:
		t.Fatal("activeSessions.Wait() returned while a session was still active (M2 race)")
	case <-time.After(100 * time.Millisecond):
		// good: Wait is correctly blocked
	}

	close(release) // let the handler finish -> Done
	select {
	case <-waitReturned:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("activeSessions.Wait() did not return after the session completed")
	}
}

// TestAcceptLoopMaxConns covers M5: at most maxConns sessions run concurrently;
// an over-limit connection is closed immediately (not handled), and a freed slot
// is reused.
func TestAcceptLoopMaxConns(t *testing.T) {
	feed := make(chan net.Conn)
	accept := func() (net.Conn, error) {
		if c, ok := <-feed; ok {
			return c, nil
		}
		return nil, net.ErrClosed
	}
	release := make(chan struct{})
	var handled, completed int32
	handle := func(net.Conn, func()) {
		atomic.AddInt32(&handled, 1)
		<-release
		atomic.AddInt32(&completed, 1)
	}

	loopDone := make(chan struct{})
	go func() { acceptLoop(accept, handle, func(time.Duration) {}, 2); close(loopDone) }()

	c1, c2, c3, c4 := &mockConn{}, &mockConn{}, &mockConn{}, &mockConn{}

	feed <- c1
	feed <- c2
	eventually(t, time.Second, func() bool { return atomic.LoadInt32(&handled) == 2 }, "first two connections should be handled")

	// Third connection exceeds the cap: rejected and closed, not handled.
	feed <- c3
	eventually(t, time.Second, func() bool { return c3.isClosed() }, "over-limit connection should be closed")
	if got := atomic.LoadInt32(&handled); got != 2 {
		t.Fatalf("handled=%d, over-limit conn must not be handled", got)
	}

	// Free both slots, then a new connection should be accepted (slot reused).
	close(release)
	eventually(t, time.Second, func() bool { return atomic.LoadInt32(&completed) == 2 }, "released handlers should complete")
	feed <- c4
	eventually(t, time.Second, func() bool { return atomic.LoadInt32(&handled) == 3 }, "a freed slot should accept a new connection")

	close(feed) // accept -> ErrClosed -> loop exits
	<-loopDone
	eventually(t, time.Second, func() bool { return atomic.LoadInt32(&completed) == 3 }, "all handlers should complete")
}

// TestAcceptLoopShutdownSmoke is a -race smoke of the real acceptLoop ->
// handleConnection wiring: several clients round-trip through the proxy, then on
// listener close the loop exits and all sessions drain (no leaked Adds).
func TestAcceptLoopShutdownSmoke(t *testing.T) {
	primaryLn := startLoopback(t, func(c net.Conn) {
		defer func() { _ = c.Close() }()
		_, _ = io.Copy(c, c) // echo
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	cfg := testSessionConfig(primaryLn.Addr().String())
	cfg.idleTimeout = 2 * time.Second
	handle := func(c net.Conn, release func()) {
		handleConnection(c, cfg, release)
	}
	accepting := make(chan struct{})
	go func() { defer close(accepting); acceptLoop(l.Accept, handle, time.Sleep, 0) }()

	var clients []net.Conn
	for i := 0; i < 5; i++ {
		c, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		clients = append(clients, c)
		if _, err := c.Write([]byte("hi")); err != nil {
			t.Fatalf("write: %v", err)
		}
		buf := make([]byte, 2)
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "hi" {
			t.Fatalf("echo through proxy: err=%v got=%q", err, buf)
		}
	}

	for _, c := range clients {
		_ = c.Close()
	}
	_ = l.Close() // unblocks Accept -> ErrClosed -> loop returns
	<-accepting

	if !waitGroupDone(&activeSessions, 3*time.Second) {
		t.Fatal("activeSessions did not drain after shutdown")
	}
}

// ---------------------------------------------------------------------------
// B-suite: fanOut / fanOutLines behavior.
// ---------------------------------------------------------------------------

func TestFanOutRelaysToPrimary(t *testing.T) {
	srcR, srcW := net.Pipe()
	primary := &mockConn{}
	stats := &sessionStats{}
	go func() {
		_, _ = srcW.Write([]byte("hello"))
		_ = srcW.Close()
	}()

	fanOut(srcR, primary, nil, 5*time.Second, 1, stats)

	if got := string(primary.bytesWritten()); got != "hello" {
		t.Fatalf("primary got %q want hello", got)
	}
	if stats.bytesIn.Load() != 5 {
		t.Fatalf("bytesIn=%d want 5", stats.bytesIn.Load())
	}
	if stats.getEndReason() != reasonClientEOF {
		t.Fatalf("reason=%s want client_eof", stats.getEndReason())
	}
}

func TestFanOutPrimaryWriteErrorStops(t *testing.T) {
	srcR, srcW := net.Pipe()
	primary := &mockConn{onWrite: func(b []byte) (int, error) {
		return 0, errors.New("primary down")
	}}
	stats := &sessionStats{}
	go func() {
		_, _ = srcW.Write([]byte("data"))
		_ = srcW.Close()
	}()

	fanOut(srcR, primary, nil, 5*time.Second, 1, stats)

	if stats.getEndReason() != reasonPrimaryWriteErr {
		t.Fatalf("reason=%s want primary_write_error", stats.getEndReason())
	}
}

// TestFanOutSlowMirrorDoesNotBlockPrimary verifies that a stalled mirror must
// not slow the primary path; excess data is dropped (and counted). Also
// exercises M3 (stop() interrupts the mirror's stuck write so teardown is
// prompt).
func TestFanOutSlowMirrorDoesNotBlockPrimary(t *testing.T) {
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	mirrorLn := startLoopback(t, func(c net.Conn) {
		defer func() { _ = c.Close() }()
		<-hold // never read -> mirror buffers fill, sends get dropped
	})

	stats := &sessionStats{}
	var mwg sync.WaitGroup
	mw := newMirrorWriter(mirrorLn.Addr().String(), &mwg, time.Second, 5*time.Second, 4, 1, stats)
	mirrors := []*mirrorWriter{mw}

	primary := &mockConn{} // fast discard
	srcR, srcW := net.Pipe()

	const chunks = 200
	go func() {
		chunk := make([]byte, copyBufSize)
		for i := 0; i < chunks; i++ {
			if _, err := srcW.Write(chunk); err != nil {
				break
			}
		}
		_ = srcW.Close()
	}()

	start := time.Now()
	fanOut(srcR, primary, mirrors, 5*time.Second, 1, stats)
	elapsed := time.Since(start)

	for _, m := range mirrors {
		m.stop()
	}
	if !waitGroupDone(&mwg, 2*time.Second) {
		t.Fatal("mirror goroutine did not stop promptly (M3)")
	}

	if elapsed > 3*time.Second {
		t.Fatalf("primary path was blocked by the slow mirror: took %v", elapsed)
	}
	if stats.mirrorDrops.Load() == 0 {
		t.Fatal("expected mirror drops with a stalled mirror")
	}
	if got, want := stats.bytesIn.Load(), uint64(chunks*copyBufSize); got != want {
		t.Fatalf("bytesIn=%d want %d", got, want)
	}
}

func TestFanOutLinesPartialPolicies(t *testing.T) {
	// "a\r\n" is a complete frame; "bb" is a partial frame at EOF.
	const input = "a\r\nbb"

	cases := []struct {
		policy      string
		wantPrimary string
		wantFrames  uint64
		wantReason  string
	}{
		{"drop", "a\r\n", 1, reasonClientEOF},
		{"forward", "a\r\nbb", 2, reasonClientEOF},
		{"error", "a\r\n", 1, reasonPartialFrame},
	}
	for _, tc := range cases {
		t.Run(tc.policy, func(t *testing.T) {
			srcR, srcW := net.Pipe()
			primary := &mockConn{}
			stats := &sessionStats{}
			go func() {
				_, _ = srcW.Write([]byte(input))
				_ = srcW.Close()
			}()

			fanOutLines(srcR, primary, nil, []byte("\r\n"), 0, defaultMaxLineLen, 5*time.Second, tc.policy, 1, stats)

			if got := string(primary.bytesWritten()); got != tc.wantPrimary {
				t.Fatalf("primary=%q want %q", got, tc.wantPrimary)
			}
			if got := stats.framesIn.Load(); got != tc.wantFrames {
				t.Fatalf("framesIn=%d want %d", got, tc.wantFrames)
			}
			if got := stats.getEndReason(); got != tc.wantReason {
				t.Fatalf("reason=%s want %s", got, tc.wantReason)
			}
		})
	}
}

func TestFanOutLinesFrameTooLarge(t *testing.T) {
	srcR, srcW := net.Pipe()
	primary := &mockConn{}
	stats := &sessionStats{}
	go func() {
		_, _ = srcW.Write([]byte(strings.Repeat("x", 100))) // no delimiter, exceeds maxframe
		_ = srcW.Close()
	}()

	fanOutLines(srcR, primary, nil, []byte("\r\n"), 0, 10, 5*time.Second, "drop", 1, stats)

	if stats.getEndReason() != reasonFrameTooLarge {
		t.Fatalf("reason=%s want frame_too_large", stats.getEndReason())
	}
	if len(primary.bytesWritten()) != 0 {
		t.Fatalf("oversized frame must not be forwarded, primary got %q", primary.bytesWritten())
	}
	if got := stats.bytesIn.Load(); got != 11 {
		t.Fatalf("bytesIn=%d want 11 (oversized-frame bytes were read off the wire)", got)
	}
}

// ---------------------------------------------------------------------------
// C-suite: session-wide idle detection (idleConn) and mirror draining.
// ---------------------------------------------------------------------------

// TestIdleConnSessionWideIdle covers the asymmetric-traffic case (e.g.
// FrontelGI: client heartbeats, server stays silent): activity on the *other*
// direction must keep a silent read alive; the timeout may only surface once
// the whole session has been idle for idleTimeout.
func TestIdleConnSessionWideIdle(t *testing.T) {
	p1, p2 := net.Pipe()
	defer func() { _ = p1.Close() }()
	defer func() { _ = p2.Close() }()

	activity := newActivityMonitor()
	ic := newIdleConn(p2, 60*time.Millisecond, activity, 1, "primary")

	// Simulate other-direction traffic: touch the shared monitor every 20ms
	// for ~300ms, then go silent. p1 never writes, so this read side sees
	// nothing but deadline wakeups the whole time.
	activeUntil := time.Now().Add(300 * time.Millisecond)
	stopTouch := make(chan struct{})
	go func() {
		defer close(stopTouch)
		for time.Now().Before(activeUntil) {
			activity.touch()
			time.Sleep(20 * time.Millisecond)
		}
	}()

	buf := make([]byte, 16)
	n, err := ic.Read(buf)
	if n != 0 || !isTimeoutErr(err) {
		t.Fatalf("n=%d err=%v, want a timeout with no data", n, err)
	}
	if time.Now().Before(activeUntil) {
		t.Fatal("read timed out while the session was still active (per-direction idle regression)")
	}
	<-stopTouch
}

// TestIdleConnGenuineIdle: with no activity anywhere the read times out after
// roughly idleTimeout.
func TestIdleConnGenuineIdle(t *testing.T) {
	p1, p2 := net.Pipe()
	defer func() { _ = p1.Close() }()
	defer func() { _ = p2.Close() }()

	ic := newIdleConn(p2, 50*time.Millisecond, newActivityMonitor(), 1, "client")
	start := time.Now()
	_, err := ic.Read(make([]byte, 8))
	if !isTimeoutErr(err) {
		t.Fatalf("err=%v want timeout", err)
	}
	if d := time.Since(start); d < 40*time.Millisecond || d > 2*time.Second {
		t.Fatalf("idle fired after %v, want ~50ms", d)
	}
}

// TestIdleConnUnblock: unblock() forces a blocked read to surface promptly as
// errReadUnblocked, which classifies as a self-inflicted close (silent
// teardown), and is not retried even though the session looks active.
func TestIdleConnUnblock(t *testing.T) {
	p1, p2 := net.Pipe()
	defer func() { _ = p1.Close() }()
	defer func() { _ = p2.Close() }()

	activity := newActivityMonitor()
	ic := newIdleConn(p2, time.Hour, activity, 1, "client")
	errCh := make(chan error, 1)
	go func() {
		_, err := ic.Read(make([]byte, 8))
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond) // let the read block
	activity.touch()                  // session looks active: a plain timeout would be retried
	ic.unblock()

	select {
	case err := <-errCh:
		if !errors.Is(err, errReadUnblocked) {
			t.Fatalf("err=%v want errReadUnblocked", err)
		}
		if !isClosedErr(err) {
			t.Fatal("errReadUnblocked must classify as a self-inflicted close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unblock did not unblock the read")
	}
}

// TestHandleConnSilentPrimaryActiveClient is the FrontelGI regression: a
// primary that never speaks (no ACKs, like an AS) must not get the session
// killed while the client is actively sending heartbeats. Also verifies the
// debug receive logging on the client side.
func TestHandleConnSilentPrimaryActiveClient(t *testing.T) {
	primaryLn := startLoopback(t, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c) // consume but never respond
		_ = c.Close()
	})
	client, in := tcpConnPair(t)

	cfg := testSessionConfig(primaryLn.Addr().String())
	cfg.idleTimeout = 80 * time.Millisecond
	done := make(chan struct{})
	logs := captureLogs(t, func() {
		go func() {
			defer close(done)
			handleConnection(in, cfg, nil)
		}()
		// Heartbeat every 20ms for ~400ms — five times the idle timeout. The
		// primary direction is silent throughout.
		for i := 0; i < 20; i++ {
			if _, err := client.Write([]byte("TEST\x03")); err != nil {
				t.Fatalf("heartbeat %d failed: %v (session killed while client active?)", i, err)
			}
			time.Sleep(20 * time.Millisecond)
			select {
			case <-done:
				t.Fatal("session ended while the client was active (per-direction idle regression)")
			default:
			}
		}
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("session did not end after client close")
		}
	})

	if strings.Contains(logs, "reason="+reasonSessionIdle) {
		t.Errorf("session must not end as idle while one direction is active:\n%s", logs)
	}
	if !strings.Contains(logs, "reason="+reasonClientEOF) {
		t.Errorf("want reason=client_eof:\n%s", logs)
	}
	if !strings.Contains(logs, `msg="data received"`) || !strings.Contains(logs, "from=client") {
		t.Errorf("expected debug receive logging for client data:\n%s", logs)
	}
}

// TestHandleConnSessionIdleBothDirections: when *neither* direction has
// traffic, the session ends as session_idle after idleTimeout.
func TestHandleConnSessionIdleBothDirections(t *testing.T) {
	primaryLn := startLoopback(t, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c)
		_ = c.Close()
	})
	client, in := tcpConnPair(t)
	_ = client // stays silent

	cfg := testSessionConfig(primaryLn.Addr().String())
	cfg.idleTimeout = 60 * time.Millisecond
	logs := captureLogs(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			handleConnection(in, cfg, nil)
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("session did not end on a fully idle session")
		}
	})

	if !strings.Contains(logs, "reason="+reasonSessionIdle) {
		t.Errorf("want reason=session_idle:\n%s", logs)
	}
}

// TestMirrorDrainFlushesQueued covers the session-teardown drain: frames still
// queued when the session ends must reach the mirror instead of being
// discarded (the truncation bug observed at the customer site).
func TestMirrorDrainFlushesQueued(t *testing.T) {
	var mu sync.Mutex
	var got []byte
	connClosed := make(chan struct{})
	mirrorLn := startLoopback(t, func(c net.Conn) {
		defer close(connClosed)
		defer func() { _ = c.Close() }()
		buf := make([]byte, 4096)
		for {
			n, err := c.Read(buf)
			mu.Lock()
			got = append(got, buf[:n]...)
			mu.Unlock()
			if err != nil {
				return
			}
		}
	})

	stats := &sessionStats{}
	var wg sync.WaitGroup
	mw := newMirrorWriter(mirrorLn.Addr().String(), &wg, time.Second, 5*time.Second, 16, 1, stats)

	want := ""
	for i := 0; i < 5; i++ {
		frame := fmt.Sprintf("frame-%d\x03", i)
		want += frame
		mw.send([]byte(frame))
	}
	mw.beginDrain() // session is over; queued frames must still flush

	if !waitGroupDone(&wg, 3*time.Second) {
		t.Fatal("mirror writer did not exit after drain")
	}
	select {
	case <-connClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("mirror connection was not closed after drain")
	}

	mu.Lock()
	defer mu.Unlock()
	if string(got) != want {
		t.Fatalf("mirror got %q want %q", got, want)
	}
	if d := stats.mirrorDrops.Load(); d != 0 {
		t.Fatalf("mirrorDrops=%d want 0 (everything flushed)", d)
	}
}

// TestMirrorForceStopCountsDrops: chunks abandoned by a force-stop are counted
// in mirror_drops instead of vanishing silently.
func TestMirrorForceStopCountsDrops(t *testing.T) {
	stats := &sessionStats{}
	mw := &mirrorWriter{
		addr:         "127.0.0.1:1", // never dialed: run() observes done first
		ch:           make(chan []byte, 8),
		done:         make(chan struct{}),
		connTimeout:  time.Second,
		writeTimeout: 5 * time.Second,
		sid:          1,
		stats:        stats,
	}
	mw.send([]byte("a"))
	mw.send([]byte("b"))
	mw.send([]byte("c"))
	mw.stop() // force-stop before run() ever gets to write

	var wg sync.WaitGroup
	wg.Add(1)
	go mw.run(&wg)
	if !waitGroupDone(&wg, 2*time.Second) {
		t.Fatal("run did not exit after stop")
	}
	if got := stats.mirrorDrops.Load(); got != 3 {
		t.Fatalf("mirrorDrops=%d want 3 (abandoned chunks must be counted)", got)
	}
}

// TestHandleConnMirrorGetsBurstBeforeClose is the end-to-end customer
// scenario: a client bursts several frames and disconnects immediately; the
// mirror must still receive every frame (pre-drain it often received none).
func TestHandleConnMirrorGetsBurstBeforeClose(t *testing.T) {
	var mu sync.Mutex
	var got []byte
	connClosed := make(chan struct{})
	mirrorLn := startLoopback(t, func(c net.Conn) {
		defer close(connClosed)
		defer func() { _ = c.Close() }()
		buf := make([]byte, 4096)
		for {
			n, err := c.Read(buf)
			mu.Lock()
			got = append(got, buf[:n]...)
			mu.Unlock()
			if err != nil {
				return
			}
		}
	})
	primaryLn := startLoopback(t, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c)
		_ = c.Close()
	})

	client, in := tcpConnPair(t)

	cfg := testSessionConfig(primaryLn.Addr().String())
	cfg.mirrorAddrs = []string{mirrorLn.Addr().String()}
	cfg.lineMode = true
	cfg.delim = []byte{0x03}
	cfg.mirrorBufSize = 16
	cfg.mirrorDrain = 2 * time.Second
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConnection(in, cfg, nil)
	}()

	burst := ""
	for i := 1; i <= 5; i++ {
		burst += fmt.Sprintf("EVENT %d\x03", i)
	}
	if _, err := client.Write([]byte(burst)); err != nil {
		t.Fatalf("burst write: %v", err)
	}
	_ = client.Close() // disconnect immediately, like a short-lived session

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleConnection did not return")
	}
	select {
	case <-connClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("mirror connection was not closed")
	}

	mu.Lock()
	defer mu.Unlock()
	if string(got) != burst {
		t.Fatalf("mirror got %q want the full burst %q", got, burst)
	}
}

// TestIdleConnRealTimeoutSurfaces: a timeout-flavored error that is NOT a
// deadline expiry (e.g. ETIMEDOUT from TCP keepalive detecting a dead peer)
// must surface to the caller immediately — not be retried as a stale idle
// deadline just because the other direction was recently active.
func TestIdleConnRealTimeoutSurfaces(t *testing.T) {
	reads := 0
	mc := &mockConn{onRead: func(b []byte) (int, error) {
		reads++
		return 0, timeoutError{msg: "connection timed out"} // Timeout()==true, not a deadline
	}}
	activity := newActivityMonitor()
	activity.touch() // session looks active: a deadline timeout WOULD be retried
	ic := newIdleConn(mc, time.Hour, activity, 1, "primary")

	_, err := ic.Read(make([]byte, 8))
	if !isTimeoutErr(err) {
		t.Fatalf("err=%v want the original timeout error", err)
	}
	if isDeadlineErr(err) {
		t.Fatalf("test bug: fake error must not look like a deadline expiry")
	}
	if reads != 1 {
		t.Fatalf("reads=%d want 1 (a real peer failure must not be retried)", reads)
	}
}

// TestCloseAllConnsShutdownReason: force-close must attribute sessions to
// shutdown (not client_eof/session_idle) and must not race a concurrent
// registerPrimary (run with -race).
func TestCloseAllConnsShutdownReason(t *testing.T) {
	const sid = uint64(1 << 40) // out of the way of real session IDs
	stats := &sessionStats{}
	c1, c2 := &mockConn{}, &mockConn{}
	registerConn(sid, c1, stats)
	defer unregisterConn(sid)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		registerPrimary(sid, c2) // races closeAllConns' snapshot under -race
	}()
	closeAllConns()
	wg.Wait()

	if got := stats.getEndReason(); got != reasonShutdown {
		t.Fatalf("reason=%q want %q", got, reasonShutdown)
	}
	if !c1.isClosed() {
		t.Fatal("client conn must be force-closed")
	}
}

// TestMirrorDialFailDropCounted: a chunk dropped because the mirror cannot be
// dialed must be counted in mirror_drops (it used to vanish silently).
func TestMirrorDialFailDropCounted(t *testing.T) {
	stats := &sessionStats{}
	mw := &mirrorWriter{
		addr:         "127.0.0.1:1", // nothing listens here; dial fails fast
		ch:           make(chan []byte, 4),
		done:         make(chan struct{}),
		connTimeout:  500 * time.Millisecond,
		writeTimeout: 5 * time.Second,
		sid:          1,
		stats:        stats,
	}
	backoff := initialBackoff
	mw.writeChunk([]byte("frame"), &backoff)
	if got := stats.mirrorDrops.Load(); got != 1 {
		t.Fatalf("mirrorDrops=%d want 1 (dial-failure drops must be counted)", got)
	}
	if got := mw.drops.Load(); got != 1 {
		t.Fatalf("per-mirror drops=%d want 1", got)
	}
}

// TestFanOutLinesPartialNotForwardedOnError: the -partial policy applies only
// to a clean client EOF; a fragment interrupted by a session-idle (or any
// other error) must never be forwarded, even with -partial forward.
func TestFanOutLinesPartialNotForwardedOnError(t *testing.T) {
	srcR, srcW := net.Pipe()
	defer func() { _ = srcW.Close() }()
	defer func() { _ = srcR.Close() }()
	ic := newIdleConn(srcR, 60*time.Millisecond, newActivityMonitor(), 1, "client")
	go func() { _, _ = srcW.Write([]byte("PARTIAL")) }() // no delimiter, then silence

	primary := &mockConn{}
	stats := &sessionStats{}
	fanOutLines(ic, primary, nil, []byte("\r\n"), 0, defaultMaxLineLen, 5*time.Second, "forward", 1, stats)

	if got := primary.bytesWritten(); len(got) != 0 {
		t.Fatalf("fragment must not be forwarded on a mid-frame idle: %q", got)
	}
	if got := stats.getEndReason(); got != reasonSessionIdle {
		t.Fatalf("reason=%q want %q", got, reasonSessionIdle)
	}
}

// TestStatsAttrs smoke-checks the aggregate stats line content.
func TestStatsAttrs(t *testing.T) {
	aggBytesIn.Add(1)
	mirrorDropCounter("statstest:1").Store(7)

	attrs := statsAttrs()
	var keys []string
	for i := 0; i < len(attrs); i += 2 {
		keys = append(keys, attrs[i].(string))
	}
	joined := strings.Join(keys, " ")
	for _, want := range []string{"active_sessions", "total_sessions", "bytes_in", "bytes_out", "mirror_drops", "drops_statstest:1"} {
		if !strings.Contains(joined, want) {
			t.Errorf("stats attrs missing %q: %v", want, joined)
		}
	}
}

// TestReopenableWriter covers the SIGHUP log-rotation plumbing: after the file
// is moved aside, Reopen() must start a fresh file at the original path.
func TestReopenableWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")
	w, err := newReopenableWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	if _, err := w.Write([]byte("before\n")); err != nil {
		t.Fatal(err)
	}
	rotated := filepath.Join(dir, "test.log.1")
	if err := os.Rename(path, rotated); err != nil {
		t.Fatal(err)
	}
	if err := w.Reopen(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("after\n")); err != nil {
		t.Fatal(err)
	}

	old, _ := os.ReadFile(rotated)
	cur, _ := os.ReadFile(path)
	if string(old) != "before\n" {
		t.Fatalf("rotated file content %q", old)
	}
	if string(cur) != "after\n" {
		t.Fatalf("reopened file content %q", cur)
	}
}

// TestBuildLogHandler covers the -logfile/-logformat plumbing: the handler
// writes to the given writer, honors the level, and emits the chosen format.
func TestBuildLogHandler(t *testing.T) {
	var buf bytes.Buffer
	slog.New(buildLogHandler(&buf, "debug", "json")).Debug("hello", "k", "v")
	if !strings.Contains(buf.String(), `"msg":"hello"`) {
		t.Fatalf("json output missing msg: %q", buf.String())
	}

	buf.Reset()
	slog.New(buildLogHandler(&buf, "info", "text")).Debug("hidden")
	if buf.Len() != 0 {
		t.Fatalf("debug must be suppressed at info level: %q", buf.String())
	}

	buf.Reset()
	slog.New(buildLogHandler(&buf, "error", "text")).Info("hidden")
	if buf.Len() != 0 {
		t.Fatalf("info must be suppressed at error level: %q", buf.String())
	}
}

// ---------------------------------------------------------------------------
// D-suite: GitHub issues #1–#5 (mirror response draining, admission slot vs.
// mirror cleanup, half-close, live stats, full-queue drop cost).
// ---------------------------------------------------------------------------

// sinkConn is a mockConn that records nothing (no write copies, no deadline
// history), for allocation-sensitive tests and benchmarks where mockConn's
// bookkeeping would dominate.
type sinkConn struct{ mockConn }

func (s *sinkConn) Write(b []byte) (int, error)      { return len(b), nil }
func (s *sinkConn) SetWriteDeadline(time.Time) error { return nil }
func (s *sinkConn) SetReadDeadline(time.Time) error  { return nil }

// fullQueueMirror returns a mirrorWriter whose queue is already full and that
// has no run() goroutine, so every send() takes the buffer-full drop path.
func fullQueueMirror() *mirrorWriter {
	mw := &mirrorWriter{addr: "full:0", ch: make(chan []byte, 1), done: make(chan struct{}), stats: &sessionStats{}}
	mw.send([]byte("x")) // occupies the single slot
	return mw
}

// TestMirrorSendFullQueueNoAlloc (issue #5): dropping because the mirror queue
// is full must not allocate or copy the payload — that work would land on the
// client→primary forwarding goroutine exactly when the mirror is overloaded.
func TestMirrorSendFullQueueNoAlloc(t *testing.T) {
	mw := fullQueueMirror()
	data := make([]byte, copyBufSize)
	before := mw.stats.mirrorDrops.Load()
	allocs := testing.AllocsPerRun(1000, func() { mw.send(data) })
	if allocs != 0 {
		t.Fatalf("full-queue send allocates %.1f objects/op, want 0", allocs)
	}
	if got := mw.stats.mirrorDrops.Load() - before; got != 1001 { // 1 warm-up + 1000 runs
		t.Fatalf("drops=%d want 1001 (every full-queue send must still be counted)", got)
	}
}

// TestMirrorSendNonFullQueueEnqueues guards the fast-path check in send():
// a queue observed non-full must enqueue a private copy (the caller's buffer
// is reused), never drop.
func TestMirrorSendNonFullQueueEnqueues(t *testing.T) {
	mw := &mirrorWriter{addr: "q:0", ch: make(chan []byte, 2), done: make(chan struct{}), stats: &sessionStats{}}
	buf := []byte("first")
	mw.send(buf)
	copy(buf, "XXXXX") // caller reuses its buffer
	mw.send([]byte("second"))
	mw.send([]byte("third")) // full: dropped
	if got := mw.stats.mirrorDrops.Load(); got != 1 {
		t.Fatalf("drops=%d want 1", got)
	}
	if got := string(<-mw.ch); got != "first" {
		t.Fatalf("queued %q want an independent copy of \"first\"", got)
	}
	if got := string(<-mw.ch); got != "second" {
		t.Fatalf("queued %q want second", got)
	}
}

// BenchmarkMirrorSendFullQueue (issue #5) tracks the cost of the buffer-full
// drop path; expect 0 B/op and 0 allocs/op.
func BenchmarkMirrorSendFullQueue(b *testing.B) {
	mw := fullQueueMirror()
	data := make([]byte, copyBufSize)
	b.ReportAllocs()
	b.SetBytes(copyBufSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mw.send(data)
	}
}

// chunkSource returns a mockConn that yields n full read buffers and then EOF.
func chunkSource(n int) *mockConn {
	i := 0
	return &mockConn{onRead: func(b []byte) (int, error) {
		if i >= n {
			return 0, io.EOF
		}
		i++
		return len(b), nil
	}}
}

// TestFanOutStalledMirrorNoPerChunkAllocs (issue #5, sustained overload):
// with a mirror that drops every chunk, the primary forwarding path must do no
// per-chunk work proportional to the discarded payload — allocations must not
// grow with the number of chunks.
func TestFanOutStalledMirrorNoPerChunkAllocs(t *testing.T) {
	run := func(chunks int) float64 {
		return testing.AllocsPerRun(20, func() {
			mw := fullQueueMirror()
			fanOut(chunkSource(chunks), &sinkConn{}, []*mirrorWriter{mw}, 5*time.Second, 1, &sessionStats{})
		})
	}
	one, many := run(1), run(500)
	if many-one > 1 {
		t.Fatalf("allocs grew with chunk count on a stalled mirror: 1 chunk=%.1f, 500 chunks=%.1f", one, many)
	}
}

// BenchmarkFanOutStalledMirror vs BenchmarkFanOutNoMirror (issue #5): the
// per-chunk cost of forwarding with a mirror that drops everything should be
// close to forwarding with no mirror at all.
func BenchmarkFanOutStalledMirror(b *testing.B) {
	mw := fullQueueMirror()
	src := chunkSource(b.N)
	b.ReportAllocs()
	b.SetBytes(copyBufSize)
	b.ResetTimer()
	fanOut(src, &sinkConn{}, []*mirrorWriter{mw}, 5*time.Second, 1, &sessionStats{})
}

func BenchmarkFanOutNoMirror(b *testing.B) {
	src := chunkSource(b.N)
	b.ReportAllocs()
	b.SetBytes(copyBufSize)
	b.ResetTimer()
	fanOut(src, &sinkConn{}, nil, 5*time.Second, 1, &sessionStats{})
}

// TestAggregateStatsLiveDuringSession (issue #4): the process byte counters
// behind the periodic stats line must grow while a long-lived session is still
// open, and the final totals must equal the traffic exactly (no double count
// when the session closes).
func TestAggregateStatsLiveDuringSession(t *testing.T) {
	primaryLn := startLoopback(t, func(c net.Conn) {
		defer func() { _ = c.Close() }()
		_, _ = io.Copy(c, c) // echo
	})
	client, in := tcpConnPair(t)

	inBefore, outBefore := aggBytesIn.Load(), aggBytesOut.Load()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConnection(in, testSessionConfig(primaryLn.Addr().String()), nil)
	}()

	const n = 4096
	if _, err := client.Write(make([]byte, n)); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(client, make([]byte, n)); err != nil {
		t.Fatalf("echo: %v", err)
	}
	// Still connected: the aggregates must already reflect the traffic.
	eventually(t, 2*time.Second, func() bool {
		return aggBytesIn.Load()-inBefore >= n && aggBytesOut.Load()-outBefore >= n
	}, "aggregate byte counters did not grow while the session was open")

	_ = client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("session did not end")
	}
	if got := aggBytesIn.Load() - inBefore; got != n {
		t.Fatalf("bytes_in delta=%d want %d (double counted at session end?)", got, n)
	}
	if got := aggBytesOut.Load() - outBefore; got != n {
		t.Fatalf("bytes_out delta=%d want %d (double counted at session end?)", got, n)
	}
}

// stalledMirror starts a mirror that accepts connections and never reads them
// (released on test cleanup), so the mirror writer ends the session with a
// full queue and a blocked in-flight write — a drain that cannot finish early.
func stalledMirror(t *testing.T) net.Listener {
	t.Helper()
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	return startLoopback(t, func(c net.Conn) {
		defer func() { _ = c.Close() }()
		<-hold
	})
}

// bigUpload is large enough to exceed any loopback socket buffer, so a mirror
// that never reads leaves the writer blocked with data still queued.
var bigUpload = make([]byte, 16<<20)

// TestMaxConnsNotHeldByMirrorDrain (issue #2): with -maxconns 1, a session
// whose client/primary traffic is complete but whose mirror is still draining
// must not block the next client.
func TestMaxConnsNotHeldByMirrorDrain(t *testing.T) {
	primaryLn := startLoopback(t, func(c net.Conn) {
		defer func() { _ = c.Close() }()
		_, _ = io.Copy(io.Discard, c) // consume the upload until client EOF
		_, _ = c.Write([]byte("OK"))
	})
	mirrorLn := stalledMirror(t)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := testSessionConfig(primaryLn.Addr().String())
	cfg.mirrorAddrs = []string{mirrorLn.Addr().String()}
	cfg.mirrorBufSize = 1024 // queue holds the whole upload: the drain has real work
	cfg.mirrorDrain = 2 * time.Second
	accepting := make(chan struct{})
	go func() {
		defer close(accepting)
		acceptLoop(l.Accept, func(c net.Conn, release func()) { handleConnection(c, cfg, release) }, time.Sleep, 1)
	}()

	roundTrip := func(payload []byte) bool {
		c, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Logf("dial: %v", err)
			return false
		}
		defer func() { _ = c.Close() }()
		if _, err := c.Write(payload); err != nil {
			t.Logf("write: %v", err)
			return false
		}
		_ = c.(*net.TCPConn).CloseWrite()
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		resp, err := io.ReadAll(c)
		if err != nil || string(resp) != "OK" {
			t.Logf("response %q err=%v", resp, err)
			return false
		}
		return true
	}

	if !roundTrip(bigUpload) {
		t.Fatal("first session failed")
	}
	// Session 1's mirror is now draining against a stalled mirror. A second
	// client must be admitted well before -mirrordrain expires.
	eventually(t, time.Second, func() bool { return roundTrip([]byte("second")) },
		"second client was rejected: the -maxconns slot is still held by mirror cleanup")

	_ = l.Close()
	<-accepting
	if !waitGroupDone(&activeSessions, 6*time.Second) {
		t.Fatal("sessions did not finish after the drain window")
	}
}

// TestMirrorDrainBudgetShedsMirrorWork (issue #2): when the drain budget is
// exhausted, a finishing session drops its queued mirror data (counted) and
// returns immediately instead of waiting out -mirrordrain.
func TestMirrorDrainBudgetShedsMirrorWork(t *testing.T) {
	setDrainBudget(1)
	t.Cleanup(func() { setDrainBudget(0) })
	*drainSlots.Load() <- struct{}{} // budget fully occupied by "another" session

	primaryLn := startLoopback(t, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c)
		_ = c.Close()
	})
	mirrorLn := stalledMirror(t)
	client, in := tcpConnPair(t)

	cfg := testSessionConfig(primaryLn.Addr().String())
	cfg.mirrorAddrs = []string{mirrorLn.Addr().String()}
	cfg.mirrorBufSize = 1024
	cfg.mirrorDrain = 5 * time.Second
	done := make(chan struct{})
	logs := captureLogs(t, func() {
		go func() {
			defer close(done)
			handleConnection(in, cfg, nil)
		}()
		if _, err := client.Write(bigUpload); err != nil {
			t.Fatal(err)
		}
		_ = client.Close()
		start := time.Now()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("handleConnection waited for the drain despite an exhausted drain budget")
		}
		if d := time.Since(start); d > time.Second {
			t.Fatalf("teardown took %v; want immediate shedding", d)
		}
	})
	if !strings.Contains(logs, "mirror_drops=") || strings.Contains(logs, "mirror_drops=0 ") || strings.HasSuffix(strings.TrimSpace(logs), "mirror_drops=0") {
		t.Errorf("shed mirror data must be counted in mirror_drops:\n%s", logs)
	}
}

// TestMirrorStopCancelsDial (issue #2): stop() must abort an in-flight mirror
// dial (DNS/connect) promptly instead of letting the session wait out -timeout.
func TestMirrorStopCancelsDial(t *testing.T) {
	var wg sync.WaitGroup
	mw := newMirrorWriter("slow.invalid:1", &wg, 30*time.Second, 5*time.Second, 4, 1, &sessionStats{})
	dialing := make(chan struct{})
	mw.dial = func(ctx context.Context, addr string) (net.Conn, error) {
		close(dialing)
		<-ctx.Done() // a connect that only ends when cancelled
		return nil, ctx.Err()
	}
	mw.send([]byte("x")) // triggers the dial
	select {
	case <-dialing:
	case <-time.After(2 * time.Second):
		t.Fatal("dial never started")
	}

	start := time.Now()
	mw.stop()
	if !waitGroupDone(&wg, 2*time.Second) {
		t.Fatal("mirror goroutine did not exit: in-flight dial was not cancelled by stop()")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("stop took %v with a 30s dial timeout; want prompt cancellation", d)
	}
	if got := mw.stats.mirrorDrops.Load(); got != 1 {
		t.Fatalf("drops=%d want 1 (the undelivered chunk)", got)
	}
}

// TestMirrorResponsesDiscarded (issue #1): a request/response mirror that
// answers every request with more than a socket buffer's worth of data must
// keep receiving later requests. Without a discard reader its response write
// blocks, it stops reading, and the mirrored stream stalls.
func TestMirrorResponsesDiscarded(t *testing.T) {
	var received atomic.Int64
	response := make([]byte, 16<<20)
	mirrorLn := startLoopback(t, func(c net.Conn) {
		defer func() { _ = c.Close() }()
		one := make([]byte, 1)
		for {
			if _, err := io.ReadFull(c, one); err != nil {
				return
			}
			received.Add(1)
			if _, err := c.Write(response); err != nil { // blocks unless someone reads
				return
			}
		}
	})

	var wg sync.WaitGroup
	mw := newMirrorWriter(mirrorLn.Addr().String(), &wg, time.Second, 5*time.Second, 16, 1, &sessionStats{})
	const requests = 4
	for i := 1; i <= requests; i++ {
		mw.send([]byte{'r'})
		eventually(t, 5*time.Second, func() bool { return received.Load() >= int64(i) },
			fmt.Sprintf("mirror never received request %d: its response write is stuck (responses not drained)", i))
	}
	mw.beginDrain()
	if !waitGroupDone(&wg, 3*time.Second) {
		t.Fatal("mirror writer (and its response reader) did not exit after drain")
	}
	if got := mw.stats.mirrorDrops.Load(); got != 0 {
		t.Fatalf("drops=%d want 0", got)
	}
}

// TestMirrorStaleReaderDoesNotAffectReconnect (issue #1): a response reader
// from a previous mirror connection that exits only after a new connection is
// already established must not close or clear the new connection.
func TestMirrorStaleReaderDoesNotAffectReconnect(t *testing.T) {
	release := make(chan struct{})
	old := &mockConn{onRead: func(b []byte) (int, error) {
		<-release // the old reader lingers until we say so
		return 0, io.EOF
	}}
	next := &mockConn{}
	var oldConn net.Conn = old
	mw := &mirrorWriter{addr: "test:0", writeTimeout: time.Second, stats: &sessionStats{}, done: make(chan struct{}), conn: old}
	mw.connPtr.Store(&oldConn)
	mw.startDiscardReader(old)

	// The old connection fails and the writer reconnects to `next`.
	mw.markDown()
	mw.dial = func(context.Context, string) (net.Conn, error) { return next, nil }
	backoff := initialBackoff
	mw.writeChunk([]byte("data"), &backoff)
	if mw.conn != next {
		t.Fatal("writer did not reconnect to the new connection")
	}

	// Now the stale reader exits — after the reconnect.
	close(release)
	mw.readers.Wait()

	if mw.conn != next {
		t.Fatal("stale reader cleared the reconnected connection")
	}
	if next.isClosed() {
		t.Fatal("stale reader closed the reconnected connection")
	}
	if p := mw.connPtr.Load(); p == nil || *p != net.Conn(next) {
		t.Fatal("connPtr no longer points at the reconnected connection")
	}
	if got := string(next.bytesWritten()); got != "data" {
		t.Fatalf("new connection got %q want data", got)
	}
	if !old.isClosed() {
		t.Fatal("old connection should have been closed by markDown")
	}
}

// TestIdleConnEnterHalfClose (issue #3) unit-tests the half-close bound on
// idleConn: with idle detection off, a blocked Read returns
// errHalfCloseTimeout after the half-close timeout, and data arriving within
// the window is delivered and refreshes the bound.
func TestIdleConnEnterHalfClose(t *testing.T) {
	srcR, srcW := net.Pipe()
	defer func() { _ = srcW.Close() }()
	defer func() { _ = srcR.Close() }()
	ic := newIdleConn(srcR, 0, newActivityMonitor(), 1, "client") // idle disabled

	result := make(chan error, 1)
	go func() {
		buf := make([]byte, 8)
		for {
			if _, err := ic.Read(buf); err != nil {
				result <- err
				return
			}
		}
	}()
	time.Sleep(20 * time.Millisecond) // Read is blocked with no deadline
	ic.enterHalfClose(120 * time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	if _, err := srcW.Write([]byte("more")); err != nil { // within the window: refreshes it
		t.Fatal(err)
	}
	select {
	case err := <-result:
		t.Fatalf("read ended early with %v; data within the window must keep the direction open", err)
	case <-time.After(90 * time.Millisecond):
	}
	select {
	case err := <-result:
		if !errors.Is(err, errHalfCloseTimeout) {
			t.Fatalf("err=%v want errHalfCloseTimeout", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("half-close bound never fired on a blocked read")
	}
}
