// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 oldwired

package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
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
	defer ln.Close()
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
	defer p1.Close()
	defer p2.Close()
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
		name    string
		line    bool
		delim   []byte
		esc     byte
		partial string
		listen  string
		primary string
		mirrors []string
		maxF    int
		wantErr bool
	}{
		{name: "valid raw", partial: "drop", listen: "localhost:8080", primary: "localhost:9090", maxF: 1024},
		{name: "valid line crlf", line: true, delim: crlf, partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024},
		{name: "C1 empty delim line mode", line: true, delim: []byte{}, partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024, wantErr: true},
		{name: "empty delim raw mode ok", line: false, delim: []byte{}, partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024},
		{name: "esc needs single-byte delim", line: true, delim: crlf, esc: 0x1B, partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024, wantErr: true},
		{name: "esc with single-byte delim ok", line: true, delim: []byte{0x03}, esc: 0x1B, partial: "drop", listen: ":8080", primary: ":9090", maxF: 1024},
		{name: "bad partial", partial: "nope", listen: ":8080", primary: ":9090", maxF: 1024, wantErr: true},
		{name: "bad listen addr", partial: "drop", listen: "no-port", primary: ":9090", maxF: 1024, wantErr: true},
		{name: "bad primary addr", partial: "drop", listen: ":8080", primary: "no-port", maxF: 1024, wantErr: true},
		{name: "L5 bad mirror addr", partial: "drop", listen: ":8080", primary: ":9090", mirrors: []string{"ok:1", "bad"}, maxF: 1024, wantErr: true},
		{name: "L5 good mirrors", partial: "drop", listen: ":8080", primary: ":9090", mirrors: []string{"a:1", "b:2"}, maxF: 1024},
		{name: "non-positive maxframe", partial: "drop", listen: ":8080", primary: ":9090", maxF: 0, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateConfig(tc.line, tc.delim, tc.esc, tc.partial, tc.listen, tc.primary, tc.mirrors, tc.maxF)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateConfig err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
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

	fanOutLines(srcR, primary, nil, []byte("\r\n"), 0, defaultMaxLineLen, 5*time.Second, 0, "drop", 1, stats)

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
		"</data>": []byte("</data>"), // custom passthrough
		"":        {},                // empty passthrough (rejected later by validateConfig in line mode)
	}
	for in, want := range cases {
		if got := parseDelimiter(in); string(got) != string(want) {
			t.Errorf("parseDelimiter(%q)=%v want %v", in, got, want)
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
		if err := writeAll(mc, []byte("hello"), 0); err != nil {
			t.Fatalf("err=%v", err)
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
		if err := writeAll(mc, []byte("hello"), 0); err != nil {
			t.Fatalf("err=%v", err)
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
		if err := writeAll(mc, []byte("x"), 0); err != io.ErrShortWrite {
			t.Fatalf("err=%v want ErrShortWrite", err)
		}
	})

	t.Run("error is propagated", func(t *testing.T) {
		boom := errors.New("boom")
		mc := &mockConn{onWrite: func(b []byte) (int, error) { return 0, boom }}
		if err := writeAll(mc, []byte("x"), 0); err != boom {
			t.Fatalf("err=%v want boom", err)
		}
	})

	t.Run("sets write deadline when timeout>0", func(t *testing.T) {
		mc := &mockConn{}
		before := time.Now()
		if err := writeAll(mc, []byte("x"), 50*time.Millisecond); err != nil {
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
		_ = writeAll(mc, []byte("x"), 0)
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
		n, err := copyWithDeadlines(dst, srcR, 0, 5*time.Second)
		if err != nil || n != 5 || string(dst.bytesWritten()) != "hello" {
			t.Fatalf("n=%d err=%v written=%q", n, err, dst.bytesWritten())
		}
	})

	t.Run("idle read timeout (src) is not wrapped", func(t *testing.T) {
		p1, srcR := net.Pipe() // nothing is ever written to p1
		defer p1.Close()       // closed only after copy returns
		dst := &mockConn{}
		n, err := copyWithDeadlines(dst, srcR, 30*time.Millisecond, 5*time.Second)
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
		defer srcW.Close()
		go func() { _, _ = srcW.Write([]byte("data")) }()
		dst := &mockConn{onWrite: func(b []byte) (int, error) {
			return 0, timeoutError{msg: "i/o timeout"}
		}}
		_, err := copyWithDeadlines(dst, srcR, 0, 50*time.Millisecond)
		var cwErr *clientWriteErr
		if !errors.As(err, &cwErr) {
			t.Fatalf("err=%v want *clientWriteErr", err)
		}
		if !isTimeoutErr(err) {
			t.Fatalf("err=%v should unwrap to a timeout", err)
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
		if _, err := copyWithDeadlines(dst, srcR, 0, 200*time.Millisecond); err != nil {
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
	defer server.Close()
	mw := &mirrorWriter{addr: "test:0", writeTimeout: 10 * time.Second, conn: client}
	mw.connPtr.Store(&client)

	writeErr := make(chan error, 1)
	go func() { writeErr <- writeAll(client, []byte("blocked"), 10*time.Second) }()

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
		defer c.Close()
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

// TestHandleConnPrimaryGoneClientSilent covers M4: when the primary closes and
// the client is silent, the session must tear down promptly even with idle
// timeout disabled (pre-M4 it hung forever), and be attributed to the primary
// (primary_eof), not client_idle.
func TestHandleConnPrimaryGoneClientSilent(t *testing.T) {
	// Primary accepts then immediately closes -> proxy sees primary EOF.
	primaryLn := startLoopback(t, func(c net.Conn) { _ = c.Close() })

	// Real client/proxy conn pair; `client` stays silent (never writes/closes).
	client, in := tcpConnPair(t)
	_ = client

	logs := captureLogs(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			// idleTimeout = 0 -> without M4 this never returns.
			handleConnection(in, primaryLn.Addr().String(), nil, false, nil, 0,
				defaultMaxLineLen, "drop", time.Second, 5*time.Second, 10, 0)
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("handleConnection did not return: fanOut still blocked on a silent client (M4 regression)")
		}
	})

	if strings.Contains(logs, "reason=client_idle") {
		t.Errorf("end reason must not be client_idle when the primary closed:\n%s", logs)
	}
	if !strings.Contains(logs, "reason=primary_eof") {
		t.Errorf("expected reason=primary_eof:\n%s", logs)
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
	handle := func(net.Conn) { handled++ }
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
	handle := func(net.Conn) {
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
	handle := func(net.Conn) {
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
		defer c.Close()
		_, _ = io.Copy(c, c) // echo
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	handle := func(c net.Conn) {
		handleConnection(c, primaryLn.Addr().String(), nil, false, nil, 0,
			defaultMaxLineLen, "drop", time.Second, 5*time.Second, 10, 2*time.Second)
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
	l.Close() // unblocks Accept -> ErrClosed -> loop returns
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

	fanOut(srcR, primary, nil, 5*time.Second, 0, 1, stats)

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

	fanOut(srcR, primary, nil, 5*time.Second, 0, 1, stats)

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
		defer c.Close()
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
	fanOut(srcR, primary, mirrors, 5*time.Second, 0, 1, stats)
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

			fanOutLines(srcR, primary, nil, []byte("\r\n"), 0, defaultMaxLineLen, 5*time.Second, 0, tc.policy, 1, stats)

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

	fanOutLines(srcR, primary, nil, []byte("\r\n"), 0, 10, 5*time.Second, 0, "drop", 1, stats)

	if stats.getEndReason() != reasonFrameTooLarge {
		t.Fatalf("reason=%s want frame_too_large", stats.getEndReason())
	}
	if len(primary.bytesWritten()) != 0 {
		t.Fatalf("oversized frame must not be forwarded, primary got %q", primary.bytesWritten())
	}
}
