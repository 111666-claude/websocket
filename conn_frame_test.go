// Copyright 2026 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package websocket

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// rawFrame builds a wire-format frame. When mask is true the payload is
// masked with key (client-to-server direction).
func rawFrame(fin bool, frameType int, mask bool, key [4]byte, payload []byte) []byte {
	b0 := byte(frameType)
	if fin {
		b0 |= finalBit
	}
	b1 := byte(0)
	if mask {
		b1 |= maskBit
	}
	var buf []byte
	n := len(payload)
	switch {
	case n > 65535:
		buf = append(buf, b0, b1|127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(buf[2:], uint64(n))
	case n > 125:
		buf = append(buf, b0, b1|126, 0, 0)
		binary.BigEndian.PutUint16(buf[2:], uint16(n))
	default:
		buf = append(buf, b0, b1|byte(n))
	}
	if mask {
		buf = append(buf, key[:]...)
		p := append([]byte(nil), payload...)
		maskBytes(key, 0, p)
		return append(buf, p...)
	}
	return append(buf, payload...)
}

// rawFrameExt builds a frame forcing the 16-bit extended length encoding,
// used to send control frames with len > 125.
func rawFrameExt(fin bool, frameType int, mask bool, key [4]byte, payload []byte) []byte {
	b0 := byte(frameType)
	if fin {
		b0 |= finalBit
	}
	b1 := byte(126)
	if mask {
		b1 |= maskBit
	}
	buf := append([]byte{b0, b1, 0, 0}, 0, 0)
	binary.BigEndian.PutUint16(buf[2:], uint16(len(payload)))
	if mask {
		buf = append(buf, key[:]...)
		p := append([]byte(nil), payload...)
		maskBytes(key, 0, p)
		return append(buf, p...)
	}
	return append(buf, payload...)
}

// pipeConns returns a client and server *Conn joined by net.Pipe, plus the
// underlying pipe ends. The conns are closed when the test finishes.
func pipeConns(t *testing.T) (client, server *Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	client = newConn(c1, false, 4096, 4096, nil, nil, nil)
	server = newConn(c2, true, 4096, 4096, nil, nil, nil)
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

// serverRawConn returns a server *Conn reading wire bytes from the returned
// net.Conn's peer. Anything the server writes (close frames, pongs) can be
// read from the returned raw end.
func serverRawConn(t *testing.T) (raw net.Conn, server *Conn) {
	t.Helper()
	raw, srv := net.Pipe()
	server = newConn(srv, true, 4096, 4096, nil, nil, nil)
	t.Cleanup(func() {
		raw.Close()
		server.Close()
	})
	return raw, server
}

var frameTestMaskKey = [4]byte{0xde, 0xad, 0xbe, 0xef}

// clientFrame builds a masked client-to-server frame.
func clientFrame(fin bool, frameType int, payload []byte) []byte {
	return rawFrame(fin, frameType, true, frameTestMaskKey, payload)
}

// TestFrameControlInterleavedWithFragments verifies that ping/pong control
// frames arriving between fragments of a data message are processed
// independently, do not corrupt the fragmented message and do not leak bytes
// from the following message into the current one.
func TestFrameControlInterleavedWithFragments(t *testing.T) {
	raw, server := serverRawConn(t)

	pingPayload := "ping-between-fragments"
	var wire []byte
	wire = append(wire, clientFrame(false, TextMessage, []byte("Hello, "))...)
	wire = append(wire, clientFrame(true, PingMessage, []byte(pingPayload))...)
	wire = append(wire, clientFrame(false, continuationFrame, []byte("big "))...)
	wire = append(wire, clientFrame(true, PongMessage, []byte("unsolicited pong"))...)
	wire = append(wire, clientFrame(true, continuationFrame, []byte("world"))...)
	wire = append(wire, clientFrame(true, TextMessage, []byte("NEXT"))...)

	replyCh := exchange(raw, wire)

	_, message, err := server.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage fragmented: %v", err)
	}
	if want := "Hello, big world"; string(message) != want {
		t.Fatalf("fragmented message = %q, want %q", message, want)
	}

	// The next message must be delivered intact: no bytes concatenated into
	// the previous message, none lost.
	_, message, err = server.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage next: %v", err)
	}
	if string(message) != "NEXT" {
		t.Fatalf("next message = %q, want %q", message, "NEXT")
	}

	// The default ping handler must have answered the interleaved ping with
	// a pong carrying the same payload.
	reply := recvFrame(t, replyCh)
	if reply.typ != PongMessage {
		t.Fatalf("reply frame type = %d, want PongMessage", reply.typ)
	}
	if string(reply.payload) != pingPayload {
		t.Fatalf("pong payload = %q, want %q", reply.payload, pingPayload)
	}
}

// rawFrameResult is a parsed wire frame received from a Conn under test.
type rawFrameResult struct {
	fin     bool
	typ     int
	payload []byte
}

// exchange writes wire to raw in a goroutine and returns a channel that
// receives the first frame the peer (the Conn under test) writes back. The
// peer's write (a close frame or pong) happens synchronously inside its read
// path, so the raw end must read concurrently to avoid deadlock.
func exchange(raw net.Conn, wire []byte) <-chan rawFrameResult {
	ch := make(chan rawFrameResult, 1)
	go func() {
		if _, err := raw.Write(wire); err != nil {
			ch <- rawFrameResult{typ: -1}
			return
		}
		fin, typ, payload := readRawFrameSafe(raw)
		ch <- rawFrameResult{fin, typ, payload}
	}()
	return ch
}

// recvFrame waits for the peer's reply frame.
func recvFrame(t *testing.T, ch <-chan rawFrameResult) rawFrameResult {
	t.Helper()
	select {
	case f := <-ch:
		if f.typ < 0 {
			t.Fatal("raw end failed to write/read")
		}
		return f
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for peer frame")
		return rawFrameResult{}
	}
}

// readRawFrameSafe is readRawFrame without t.Helper fatals, for use in
// goroutines where a failed read (closed pipe) just ends the goroutine.
func readRawFrameSafe(r io.Reader) (bool, int, []byte) {
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return false, -1, nil
	}
	fin := hdr[0]&finalBit != 0
	frameType := int(hdr[0] & 0xf)
	n := int64(hdr[1] & 0x7f)
	switch n {
	case 126:
		b := make([]byte, 2)
		if _, err := io.ReadFull(r, b); err != nil {
			return false, -1, nil
		}
		n = int64(binary.BigEndian.Uint16(b))
	case 127:
		b := make([]byte, 8)
		if _, err := io.ReadFull(r, b); err != nil {
			return false, -1, nil
		}
		n = int64(binary.BigEndian.Uint64(b))
	}
	var key [4]byte
	if hdr[1]&maskBit != 0 {
		if _, err := io.ReadFull(r, key[:]); err != nil {
			return false, -1, nil
		}
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return false, -1, nil
	}
	if hdr[1]&maskBit != 0 {
		maskBytes(key, 0, payload)
	}
	return fin, frameType, payload
}

// TestFrameLargeMessageWithConcurrentControl writes a large (multi-fragment)
// message while a ping is written concurrently from another goroutine. The
// message must be reassembled byte-for-byte and the ping must be handled.
func TestFrameLargeMessageWithConcurrentControl(t *testing.T) {
	client, server := pipeConns(t)

	const size = 300000
	want := make([]byte, size)
	for i := range want {
		want[i] = byte(i * 31)
	}

	pingCh := make(chan string, 1)
	server.SetPingHandler(func(appData string) error {
		pingCh <- appData
		return nil // record only; default pong path covered elsewhere
	})

	type readResult struct {
		data []byte
		err  error
	}
	readCh := make(chan readResult, 1)
	go func() {
		_, data, err := server.ReadMessage()
		readCh <- readResult{data, err}
	}()

	writeErr := make(chan error, 1)
	go func() {
		writeErr <- client.WriteMessage(BinaryMessage, want)
	}()

	// Interleave a control frame while the large message is being written.
	if err := client.WriteControl(PingMessage, []byte("mid-message"), time.Now().Add(5*time.Second)); err != nil {
		t.Fatalf("WriteControl ping: %v", err)
	}

	if err := <-writeErr; err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	select {
	case got := <-pingCh:
		if got != "mid-message" {
			t.Fatalf("ping payload = %q, want %q", got, "mid-message")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ping interleaved in large message was not handled")
	}

	select {
	case res := <-readCh:
		if res.err != nil {
			t.Fatalf("ReadMessage: %v", res.err)
		}
		if !bytes.Equal(res.data, want) {
			t.Fatalf("large message corrupted: got %d bytes, equal=%v", len(res.data), bytes.Equal(res.data, want))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out reading large message")
	}
}

// TestFrameMaskRoundTrip verifies masked client frames are unmasked
// correctly, including mask position reset across fragments.
func TestFrameMaskRoundTrip(t *testing.T) {
	raw, server := serverRawConn(t)

	var wire []byte
	// Payload lengths chosen so the mask phase at fragment boundaries is
	// non-trivial.
	wire = append(wire, clientFrame(false, TextMessage, []byte("abcdefg"))...)
	wire = append(wire, clientFrame(false, continuationFrame, []byte("hi"))...)
	wire = append(wire, clientFrame(true, continuationFrame, []byte("jklmnop"))...)

	go func() { _, _ = raw.Write(wire) }()

	_, message, err := server.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if want := "abcdefghijklmnop"; string(message) != want {
		t.Fatalf("message = %q, want %q", message, want)
	}
}

// TestFrameBadMask verifies that frames with the MASK bit set incorrectly
// for the connection direction are rejected with a protocol error, that the
// peer receives a CloseProtocolError close frame, and that the read error is
// sticky.
func TestFrameBadMask(t *testing.T) {
	t.Run("server rejects unmasked", func(t *testing.T) {
		raw, server := serverRawConn(t)
		replyCh := exchange(raw, rawFrame(true, TextMessage, false, [4]byte{}, []byte("hi")))

		_, _, err := server.ReadMessage()
		if err == nil || !strings.Contains(err.Error(), "bad MASK") {
			t.Fatalf("ReadMessage err = %v, want bad MASK protocol error", err)
		}

		reply := recvFrame(t, replyCh)
		if reply.typ != CloseMessage {
			t.Fatalf("frame sent to peer = type %d, want CloseMessage", reply.typ)
		}
		if code := int(binary.BigEndian.Uint16(reply.payload)); code != CloseProtocolError {
			t.Fatalf("close code = %d, want %d", code, CloseProtocolError)
		}

		// Sticky error.
		if _, _, err2 := server.ReadMessage(); err2 != err {
			t.Fatalf("second ReadMessage err = %v, want sticky %v", err2, err)
		}
	})

	t.Run("client rejects masked", func(t *testing.T) {
		raw, srv := net.Pipe()
		client := newConn(srv, false, 4096, 4096, nil, nil, nil)
		t.Cleanup(func() { raw.Close(); client.Close() })

		replyCh := exchange(raw, rawFrame(true, TextMessage, true, frameTestMaskKey, []byte("hi")))

		_, _, err := client.ReadMessage()
		if err == nil || !strings.Contains(err.Error(), "bad MASK") {
			t.Fatalf("ReadMessage err = %v, want bad MASK protocol error", err)
		}
		if reply := recvFrame(t, replyCh); reply.typ != CloseMessage {
			t.Fatalf("frame sent to peer = type %d, want CloseMessage", reply.typ)
		}
	})
}

// TestFrameProtocolErrors verifies framing violations are rejected with
// protocol errors and a CloseProtocolError close frame to the peer.
func TestFrameProtocolErrors(t *testing.T) {
	tests := []struct {
		name string
		wire []byte
		want string
	}{
		{
			name: "continuation after FIN",
			wire: clientFrame(true, continuationFrame, []byte("x")),
			want: "continuation after FIN",
		},
		{
			name: "data before FIN",
			wire: append(clientFrame(false, TextMessage, []byte("a")),
				clientFrame(true, TextMessage, []byte("b"))...),
			want: "data before FIN",
		},
		{
			name: "bad opcode",
			wire: clientFrame(true, 3, []byte("x")),
			want: "bad opcode 3",
		},
		{
			name: "RSV1 set without negotiated compression",
			wire: func() []byte {
				f := clientFrame(true, TextMessage, []byte("x"))
				f[0] |= rsv1Bit
				return f
			}(),
			want: "RSV1 set",
		},
		{
			name: "fragmented control frame",
			wire: clientFrame(false, PingMessage, []byte("x")),
			want: "FIN not set on control",
		},
		{
			name: "control frame too long",
			wire: rawFrameExt(true, PingMessage, true, frameTestMaskKey, make([]byte, 126)),
			want: "len > 125 for control",
		},
		{
			name: "invalid close code",
			wire: clientFrame(true, CloseMessage, FormatCloseMessage(CloseAbnormalClosure, "")),
			want: "bad close code 1006",
		},
		{
			name: "invalid utf8 in close reason",
			wire: clientFrame(true, CloseMessage,
				append(FormatCloseMessage(CloseNormalClosure, ""), 0xff, 0xfe)),
			want: "invalid utf8 payload in close frame",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, server := serverRawConn(t)
			replyCh := exchange(raw, tc.wire)

			_, _, err := server.ReadMessage()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ReadMessage err = %v, want error containing %q", err, tc.want)
			}

			reply := recvFrame(t, replyCh)
			if reply.typ != CloseMessage {
				t.Fatalf("frame sent to peer = type %d, want CloseMessage", reply.typ)
			}
			if code := int(binary.BigEndian.Uint16(reply.payload)); code != CloseProtocolError {
				t.Fatalf("close code = %d, want %d", code, CloseProtocolError)
			}

			// Error must be sticky: subsequent reads return the same error.
			if _, _, err2 := server.NextReader(); err2 != err {
				t.Fatalf("second NextReader err = %v, want sticky %v", err2, err)
			}
		})
	}
}

// TestFrameTruncated verifies that a connection dropped mid-frame (header,
// extended length, mask key or payload) yields an abnormal-closure close
// error and a sticky read error.
func TestFrameTruncated(t *testing.T) {
	full := clientFrame(true, TextMessage, []byte("hello"))

	tests := []struct {
		name string
		wire []byte
	}{
		{"partial header", full[:1]},
		{"header only no payload", full[:6]},
		{"partial payload", full[:8]},
		{"partial extended length", clientFrame(true, TextMessage, make([]byte, 200))[:3]},
		{"partial mask key", clientFrame(true, TextMessage, []byte("hi"))[:4]},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, server := serverRawConn(t)
			go func() {
				_, _ = raw.Write(tc.wire)
				raw.Close() // peer goes away mid-frame
			}()

			_, _, err := server.ReadMessage()
			if !IsCloseError(err, CloseAbnormalClosure) {
				t.Fatalf("ReadMessage err = %v, want abnormal closure close error", err)
			}
			if _, _, err2 := server.ReadMessage(); err2 != err {
				t.Fatalf("second ReadMessage err = %v, want sticky %v", err2, err)
			}
		})
	}
}

// TestFrameReadLimit verifies that messages exceeding the read limit are
// rejected with ErrReadLimit and a CloseMessageTooBig close frame, both for
// single-frame and fragmented messages, and that the error is sticky.
func TestFrameReadLimit(t *testing.T) {
	t.Run("single frame", func(t *testing.T) {
		raw, server := serverRawConn(t)
		server.SetReadLimit(10)

		replyCh := exchange(raw, clientFrame(true, BinaryMessage, make([]byte, 20)))

		_, _, err := server.ReadMessage()
		if !errors.Is(err, ErrReadLimit) {
			t.Fatalf("ReadMessage err = %v, want ErrReadLimit", err)
		}
		reply := recvFrame(t, replyCh)
		if reply.typ != CloseMessage {
			t.Fatalf("frame sent to peer = type %d, want CloseMessage", reply.typ)
		}
		if code := int(binary.BigEndian.Uint16(reply.payload)); code != CloseMessageTooBig {
			t.Fatalf("close code = %d, want %d", code, CloseMessageTooBig)
		}
		if _, _, err2 := server.ReadMessage(); !errors.Is(err2, ErrReadLimit) {
			t.Fatalf("second ReadMessage err = %v, want sticky ErrReadLimit", err2)
		}
	})

	t.Run("fragmented cumulative", func(t *testing.T) {
		raw, server := serverRawConn(t)
		server.SetReadLimit(10)

		var wire []byte
		wire = append(wire, clientFrame(false, BinaryMessage, make([]byte, 8))...)
		wire = append(wire, clientFrame(true, continuationFrame, make([]byte, 8))...)
		replyCh := exchange(raw, wire)

		_, r, err := server.NextReader()
		if err != nil {
			t.Fatalf("NextReader: %v", err)
		}
		_, err = io.ReadAll(r)
		if !errors.Is(err, ErrReadLimit) {
			t.Fatalf("Read err = %v, want ErrReadLimit after cumulative limit", err)
		}
		if reply := recvFrame(t, replyCh); reply.typ != CloseMessage {
			t.Fatalf("frame sent to peer = type %d, want CloseMessage", reply.typ)
		}
	})
}

// TestFrameCloseHandshake verifies a well-formed close frame produces a
// CloseError carrying the peer's code and reason, and that the default close
// handler echoes a close frame back.
func TestFrameCloseHandshake(t *testing.T) {
	raw, server := serverRawConn(t)

	replyCh := exchange(raw, clientFrame(true, CloseMessage, FormatCloseMessage(CloseGoingAway, "bye")))

	_, _, err := server.ReadMessage()
	ce, ok := err.(*CloseError)
	if !ok {
		t.Fatalf("ReadMessage err = %v (%T), want *CloseError", err, err)
	}
	if ce.Code != CloseGoingAway || ce.Text != "bye" {
		t.Fatalf("CloseError = {%d %q}, want {%d %q}", ce.Code, ce.Text, CloseGoingAway, "bye")
	}

	reply := recvFrame(t, replyCh)
	if reply.typ != CloseMessage {
		t.Fatalf("frame sent to peer = type %d, want CloseMessage", reply.typ)
	}
	if code := int(binary.BigEndian.Uint16(reply.payload)); code != CloseGoingAway {
		t.Fatalf("echoed close code = %d, want %d", code, CloseGoingAway)
	}
}

// TestFrameReadDeadline verifies read timeouts surface as net.Error timeouts
// and are sticky.
func TestFrameReadDeadline(t *testing.T) {
	_, server := pipeConns(t)

	if err := server.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	_, _, err := server.ReadMessage()
	ne, ok := err.(net.Error)
	if !ok || !ne.Timeout() {
		t.Fatalf("ReadMessage err = %v (%T), want net.Error timeout", err, err)
	}
	if _, _, err2 := server.ReadMessage(); err2 != err {
		t.Fatalf("second ReadMessage err = %v, want sticky %v", err2, err)
	}
}

// TestFrameWriteDeadline verifies write timeouts poison the write state:
// all subsequent writes return the same error.
func TestFrameWriteDeadline(t *testing.T) {
	client, _ := pipeConns(t) // nobody reads from server end

	client.SetWriteDeadline(time.Now().Add(-time.Second))
	err := client.WriteMessage(TextMessage, []byte("x"))
	if err == nil {
		t.Fatal("WriteMessage with expired deadline succeeded")
	}
	if err2 := client.WriteMessage(TextMessage, []byte("y")); err2 != err {
		t.Fatalf("second WriteMessage err = %v, want sticky %v", err2, err)
	}
}

// TestFrameHalfClose verifies that a peer vanishing without a close frame
// yields an abnormal-closure error on read and deterministic write failures.
func TestFrameHalfClose(t *testing.T) {
	raw, server := serverRawConn(t)
	raw.Close() // abrupt close, no close frame

	_, _, err := server.ReadMessage()
	if !IsCloseError(err, CloseAbnormalClosure) {
		t.Fatalf("ReadMessage err = %v, want abnormal closure", err)
	}

	err = server.WriteMessage(TextMessage, []byte("x"))
	if err == nil {
		t.Fatal("WriteMessage after peer close succeeded")
	}
	if err2 := server.WriteMessage(TextMessage, []byte("y")); err2 != err {
		t.Fatalf("second WriteMessage err = %v, want sticky %v", err2, err)
	}
}

// TestFrameDoubleClose verifies Close is idempotent-safe: neither the first
// nor a repeated Close panics or blocks, and after Close reads and writes
// fail deterministically. Also verifies writes after a sent close message
// fail with ErrCloseSent.
func TestFrameDoubleClose(t *testing.T) {
	_, server := pipeConns(t)

	if err := server.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// A repeated Close must be safe: no panic, no deadlock. (net.Pipe's
	// Close is idempotent and returns nil; other transports may return an
	// error — both are acceptable, blocking or panicking is not.)
	_ = server.Close()

	// After Close, reads and writes fail deterministically.
	if _, _, err := server.ReadMessage(); err == nil {
		t.Fatal("ReadMessage after Close returned nil error")
	}
	if err := server.WriteMessage(TextMessage, []byte("x")); err == nil {
		t.Fatal("WriteMessage after Close returned nil error")
	}

	// Write path on a fresh connection: after sending a close message, all
	// writes fail with ErrCloseSent.
	client, server2 := pipeConns(t)
	readCh := make(chan struct{})
	go func() {
		defer close(readCh)
		_, _, _ = client.ReadMessage() // consume the close frame
	}()
	if err := server2.WriteMessage(CloseMessage, FormatCloseMessage(CloseNormalClosure, "")); err != nil {
		t.Fatalf("WriteMessage close: %v", err)
	}
	<-readCh
	if err := server2.WriteMessage(TextMessage, []byte("x")); !errors.Is(err, ErrCloseSent) {
		t.Fatalf("WriteMessage after close err = %v, want ErrCloseSent", err)
	}
	if err := server2.WriteMessage(CloseMessage, FormatCloseMessage(CloseNormalClosure, "")); !errors.Is(err, ErrCloseSent) {
		t.Fatalf("second close write err = %v, want ErrCloseSent", err)
	}
}

// TestFrameWriteControlLimits verifies WriteControl rejects non-control
// opcodes and payloads over 125 bytes without touching the connection.
func TestFrameWriteControlLimits(t *testing.T) {
	client, _ := pipeConns(t)

	if err := client.WriteControl(TextMessage, nil, time.Now().Add(time.Second)); !errors.Is(err, errBadWriteOpCode) {
		t.Fatalf("WriteControl(TextMessage) err = %v, want errBadWriteOpCode", err)
	}
	if err := client.WriteControl(PingMessage, make([]byte, maxControlFramePayloadSize+1), time.Now().Add(time.Second)); !errors.Is(err, errInvalidControlFrame) {
		t.Fatalf("WriteControl(126-byte ping) err = %v, want errInvalidControlFrame", err)
	}
	if err := client.WriteControl(PingMessage, []byte("x"), time.Now().Add(-time.Second)); !errors.Is(err, errWriteTimeout) {
		t.Fatalf("WriteControl expired deadline err = %v, want errWriteTimeout", err)
	}
}

// TestFrameConcurrentReadWrite runs a full-duplex echo exchange over
// net.Pipe: one reader, one writer and one concurrent WriteControl caller on
// each side — the concurrency pattern the package documents. Run with -race.
func TestFrameConcurrentReadWrite(t *testing.T) {
	client, server := pipeConns(t)

	const messages = 64

	// Echo server.
	serverErr := make(chan error, 1)
	go func() {
		for {
			mt, message, err := server.ReadMessage()
			if err != nil {
				serverErr <- nil // peer closed after test done
				return
			}
			if err := server.WriteMessage(mt, message); err != nil {
				serverErr <- err
				return
			}
		}
	}()

	payloads := make([][]byte, messages)
	for i := range payloads {
		p := make([]byte, 100+i*37)
		for j := range p {
			p[j] = byte(i + j)
		}
		payloads[i] = p
	}

	var wg sync.WaitGroup

	// Single writer goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, p := range payloads {
			if err := client.WriteMessage(BinaryMessage, p); err != nil {
				t.Errorf("WriteMessage: %v", err)
				return
			}
		}
	}()

	// Concurrent control writer (documented as safe alongside one writer).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < messages; i++ {
			if err := client.WriteControl(PingMessage, []byte("ping"), time.Now().Add(5*time.Second)); err != nil {
				return // connection may already be winding down
			}
		}
	}()

	// Single reader (this goroutine): every payload must come back intact
	// and in order.
	for i, want := range payloads {
		mt, got, err := client.ReadMessage()
		if err != nil {
			t.Fatalf("ReadMessage %d: %v", i, err)
		}
		if mt != BinaryMessage {
			t.Fatalf("message %d type = %d, want BinaryMessage", i, mt)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("message %d corrupted: got %d bytes, want %d", i, len(got), len(want))
		}
	}

	wg.Wait()
	client.Close()
	if err := <-serverErr; err != nil {
		t.Fatalf("echo server: %v", err)
	}
}
