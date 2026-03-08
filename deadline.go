package cloudpump

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/miretskiy/dio/netbuf"
)

// deadlineConn wraps a net.Conn and resets the read deadline before every
// Read call. This bounds how long the engine can block on a stalled
// connection — one that accepted TCP and returned the response header but
// then stopped delivering body bytes — to readDeadline rather than waiting
// for an OS-level keepalive timeout (typically minutes).
//
// A network timeout from Read surfaces as a net.Error with Timeout() == true,
// which DefaultIsRetryable classifies as retryable, so the chunk worker will
// attempt to reconnect from the point of partial progress.
type deadlineConn struct {
	net.Conn
	d time.Duration
}

func (c *deadlineConn) Read(p []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.d)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

// dialContext returns a DialContext hook for http.Transport.
// When readDeadline > 0 it wraps the connection with a deadlineConn so that
// plain-HTTP connections (used in tests and any non-TLS endpoint) also benefit
// from per-read deadline enforcement.
func dialContext(readDeadline time.Duration) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if readDeadline > 0 {
			return &deadlineConn{Conn: conn, d: readDeadline}, nil
		}
		return conn, nil
	}
}

// dialTLSContext returns a DialTLSContext hook for http.Transport that
// combines two concerns in a single dial:
//
//  1. TLS rawInput pre-sizing (golang/go#47672): tls.Conn.rawInput starts at
//     zero capacity and grows one 16 KiB TLS record at a time, so even a
//     transport with a large ReadBufferSize issues many small read(2) syscalls.
//     Pre-sizing to netbuf.TLSBufSize lets the kernel fill ~16 records per
//     syscall. This supersedes the netbuf.DialTLSContext hook.
//
//  2. Per-read deadline (optional): when readDeadline > 0 each Read on the
//     returned connection resets a deadline, detecting stalls mid-body.
//     When readDeadline == 0, the returned connection is a plain *tls.Conn
//     with the larger rawInput; behaviour is identical to netbuf.DialTLSContext.
func dialTLSContext(readDeadline time.Duration) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		tcpConn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		tlsConn := tls.Client(tcpConn, &tls.Config{ServerName: host})
		netbuf.GrowTLSReadBuf(tlsConn)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			tcpConn.Close()
			return nil, err
		}
		if readDeadline > 0 {
			return &deadlineConn{Conn: tlsConn, d: readDeadline}, nil
		}
		return tlsConn, nil
	}
}
