package cloudpump

// increaseTLSBufferSizeUnsafely pre-allocates tls.Conn's internal rawInput
// buffer to tlsRawInputBufSize bytes, so the TLS layer can issue large read(2)
// syscalls instead of reading one 16 KiB TLS record at a time.
//
// Without this, even a transport with ReadBufferSize=256 KiB still triggers
// tiny kernel reads because rawInput caps each syscall at its current spare
// capacity (~16-32 KiB after the first record).  See golang/go#47672.
//
// This is fragile: it reaches into an unexported field by name via reflect.
// If the Go runtime ever renames or removes tls.Conn.rawInput, the FieldByName
// call returns a zero Value and UnsafeAddr panics.  A test in tls_rawbuf_test.go
// verifies the field exists at the correct type on every Go upgrade.
//
// Remove once golang/go#47672 is resolved and a stable API exists.

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"reflect"
	"unsafe"
)

const tlsRawInputBufSize = 256 << 10 // 256 KiB — matches dd-source blob library

// dialTLSWithLargeBuffer is an http.Transport.DialTLSContext replacement that
// pre-sizes rawInput before the TLS handshake so all subsequent reads use a
// large kernel buffer.
func dialTLSWithLargeBuffer(ctx context.Context, network, addr string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	tcpConn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(tcpConn, &tls.Config{ServerName: host})
	increaseTLSBufferSizeUnsafely(tlsConn)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		tcpConn.Close()
		return nil, err
	}
	return tlsConn, nil
}

func increaseTLSBufferSizeUnsafely(tlsConn *tls.Conn) {
	member := reflect.Indirect(reflect.ValueOf(tlsConn)).FieldByName("rawInput")
	ptr := (*bytes.Buffer)(unsafe.Pointer(member.UnsafeAddr()))
	*ptr = *bytes.NewBuffer(make([]byte, 0, tlsRawInputBufSize))
}
