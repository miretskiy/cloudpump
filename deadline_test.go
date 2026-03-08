package cloudpump_test

// Tests for deadlineConn using net.Pipe() and synctest.
//
// net.Pipe() implements its deadline via time.AfterFunc internally using
// channels, so it is "durably blocking" inside a synctest bubble and the
// fake clock can advance past the deadline without any real-time wait.
//
// Both net.Pipe() connections must be created inside the bubble so their
// internal channel machinery is bubble-associated.

import (
	"errors"
	"net"
	"testing"
	"testing/synctest"
	"time"

	cloudpump "github.com/miretskiy/cloudpump"
)

// TestDeadlineConn_Timeout verifies that deadlineConn resets the per-Read
// deadline before every call. Three consecutive reads on a server that never
// writes must each time out individually.
//
// If deadlineConn only set the deadline once, the second and third reads would
// have a stale (already-expired) deadline; under synctest their behaviour would
// be implementation-defined rather than consistently timing out. The test is
// therefore a precise probe of the per-call reset property.
func TestDeadlineConn_Timeout(t *testing.T) {
	const d = 5 * time.Millisecond

	synctest.Test(t, func(t *testing.T) {
		server, client := net.Pipe() // must be inside the bubble
		defer server.Close()

		dc := cloudpump.WrapWithReadDeadline(client, d)
		defer dc.Close()

		buf := make([]byte, 1)
		for i := range 3 {
			_, err := dc.Read(buf)
			var netErr net.Error
			if !errors.As(err, &netErr) || !netErr.Timeout() {
				t.Errorf("read %d: expected timeout error, got %v", i, err)
				return
			}
		}
	})
}

// TestDeadlineConn_DataBeforeDeadline verifies that deadlineConn does not
// interfere with reads that complete before the deadline fires.
func TestDeadlineConn_DataBeforeDeadline(t *testing.T) {
	const d = 5 * time.Millisecond

	synctest.Test(t, func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()

		dc := cloudpump.WrapWithReadDeadline(client, d)
		defer dc.Close()

		payload := []byte("hello")
		go func() { _, _ = server.Write(payload) }()

		buf := make([]byte, len(payload))
		n, err := dc.Read(buf)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		if n != len(payload) || string(buf[:n]) != string(payload) {
			t.Errorf("got %q, want %q", buf[:n], payload)
		}
	})
}
