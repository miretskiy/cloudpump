// export_test.go exposes internal types for white-box testing.
// Compiled only during `go test`; invisible to library consumers.
package cloudpump

import (
	"net"
	"time"
)

// WrapWithReadDeadline wraps conn with the same deadlineConn used by the
// engine's dialer. Exported for use in package cloudpump_test.
func WrapWithReadDeadline(conn net.Conn, d time.Duration) net.Conn {
	return &deadlineConn{Conn: conn, d: d}
}
