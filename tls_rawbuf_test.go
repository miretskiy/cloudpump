package cloudpump

import (
	"bytes"
	"crypto/tls"
	"reflect"
	"testing"
)

// TestTLSRawInputFieldExists guards against Go runtime changes that rename or
// remove tls.Conn.rawInput, which would cause increaseTLSBufferSizeUnsafely
// to panic at runtime rather than fail at test time.
func TestTLSRawInputFieldExists(t *testing.T) {
	conn := &tls.Conn{}
	f := reflect.Indirect(reflect.ValueOf(conn)).FieldByName("rawInput")
	if !f.IsValid() {
		t.Fatal("tls.Conn.rawInput field not found: increaseTLSBufferSizeUnsafely will panic")
	}
	if f.Type() != reflect.TypeOf(bytes.Buffer{}) {
		t.Fatalf("tls.Conn.rawInput has type %v, want bytes.Buffer", f.Type())
	}
}
