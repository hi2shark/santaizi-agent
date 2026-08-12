package util

import (
	"testing"
	"time"
)

func TestInterfaceLocalAddrRequiresExistingInterface(t *testing.T) {
	if _, err := InterfaceLocalAddr("santaizi-missing-iface", false); err == nil {
		t.Fatal("expected missing interface error")
	}
}

func TestNewSingleStackHTTPClientAcceptsInterfaceArg(t *testing.T) {
	client := NewSingleStackHTTPClient(time.Second, time.Second, time.Second, false, "lo")
	if client == nil {
		t.Fatal("expected client")
	}
}
