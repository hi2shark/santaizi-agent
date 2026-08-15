package main

import (
	"testing"
)

func TestServerNameOfKeepsHost(t *testing.T) {
	if got := serverNameOf("grpc.example.invalid:5555"); got != "grpc.example.invalid" {
		t.Fatalf("host = %q", got)
	}
	if got := serverNameOf("[2001:db8::10]:5555"); got != "2001:db8::10" {
		t.Fatalf("ipv6 = %q", got)
	}
}
