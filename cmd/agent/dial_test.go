package main

import (
	"crypto/tls"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestServerNameOfKeepsHost(t *testing.T) {
	if got := serverNameOf("grpc.example.invalid:5555"); got != "grpc.example.invalid" {
		t.Fatalf("host = %q", got)
	}
	if got := serverNameOf("[2001:db8::10]:5555"); got != "2001:db8::10" {
		t.Fatalf("ipv6 = %q", got)
	}
}

func TestNewClientRequiresAuthorityMatchServerName(t *testing.T) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "grpc.example.invalid"}
	creds := grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))
	_, err := grpc.NewClient("192.0.2.10:5555", creds, grpc.WithAuthority("grpc.example.invalid:5555"))
	if err == nil || !strings.Contains(err.Error(), "don't match") {
		t.Fatalf("port mismatch should fail: %v", err)
	}
	conn, err := grpc.NewClient("192.0.2.10:5555", creds, grpc.WithAuthority("grpc.example.invalid"))
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}
