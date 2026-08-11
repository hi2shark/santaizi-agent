package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hi2shark/santaizi-agent/model"
	pb "github.com/hi2shark/santaizi-agent/proto"
)

func TestExecuteHTTPProbeAgainstLocalServer(t *testing.T) {
	original := agentConfig.Capabilities
	agentConfig.Capabilities = model.DefaultCapabilities()
	t.Cleanup(func() { agentConfig.Capabilities = original })

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	result := executeProbe(&pb.ProbeRequest{
		ProbeId: "http-local",
		Target:  &pb.ProbeRequest_Http{Http: &pb.HTTPProbeRequest{Url: server.URL, TimeoutMs: 2000}},
	})
	if !result.GetSuccessful() || result.GetKind() != pb.ProbeKind_PROBE_KIND_HTTP || result.GetHttp().GetStatusCode() != http.StatusNoContent {
		t.Fatalf("HTTP probe result=%#v", result)
	}
}

func TestExecuteProbeRejectsDisabledCapability(t *testing.T) {
	original := agentConfig.Capabilities
	agentConfig.Capabilities = model.DefaultCapabilities()
	agentConfig.Capabilities.HTTPProbe = false
	t.Cleanup(func() { agentConfig.Capabilities = original })

	result := executeProbe(&pb.ProbeRequest{
		ProbeId: "disabled",
		Target:  &pb.ProbeRequest_Http{Http: &pb.HTTPProbeRequest{Url: "http://127.0.0.1"}},
	})
	if result.GetSuccessful() || result.GetError() == "" {
		t.Fatalf("disabled probe result=%#v", result)
	}
}

func TestExecuteTCPProbeAgainstLocalListener(t *testing.T) {
	original := agentConfig.Capabilities
	agentConfig.Capabilities = model.DefaultCapabilities()
	t.Cleanup(func() { agentConfig.Capabilities = original })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = connection.Close()
		}
	}()
	port := uint32(listener.Addr().(*net.TCPAddr).Port)
	result := executeProbe(&pb.ProbeRequest{
		ProbeId: "tcp-local",
		Target:  &pb.ProbeRequest_Tcp{Tcp: &pb.TCPProbeRequest{Host: "127.0.0.1", Port: port, TimeoutMs: 2000}},
	})
	if !result.GetSuccessful() || result.GetTcp().GetResolvedIp() != "127.0.0.1" {
		t.Fatalf("TCP probe result=%#v", result)
	}
}
