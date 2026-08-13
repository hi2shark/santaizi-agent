package main

import (
	"io"
	"testing"
	"time"

	pb "github.com/hi2shark/santaizi-agent/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetainKnownHostIP(t *testing.T) {
	if got := retainKnownHostIP(&pb.Host{Ip: "", Platform: "linux"}, "203.0.113.10").GetIp(); got != "203.0.113.10" {
		t.Fatalf("empty incoming should keep previous, got %q", got)
	}
	if got := retainKnownHostIP(&pb.Host{Ip: "198.51.100.8"}, "203.0.113.10").GetIp(); got != "198.51.100.8" {
		t.Fatalf("non-empty incoming should win, got %q", got)
	}
	if got := retainKnownHostIP(&pb.Host{Ip: ""}, "").GetIp(); got != "" {
		t.Fatalf("both empty should stay empty, got %q", got)
	}
	if retainKnownHostIP(nil, "203.0.113.10") != nil {
		t.Fatal("nil host should stay nil")
	}
}

func TestSetSinkRTTRecordsSample(t *testing.T) {
	manager := &telemetryManager{sinkRuntime: map[string]*pb.SinkRuntime{
		"primary": {EndpointId: "primary", Generation: 1},
	}}
	manager.setSinkRTT(&pb.TelemetryEndpoint{EndpointId: "primary", Generation: 1}, 12*time.Millisecond)
	got := manager.sinkRuntime["primary"]
	if got.GetLastRttMs() < 11.9 || got.GetLastRttMs() > 12.1 || got.GetRttSampledAtUnixNano() == 0 {
		t.Fatalf("rtt=%#v", got)
	}
}

func TestPingUnsupportedDetectsInvalidArgument(t *testing.T) {
	if !pingUnsupported(status.Error(codes.InvalidArgument, "unexpected telemetry request")) {
		t.Fatal("invalid argument should disable ping")
	}
	if pingUnsupported(io.EOF) {
		t.Fatal("EOF should not disable ping")
	}
}

func TestDurationMilliseconds(t *testing.T) {
	if got := durationMilliseconds(2500 * time.Microsecond); got < 2.4 || got > 2.6 {
		t.Fatalf("got %v", got)
	}
	if durationMilliseconds(-time.Millisecond) != 0 {
		t.Fatal("negative duration should clamp to 0")
	}
}

func TestPressureRollupAggregatesOneMinuteWithoutNegativeCounters(t *testing.T) {
	start := time.Unix(1_800_000_000, 0).Truncate(time.Minute)
	rollup := newPressureStateRollup(start)
	rollup.add(&pb.State{
		Cpu: 10, MemUsed: 100, DiskUsed: 1000, Load1: 1, TcpConnCount: 10,
		Uptime: 100, NetInTransfer: 1000, NetOutTransfer: 2000,
	})
	rollup.add(&pb.State{
		Cpu: 30, MemUsed: 300, DiskUsed: 3000, Load1: 3, TcpConnCount: 30,
		Uptime: 105, NetInTransfer: 1250, NetOutTransfer: 2400,
	})
	// A reboot/reset contributes no negative traffic.
	rollup.add(&pb.State{
		Cpu: 20, MemUsed: 200, DiskUsed: 2000, Load1: 2, TcpConnCount: 20,
		Uptime: 2, NetInTransfer: 100, NetOutTransfer: 100,
	})
	payload := rollup.payload(start.Add(time.Minute))
	if payload.GetSampleCount() != 3 || payload.GetMinimum().GetCpu() != 10 || payload.GetAverage().GetCpu() != 20 || payload.GetMaximum().GetCpu() != 30 {
		t.Fatalf("rollup=%#v", payload)
	}
	if payload.GetNetInTotal() != 250 || payload.GetNetOutTotal() != 400 {
		t.Fatalf("network totals in=%d out=%d", payload.GetNetInTotal(), payload.GetNetOutTotal())
	}
}
