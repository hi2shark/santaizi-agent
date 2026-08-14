package main

import (
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/hi2shark/santaizi-agent/pkg/wal"
	pb "github.com/hi2shark/santaizi-agent/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHostLocationChanged(t *testing.T) {
	previous := &pb.Host{Ip: "203.0.113.10", CountryCode: "us"}
	if hostLocationChanged("203.0.113.10", "us", previous) {
		t.Fatal("unchanged location should not flush")
	}
	if !hostLocationChanged("198.51.100.8", "us", previous) {
		t.Fatal("IP change should flush")
	}
	if !hostLocationChanged("203.0.113.10", "hk", previous) {
		t.Fatal("country change should flush")
	}
	if hostLocationChanged("", "hk", &pb.Host{Ip: "203.0.113.10", CountryCode: "hk"}) {
		t.Fatal("empty IP with same country should not flush")
	}
	if !hostLocationChanged("", "hk", &pb.Host{CountryCode: "us"}) {
		t.Fatal("country-only change should flush")
	}
	if hostLocationChanged("", "", previous) {
		t.Fatal("empty cache should not flush")
	}
	if !hostLocationChanged("203.0.113.10", "hk", nil) {
		t.Fatal("first fill should flush")
	}
	if hostLocationChanged("203.0.113.10", "", previous) {
		t.Fatal("empty new country should not wipe")
	}
}

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

func TestHasPendingRecordsCaughtUp(t *testing.T) {
	session := "aabbccddeeff00112233445566778899"
	endpoint := &wal.EndpointState{
		Reliable:    true,
		Cursors:     map[string]uint64{session: 10},
		Activations: map[string]uint64{session: 1},
	}
	if hasPendingRecords(map[string]uint64{session: 10}, endpoint) {
		t.Fatal("caught-up cursor should skip WAL scan")
	}
	if !hasPendingRecords(map[string]uint64{session: 11}, endpoint) {
		t.Fatal("newer durable sequence should be pending")
	}
	if hasPendingRecords(map[string]uint64{"other": 3}, endpoint) {
		t.Fatal("unassigned session should not be pending")
	}
	if hasPendingRecords(map[string]uint64{session: 10}, &wal.EndpointState{Reliable: false, Cursors: map[string]uint64{session: 0}}) {
		t.Fatal("unreliable endpoint should not be pending")
	}
}

func TestShouldSendRealtimeSnapshotSkipsUnchangedSequence(t *testing.T) {
	worker := &endpointWorker{}
	if shouldSendRealtimeSnapshot(worker, 0) {
		t.Fatal("zero sequence should not send")
	}
	if !shouldSendRealtimeSnapshot(worker, 4) {
		t.Fatal("first sequence should send")
	}
	worker.lastSnapshotSeq = 4
	if shouldSendRealtimeSnapshot(worker, 4) {
		t.Fatal("unchanged sequence should skip clone and send")
	}
	if !shouldSendRealtimeSnapshot(worker, 5) {
		t.Fatal("newer sequence should send")
	}
}

func TestRecordAckedByRequiresEveryReliableEndpoint(t *testing.T) {
	session := hex.EncodeToString(append(make([]byte, 15), 1))
	record := &pb.TelemetryRecord{Record: &pb.TelemetryRecord_Event{Event: &pb.TelemetryEvent{
		SessionId: append(make([]byte, 15), 1), Sequence: 4,
	}}}
	snapshot := wal.CursorState{Endpoints: map[string]*wal.EndpointState{
		"primary": {Reliable: true, Cursors: map[string]uint64{session: 4}, Activations: map[string]uint64{session: 1}},
		"slow":    {Reliable: true, Cursors: map[string]uint64{session: 3}, Activations: map[string]uint64{session: 1}},
	}}
	if recordAckedBy(snapshot, record) {
		t.Fatal("lagging reliable sink should block reclaim")
	}
	snapshot.Endpoints["slow"].Cursors[session] = 4
	if !recordAckedBy(snapshot, record) {
		t.Fatal("all reliable sinks caught up should allow reclaim")
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
