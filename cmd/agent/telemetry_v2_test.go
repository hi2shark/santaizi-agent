package main

import (
	"testing"
	"time"

	pb "github.com/hi2shark/santaizi-agent/proto"
)

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
