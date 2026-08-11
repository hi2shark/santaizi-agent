package monitor

import (
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	psnet "github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/sensors"

	"github.com/hi2shark/santaizi-agent/model"
)

func TestDisabledCapabilitiesSkipCollectors(t *testing.T) {
	originalConfig := agentConfig
	originalHostInfo := hostInfoFn
	originalCPUInfo := cpuInfoFn
	originalCPUPercent := cpuPercentFn
	originalMemory := virtualMemoryFn
	originalSwap := swapMemoryFn
	originalLoad := loadAverageFn
	originalProcesses := processIDsFn
	originalPartitions := diskPartitionsFn
	originalDiskUsage := diskUsageFn
	originalNetwork := networkCountersFn
	originalSensors := sensorTemperaturesFn
	t.Cleanup(func() {
		agentConfig = originalConfig
		hostInfoFn = originalHostInfo
		cpuInfoFn = originalCPUInfo
		cpuPercentFn = originalCPUPercent
		virtualMemoryFn = originalMemory
		swapMemoryFn = originalSwap
		loadAverageFn = originalLoad
		processIDsFn = originalProcesses
		diskPartitionsFn = originalPartitions
		diskUsageFn = originalDiskUsage
		networkCountersFn = originalNetwork
		sensorTemperaturesFn = originalSensors
	})

	cfg := &model.AgentConfig{}
	InitConfig(cfg)
	calls := 0
	hostInfoFn = func() (*host.InfoStat, error) {
		return &host.InfoStat{BootTime: uint64(time.Now().Add(-time.Minute).Unix())}, nil
	}
	cpuInfoFn = func() ([]cpu.InfoStat, error) { calls++; return nil, nil }
	cpuPercentFn = func(time.Duration, bool) ([]float64, error) { calls++; return nil, nil }
	virtualMemoryFn = func() (*mem.VirtualMemoryStat, error) { calls++; return nil, nil }
	swapMemoryFn = func() (*mem.SwapMemoryStat, error) { calls++; return nil, nil }
	loadAverageFn = func() (*load.AvgStat, error) { calls++; return nil, nil }
	processIDsFn = func() ([]int32, error) { calls++; return nil, nil }
	diskPartitionsFn = func(bool) ([]disk.PartitionStat, error) { calls++; return nil, nil }
	diskUsageFn = func(string) (*disk.UsageStat, error) { calls++; return nil, nil }
	networkCountersFn = func(bool) ([]psnet.IOCountersStat, error) { calls++; return nil, nil }
	sensorTemperaturesFn = func() ([]sensors.TemperatureStat, error) { calls++; return nil, nil }

	hostState := GetHost()
	state := GetState()
	TrackNetworkSpeed()
	if calls != 0 {
		t.Fatalf("disabled collectors were called %d times", calls)
	}
	if hostState.BootTime == 0 || state.Uptime == 0 {
		t.Fatalf("heartbeat continuity fields were not retained: host=%#v state=%#v", hostState, state)
	}
}
