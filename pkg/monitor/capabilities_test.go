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

func TestSlowStatsReuseCachedSample(t *testing.T) {
	originalConfig := agentConfig
	originalPartitions := diskPartitionsFn
	originalDiskUsage := diskUsageFn
	originalProcesses := processIDsFn
	originalCPUPercent := cpuPercentFn
	originalMemory := virtualMemoryFn
	originalLoad := loadAverageFn
	originalSockstat := readSockstatFn
	t.Cleanup(func() {
		agentConfig = originalConfig
		diskPartitionsFn = originalPartitions
		diskUsageFn = originalDiskUsage
		processIDsFn = originalProcesses
		cpuPercentFn = originalCPUPercent
		virtualMemoryFn = originalMemory
		loadAverageFn = originalLoad
		readSockstatFn = originalSockstat
		resetSlowStatCache()
	})
	resetSlowStatCache()
	cfg := &model.AgentConfig{Capabilities: model.DefaultCapabilities()}
	InitConfig(cfg)
	diskCalls, procCalls := 0, 0
	cpuPercentFn = func(time.Duration, bool) ([]float64, error) { return []float64{1}, nil }
	virtualMemoryFn = func() (*mem.VirtualMemoryStat, error) { return &mem.VirtualMemoryStat{Total: 100, Available: 40}, nil }
	loadAverageFn = func() (*load.AvgStat, error) { return &load.AvgStat{}, nil }
	readSockstatFn = func() (uint64, uint64, bool) { return 1, 1, true }
	diskPartitionsFn = func(bool) ([]disk.PartitionStat, error) {
		return []disk.PartitionStat{{Device: "/dev/sda1", Fstype: "ext4", Mountpoint: "/"}}, nil
	}
	diskUsageFn = func(string) (*disk.UsageStat, error) {
		diskCalls++
		return &disk.UsageStat{Total: 100, Used: 40}, nil
	}
	processIDsFn = func() ([]int32, error) {
		procCalls++
		return []int32{1, 2, 3}, nil
	}
	first := GetState()
	second := GetState()
	if diskCalls != 1 || procCalls != 1 {
		t.Fatalf("slow stats were recollected: disk=%d proc=%d", diskCalls, procCalls)
	}
	if first.DiskUsed != 40 || second.DiskUsed != 40 || first.ProcessCount != 3 || second.ProcessCount != 3 {
		t.Fatalf("cached values mismatch: first=%#v second=%#v", first, second)
	}
}

func TestParseSockstatCountsInuse(t *testing.T) {
	data := "sockets: used 276\nTCP: inuse 32 orphan 0 tw 0 alloc 34 mem 3\nUDP: inuse 8 mem 0\n"
	tcp, udp, ok := parseSockstat(data, "TCP:", "UDP:")
	if !ok || tcp != 32 || udp != 8 {
		t.Fatalf("tcp=%d udp=%d ok=%v", tcp, udp, ok)
	}
	tcp6, udp6, ok6 := parseSockstat("TCP6: inuse 4\nUDP6: inuse 1\n", "TCP6:", "UDP6:")
	if !ok6 || tcp6 != 4 || udp6 != 1 {
		t.Fatalf("tcp6=%d udp6=%d ok=%v", tcp6, udp6, ok6)
	}
	if _, _, ok := parseSockstat("sockets: used 1\n", "TCP:", "UDP:"); ok {
		t.Fatal("missing protocols should not parse")
	}
}
