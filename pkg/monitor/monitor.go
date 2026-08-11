package monitor

import (
	"fmt"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dean2021/goss"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/shirou/gopsutil/v4/sensors"

	"github.com/hi2shark/santaizi-agent/model"
	"github.com/hi2shark/santaizi-agent/pkg/gpu"
	gpustat "github.com/hi2shark/santaizi-agent/pkg/gpu/stat"
	"github.com/hi2shark/santaizi-agent/pkg/util"
)

var (
	Version           string
	expectDiskFsTypes = []string{
		"apfs", "ext4", "ext3", "ext2", "f2fs", "reiserfs", "jfs", "btrfs",
		"fuseblk", "zfs", "simfs", "ntfs", "fat32", "exfat", "xfs", "fuse.rclone",
	}
	excludeNetInterfaces = []string{
		"lo", "tun", "docker", "veth", "br-", "vmbr", "vnet", "kube",
	}
	sensorIgnoreList = []string{
		"PMU tcal", // the calibration sensor on arm macs, value is fixed
		"noname",
	}
	agentConfig *model.AgentConfig

	hostInfoFn           = host.Info
	cpuInfoFn            = cpu.Info
	cpuPercentFn         = cpu.Percent
	virtualMemoryFn      = mem.VirtualMemory
	swapMemoryFn         = mem.SwapMemory
	loadAverageFn        = load.Avg
	processIDsFn         = process.Pids
	diskPartitionsFn     = disk.Partitions
	diskUsageFn          = disk.Usage
	networkCountersFn    = net.IOCounters
	networkConnectionsFn = net.Connections
	sensorTemperaturesFn = sensors.SensorsTemperatures
)

var (
	netInSpeed, netOutSpeed, netInTransfer, netOutTransfer, lastUpdateNetStats uint64
	cachedBootTime                                                             time.Time
	temperatureStat                                                            []model.SensorTemperature
)

// 获取设备数据的最大尝试次数
const maxDeviceDataFetchAttempts = 3

// 获取主机数据的尝试次数，Key 为 Host 的属性名
var hostDataFetchAttempts = map[string]int{
	"CPU": 0,
	"GPU": 0,
}

// 获取状态数据的尝试次数，Key 为 HostState 的属性名
var statDataFetchAttempts = map[string]int{
	"CPU":          0,
	"Load":         0,
	"GPU":          0,
	"Temperatures": 0,
}

var (
	updateTempStatus = new(atomic.Bool)
)

func InitConfig(cfg *model.AgentConfig) {
	agentConfig = cfg
}

// GetHost 获取主机硬件信息
func GetHost() *model.Host {
	var ret model.Host
	caps := agentConfig.Capabilities

	var cpuType string
	hi, err := hostInfoFn()
	if err != nil {
		printf("host.Info error: %v", err)
	} else {
		cachedBootTime = time.Unix(int64(hi.BootTime), 0)
		ret.BootTime = hi.BootTime
		if caps.HostInfo {
			if hi.VirtualizationRole == "guest" {
				cpuType = "Virtual"
				ret.Virtualization = hi.VirtualizationSystem
			} else {
				cpuType = "Physical"
				ret.Virtualization = ""
			}
			ret.Platform = hi.Platform
			ret.PlatformVersion = hi.PlatformVersion
			ret.Arch = hi.KernelArch
		}
	}

	cpuModelCount := make(map[string]int)
	if caps.HostInfo && caps.CPU && hostDataFetchAttempts["CPU"] < maxDeviceDataFetchAttempts {
		ci, err := cpuInfoFn()
		if err != nil {
			hostDataFetchAttempts["CPU"]++
			printf("cpu.Info error: %v, attempt: %d", err, hostDataFetchAttempts["CPU"])
		} else {
			hostDataFetchAttempts["CPU"] = 0
			for i := 0; i < len(ci); i++ {
				cpuModelCount[ci[i].ModelName]++
			}
			for model, count := range cpuModelCount {
				if len(ci) > 1 {
					ret.CPU = append(ret.CPU, fmt.Sprintf("%s %d %s Core", model, count, cpuType))
				} else {
					ret.CPU = append(ret.CPU, fmt.Sprintf("%s %d %s Core", model, ci[0].Cores, cpuType))
				}
			}
		}
	}

	if caps.HostInfo && caps.GPU {
		if hostDataFetchAttempts["GPU"] < maxDeviceDataFetchAttempts {
			ret.GPU, err = gpu.GetGPUModel()
			if err != nil {
				hostDataFetchAttempts["GPU"]++
				printf("gpu.GetGPUModel error: %v, attempt: %d", err, hostDataFetchAttempts["GPU"])
			} else {
				hostDataFetchAttempts["GPU"] = 0
			}
		}
	}

	if caps.HostInfo && caps.Disk {
		ret.DiskTotal, _ = getDiskTotalAndUsed()
	}

	if caps.HostInfo && caps.Memory {
		mv, err := virtualMemoryFn()
		if err != nil {
			printf("mem.VirtualMemory error: %v", err)
		} else {
			ret.MemTotal = mv.Total
			if runtime.GOOS != "windows" {
				ret.SwapTotal = mv.SwapTotal
			}
		}

		if runtime.GOOS == "windows" {
			ms, err := swapMemoryFn()
			if err != nil {
				printf("mem.SwapMemory error: %v", err)
			} else {
				ret.SwapTotal = ms.Total
			}
		}
	}

	if caps.IPReport {
		ret.IP = CachedIP
	}
	ret.Version = Version

	return &ret
}

func GetState() *model.HostState {
	var ret model.HostState
	caps := agentConfig.Capabilities

	if caps.CPU && statDataFetchAttempts["CPU"] < maxDeviceDataFetchAttempts {
		cp, err := cpuPercentFn(0, false)
		if err != nil || len(cp) == 0 {
			statDataFetchAttempts["CPU"]++
			printf("cpu.Percent error: %v, attempt: %d", err, statDataFetchAttempts["CPU"])
		} else {
			statDataFetchAttempts["CPU"] = 0
			ret.CPU = cp[0]
		}
	}

	if caps.Memory {
		vm, err := virtualMemoryFn()
		if err != nil {
			printf("mem.VirtualMemory error: %v", err)
		} else {
			ret.MemUsed = vm.Total - vm.Available
			if runtime.GOOS != "windows" {
				ret.SwapUsed = vm.SwapTotal - vm.SwapFree
			}
		}
		if runtime.GOOS == "windows" {
			ms, err := swapMemoryFn()
			if err != nil {
				printf("mem.SwapMemory error: %v", err)
			} else {
				ret.SwapUsed = ms.Used
			}
		}
	}

	if caps.Disk {
		_, ret.DiskUsed = getDiskTotalAndUsed()
	}

	if caps.CPU && statDataFetchAttempts["Load"] < maxDeviceDataFetchAttempts {
		loadStat, err := loadAverageFn()
		if err != nil {
			statDataFetchAttempts["Load"]++
			printf("load.Avg error: %v, attempt: %d", err, statDataFetchAttempts["Load"])
		} else {
			statDataFetchAttempts["Load"] = 0
			ret.Load1 = loadStat.Load1
			ret.Load5 = loadStat.Load5
			ret.Load15 = loadStat.Load15
		}
	}

	if caps.Processes {
		procs, err := processIDsFn()
		if err != nil {
			printf("process.Pids error: %v", err)
		} else {
			ret.ProcessCount = uint64(len(procs))
		}
	}

	if caps.Temperature {
		go updateTemperatureStat()
		ret.Temperatures = temperatureStat
	}

	if caps.GPU {
		ret.GPU = updateGPUStat()
	}

	if caps.Network {
		ret.NetInTransfer, ret.NetOutTransfer = netInTransfer, netOutTransfer
		ret.NetInSpeed, ret.NetOutSpeed = netInSpeed, netOutSpeed
	}
	if !cachedBootTime.IsZero() {
		ret.Uptime = uint64(time.Since(cachedBootTime).Seconds())
	}
	if caps.Connections {
		ret.TcpConnCount, ret.UdpConnCount = getConns()
	}

	return &ret
}

// TrackNetworkSpeed NIC监控，统计流量与速度
func TrackNetworkSpeed() {
	if !agentConfig.Capabilities.Network {
		return
	}
	var innerNetInTransfer, innerNetOutTransfer uint64
	nc, err := networkCountersFn(true)
	if err == nil {
		for _, v := range nc {
			if len(agentConfig.NICAllowlist) > 0 {
				if !agentConfig.NICAllowlist[v.Name] {
					continue
				}
			} else {
				if util.ContainsStr(excludeNetInterfaces, v.Name) {
					continue
				}
			}
			innerNetInTransfer += v.BytesRecv
			innerNetOutTransfer += v.BytesSent
		}
		now := uint64(time.Now().Unix())
		diff := now - lastUpdateNetStats
		if diff > 0 {
			netInSpeed = (innerNetInTransfer - netInTransfer) / diff
			netOutSpeed = (innerNetOutTransfer - netOutTransfer) / diff
		}
		netInTransfer = innerNetInTransfer
		netOutTransfer = innerNetOutTransfer
		lastUpdateNetStats = now
	}
}

func getDiskTotalAndUsed() (total uint64, used uint64) {
	devices := make(map[string]string)

	if len(agentConfig.HardDrivePartitionAllowlist) > 0 {
		// 如果配置了白名单，使用白名单的列表
		for i, v := range agentConfig.HardDrivePartitionAllowlist {
			devices[strconv.Itoa(i)] = v
		}
	} else {
		// 否则使用默认过滤规则
		diskList, _ := diskPartitionsFn(false)
		for _, d := range diskList {
			fsType := strings.ToLower(d.Fstype)
			// 不统计 K8s 的虚拟挂载点：https://github.com/shirou/gopsutil/issues/1007
			if devices[d.Device] == "" && util.ContainsStr(expectDiskFsTypes, fsType) && !strings.Contains(d.Mountpoint, "/var/lib/kubelet") {
				devices[d.Device] = d.Mountpoint
			}
		}
	}

	for _, mountPath := range devices {
		diskUsageOf, err := diskUsageFn(mountPath)
		if err == nil {
			total += diskUsageOf.Total
			used += diskUsageOf.Used
		}
	}

	// Fallback 到这个方法,仅统计根路径,适用于OpenVZ之类的.
	if runtime.GOOS == "linux" && total == 0 && used == 0 {
		cmd := exec.Command("df")
		out, err := cmd.CombinedOutput()
		if err == nil {
			s := strings.Split(string(out), "\n")
			for _, c := range s {
				info := strings.Fields(c)
				if len(info) == 6 {
					if info[5] == "/" {
						total, _ = strconv.ParseUint(info[1], 0, 64)
						used, _ = strconv.ParseUint(info[2], 0, 64)
						// 默认获取的是1K块为单位的.
						total = total * 1024
						used = used * 1024
					}
				}
			}
		}
	}

	return
}

func getConns() (tcpConnCount, udpConnCount uint64) {
	ss_err := true
	if runtime.GOOS == "linux" {
		tcpStat, err_tcp := goss.ConnectionsWithProtocol(goss.AF_INET, syscall.IPPROTO_TCP)
		udpStat, err_udp := goss.ConnectionsWithProtocol(goss.AF_INET, syscall.IPPROTO_UDP)
		if err_tcp == nil && err_udp == nil {
			ss_err = false
			tcpConnCount = uint64(len(tcpStat))
			udpConnCount = uint64(len(udpStat))
		}
		if strings.Contains(CachedIP, ":") {
			tcpStat6, err_tcp := goss.ConnectionsWithProtocol(goss.AF_INET6, syscall.IPPROTO_TCP)
			udpStat6, err_udp := goss.ConnectionsWithProtocol(goss.AF_INET6, syscall.IPPROTO_UDP)
			if err_tcp == nil && err_udp == nil {
				ss_err = false
				tcpConnCount += uint64(len(tcpStat6))
				udpConnCount += uint64(len(udpStat6))
			}
		}
	}
	if ss_err {
		conns, _ := networkConnectionsFn("all")
		for i := 0; i < len(conns); i++ {
			switch conns[i].Type {
			case syscall.SOCK_STREAM:
				tcpConnCount++
			case syscall.SOCK_DGRAM:
				udpConnCount++
			}
		}
	}
	return tcpConnCount, udpConnCount
}

func updateGPUStat() float64 {
	if agentConfig.Capabilities.GPU {
		if statDataFetchAttempts["GPU"] < maxDeviceDataFetchAttempts {
			gs, err := gpustat.GetGPUStat()
			if err != nil {
				statDataFetchAttempts["GPU"]++
				printf("gpustat.GetGPUStat error: %v, attempt: %d", err, statDataFetchAttempts["GPU"])
				return 0
			} else {
				statDataFetchAttempts["GPU"] = 0
				return gs
			}
		}
	}
	return 0
}

func updateTemperatureStat() {
	if !updateTempStatus.CompareAndSwap(false, true) {
		return
	}
	defer updateTempStatus.Store(false)

	if statDataFetchAttempts["Temperatures"] < maxDeviceDataFetchAttempts {
		temperatures, err := sensorTemperaturesFn()
		if err != nil {
			statDataFetchAttempts["Temperatures"]++
			printf("host.SensorsTemperatures error: %v, attempt: %d", err, statDataFetchAttempts["Temperatures"])
		} else {
			statDataFetchAttempts["Temperatures"] = 0
			tempStat := []model.SensorTemperature{}
			for _, t := range temperatures {
				if t.Temperature > 0 && !util.ContainsStr(sensorIgnoreList, t.SensorKey) {
					tempStat = append(tempStat, model.SensorTemperature{
						Name:        t.SensorKey,
						Temperature: t.Temperature,
					})
				}
			}

			sort.Slice(tempStat, func(i, j int) bool {
				return tempStat[i].Name < tempStat[j].Name
			})

			temperatureStat = tempStat
		}
	}
}

func printf(format string, v ...interface{}) {
	util.Printf(agentConfig.Debug, format, v...)
}
