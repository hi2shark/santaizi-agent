package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/nezhahq/service"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"

	"github.com/hi2shark/santaizi-agent/model"
	"github.com/hi2shark/santaizi-agent/pkg/monitor"
	"github.com/hi2shark/santaizi-agent/pkg/util"
)

type AgentCliParam struct {
	DisableCPU         bool
	DisableMemory      bool
	DisableDisk        bool
	DisableNetwork     bool
	DisableConnections bool
	DisableProcesses   bool
	EnableTemperature  bool
	EnableGPU          bool
	DisableHostInfo    bool
	DisableIPReport    bool
	DisableHTTPProbe   bool
	DisableICMPProbe   bool
	DisableTCPProbe    bool
	DisableNAT         bool
	Server             string
	ClientSecret       string
	ReportDelay        int
	TLS                bool
	InsecureTLS        bool
	Version            bool
	IPReportPeriod     uint32
	UseIPv6CountryCode bool
	IPReportInterface  string
	CountryCode        string
	ConfigPath         string
	DataDir            string
}

var (
	version string
	arch    string

	agentCliParam AgentCliParam
	agentConfig   model.AgentConfig

	runCancelMu sync.Mutex
	runCancel   context.CancelFunc
)

var agentCmd = &cobra.Command{
	Use:               "agent",
	Run:               func(cmd *cobra.Command, args []string) { runService("", nil) },
	PreRun:            preRun,
	PersistentPreRun:  persistPreRun,
	SilenceUsage:      true,
	DisableAutoGenTag: true,
}

const delayWhenError = 10 * time.Second

func init() {
	resolver.SetDefaultScheme("passthrough")
	net.DefaultResolver.PreferGo = true
	net.DefaultResolver.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialer := net.Dialer{Timeout: 5 * time.Second}
		dnsServers := util.DNSServersAll
		if len(agentConfig.DNS) > 0 {
			dnsServers = agentConfig.DNS
		}
		index := int(time.Now().Unix()) % len(dnsServers)
		var lastErr error
		for _, candidate := range generateQueue(index, len(dnsServers)) {
			conn, err := dialer.DialContext(ctx, "udp", dnsServers[candidate])
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		return nil, lastErr
	}

	agentCmd.PersistentFlags().StringVarP(&agentCliParam.Server, "server", "s", "localhost:5555", "管理面板 RPC 地址")
	agentCmd.PersistentFlags().StringVarP(&agentCliParam.ClientSecret, "password", "p", "", "探针连接密钥")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.TLS, "tls", false, "启用 TLS")
	agentCmd.PersistentFlags().BoolVarP(&agentCliParam.InsecureTLS, "insecure", "k", false, "跳过 TLS 证书校验")
	agentCmd.PersistentFlags().BoolVarP(&agentConfig.Debug, "debug", "d", false, "开启调试日志")
	agentCmd.PersistentFlags().IntVar(&agentCliParam.ReportDelay, "report-delay", 5, "系统状态采集间隔")
	agentCmd.PersistentFlags().StringVar(&agentCliParam.ConfigPath, "config", "/etc/santaizi/agent.yaml", "配置文件路径")
	agentCmd.PersistentFlags().StringVar(&agentCliParam.DataDir, "data-dir", "/var/lib/santaizi-agent", "可靠探测数据目录")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableCPU, "disable-cpu", false, "不采集 CPU 与负载")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableMemory, "disable-memory", false, "不采集内存与 Swap")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableDisk, "disable-disk", false, "不采集磁盘")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableNetwork, "disable-network", false, "不采集网络流量")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableConnections, "disable-connections", false, "不采集连接数")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableProcesses, "disable-processes", false, "不采集进程数")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.EnableTemperature, "temperature", false, "采集温度")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.EnableGPU, "gpu", false, "采集 GPU")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableHostInfo, "disable-host-info", false, "不采集硬件与系统信息")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableIPReport, "disable-ip-report", false, "不查询或上报公网 IP")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableHTTPProbe, "disable-http-probe", false, "禁止 HTTP 探测")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableICMPProbe, "disable-icmp-probe", false, "禁止 ICMP 探测")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableTCPProbe, "disable-tcp-probe", false, "禁止 TCP 探测")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.DisableNAT, "disable-nat", false, "禁止内网穿透")
	agentCmd.PersistentFlags().BoolVar(&agentCliParam.UseIPv6CountryCode, "use-ipv6-countrycode", false, "优先使用 IPv6 位置信息")
	agentCmd.PersistentFlags().StringVar(&agentCliParam.IPReportInterface, "ip-report-interface", "", "公网 IP 探测与流量统计绑定的网卡名")
	agentCmd.PersistentFlags().StringVar(&agentCliParam.CountryCode, "country-code", "", "手填国家/区域识别码并直接上报")
	agentCmd.PersistentFlags().Uint32VarP(&agentCliParam.IPReportPeriod, "ip-report-period", "u", 30*60, "公网 IP 更新间隔（秒）")
	agentCmd.Flags().BoolVarP(&agentCliParam.Version, "version", "v", false, "显示版本")

	agentConfig.ApplyDefaults(agentCliParam.DataDir)
	monitor.InitConfig(&agentConfig)
	initProbeClients()
}

func main() {
	if err := agentCmd.Execute(); err != nil {
		println(err)
		os.Exit(1)
	}
}

func persistPreRun(cmd *cobra.Command, args []string) {
	if runtime.GOOS != "windows" {
		return
	}
	hostArch, err := host.KernelArch()
	if err != nil {
		panic(err)
	}
	switch hostArch {
	case "i386", "i686", "ia64":
		hostArch = "amd64"
	case "x86_64":
		hostArch = "amd64"
	case "aarch64":
		hostArch = "arm64"
	}
	if arch != hostArch {
		panic(fmt.Sprintf("当前二进制为 %s_%s，主机需要 %s_%s", runtime.GOOS, arch, runtime.GOOS, hostArch))
	}
}

func preRun(cmd *cobra.Command, args []string) {
	if err := agentConfig.Read(agentCliParam.ConfigPath); err != nil {
		panic(fmt.Sprintf("读取配置失败: %v", err))
	}
	dataDirOverride := ""
	if flagChanged(cmd, "data-dir") {
		dataDirOverride = agentCliParam.DataDir
	}
	agentConfig.ApplyDefaults(dataDirOverride)
	applyCapabilityFlags(cmd)
	monitor.InitConfig(&agentConfig)
	monitor.ConfigureIPReport(agentCliParam.IPReportInterface, agentCliParam.CountryCode)
	monitor.Version = version

	if agentCliParam.Version {
		fmt.Println(version)
		os.Exit(0)
	}
	if agentCliParam.ClientSecret == "" {
		_ = cmd.Help()
		os.Exit(1)
	}
	if agentCliParam.ReportDelay < 1 || agentCliParam.ReportDelay > 3600 {
		panic("report-delay 必须在 1 到 3600 秒之间")
	}
}

func flagChanged(cmd *cobra.Command, name string) bool {
	if flag := cmd.Flags().Lookup(name); flag != nil {
		return flag.Changed
	}
	if flag := cmd.InheritedFlags().Lookup(name); flag != nil {
		return flag.Changed
	}
	return false
}

func applyCapabilityFlags(cmd *cobra.Command) {
	caps := &agentConfig.Capabilities
	if flagChanged(cmd, "disable-cpu") && agentCliParam.DisableCPU {
		caps.CPU = false
	}
	if flagChanged(cmd, "disable-memory") && agentCliParam.DisableMemory {
		caps.Memory = false
	}
	if flagChanged(cmd, "disable-disk") && agentCliParam.DisableDisk {
		caps.Disk = false
	}
	if flagChanged(cmd, "disable-network") && agentCliParam.DisableNetwork {
		caps.Network = false
	}
	if flagChanged(cmd, "disable-connections") && agentCliParam.DisableConnections {
		caps.Connections = false
	}
	if flagChanged(cmd, "disable-processes") && agentCliParam.DisableProcesses {
		caps.Processes = false
	}
	if flagChanged(cmd, "temperature") && agentCliParam.EnableTemperature {
		caps.Temperature = true
	}
	if flagChanged(cmd, "gpu") && agentCliParam.EnableGPU {
		caps.GPU = true
	}
	if flagChanged(cmd, "disable-host-info") && agentCliParam.DisableHostInfo {
		caps.HostInfo = false
	}
	if flagChanged(cmd, "disable-ip-report") && agentCliParam.DisableIPReport {
		caps.IPReport = false
	}
	if flagChanged(cmd, "disable-http-probe") && agentCliParam.DisableHTTPProbe {
		caps.HTTPProbe = false
	}
	if flagChanged(cmd, "disable-icmp-probe") && agentCliParam.DisableICMPProbe {
		caps.ICMPProbe = false
	}
	if flagChanged(cmd, "disable-tcp-probe") && agentCliParam.DisableTCPProbe {
		caps.TCPProbe = false
	}
	if flagChanged(cmd, "disable-nat") && agentCliParam.DisableNAT {
		caps.NAT = false
	}
}

func run() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	runCancelMu.Lock()
	runCancel = cancel
	runCancelMu.Unlock()
	defer func() {
		cancel()
		runCancelMu.Lock()
		runCancel = nil
		runCancelMu.Unlock()
	}()

	auth := model.AuthHandler{ClientSecret: agentCliParam.ClientSecret}
	manager, err := startV2Telemetry(ctx, auth)
	if err != nil {
		printf("启动可靠探测失败: %v", err)
		return
	}
	defer func() {
		if err := manager.Close(); err != nil {
			printf("关闭可靠探测失败: %v", err)
		}
	}()
	if agentConfig.Capabilities.IPReport {
		go monitor.UpdateIP(agentCliParam.UseIPv6CountryCode, agentCliParam.IPReportPeriod)
	}
	<-ctx.Done()
}

func stopRunningAgent() {
	runCancelMu.Lock()
	defer runCancelMu.Unlock()
	if runCancel != nil {
		runCancel()
	}
}

func runService(action string, flags []string) {
	dir, err := os.Getwd()
	if err != nil {
		printf("获取当前工作目录失败: %v", err)
		return
	}
	config := &service.Config{
		Name: "santaizi-agent", DisplayName: "Santaizi Agent", Description: "三太子探针监控端",
		Arguments: flags, WorkingDirectory: dir, Option: map[string]interface{}{"OnFailure": "restart"},
	}
	program := &program{exit: make(chan struct{})}
	installedService, err := service.New(program, config)
	if err != nil {
		printf("创建系统服务失败，以前台模式运行: %v", err)
		run()
		return
	}
	program.service = installedService
	if agentConfig.Debug {
		serviceLogger, loggerErr := installedService.Logger(nil)
		if loggerErr != nil {
			printf("获取系统服务日志器失败: %v", loggerErr)
		} else {
			util.Logger = serviceLogger
		}
	}
	if action == "install" {
		println("Init system is:", installedService.Platform())
	}
	if action != "" {
		if err := service.Control(installedService, action); err != nil {
			log.Print(err)
		}
		return
	}
	if err := installedService.Run(); err != nil {
		util.Logger.Error(err)
	}
}

func dialOptions(useTLS, insecureTLS bool, auth *model.AuthHandler) []grpc.DialOption {
	var transport grpc.DialOption
	if useTLS {
		transport = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: insecureTLS}))
	} else {
		transport = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	options := []grpc.DialOption{transport}
	if auth != nil {
		options = append(options, grpc.WithPerRPCCredentials(auth))
	}
	return options
}

func println(v ...interface{}) { util.Println(agentConfig.Debug, v...) }

func printf(format string, v ...interface{}) { util.Printf(agentConfig.Debug, format, v...) }

func generateQueue(start, size int) []int {
	result := make([]int, 0, size)
	for i := start; i < start+size; i++ {
		result = append(result, i%size)
	}
	return result
}
