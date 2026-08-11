package model

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
	"sigs.k8s.io/yaml"
)

type TelemetryEndpointConfig struct {
	ID          string `mapstructure:"id" yaml:"id"`
	Address     string `mapstructure:"address" yaml:"address"`
	TLS         bool   `mapstructure:"tls" yaml:"tls"`
	InsecureTLS bool   `mapstructure:"insecure_tls" yaml:"insecure_tls"`
}

type WALConfig struct {
	SegmentSizeBytes int64         `mapstructure:"segment_size_bytes" yaml:"segment_size_bytes"`
	MaxSizeBytes     int64         `mapstructure:"max_size_bytes" yaml:"max_size_bytes"`
	ReserveBytes     int64         `mapstructure:"reserve_bytes" yaml:"reserve_bytes"`
	FsyncInterval    time.Duration `mapstructure:"fsync_interval" yaml:"fsync_interval"`
	FsyncRecords     int           `mapstructure:"fsync_records" yaml:"fsync_records"`
}

type TelemetryConfig struct {
	DataDir           string                    `mapstructure:"data_dir" yaml:"data_dir"`
	StateInterval     time.Duration             `mapstructure:"state_interval" yaml:"state_interval"`
	HeartbeatInterval time.Duration             `mapstructure:"heartbeat_interval" yaml:"heartbeat_interval"`
	HostInterval      time.Duration             `mapstructure:"host_interval" yaml:"host_interval"`
	BatchSize         int                       `mapstructure:"batch_size" yaml:"batch_size"`
	DisabledRemoteIDs []string                  `mapstructure:"disabled_remote_ids" yaml:"disabled_remote_ids"`
	Collectors        []TelemetryEndpointConfig `mapstructure:"collectors" yaml:"collectors"`
	WAL               WALConfig                 `mapstructure:"wal" yaml:"wal"`
}

type CapabilityConfig struct {
	CPU         bool `mapstructure:"cpu" yaml:"cpu"`
	Memory      bool `mapstructure:"memory" yaml:"memory"`
	Disk        bool `mapstructure:"disk" yaml:"disk"`
	Network     bool `mapstructure:"network" yaml:"network"`
	Connections bool `mapstructure:"connections" yaml:"connections"`
	Processes   bool `mapstructure:"processes" yaml:"processes"`
	Temperature bool `mapstructure:"temperature" yaml:"temperature"`
	GPU         bool `mapstructure:"gpu" yaml:"gpu"`
	HostInfo    bool `mapstructure:"host_info" yaml:"host_info"`
	IPReport    bool `mapstructure:"ip_report" yaml:"ip_report"`
	HTTPProbe   bool `mapstructure:"http_probe" yaml:"http_probe"`
	ICMPProbe   bool `mapstructure:"icmp_probe" yaml:"icmp_probe"`
	TCPProbe    bool `mapstructure:"tcp_probe" yaml:"tcp_probe"`
	NAT         bool `mapstructure:"nat" yaml:"nat"`
}

func DefaultCapabilities() CapabilityConfig {
	return CapabilityConfig{
		CPU: true, Memory: true, Disk: true, Network: true, Connections: true, Processes: true,
		HostInfo: true, IPReport: true, HTTPProbe: true, ICMPProbe: true, TCPProbe: true, NAT: true,
	}
}

type AgentConfig struct {
	HardDrivePartitionAllowlist []string         `mapstructure:"hard_drive_partition_allowlist" yaml:"hard_drive_partition_allowlist,omitempty"`
	NICAllowlist                map[string]bool  `mapstructure:"nic_allowlist" yaml:"nic_allowlist,omitempty"`
	DNS                         []string         `mapstructure:"dns" yaml:"dns,omitempty"`
	Capabilities                CapabilityConfig `mapstructure:"capabilities" yaml:"capabilities"`
	Debug                       bool             `mapstructure:"debug" yaml:"debug"`
	Telemetry                   TelemetryConfig  `mapstructure:"telemetry" yaml:"telemetry"`
	v                           *viper.Viper     `yaml:"-"`
}

// Read 从给定的文件目录加载配置文件
func (c *AgentConfig) Read(path string) error {
	c.v = viper.New()
	c.v.SetConfigFile(path)
	defaults := DefaultCapabilities()
	c.v.SetDefault("capabilities.cpu", defaults.CPU)
	c.v.SetDefault("capabilities.memory", defaults.Memory)
	c.v.SetDefault("capabilities.disk", defaults.Disk)
	c.v.SetDefault("capabilities.network", defaults.Network)
	c.v.SetDefault("capabilities.connections", defaults.Connections)
	c.v.SetDefault("capabilities.processes", defaults.Processes)
	c.v.SetDefault("capabilities.temperature", defaults.Temperature)
	c.v.SetDefault("capabilities.gpu", defaults.GPU)
	c.v.SetDefault("capabilities.host_info", defaults.HostInfo)
	c.v.SetDefault("capabilities.ip_report", defaults.IPReport)
	c.v.SetDefault("capabilities.http_probe", defaults.HTTPProbe)
	c.v.SetDefault("capabilities.icmp_probe", defaults.ICMPProbe)
	c.v.SetDefault("capabilities.tcp_probe", defaults.TCPProbe)
	c.v.SetDefault("capabilities.nat", defaults.NAT)
	c.v.SetEnvPrefix("SANTAIZI")
	c.v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	c.v.AutomaticEnv()
	if err := c.v.ReadInConfig(); err != nil && !errors.As(err, new(viper.ConfigFileNotFoundError)) && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := c.v.Unmarshal(c); err != nil {
		return err
	}
	c.ApplyDefaults("")
	return nil
}

func (c *AgentConfig) ApplyDefaults(dataDirOverride string) {
	if c.v == nil && c.Capabilities == (CapabilityConfig{}) {
		c.Capabilities = DefaultCapabilities()
	}
	if dataDirOverride != "" {
		c.Telemetry.DataDir = dataDirOverride
	}
	if c.Telemetry.DataDir == "" {
		c.Telemetry.DataDir = "/var/lib/santaizi-agent"
	}
	if c.Telemetry.StateInterval <= 0 {
		c.Telemetry.StateInterval = 5 * time.Second
	}
	if c.Telemetry.HeartbeatInterval <= 0 {
		c.Telemetry.HeartbeatInterval = 10 * time.Second
	}
	if c.Telemetry.HostInterval <= 0 {
		c.Telemetry.HostInterval = 10 * time.Minute
	}
	if c.Telemetry.BatchSize <= 0 {
		c.Telemetry.BatchSize = 256
	}
	if c.Telemetry.WAL.SegmentSizeBytes <= 0 {
		c.Telemetry.WAL.SegmentSizeBytes = 8 << 20
	}
	if c.Telemetry.WAL.MaxSizeBytes <= 0 {
		c.Telemetry.WAL.MaxSizeBytes = 256 << 20
	}
	if c.Telemetry.WAL.ReserveBytes <= 0 {
		c.Telemetry.WAL.ReserveBytes = 1 << 20
	}
	if c.Telemetry.WAL.FsyncInterval <= 0 {
		c.Telemetry.WAL.FsyncInterval = time.Second
	}
	if c.Telemetry.WAL.FsyncRecords <= 0 {
		c.Telemetry.WAL.FsyncRecords = 64
	}
}

func (c *AgentConfig) Save() error {
	if c.v == nil || c.v.ConfigFileUsed() == "" {
		return errors.New("agent configuration path is not initialized")
	}
	data, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.v.ConfigFileUsed()), 0750); err != nil {
		return err
	}
	return os.WriteFile(c.v.ConfigFileUsed(), data, 0600)
}
