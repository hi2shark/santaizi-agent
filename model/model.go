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
	ID          string `json:"id" mapstructure:"id" yaml:"id"`
	Address     string `json:"address" mapstructure:"address" yaml:"address"`
	TLS         bool   `json:"tls" mapstructure:"tls" yaml:"tls"`
	InsecureTLS bool   `json:"insecure_tls" mapstructure:"insecure_tls" yaml:"insecure_tls"`
}

type WALConfig struct {
	SegmentSizeBytes int64         `json:"segment_size_bytes" mapstructure:"segment_size_bytes" yaml:"segment_size_bytes"`
	MaxSizeBytes     int64         `json:"max_size_bytes" mapstructure:"max_size_bytes" yaml:"max_size_bytes"`
	ReserveBytes     int64         `json:"reserve_bytes" mapstructure:"reserve_bytes" yaml:"reserve_bytes"`
	FsyncInterval    time.Duration `json:"fsync_interval" mapstructure:"fsync_interval" yaml:"fsync_interval"`
	FsyncRecords     int           `json:"fsync_records" mapstructure:"fsync_records" yaml:"fsync_records"`
}

type TelemetryConfig struct {
	DataDir           string                    `json:"data_dir" mapstructure:"data_dir" yaml:"data_dir"`
	StateInterval     time.Duration             `json:"state_interval" mapstructure:"state_interval" yaml:"state_interval"`
	HeartbeatInterval time.Duration             `json:"heartbeat_interval" mapstructure:"heartbeat_interval" yaml:"heartbeat_interval"`
	HostInterval      time.Duration             `json:"host_interval" mapstructure:"host_interval" yaml:"host_interval"`
	BatchSize         int                       `json:"batch_size" mapstructure:"batch_size" yaml:"batch_size"`
	DisabledRemoteIDs []string                  `json:"disabled_remote_ids" mapstructure:"disabled_remote_ids" yaml:"disabled_remote_ids"`
	Collectors        []TelemetryEndpointConfig `json:"collectors" mapstructure:"collectors" yaml:"collectors"`
	CertRenewDays     int                       `json:"cert_renew_days,omitempty" mapstructure:"cert_renew_days" yaml:"cert_renew_days,omitempty"`
	WAL               WALConfig                 `json:"wal" mapstructure:"wal" yaml:"wal"`
}

type CapabilityConfig struct {
	CPU         bool `json:"cpu" mapstructure:"cpu" yaml:"cpu"`
	Memory      bool `json:"memory" mapstructure:"memory" yaml:"memory"`
	Disk        bool `json:"disk" mapstructure:"disk" yaml:"disk"`
	Network     bool `json:"network" mapstructure:"network" yaml:"network"`
	Connections bool `json:"connections" mapstructure:"connections" yaml:"connections"`
	Processes   bool `json:"processes" mapstructure:"processes" yaml:"processes"`
	Temperature bool `json:"temperature" mapstructure:"temperature" yaml:"temperature"`
	GPU         bool `json:"gpu" mapstructure:"gpu" yaml:"gpu"`
	HostInfo    bool `json:"host_info" mapstructure:"host_info" yaml:"host_info"`
	IPReport    bool `json:"ip_report" mapstructure:"ip_report" yaml:"ip_report"`
	HTTPProbe   bool `json:"http_probe" mapstructure:"http_probe" yaml:"http_probe"`
	ICMPProbe   bool `json:"icmp_probe" mapstructure:"icmp_probe" yaml:"icmp_probe"`
	TCPProbe    bool `json:"tcp_probe" mapstructure:"tcp_probe" yaml:"tcp_probe"`
	NAT         bool `json:"nat" mapstructure:"nat" yaml:"nat"`
}

func DefaultCapabilities() CapabilityConfig {
	return CapabilityConfig{
		CPU: true, Memory: true, Disk: true, Network: true, Connections: true, Processes: true,
		HostInfo: true, IPReport: true, HTTPProbe: true, ICMPProbe: true, TCPProbe: true, NAT: true,
	}
}

type AgentConfig struct {
	Server                      string           `json:"server" mapstructure:"server" yaml:"server"`
	ClientSecret                string           `json:"client_secret" mapstructure:"client_secret" yaml:"client_secret"`
	TLS                         bool             `json:"tls" mapstructure:"tls" yaml:"tls"`
	InsecureTLS                 bool             `json:"insecure_tls" mapstructure:"insecure_tls" yaml:"insecure_tls"`
	TLSCAFile                   string           `json:"tls_ca_file,omitempty" mapstructure:"tls_ca_file" yaml:"tls_ca_file,omitempty"`
	ReportDelay                 int              `json:"report_delay" mapstructure:"report_delay" yaml:"report_delay"`
	IPReportPeriod              uint32           `json:"ip_report_period" mapstructure:"ip_report_period" yaml:"ip_report_period"`
	IPReportInterface           string           `json:"ip_report_interface,omitempty" mapstructure:"ip_report_interface" yaml:"ip_report_interface,omitempty"`
	CountryCode                 string           `json:"country_code,omitempty" mapstructure:"country_code" yaml:"country_code,omitempty"`
	UseIPv6CountryCode          bool             `json:"use_ipv6_countrycode,omitempty" mapstructure:"use_ipv6_countrycode" yaml:"use_ipv6_countrycode,omitempty"`
	HardDrivePartitionAllowlist []string         `json:"hard_drive_partition_allowlist,omitempty" mapstructure:"hard_drive_partition_allowlist" yaml:"hard_drive_partition_allowlist,omitempty"`
	NICAllowlist                map[string]bool  `json:"nic_allowlist,omitempty" mapstructure:"nic_allowlist" yaml:"nic_allowlist,omitempty"`
	DNS                         []string         `json:"dns,omitempty" mapstructure:"dns" yaml:"dns,omitempty"`
	Capabilities                CapabilityConfig `json:"capabilities" mapstructure:"capabilities" yaml:"capabilities"`
	Debug                       bool             `json:"debug" mapstructure:"debug" yaml:"debug"`
	Telemetry                   TelemetryConfig  `json:"telemetry" mapstructure:"telemetry" yaml:"telemetry"`
	v                           *viper.Viper     `json:"-" yaml:"-"`
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
	if c.ReportDelay <= 0 {
		c.ReportDelay = 5
	}
	if c.IPReportPeriod == 0 {
		c.IPReportPeriod = 30 * 60
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
	if c.Telemetry.CertRenewDays <= 0 {
		c.Telemetry.CertRenewDays = 7
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
