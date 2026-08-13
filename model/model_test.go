package model

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMissingConfigUsesReliableTelemetryDefaults(t *testing.T) {
	var config AgentConfig
	if err := config.Read(filepath.Join(t.TempDir(), "missing.yaml")); err != nil {
		t.Fatal(err)
	}
	config.ApplyDefaults(filepath.Join(t.TempDir(), "agent-data"))
	if config.Telemetry.StateInterval != 5*time.Second || config.Telemetry.WAL.SegmentSizeBytes != 8<<20 || config.Telemetry.WAL.MaxSizeBytes != 256<<20 {
		t.Fatalf("telemetry defaults=%#v", config.Telemetry)
	}
	if !config.Capabilities.CPU || !config.Capabilities.Memory || !config.Capabilities.HTTPProbe || !config.Capabilities.NAT {
		t.Fatalf("capability defaults=%#v", config.Capabilities)
	}
	if config.Capabilities.GPU || config.Capabilities.Temperature {
		t.Fatalf("optional capability defaults=%#v", config.Capabilities)
	}
}

func TestAgentConfigHonorsExplicitDisabledCapabilities(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(path, []byte(`capabilities:
  cpu: false
  memory: false
  disk: false
  network: false
  connections: false
  processes: false
  host_info: false
  ip_report: false
  http_probe: false
  icmp_probe: false
  tcp_probe: false
  nat: false
  temperature: true
  gpu: true
`), 0600); err != nil {
		t.Fatal(err)
	}
	var config AgentConfig
	if err := config.Read(path); err != nil {
		t.Fatal(err)
	}
	if config.Capabilities.CPU || config.Capabilities.HTTPProbe || config.Capabilities.NAT {
		t.Fatalf("disabled capabilities=%#v", config.Capabilities)
	}
	if !config.Capabilities.Temperature || !config.Capabilities.GPU {
		t.Fatalf("enabled optional capabilities=%#v", config.Capabilities)
	}
}

func TestAgentConfigRoundTripConnectionSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.yaml")
	var config AgentConfig
	if err := config.Read(path); err != nil {
		t.Fatal(err)
	}
	config.Server = "10.0.0.10:5555"
	config.ClientSecret = "test-client-secret"
	config.TLS = true
	config.InsecureTLS = true
	config.ReportDelay = 9
	config.IPReportPeriod = 120
	config.IPReportInterface = "eth0"
	config.CountryCode = "CN"
	config.UseIPv6CountryCode = true
	config.Debug = true
	config.Capabilities.NAT = false
	config.Telemetry.DataDir = filepath.Join(t.TempDir(), "agent-data")
	if err := config.Save(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "client_secret:") || strings.Contains(string(raw), "ClientSecret:") {
		t.Fatalf("expected snake_case secret key in yaml:\n%s", raw)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("config mode=%v", info.Mode().Perm())
	}

	var loaded AgentConfig
	if err := loaded.Read(path); err != nil {
		t.Fatal(err)
	}
	if loaded.Server != "10.0.0.10:5555" || loaded.ClientSecret != "test-client-secret" {
		t.Fatalf("connection=%q %q", loaded.Server, loaded.ClientSecret)
	}
	if !loaded.TLS || !loaded.InsecureTLS || loaded.ReportDelay != 9 || loaded.IPReportPeriod != 120 {
		t.Fatalf("runtime=%#v", loaded)
	}
	if loaded.IPReportInterface != "eth0" || loaded.CountryCode != "CN" || !loaded.UseIPv6CountryCode || !loaded.Debug {
		t.Fatalf("ip report=%#v", loaded)
	}
	if loaded.Capabilities.NAT {
		t.Fatalf("capabilities=%#v", loaded.Capabilities)
	}
}

func TestAgentConfigHonorsConfiguredDataDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(path, []byte(`telemetry:
  data_dir: /tmp/santaizi-agent-configured
  state_interval: 7s
  wal:
    max_size_bytes: 10485760
`), 0600); err != nil {
		t.Fatal(err)
	}
	var config AgentConfig
	if err := config.Read(path); err != nil {
		t.Fatal(err)
	}
	if config.Telemetry.DataDir != "/tmp/santaizi-agent-configured" || config.Telemetry.StateInterval != 7*time.Second || config.Telemetry.WAL.MaxSizeBytes != 10<<20 {
		t.Fatalf("telemetry=%#v", config.Telemetry)
	}
}
