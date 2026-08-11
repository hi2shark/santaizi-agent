package model

import (
	"os"
	"path/filepath"
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
