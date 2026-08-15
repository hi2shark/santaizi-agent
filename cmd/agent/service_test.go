package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/hi2shark/santaizi-agent/model"
	"github.com/hi2shark/santaizi-agent/pkg/dialcache"
)

func resetAgentState() {
	agentConfig = model.AgentConfig{}
	agentCliParam = AgentCliParam{}
}

func TestServiceRuntimeArgumentsOnlyIncludeConfig(t *testing.T) {
	path := "/etc/santaizi/agent.yaml"
	args := serviceRuntimeArguments(path)
	if len(args) != 2 || args[0] != "--config" || args[1] != path {
		t.Fatalf("args=%v", args)
	}
	joined := " " + strings.Join(args, " ") + " "
	for _, forbidden := range []string{"test-client-secret", " -p ", " -s ", " --tls ", " --disable-nat ", " --data-dir ", " --server-ip "} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("service arguments leaked %q: %v", forbidden, args)
		}
	}
}

func TestPersistRuntimeConfigWritesSecretAndKeepsServiceArgsClean(t *testing.T) {
	t.Cleanup(resetAgentState)
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	if err := agentConfig.Read(path); err != nil {
		t.Fatal(err)
	}
	agentCliParam = AgentCliParam{
		Server:       "10.0.0.10:5555",
		ClientSecret: "test-client-secret",
		TLS:          true,
		ConfigPath:   path,
		DataDir:      filepath.Join(dir, "data"),
		ReportDelay:  5,
		DisableNAT:   true,
	}
	agentConfig.Capabilities = model.DefaultCapabilities()
	agentConfig.Capabilities.NAT = false
	if err := persistRuntimeConfig(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, "10.0.0.10:5555") || !strings.Contains(text, "test-client-secret") {
		t.Fatalf("config missing connection fields:\n%s", text)
	}
	if !strings.Contains(text, "tls: true") {
		t.Fatalf("config missing tls:\n%s", text)
	}
	args := serviceRuntimeArguments(path)
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "test-client-secret") || strings.Contains(joined, "-p") || strings.Contains(joined, "-s") || strings.Contains(joined, "server-ip") {
		t.Fatalf("service arguments=%v", args)
	}
}

func TestSeedPrimaryServerIPsWritesCacheNotYAML(t *testing.T) {
	t.Cleanup(resetAgentState)
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	dataDir := filepath.Join(dir, "data")
	if err := agentConfig.Read(path); err != nil {
		t.Fatal(err)
	}
	agentCliParam = AgentCliParam{
		Server:       "grpc.example.invalid:5555",
		ClientSecret: "test-client-secret",
		ConfigPath:   path,
		DataDir:      dataDir,
		ReportDelay:  5,
		ServerIPs:    []string{"192.0.2.10", "198.51.100.4"},
	}
	if err := persistRuntimeConfig(); err != nil {
		t.Fatal(err)
	}
	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(text), "192.0.2.10") || strings.Contains(string(text), "server-ip") || strings.Contains(string(text), "server_ip") {
		t.Fatalf("yaml should not store hint IPs:\n%s", text)
	}
	if err := seedPrimaryServerIPs(dataDir, agentCliParam.Server, agentCliParam.ServerIPs); err != nil {
		t.Fatal(err)
	}
	store, err := dialcache.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	got := store.Get(dialcache.PrimaryKey, "grpc.example.invalid", "5555")
	if len(got) != 2 || got[0] != "192.0.2.10" || got[1] != "198.51.100.4" {
		t.Fatalf("seeded=%v", got)
	}
}

func TestSeedPrimaryServerIPsRejectsInvalid(t *testing.T) {
	err := seedPrimaryServerIPs(t.TempDir(), "grpc.example.invalid:5555", []string{"not-an-ip"})
	if err == nil {
		t.Fatal("expected invalid IP")
	}
}

func TestSeedPrimaryServerIPsSkipsLiteralServer(t *testing.T) {
	dir := t.TempDir()
	if err := seedPrimaryServerIPs(dir, "192.0.2.10:5555", []string{"198.51.100.4"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "endpoint-cache.json")); !os.IsNotExist(err) {
		t.Fatalf("literal server should not write cache: %v", err)
	}
}

func TestMergeCLIFromConfigUsesYAMLWhenFlagsUnchanged(t *testing.T) {
	t.Cleanup(resetAgentState)
	agentConfig = model.AgentConfig{
		Server:             "10.0.0.8:5555",
		ClientSecret:       "yaml-secret",
		TLS:                true,
		InsecureTLS:        true,
		ReportDelay:        11,
		IPReportPeriod:     90,
		IPReportInterface:  "eth0",
		CountryCode:        "CN",
		UseIPv6CountryCode: true,
	}
	agentCliParam = AgentCliParam{Server: "localhost:5555", ReportDelay: 5, IPReportPeriod: 1800}
	mergeCLIFromConfig(func(string) bool { return false })
	if agentCliParam.Server != "10.0.0.8:5555" || agentCliParam.ClientSecret != "yaml-secret" {
		t.Fatalf("connection=%#v", agentCliParam)
	}
	if !agentCliParam.TLS || !agentCliParam.InsecureTLS || agentCliParam.ReportDelay != 11 || agentCliParam.IPReportPeriod != 90 {
		t.Fatalf("runtime=%#v", agentCliParam)
	}
	if agentCliParam.IPReportInterface != "eth0" || agentCliParam.CountryCode != "CN" || !agentCliParam.UseIPv6CountryCode {
		t.Fatalf("ip report=%#v", agentCliParam)
	}
}

func TestMergeCLIFromConfigKeepsChangedFlags(t *testing.T) {
	t.Cleanup(resetAgentState)
	agentConfig = model.AgentConfig{Server: "from-yaml:5555", ClientSecret: "yaml-secret", TLS: true}
	agentCliParam = AgentCliParam{Server: "from-cli:5555", ClientSecret: "cli-secret", TLS: false}
	mergeCLIFromConfig(func(name string) bool { return name == "server" || name == "password" || name == "tls" })
	if agentCliParam.Server != "from-cli:5555" || agentCliParam.ClientSecret != "cli-secret" || agentCliParam.TLS {
		t.Fatalf("cli should win: %#v", agentCliParam)
	}
}

func TestUnixUninstallWrapperExecsPurgeWithoutSecrets(t *testing.T) {
	script := unixUninstallWrapper("/opt/santaizi/agent/santaizi-agent", "/etc/santaizi/agent.yaml")
	if !strings.Contains(script, "uninstall --purge --config") {
		t.Fatalf("script=%s", script)
	}
	if strings.Contains(script, "client_secret") || strings.Contains(script, " -p ") || strings.Contains(script, " -s ") {
		t.Fatalf("wrapper leaked secret flags: %s", script)
	}
	if !strings.HasPrefix(script, "#!/bin/sh\n") {
		t.Fatalf("missing shebang: %s", script[:20])
	}
}

func TestIsManagedInstallDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		if !isManagedInstallDir(windowsAgentDir()) {
			t.Fatalf("windows agent dir should be managed")
		}
		if isManagedInstallDir(`C:\Windows\Temp`) {
			t.Fatal("temp should not be managed")
		}
		return
	}
	if !isManagedInstallDir("/opt/santaizi/agent") {
		t.Fatal("unix agent dir should be managed")
	}
	if isManagedInstallDir("/tmp/santaizi-agent") || isManagedInstallDir("/usr/local/bin") {
		t.Fatal("non-install paths must not be deleted")
	}
}
