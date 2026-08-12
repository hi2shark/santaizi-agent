package monitor

import (
	"testing"

	"github.com/hi2shark/santaizi-agent/model"
)

func TestConfigureIPReportSetsCountryAndNICAllowlist(t *testing.T) {
	cfg := &model.AgentConfig{Capabilities: model.DefaultCapabilities()}
	InitConfig(cfg)
	ConfigureIPReport("eth0", " CN ")
	if CachedCountryCode != "CN" {
		t.Fatalf("country=%q", CachedCountryCode)
	}
	if !cfg.NICAllowlist["eth0"] || len(cfg.NICAllowlist) != 1 {
		t.Fatalf("allowlist=%v", cfg.NICAllowlist)
	}
	host := GetHost()
	if host.CountryCode != "CN" {
		t.Fatalf("host country=%q", host.CountryCode)
	}
	ConfigureIPReport("", "")
	if CachedCountryCode != "" {
		t.Fatalf("expected cleared country, got %q", CachedCountryCode)
	}
}
