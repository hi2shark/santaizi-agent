package dialcache

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestOpenMissingFileIsEmpty(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if got := store.Get(PrimaryKey, "grpc.example.invalid", "5555"); len(got) != 0 {
		t.Fatalf("empty cache = %v", got)
	}
}

func TestPutGetAndHostMismatch(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(PrimaryKey, "grpc.example.invalid", "5555", []string{"192.0.2.10", "192.0.2.10"}); err != nil {
		t.Fatal(err)
	}
	if got := store.Get(PrimaryKey, "grpc.example.invalid", "5555"); len(got) != 1 || got[0] != "192.0.2.10" {
		t.Fatalf("get = %v", got)
	}
	if got := store.Get(PrimaryKey, "other.example.invalid", "5555"); len(got) != 0 {
		t.Fatalf("host mismatch should miss: %v", got)
	}
	if got := store.Get(PrimaryKey, "grpc.example.invalid", "443"); len(got) != 0 {
		t.Fatalf("port mismatch should miss: %v", got)
	}
	reloaded, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Get(PrimaryKey, "GRPC.example.invalid", "5555"); len(got) != 1 || got[0] != "192.0.2.10" {
		t.Fatalf("reload = %v", got)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(dir, FileName))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("perm = %o", info.Mode().Perm())
		}
	}
}

func TestSeedDoesNotOverwriteMatchingEntry(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(PrimaryKey, "grpc.example.invalid", "5555", []string{"192.0.2.10"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Seed(PrimaryKey, "grpc.example.invalid", "5555", []string{"198.51.100.4"}); err != nil {
		t.Fatal(err)
	}
	if got := store.Get(PrimaryKey, "grpc.example.invalid", "5555"); len(got) != 1 || got[0] != "192.0.2.10" {
		t.Fatalf("seed overwrote: %v", got)
	}
	if err := store.Seed(PrimaryKey, "new.example.invalid", "5555", []string{"198.51.100.4"}); err != nil {
		t.Fatal(err)
	}
	if got := store.Get(PrimaryKey, "new.example.invalid", "5555"); len(got) != 1 || got[0] != "198.51.100.4" {
		t.Fatalf("seed new host = %v", got)
	}
}

func TestPutRejectsInvalidIP(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(PrimaryKey, "grpc.example.invalid", "5555", []string{"not-an-ip"}); err == nil {
		t.Fatal("expected invalid IP")
	}
}

func TestPlanLiteralIPSkipsCache(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(PrimaryKey, "192.0.2.10", "5555", []string{"198.51.100.4"}); err != nil {
		t.Fatal(err)
	}
	targets, err := Plan(context.Background(), store, PrimaryKey, "192.0.2.10:5555")
	if err != nil || len(targets) != 1 {
		t.Fatalf("targets=%v err=%v", targets, err)
	}
	if targets[0].FromDNS || targets[0].IP != "192.0.2.10" || targets[0].DialAddr != "192.0.2.10:5555" {
		t.Fatalf("literal = %#v", targets[0])
	}
	if targets[0].Authority != "192.0.2.10" || targets[0].ServerName != "192.0.2.10" {
		t.Fatalf("literal authority = %#v", targets[0])
	}
}

func TestPlanPrefersDNSThenCache(t *testing.T) {
	original := lookupIPAddr
	t.Cleanup(func() { lookupIPAddr = original })
	lookupIPAddr = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("192.0.2.20")}}, nil
	}
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(PrimaryKey, "grpc.example.invalid", "5555", []string{"198.51.100.4", "192.0.2.20"}); err != nil {
		t.Fatal(err)
	}
	targets, err := Plan(context.Background(), store, PrimaryKey, "grpc.example.invalid:5555")
	if err != nil || len(targets) != 2 {
		t.Fatalf("targets=%v err=%v", targets, err)
	}
	if !targets[0].FromDNS || targets[0].IP != "192.0.2.20" {
		t.Fatalf("dns first = %#v", targets[0])
	}
	if targets[1].FromDNS || targets[1].IP != "198.51.100.4" {
		t.Fatalf("cache second = %#v", targets[1])
	}
	for i, target := range targets {
		if target.Authority != "grpc.example.invalid" || target.ServerName != "grpc.example.invalid" {
			t.Fatalf("target[%d] authority = %#v", i, target)
		}
	}
}

func TestPlanDNSFailureUsesCache(t *testing.T) {
	original := lookupIPAddr
	t.Cleanup(func() { lookupIPAddr = original })
	lookupIPAddr = func(context.Context, string) ([]net.IPAddr, error) {
		return nil, errors.New("dig failed")
	}
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Seed(PrimaryKey, "grpc.example.invalid", "5555", []string{"192.0.2.10"}); err != nil {
		t.Fatal(err)
	}
	targets, err := Plan(context.Background(), store, PrimaryKey, "grpc.example.invalid:5555")
	if err != nil || len(targets) != 1 || targets[0].FromDNS || targets[0].IP != "192.0.2.10" {
		t.Fatalf("cache fallback = %v err=%v", targets, err)
	}
}

func TestPlanDNSFailureWithoutCache(t *testing.T) {
	original := lookupIPAddr
	t.Cleanup(func() { lookupIPAddr = original })
	lookupIPAddr = func(context.Context, string) ([]net.IPAddr, error) {
		return nil, errors.New("dig failed")
	}
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Plan(context.Background(), store, PrimaryKey, "grpc.example.invalid:5555"); err == nil {
		t.Fatal("expected resolve error")
	}
}

func TestOpenIgnoresCorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(PrimaryKey, "grpc.example.invalid", "5555", []string{"192.0.2.10"}); err != nil {
		t.Fatal(err)
	}
	var loaded file
	raw, err := os.ReadFile(filepath.Join(dir, FileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &loaded); err != nil || loaded.Endpoints[PrimaryKey].IPs[0] != "192.0.2.10" {
		t.Fatalf("rewritten=%s err=%v", raw, err)
	}
}

func TestPlanIPv6LiteralAuthorityOmitsPort(t *testing.T) {
	targets, err := Plan(context.Background(), nil, PrimaryKey, "[2001:db8::10]:5555")
	if err != nil || len(targets) != 1 {
		t.Fatalf("targets=%v err=%v", targets, err)
	}
	if targets[0].DialAddr != "[2001:db8::10]:5555" || targets[0].Authority != "2001:db8::10" || targets[0].ServerName != "2001:db8::10" {
		t.Fatalf("ipv6 = %#v", targets[0])
	}
}

func TestCollectorKey(t *testing.T) {
	if CollectorKey(" abc ") != "collector:abc" {
		t.Fatalf("key=%s", CollectorKey(" abc "))
	}
}
