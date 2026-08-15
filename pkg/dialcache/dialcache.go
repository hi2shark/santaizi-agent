package dialcache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hi2shark/santaizi-agent/pkg/util"
)

const (
	FileName   = "endpoint-cache.json"
	PrimaryKey = "primary"
	fileMode   = 0600
	version    = 1
)

var lookupIPAddr = func(ctx context.Context, host string) ([]net.IPAddr, error) {
	return net.DefaultResolver.LookupIPAddr(ctx, host)
}

func CollectorKey(endpointID string) string {
	return "collector:" + strings.TrimSpace(endpointID)
}

type Store struct {
	mu   sync.Mutex
	path string
	data file
}

type file struct {
	Version   int              `json:"version"`
	Endpoints map[string]Entry `json:"endpoints"`
}

type Entry struct {
	Host          string   `json:"host"`
	Port          string   `json:"port"`
	IPs           []string `json:"ips"`
	UpdatedAtUnix int64    `json:"updated_at_unix"`
}

type Target struct {
	DialAddr   string
	Authority  string
	ServerName string
	Host       string
	Port       string
	IP         string
	FromDNS    bool
}

func Open(dataDir string) (*Store, error) {
	if strings.TrimSpace(dataDir) == "" {
		return nil, errors.New("dial cache data directory is empty")
	}
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("create dial cache directory: %w", err)
	}
	store := &Store{path: filepath.Join(dataDir, FileName), data: file{Version: version, Endpoints: map[string]Entry{}}}
	raw, err := os.ReadFile(store.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return store, nil
		}
		return nil, err
	}
	var loaded file
	if json.Unmarshal(raw, &loaded) != nil || loaded.Endpoints == nil {
		return store, nil
	}
	if loaded.Version == 0 {
		loaded.Version = version
	}
	store.data = loaded
	return store, nil
}

func (s *Store) Get(key, host, port string) []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.data.Endpoints[key]
	if !ok || !sameEndpoint(entry.Host, entry.Port, host, port) {
		return nil
	}
	return append([]string(nil), entry.IPs...)
}

func (s *Store) Put(key, host, port string, ips []string) error {
	if s == nil {
		return errors.New("dial cache is not open")
	}
	normalized, err := normalizeIPs(ips)
	if err != nil {
		return err
	}
	if len(normalized) == 0 {
		return errors.New("dial cache IPs are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.Endpoints == nil {
		s.data.Endpoints = map[string]Entry{}
	}
	s.data.Version = version
	s.data.Endpoints[key] = Entry{Host: host, Port: port, IPs: normalized, UpdatedAtUnix: time.Now().Unix()}
	return s.writeLocked()
}

func (s *Store) Seed(key, host, port string, ips []string) error {
	if len(s.Get(key, host, port)) > 0 {
		return nil
	}
	return s.Put(key, host, port, ips)
}

func Plan(ctx context.Context, store *Store, key, address string) ([]Target, error) {
	host, port, err := SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	authority := net.JoinHostPort(host, port)
	if parsed := net.ParseIP(host); parsed != nil {
		ip := parsed.String()
		return []Target{{
			DialAddr: authority, Authority: authority, ServerName: host,
			Host: host, Port: port, IP: ip,
		}}, nil
	}
	var targets []Target
	seen := map[string]bool{}
	if ips, lookupErr := LookupIPs(ctx, host); lookupErr == nil {
		for _, ip := range ips {
			seen[ip] = true
			targets = append(targets, Target{
				DialAddr: net.JoinHostPort(ip, port), Authority: authority, ServerName: host,
				Host: host, Port: port, IP: ip, FromDNS: true,
			})
		}
	} else {
		err = lookupErr
	}
	for _, ip := range store.Get(key, host, port) {
		if seen[ip] {
			continue
		}
		seen[ip] = true
		targets = append(targets, Target{
			DialAddr: net.JoinHostPort(ip, port), Authority: authority, ServerName: host,
			Host: host, Port: port, IP: ip,
		})
	}
	if len(targets) == 0 {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("cannot resolve %s", host)
	}
	return targets, nil
}

func LookupIPs(ctx context.Context, host string) ([]string, error) {
	if parsed := net.ParseIP(host); parsed != nil {
		return []string{parsed.String()}, nil
	}
	addresses, err := lookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	ips, normErr := normalizeIPAddrs(addresses)
	if normErr != nil {
		return nil, normErr
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("cannot resolve %s", host)
	}
	return ips, nil
}

func SplitHostPort(address string) (string, string, error) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return "", "", fmt.Errorf("dial address %q: %w", address, err)
	}
	if host == "" || port == "" {
		return "", "", fmt.Errorf("dial address %q is missing host or port", address)
	}
	return host, port, nil
}

func sameEndpoint(cachedHost, cachedPort, host, port string) bool {
	return strings.EqualFold(cachedHost, host) && cachedPort == port
}

func normalizeIPs(ips []string) ([]string, error) {
	seen := map[string]bool{}
	out := make([]string, 0, len(ips))
	for _, raw := range ips {
		ip := net.ParseIP(strings.TrimSpace(raw))
		if ip == nil {
			return nil, fmt.Errorf("invalid IP %q", raw)
		}
		if !usableIP(ip) {
			continue
		}
		value := ip.String()
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out, nil
}

func normalizeIPAddrs(addresses []net.IPAddr) ([]string, error) {
	ips := make([]string, 0, len(addresses))
	for _, address := range addresses {
		if address.IP == nil {
			continue
		}
		ips = append(ips, address.IP.String())
	}
	return normalizeIPs(ips)
}

func usableIP(ip net.IP) bool {
	return ip != nil && !ip.IsUnspecified() && !ip.IsMulticast()
}

func (s *Store) writeLocked() error {
	s.data.Version = version
	payload, err := json.Marshal(s.data)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".endpoint-cache-*.tmp")
	if err != nil {
		return fmt.Errorf("create dial cache temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(fileMode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("install dial cache file: %w", err)
	}
	return util.SyncDir(dir)
}
