package main

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/hi2shark/santaizi-agent/pkg/dialcache"
	"google.golang.org/grpc"
)

type dialAttempt struct {
	remembered bool
	detach     bool
	rememberFn func()
}

func (a *dialAttempt) Remember() {
	if a == nil || a.remembered {
		return
	}
	a.remembered = true
	if a.rememberFn != nil {
		a.rememberFn()
	}
}

func (a *dialAttempt) Detach() {
	if a != nil {
		a.detach = true
	}
}

func (m *telemetryManager) tryDials(ctx context.Context, key, address string, options []grpc.DialOption, fn func(*grpc.ClientConn, *dialAttempt) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	targets, err := dialcache.Plan(ctx, m.dialCache, key, address)
	if err != nil {
		return err
	}
	var lastErr error
	for _, target := range targets {
		opts := append(append([]grpc.DialOption{}, options...), grpc.WithAuthority(target.Authority))
		conn, dialErr := grpc.NewClient(target.DialAddr, opts...)
		if dialErr != nil {
			lastErr = dialErr
			continue
		}
		attempt := &dialAttempt{rememberFn: func() {
			if !target.FromDNS || m.dialCache == nil {
				return
			}
			if putErr := m.dialCache.Put(key, target.Host, target.Port, []string{target.IP}); putErr != nil {
				printf("写入拨号缓存失败: %v", putErr)
			}
		}}
		fnErr := fn(conn, attempt)
		if !attempt.detach {
			_ = conn.Close()
		}
		if attempt.remembered || attempt.detach || fnErr == nil {
			return fnErr
		}
		lastErr = fnErr
	}
	if lastErr == nil {
		lastErr = errors.New("no dial target")
	}
	return lastErr
}

func seedPrimaryServerIPs(dataDir, server string, ips []string) error {
	if len(ips) == 0 {
		return nil
	}
	host, port, err := dialcache.SplitHostPort(server)
	if err != nil {
		return err
	}
	if net.ParseIP(host) != nil {
		return nil
	}
	store, err := dialcache.Open(dataDir)
	if err != nil {
		return err
	}
	if err := store.Seed(dialcache.PrimaryKey, host, port, ips); err != nil {
		return fmt.Errorf("seed primary dial cache: %w", err)
	}
	return nil
}

func serverNameOf(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}
	return host
}
