//go:build !windows

package main

import (
	"errors"
	"os"
)

func requireAdmin() error {
	if os.Geteuid() != 0 {
		return errors.New("请使用 root 运行 santaizi-agent-uninstall")
	}
	return nil
}
