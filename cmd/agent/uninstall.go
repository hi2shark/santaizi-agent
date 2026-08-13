package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/hi2shark/santaizi-agent/model"
	"github.com/spf13/cobra"
)

var uninstallPurge bool

var uninstallCmd = &cobra.Command{
	Use:   "uninstall",
	Short: "卸载探针服务、配置与数据",
	Args:  cobra.NoArgs,
	Run:   runUninstall,
}

func init() {
	uninstallCmd.Flags().BoolVar(&uninstallPurge, "purge", false, "删除服务、配置、身份与数据目录")
	agentCmd.AddCommand(uninstallCmd)
}

func runUninstall(cmd *cobra.Command, args []string) {
	if !uninstallPurge {
		printf("请使用 uninstall --purge，或运行 santaizi-agent-uninstall")
		os.Exit(1)
	}
	if err := requireAdmin(); err != nil {
		printf("%v", err)
		os.Exit(1)
	}
	if err := purgeAgent(); err != nil {
		printf("卸载失败: %v", err)
		os.Exit(1)
	}
	println("Santaizi Agent 已卸载。")
}

func purgeAgent() error {
	configPath := agentCliParam.ConfigPath
	configPaths := uniqueNonEmpty(platformDefaultConfigPath(), configPath)
	dataDirs := uniqueNonEmpty(platformDefaultDataDir())

	var loaded model.AgentConfig
	if err := loaded.Read(configPath); err == nil && strings.TrimSpace(loaded.Telemetry.DataDir) != "" {
		dataDirs = uniqueNonEmpty(append(dataDirs, loaded.Telemetry.DataDir)...)
	}

	if err := runService("uninstall", nil); err != nil {
		printf("移除系统服务: %v", err)
	}

	for _, path := range configPaths {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			printf("删除配置 %s 失败: %v", path, err)
		}
	}
	for _, dir := range dataDirs {
		if err := os.RemoveAll(dir); err != nil {
			printf("删除数据目录 %s 失败: %v", dir, err)
		}
	}

	if exe, err := os.Executable(); err == nil {
		dir := filepath.Clean(filepath.Dir(exe))
		if isManagedInstallDir(dir) {
			if err := os.RemoveAll(dir); err != nil {
				printf("删除程序目录 %s 失败: %v", dir, err)
			}
			_ = os.Remove(filepath.Dir(dir))
		}
	}

	removeLeftoverUnitFiles()
	for _, wrapper := range uninstallWrapperCandidates() {
		_ = os.Remove(wrapper)
	}
	return nil
}

func installUninstallWrapper() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	path := uninstallWrapperPath()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	mode := os.FileMode(0755)
	if runtime.GOOS == "windows" {
		mode = 0644
	}
	return os.WriteFile(path, []byte(uninstallWrapperContents(exe, agentCliParam.ConfigPath)), mode)
}

func uninstallWrapperContents(exe, configPath string) string {
	if runtime.GOOS == "windows" {
		return windowsUninstallWrapper(exe, configPath)
	}
	return unixUninstallWrapper(exe, configPath)
}

func unixUninstallWrapper(exe, configPath string) string {
	return fmt.Sprintf(`#!/bin/sh
set -eu
BIN=%s
CONFIG=%s
if [ -x "$BIN" ]; then
  exec "$BIN" uninstall --purge --config "$CONFIG"
fi
if command -v systemctl >/dev/null 2>&1; then
  systemctl stop santaizi-agent >/dev/null 2>&1 || true
  systemctl disable santaizi-agent >/dev/null 2>&1 || true
  rm -f /etc/systemd/system/santaizi-agent.service /lib/systemd/system/santaizi-agent.service
  systemctl daemon-reload >/dev/null 2>&1 || true
fi
if command -v launchctl >/dev/null 2>&1; then
  launchctl bootout system/santaizi-agent >/dev/null 2>&1 || true
  rm -f /Library/LaunchDaemons/santaizi-agent.plist
fi
rm -rf /opt/santaizi/agent /var/lib/santaizi-agent
rm -f /etc/santaizi/agent.yaml
rmdir /opt/santaizi >/dev/null 2>&1 || true
rm -f /usr/local/bin/santaizi-agent-uninstall /usr/bin/santaizi-agent-uninstall
echo "Santaizi Agent uninstalled."
`, shellSingleQuote(exe), shellSingleQuote(configPath))
}

func windowsUninstallWrapper(exe, configPath string) string {
	return fmt.Sprintf("@echo off\r\nsetlocal\r\nset \"BIN=%s\"\r\nset \"CONFIG=%s\"\r\nif exist \"%%BIN%%\" (\r\n  \"%%BIN%%\" uninstall --purge --config \"%%CONFIG%%\"\r\n)\r\nrmdir /s /q \"%s\" 2>nul\r\nrmdir /s /q \"%s\" 2>nul\r\ndel /f /q \"%s\" 2>nul\r\necho Santaizi Agent uninstalled.\r\n",
		exe, configPath, windowsAgentDir(), windowsDefaultDataDir(), windowsDefaultConfigPath())
}

func uninstallWrapperPath() string {
	if runtime.GOOS == "windows" {
		return filepath.Join(windowsAgentDir(), "santaizi-agent-uninstall.cmd")
	}
	if info, err := os.Stat("/usr/local/bin"); err == nil && info.IsDir() {
		return "/usr/local/bin/santaizi-agent-uninstall"
	}
	return "/usr/bin/santaizi-agent-uninstall"
}

func uninstallWrapperCandidates() []string {
	if runtime.GOOS == "windows" {
		return []string{filepath.Join(windowsAgentDir(), "santaizi-agent-uninstall.cmd")}
	}
	return []string{"/usr/local/bin/santaizi-agent-uninstall", "/usr/bin/santaizi-agent-uninstall"}
}

func platformDefaultConfigPath() string {
	if runtime.GOOS == "windows" {
		return windowsDefaultConfigPath()
	}
	return "/etc/santaizi/agent.yaml"
}

func platformDefaultDataDir() string {
	if runtime.GOOS == "windows" {
		return windowsDefaultDataDir()
	}
	return "/var/lib/santaizi-agent"
}

func windowsSystemDrive() string {
	drive := os.Getenv("SystemDrive")
	if drive == "" {
		drive = "C:"
	}
	return drive
}

func windowsAgentDir() string {
	return filepath.Join(windowsSystemDrive(), "santaizi")
}

func windowsProgramData() string {
	if value := os.Getenv("ProgramData"); value != "" {
		return value
	}
	return filepath.Join(windowsSystemDrive(), "ProgramData")
}

func windowsDefaultConfigPath() string {
	return filepath.Join(windowsProgramData(), "santaizi", "agent.yaml")
}

func windowsDefaultDataDir() string {
	return filepath.Join(windowsProgramData(), "santaizi-agent")
}

func isManagedInstallDir(dir string) bool {
	dir = filepath.Clean(dir)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(dir, windowsAgentDir())
	}
	return dir == "/opt/santaizi/agent"
}

func removeLeftoverUnitFiles() {
	for _, path := range []string{
		"/etc/systemd/system/santaizi-agent.service",
		"/lib/systemd/system/santaizi-agent.service",
		"/Library/LaunchDaemons/santaizi-agent.plist",
	} {
		_ = os.Remove(path)
	}
}

func shellSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func uniqueNonEmpty(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := value
		if runtime.GOOS == "windows" {
			key = strings.ToLower(filepath.Clean(value))
		} else {
			key = filepath.Clean(value)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}
