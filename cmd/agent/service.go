package main

import (
	"log"
	"os"

	"github.com/hi2shark/santaizi-agent/pkg/util"
	"github.com/nezhahq/service"
	"github.com/spf13/cobra"
)

type program struct {
	exit    chan struct{}
	service service.Service
}

var serviceCmd = &cobra.Command{
	Use:    "service <install/uninstall/start/stop/restart>",
	Short:  "服务与自启动设置",
	Args:   cobra.ExactArgs(1),
	Run:    serviceActions,
	PreRun: servicePreRun,
}

func (p *program) Start(s service.Service) error {
	go p.run()
	return nil
}

func (p *program) Stop(s service.Service) error {
	stopRunningAgent()
	close(p.exit)
	if service.Interactive() {
		os.Exit(0)
	}
	return nil
}

func (p *program) run() {
	defer func() {
		if service.Interactive() {
			p.Stop(p.service)
		} else {
			p.service.Stop()
		}
	}()

	run()
}

func init() {
	agentCmd.AddCommand(serviceCmd)
}

func servicePreRun(cmd *cobra.Command, args []string) {
	if args[0] == "install" {
		preRun(cmd, args)
	}
}

func serviceRuntimeArguments(configPath string) []string {
	if configPath == "" {
		configPath = "/etc/santaizi/agent.yaml"
	}
	return []string{"--config", configPath}
}

func serviceActions(cmd *cobra.Command, args []string) {
	action := args[0]
	var flags []string
	if action == "install" {
		if err := persistRuntimeConfig(); err != nil {
			log.Printf("写入配置文件失败: %v", err)
			os.Exit(1)
		}
		if err := seedPrimaryServerIPs(agentCliParam.DataDir, agentCliParam.Server, agentCliParam.ServerIPs); err != nil {
			log.Printf("写入主端拨号缓存失败: %v", err)
			os.Exit(1)
		}
		flags = serviceRuntimeArguments(agentCliParam.ConfigPath)
	}
	if err := runService(action, flags); err != nil && action != "" {
		os.Exit(1)
	}
}

func runService(action string, flags []string) error {
	dir, err := os.Getwd()
	if err != nil {
		printf("获取当前工作目录失败: %v", err)
		return err
	}
	config := &service.Config{
		Name: "santaizi-agent", DisplayName: "Santaizi Agent", Description: "三太子探针监控端",
		Arguments: flags, WorkingDirectory: dir, Option: map[string]interface{}{"OnFailure": "restart"},
	}
	program := &program{exit: make(chan struct{})}
	installedService, err := service.New(program, config)
	if err != nil {
		printf("创建系统服务失败，以前台模式运行: %v", err)
		run()
		return err
	}
	program.service = installedService
	if agentConfig.Debug {
		serviceLogger, loggerErr := installedService.Logger(nil)
		if loggerErr != nil {
			printf("获取系统服务日志器失败: %v", loggerErr)
		} else {
			util.Logger = serviceLogger
		}
	}
	if action == "install" {
		println("Init system is:", installedService.Platform())
	}
	if action != "" {
		if err := service.Control(installedService, action); err != nil {
			log.Print(err)
			return err
		}
		if action == "install" {
			if err := installUninstallWrapper(); err != nil {
				printf("注册卸载命令失败（服务已安装）: %v", err)
			}
		}
		return nil
	}
	if err := installedService.Run(); err != nil {
		util.Logger.Error(err)
		return err
	}
	return nil
}
