# Santaizi Agent

三太子监控探针。部署在被监控主机上，向主面板与从端上报。

## 版权与致谢

本项目基于 [nezhahq/agent](https://github.com/nezhahq/agent)（哪吒监控探针）衍生修改，原作者版权保留。详见 [`LICENSE`](./LICENSE) 与 [`NOTICE`](./NOTICE)。

产品品牌为 **三太子 / Santaizi**；gRPC 服务为 `SantaiziService`，须与 Dashboard 成对升级。

## 可靠探测配置

默认配置文件为 `/etc/santaizi/agent.yaml`，可靠探测数据目录为 `/var/lib/santaizi-agent/`，二进制安装在 `/opt/santaizi/agent/`。`--config` 与 `--data-dir` 可覆盖默认路径。

```yaml
server: 10.0.0.10:5555
client_secret: "your-agent-secret"
tls: false
insecure_tls: false
tls_ca_file: ""

report_delay: 5

telemetry:
  data_dir: /var/lib/santaizi-agent
  state_interval: 5s
  heartbeat_interval: 10s
  host_interval: 10m
  batch_size: 256
  disabled_remote_ids: []
  collectors: []
  wal:
    segment_size_bytes: 8388608
    max_size_bytes: 268435456
    reserve_bytes: 1048576
    fsync_interval: 1s
    fsync_records: 64

capabilities:
  cpu: true
  memory: true
  disk: true
  network: true
  connections: true
  processes: true
  temperature: false
  gpu: false
  host_info: true
  ip_report: true
  http_probe: true
  icmp_probe: true
  tcp_probe: true
  nat: true
```

探针首次启动会持久化 Node UUID，每次进程启动创建新 Session；所有可靠事件先写 Segment WAL，达到 fsync 条件后才允许发送。远端从端 Assignment 与本地配置合并，每个稳定 Endpoint ID/Generation 使用独立连接和 ACK Cursor。

## 权限边界

控制流只能下发类型化 HTTP、ICMP、TCP 探测和 NAT 建链请求。NAT 数据使用独立 `SantaiziNATService/NATStream`，协议不包含通用命令、终端、文件管理或更新能力。心跳与可靠身份始终启用；其他采集及网络能力可通过 `capabilities` 或对应 CLI 参数关闭。

运行 `agent --help` 可查看完整参数。`service install` 会把面板地址、密钥、TLS 和能力开关写入配置文件（权限 `0600`）；系统服务启动参数只有 `--config`，不会把密钥放进 `ps` / `systemctl cat`。前台调试仍可用 `-s` / `-p`。`--insecure` 会跳过服务端证书校验，**仅测试**，启动时会打 Warning。

设备证书落在数据目录 `pki/{client.key,client.crt,ca.crt}`。有证后 Control 走 mTLS，不再带 `client_secret`。仅当面板返回 `Unimplemented` 才回退旧密钥认证；网络 / TLS / Unauthenticated / PermissionDenied 不会当成旧面板。


已用旧方式安装、unit 里仍带 `-p` 的实例，需要重新执行安装（或再次 `service install`）以重写服务定义。

升级（本机执行，保留配置 / 身份 / WAL）：

```bash
curl -fSL https://raw.githubusercontent.com/hi2shark/santaizi-dashboard/master/script/upgrade_agent.sh | bash
```

Windows 使用同仓 `script/upgrade.ps1`。须先升级面板；协议不兼容时走清洁安装，不要用升级脚本。

卸载：

```bash
santaizi-agent-uninstall
```

Windows 为 `C:\santaizi\santaizi-agent-uninstall.cmd`。该命令会停止并删除服务，以及程序目录、配置和数据。
