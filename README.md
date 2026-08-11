# Santaizi Agent

Agent（探针）of 三太子监控 · Santaizi Monitoring

## 版权与致谢

本项目基于 [nezhahq/agent](https://github.com/nezhahq/agent)（哪吒监控探针）衍生修改，原作者版权保留。详见 [`LICENSE`](./LICENSE) 与 [`NOTICE`](./NOTICE)。

产品品牌为 **三太子 / Santaizi**；gRPC 服务为 `SantaiziService`，须与 Dashboard 成对升级。

## 可靠遥测配置

默认配置文件为 `/etc/santaizi/agent.yaml`，可靠遥测数据目录为 `/var/lib/santaizi-agent/`，二进制安装在 `/opt/santaizi/agent/`。`--config` 与 `--data-dir` 可覆盖默认路径。

```yaml
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

Agent 首次启动持久化 Node UUID，每次进程启动创建新 Session；所有可靠事件先写 Segment WAL，达到 fsync 条件后才允许发送。远端 Collector Assignment 与本地配置合并，每个稳定 Endpoint ID/Generation 使用独立连接和 ACK Cursor。

## 权限边界

控制流只能下发类型化 HTTP、ICMP、TCP 探测和 NAT 建链请求。NAT 数据使用独立 `SantaiziNATService/NATStream`，协议不包含通用命令、终端、文件管理或更新能力。心跳与可靠身份始终启用；其他采集及网络能力可通过 `capabilities` 或对应 CLI 参数关闭。

运行 `agent --help` 可查看完整参数。安装系统服务时传入的能力参数会写入服务启动参数，例如 `--disable-cpu`、`--disable-http-probe`、`--disable-nat`、`--temperature` 和 `--gpu`。

## Contributors

<!--GAMFC_DELIMITER--><a href="https://github.com/naiba" title="naiba"><img src="https://avatars.githubusercontent.com/u/29243953?v=4" width="50;" alt="naiba"/></a>
<a href="https://github.com/uubulb" title="UUBulb"><img src="https://avatars.githubusercontent.com/u/35923940?v=4" width="50;" alt="UUBulb"/></a>
<a href="https://github.com/funnyzak" title="Leon"><img src="https://avatars.githubusercontent.com/u/2562087?v=4" width="50;" alt="Leon"/></a>
<a href="https://github.com/zhangnew" title="zhangnew"><img src="https://avatars.githubusercontent.com/u/9146834?v=4" width="50;" alt="zhangnew"/></a>
<a href="https://github.com/AEnjoy" title="AEnjoy"><img src="https://avatars.githubusercontent.com/u/37976919?v=4" width="50;" alt="AEnjoy"/></a>
<a href="https://github.com/wwng2333" title=":D"><img src="https://avatars.githubusercontent.com/u/17147265?v=4" width="50;" alt=":D"/></a>
<a href="https://github.com/DarcJC" title="Darc Z."><img src="https://avatars.githubusercontent.com/u/53445798?v=4" width="50;" alt="Darc Z."/></a>
<a href="https://github.com/xykt" title="xykt"><img src="https://avatars.githubusercontent.com/u/152045469?v=4" width="50;" alt="xykt"/></a>
<a href="https://github.com/Erope" title="卖女孩的小火柴"><img src="https://avatars.githubusercontent.com/u/44471469?v=4" width="50;" alt="卖女孩的小火柴"/></a>
<a href="https://github.com/liuran001" title="Chisato22"><img src="https://avatars.githubusercontent.com/u/32791471?v=4" width="50;" alt="Chisato22"/></a><!--GAMFC_DELIMITER_END-->
