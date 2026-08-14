# 安全策略

## 受支持的版本

仅 `main` 分支与最新 SemVer Release。旧 tag 不回溯修复。

探针与主面板（[`santaizi-dashboard`](https://github.com/hi2shark/santaizi-dashboard)）线协议不兼容旧版，须成对升级。

## 报告漏洞

请通过 [GitHub 私密安全公告](https://github.com/hi2shark/santaizi-agent/security/advisories/new) 提交，**不要**用公开 Issue 或 PR 披露。

请附复现步骤、探针版本与运行平台。报告中请勿包含真实主机地址、`client_secret` 或私钥。

修复发布前请勿公开细节。

## 不在范围内

* 探针以 root 运行、配置文件权限被放宽等部署选择造成的后果
* 上游 [nezhahq/agent](https://github.com/nezhahq/agent) 未经本项目修改的代码 —— 请报给上游
* 第三方依赖自身漏洞 —— 请报给对应项目；如本项目未及时升级，可另行提 Issue
