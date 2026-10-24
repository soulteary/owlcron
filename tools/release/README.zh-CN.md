# OwlCron 二进制发行包

[English](README.md) | 简体中文

本压缩包包含 `owlcron-engine`、`owlcron-script`、Echo Plugin、前端资源、
示例配置、构建元数据、中英文[发行说明](docs/zh-CN/release-notes.md)及第三方许可。请在此目录中执行命令，确保服务可以找到
前端资源和插件。

Linux 需要系统 CA 证书和 IANA 时区数据；服务的 `PATH` 中应能找到 `procps` 的
`ps`，用于 CPU/内存监控和任务资源限制。Debian/Ubuntu 应安装
`ca-certificates tzdata procps`。没有监控工具时普通任务成功，不代表资源限制
已经验证。自定义脚本也需要解释器，例如 `/bin/sh`。

```sh
cp config.json config.local.json
chmod 600 config.local.json
```

编辑 `config.local.json`，将 `secret_key` 设为新生成的至少 32 字节随机密钥；独立单节点应设置
`udp_broadcast_port: -1` 以关闭发现，并为数据、日志和队列选择归服务账号所有的
持久化目录。将配置文件权限设为 600，再初始化并启动：

```sh
./owlcron-engine version
./owlcron-engine --config config.local.json setup
./owlcron-engine --config config.local.json
```

使用空存储首次启动后，在可信终端中读取配置的存储目录下 `bootstrap-admin.json`
中的自动生成管理员凭据。确认登录成功后删除此文件。不要将真实密码写入命令参数
或命令历史。
数据和日志目录需要可写并限制访问，升级前做好备份，对外服务应限制访问并配置
HTTPS。示例配置不代表已经完成生产环境安全加固。

请阅读[部署文档](https://github.com/soulteary/owlcron/blob/main/docs/zh-CN/deployment.md)
和[发行政策](https://github.com/soulteary/owlcron/blob/main/docs/zh-CN/releases.md)。
项目仍处于实验阶段，不应假设可以跨版本回滚数据。下载后先验证压缩包旁的
`.sha256` 文件，再解压运行。`BUILD.json` 记录版本、源码提交、目标平台和 Go
工具链。分发时保留 [LICENSE](LICENSE)、[第三方声明](THIRD_PARTY_NOTICES.zh-CN.md)
及 `licenses/`。

首版面向独立单节点部署，集群行为仍属于实验能力。服务本身不需要 Go、Bun
或 Node.js；自定义插件仍需安装各自的运行依赖。Cronicle 迁移预检器是需要
单独从源码构建的工具，不包含在此压缩包中。
