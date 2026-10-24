# OwlCron

[English](README.md) | 简体中文

通过 Web 界面调度任务、执行插件，并查看执行历史与日志。

[![Build and Test](https://github.com/soulteary/owlcron/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/soulteary/owlcron/actions/workflows/ci.yml)
[![Browser Tests](https://github.com/soulteary/owlcron/actions/workflows/browser.yml/badge.svg?branch=main)](https://github.com/soulteary/owlcron/actions/workflows/browser.yml)
[![Container and Deployment](https://github.com/soulteary/owlcron/actions/workflows/container.yml/badge.svg?branch=main)](https://github.com/soulteary/owlcron/actions/workflows/container.yml)
[![Go Test Report](https://github.com/soulteary/owlcron/actions/workflows/go-quality-reports.yml/badge.svg)](https://github.com/soulteary/owlcron/actions/workflows/go-quality-reports.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/soulteary/owlcron)](https://goreportcard.com/report/github.com/soulteary/owlcron)
[![Release](https://img.shields.io/github/v/release/soulteary/owlcron)](https://github.com/soulteary/owlcron/releases)
[![Go](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go&logoColor=white)](docs/zh-CN/releases.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

OwlCron 是一个使用 Go 编写、以替代 [Cronicle](https://github.com/jhuckaby/Cronicle) 为目标的任务调度服务，提供 Web 管理界面、定时和按需任务、插件执行、任务日志及通知，并对 Cronicle 的部分工作流、API 和插件约定提供兼容。迁移范围见 [Cronicle 兼容性](docs/zh-CN/compatibility.md)。

项目目前处于实验性阶段。集群持续收敛、远程派发确认和生产环境容量仍有[已知限制](docs/zh-CN/limitations.md)，请先在隔离环境验证。

![OwlCron](.github/assets/owlcron-banner.png)

## 主要功能

- **管理任务**：通过 Web 界面配置定时任务和按需执行任务。
- **执行插件**：使用示例 Echo Plugin 验证流程，再接入自己的脚本；见[运行脚本文件](docs/zh-CN/script-files.md)。
- **排查执行问题**：查看任务历史、日志和通知，结合[运维与指标](docs/zh-CN/operations.md)进行诊断。
- **评估 Cronicle 迁移**：先运行[迁移预检](docs/zh-CN/migration.md)，再核对[兼容范围](docs/zh-CN/compatibility.md)。

## 选择安装方式

- **从源码开始**：按下方步骤构建并运行，适合本地体验和开发。
- **使用发布归档**：阅读[版本与发布](docs/zh-CN/releases.md)，了解支持平台、构建信息和归档校验方法；可用版本见 [GitHub Releases](https://github.com/soulteary/owlcron/releases)。
- **容器与 Kubernetes**：参照[容器部署指南](docs/zh-CN/containers.md)准备配置和持久化数据。

## 快速开始

需要 Go、Bun 1.4.2、Python 3 和 make。模块默认建议 Go 1.27.3；使用其他工具链前请阅读[部署要求](docs/zh-CN/deployment.md)。

```sh
git clone https://github.com/soulteary/owlcron.git
cd owlcron
bun install --cwd htdocs-deps --frozen-lockfile
make frontend build
cp config.json config.local.json
chmod 600 config.local.json
```

编辑 `config.local.json`，将 `secret_key` 替换为至少 32 字节的随机秘密，然后初始化并启动：

```sh
./owlcron-engine -config config.local.json setup
./owlcron-engine -config config.local.json
```

首次启动空用户数据库时，程序生成管理员凭据并保存到 `data/bootstrap-admin.json`。通过受控终端读取后，访问 `http://localhost:3012` 登录。也可在首次启动前注入管理员凭据，方法见[部署指南](docs/zh-CN/deployment.md#安装和启动)。

初始化会提供 General 分类和 Echo Plugin 示例插件。可以创建使用该插件的本机事件，手动执行后查看历史与日志；`make smoke` 会在独立临时数据目录中自动验证这条链路。

HTTP 服务默认监听所有网卡，`base_app_url` 不限制监听地址。对外部署前请限制后台端口访问并配置 HTTPS。不要将配置秘密、运行数据和日志加入版本控制。

## 文档

- [文档索引](docs/zh-CN/README.md)：按部署、运行任务、运维和开发查找完整指南。
- [Cronicle 迁移预检](docs/zh-CN/migration.md)与[兼容性](docs/zh-CN/compatibility.md)：迁移前检查数据及行为差异。
- [贡献指南](CONTRIBUTING.zh-CN.md)与[测试指南](docs/zh-CN/testing.md)：开发流程、检查命令和质量报告。
- [安全策略](SECURITY.zh-CN.md)：安全问题报告方式。

## 许可证

OwlCron 原创代码采用 [MIT License](LICENSE)。继承的 Cronicle 代码保留原作者版权和 MIT 许可。第三方代码与字体遵循各自许可证，详见[第三方声明](THIRD_PARTY_NOTICES.zh-CN.md)及 `licenses/`。
