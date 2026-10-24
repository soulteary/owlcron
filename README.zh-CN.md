# OwlCron

[English](README.md) | 简体中文

OwlCron 是一个使用 Go 编写、以替代 [Cronicle](https://github.com/jhuckaby/Cronicle) 为目标的任务调度服务，提供 Web 管理界面、定时和按需任务、插件执行、任务日志及通知，并对 Cronicle 的部分工作流、API 和插件约定提供兼容。迁移范围见 [Cronicle 兼容性](docs/zh-CN/compatibility.md)。

项目目前处于实验性阶段。集群持续收敛、远程派发确认和生产环境容量仍有[已知限制](docs/zh-CN/limitations.md)，请先在隔离环境验证。

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

- [部署与首次运行](docs/zh-CN/deployment.md)：安装、HTTPS、权限、备份和升级。
- [配置与运行机制](docs/zh-CN/configuration.md)：存储、保留策略、通知和节点身份。
- [Cronicle 兼容性](docs/zh-CN/compatibility.md)：支持范围和迁移差异。
- [架构](docs/zh-CN/architecture.md)：调度、持久化、完成确认和恢复。
- [测试](docs/zh-CN/testing.md)：本地检查、CI 和故障验收。
- [已知限制](docs/zh-CN/limitations.md)：部署前需要了解的边界。
- [贡献指南](CONTRIBUTING.zh-CN.md)与[安全策略](SECURITY.zh-CN.md)。

文档也可从[中文索引](docs/zh-CN/README.md)浏览。

## 开发检查

在仓库根目录执行：

```sh
bun install --cwd htdocs-deps --frozen-lockfile
make frontend build
make fmt-check vet test frontend-test licenses-check docs-check smoke
```

`make race` 运行竞态检查；需要 Docker 的故障验收见[测试文档](docs/zh-CN/testing.md)。示例插件是独立 Go 模块，修改时还需在 `bin/echo-plugin` 中运行测试。

可选的[真实浏览器冒烟测试](tests/browser/README.zh-CN.md)在 Chromium 中覆盖登录、事件编辑和执行、历史、日志以及主题偏好持久化。

## 许可证

OwlCron 原创代码采用 [MIT License](LICENSE)。继承的 Cronicle 代码保留原作者版权和 MIT 许可。第三方代码与字体遵循各自许可证，详见[第三方声明](THIRD_PARTY_NOTICES.zh-CN.md)及 `licenses/`。
