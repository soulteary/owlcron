# 文档

[English](../README.md) | 简体中文

快速开始见[项目 README](../../README.zh-CN.md)。英文为默认版本，每个页面顶部都可以切换到对应的简体中文或英文页面。

## 部署与升级

- [部署](deployment.md)：安装、凭据、HTTPS、备份和升级。
- [容器与 Kubernetes](containers.md)
- [配置](configuration.md)：存储、日志、保留策略、通知和集群身份。
- [版本和发行](releases.md)：支持的工具链、发行包验证和草稿发布流程。

## 运行任务与迁移

- [运行脚本文件](script-files.md)
- [迁移预检](migration.md)：只读检查 Cronicle 导出。
- [兼容性](compatibility.md)：Cronicle 兼容范围及迁移到 OwlCron 的要求。

## 账号

- [账号邮件策略](configuration.md#账号邮件策略)：开关、配置和限额。
- [邮箱验证与密码找回](operations.md#账号邮件与密码找回)：用户操作、投递故障和恢复。

## 运维与可靠性

- [运维与指标](operations.md)：采集、告警和恢复流程。
- [已知限制](limitations.md)：可靠性边界及不支持的场景。
- [架构](architecture.md)：模块职责、持久化与恢复。
- [持久投递队列](delivery-queue.md)：存储与投递契约、故障恢复和拆分边界。

## 参与开发

- [测试](testing.md)：开发检查、CI 和故障场景。
- [贡献指南](../../CONTRIBUTING.zh-CN.md)、[安全策略](../../SECURITY.zh-CN.md)和[第三方声明](../../THIRD_PARTY_NOTICES.zh-CN.md)。
