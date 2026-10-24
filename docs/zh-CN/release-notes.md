# 发行说明

[English](../release-notes.md) | 简体中文

## v0.1.0-rc.1024

这是 OwlCron 首个候选版本，用于评估以单节点方式替换 Cronicle，保留熟悉的
页面和部分兼容操作流程。候选包正在准备中；本文不代表版本已经发布。
受支持的二进制发行平台为 Linux amd64。

### 包含的操作流程

- 调度和手动运行本地任务，配置重试和超时，查看历史与日志，并执行成功或失败任务链。
- 使用内置 Echo Plugin 和 `owlcron-script` 适配器。自定义插件仍需自行安装运行时，
  并审核执行协议。
- 管理用户、权限、分类、插件和 API Key。公开注册仍然关闭。
- 显式配置 HTTPS 公开地址和 SMTP TLS 服务后，启用邮箱验证及邮件找回密码。
  账号邮件功能默认关闭。
- 在窄屏下使用 Cronicle 风格页面，通过键盘操作控件、管理弹窗焦点、取消弹窗和滚动表格。
- 在根路径或指定 URL 前缀部署，通过受信任的 HTTPS 反向代理使用安全会话。

### 验证与安装

解压前核对压缩包校验和，通过 `owlcron-engine version` 和 `BUILD.json` 确认
版本及源码提交。压缩包包含中英文安装说明、本文、MIT 许可证、第三方声明
和依赖许可证文本。

发行流水线在 Linux amd64 上从干净的已提交源码构建。它验证解压后的程序在没有
Go、Bun 或 Node.js 时能够运行，执行 Echo 和 shell 任务，并检查重启、历史、
日志及离线恢复到空存储。额外验收使用 Nginx 和本地受信任的测试证书，分别检查
`/` 和 `/tools/cron`，覆盖 HTTPS 资源、会话 Cookie、WSS 实时日志、只读用户权限
和合成 Cronicle 数据的迁移演练。

Chromium、Firefox 和 WebKit 检查覆盖主页面和管理页面、320/375/768 CSS 像素布局、
键盘操作及隔离的账号邮件流程。这些是自动化浏览器引擎检查，不代表认证了所有
浏览器版本、实体移动设备或屏幕阅读器。请查看 `BUILD.json` 中源码提交对应的检查；
仅有本文不能证明某个构建已经通过。

对外提供服务前，请遵循[部署指南](https://github.com/soulteary/owlcron/blob/main/docs/zh-CN/deployment.md)，
使用独立服务账号、私有持久化目录、新生成的应用密钥，并限制后端访问。
显式配置公开地址和代理信任范围，保留经过恢复验证的备份。

### 从 Cronicle 迁移

遵循[迁移指南](https://github.com/soulteary/owlcron/blob/main/docs/zh-CN/migration.md)。
需要单独从源码构建的预检器读取 Cronicle Data Export v1.0，并报告转换要求；
它不会导入记录，也不能证明业务行为正确。重新创建审核后的配置、凭据和本地目标，
将内联 shell 脚本移到审核后的文件，通过 `owlcron-script` 执行。不要直接将
Cronicle 导出交给 OwlCron 的 import 命令，也不要复用原存储目录。

内置演练使用人工编写的合成数据。切换前，请在隔离部署中验证自己的插件、时区、
调度、权限、通知收件人和外部副作用。启用替代调度器时保持原调度器停止，避免重复执行。

### 已知边界

- 本候选版本适合评估和受控部署，尚未确立稳定 API、跨版本数据兼容或生产容量保证。
- 集群发现、故障转移和远程派发不在本次兼容承诺内。OwlCron 节点不能加入 Cronicle 集群。
- 兼容范围是部分 UI/API/插件行为，并非原地替换数据或集群。预检器不会迁移 Cronicle
  已完成任务的历史和日志；分离执行的任务不能跨引擎重启继续运行。
- 插件使用服务账号权限运行，未进行沙箱隔离。外部副作用不保证恰好一次，需要业务幂等。
- 长时间稳定性、真实 SMTP 服务、具体部署的 TLS 和代理行为、容量及跨版本升级/回滚
  仍需运维方验证。本地证书和 SMTP 测试夹具不能证明这些能力。
- 校验和用于完整性检查。当前产物没有签名，也未附带 SBOM 或来源证明。

选择工作负载前，请阅读[兼容范围](https://github.com/soulteary/owlcron/blob/main/docs/zh-CN/compatibility.md)
和[已知限制](https://github.com/soulteary/owlcron/blob/main/docs/zh-CN/limitations.md)。
报告可复现问题时提供源码提交、平台及脱敏步骤；不要将凭据、导出数据或私有日志
放入公开 issue。
