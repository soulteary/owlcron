# 浏览器验收测试

[English](README.md) | 简体中文

两个测试程序均启动独立的 OwlCron 实例并使用临时存储，不读取现有服务配置，
也不能用于测试已部署实例。

- `test` 验证通用界面流程：登录与账号帮助、个人资料保存、六个管理页面和临时
  资源修改、主题持久化、事件编辑与执行、历史、日志和图表。
- `test:account-mail` 启动进程内 SMTP 服务，仅在临时配置中开启账号邮件。
  验证邮箱确认、找回密码表单、匹配与不匹配账号的响应、主动确认密码重置、
  密码修改通知、令牌不可重用、旧密码与会话失效，以及使用新密码成功登录。
  同时验证新页面及已有页面内的邮件链接跳转，确保令牌不进入浏览器日志。

两项测试均使用无头 Chromium，未捕获的浏览器 JavaScript 异常会使测试失败。
账号邮件测试不会向外部发送邮件，公开注册保持关闭。

在仓库根目录使用 Bun 1.4.2 和项目要求的 Go 工具链运行：

```sh
bun install --cwd htdocs-deps --frozen-lockfile
bun install --cwd tests/browser --frozen-lockfile
(cd tests/browser && bunx --bun --no-install playwright install --with-deps --only-shell chromium)
make build frontend
bun run --cwd tests/browser licenses
bun run --cwd tests/browser test
bun run --cwd tests/browser test:account-mail
```

本地可将 `OWLCRON_BROWSER_EXECUTABLE` 设置为已安装且兼容的 Chrome/Chromium
可执行文件绝对路径。CI 使用 Playwright 锁定的 Chromium 版本。测试需要允许
绑定本地端口，并启动浏览器和服务进程。

通用冒烟测试会把失败截图保存到 `tests/browser/test-results/`，分享前应先检查。
账号邮件测试不保存截图，也不输出 SMTP 邮件、带令牌的 URL 或服务日志。
凭据、邮件和服务数据仅在临时环境中使用，并在每次运行结束时清理。

按路径筛选的 [CI / Browser Tests](../../.github/workflows/browser.yml) 工作流
运行这两项测试，缓存浏览器，并设置五分钟外层超时以覆盖冷启动安装。浏览器安装
不进入 Go 快速检查任务。这些测试只覆盖选定的 Chromium 流程，不代表完整的
跨浏览器或无障碍验证。

更新依赖后，应同步更新锁文件和许可副本：

```sh
bun tests/browser/check-licenses.mjs --write
bun run --cwd tests/browser licenses
bun audit --cwd tests/browser --audit-level=moderate
```

扩展冒烟测试还检查时区、定时、重试、超时和队列参数保存，历史任务再次执行、
终止运行任务、失败日志、有限重试、超时、入队与清空、成功与失败任务链，以及独立只读账号的
控件隐藏和 API 拒绝。测试复用已有
临时服务和 Chromium 进程，不等待分钟边界的定时任务触发，也不访问外部服务。
[单节点覆盖清单](../../docs/zh-CN/testing.md#单节点界面发布门槛)区分浏览器断言、
引擎回归和人工发布检查。
