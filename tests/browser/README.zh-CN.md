# 真实浏览器冒烟测试

[English](README.md) | 简体中文

该独立开发工具使用无头 Chromium 访问全新的 OwlCron 实例，验证首次管理员
登录、明暗主题切换及持久化、事件创建与编辑（包括日志保留期）、手动执行，
以及历史和任务日志展示。还会验证实时与归档图表、时间轴提示和主题切换后的
图表配色。未捕获的浏览器 JavaScript 异常也会使测试失败。
测试使用临时存储，不读取已有服务配置。

在仓库根目录使用 Bun 1.4.2 和项目要求的 Go 工具链运行：

```sh
bun install --cwd htdocs-deps --frozen-lockfile
bun install --cwd tests/browser --frozen-lockfile
(cd tests/browser && bunx --bun --no-install playwright install --with-deps --only-shell chromium)
make build frontend
bun run --cwd tests/browser licenses
bun run --cwd tests/browser test
```

本地可将 `OWLCRON_BROWSER_EXECUTABLE` 设置为已安装且兼容的 Chrome/Chromium
可执行文件绝对路径。CI 使用 Playwright 锁定的 Chromium 版本。失败截图写入
`tests/browser/test-results/`，管理员密码和服务数据仅保存在临时目录。
不要将本测试指向已部署的实例。

独立工作流仅在相关路径变更时运行，缓存浏览器，不影响原有 Go 快速检查。
目前只覆盖 Chromium 关键流程，不代表完整的跨浏览器或无障碍验证。首次安装
可能较慢，因此设置五分钟外层超时。更新依赖后，应同步更新锁文件和许可副本：

```sh
bun tests/browser/check-licenses.mjs --write
bun run --cwd tests/browser licenses
bun audit --cwd tests/browser --audit-level=moderate
```

浏览器测试也覆盖账号访问帮助、个人资料保存、六个管理子页面，以及临时用户、API Key、插件和分类的创建、编辑与删除。所有操作都使用临时服务数据。
