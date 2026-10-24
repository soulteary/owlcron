# 第三方声明

[English](THIRD_PARTY_NOTICES.md) | 简体中文

OwlCron 原创代码采用 MIT 许可证，见 [LICENSE](LICENSE)。第三方代码、字体和其他资源保留各自的许可证与版权声明，根目录 MIT 许可证不会替代这些条款。分发源码或构建产物时，应一并保留本文和 `licenses/` 目录。

## Cronicle

浏览器应用、样式、图像和前端构建工具源自 [Joseph Huckaby / PixlCore 的 Cronicle](https://github.com/jhuckaby/Cronicle)。文件中的原始版权声明予以保留，上游 MIT 许可见 [licenses/Cronicle.txt](licenses/Cronicle.txt)。上游许可核对日期为 2026-10-24，这不表示本地代码与上游最新版本一致。有关 Cronicle 协议和兼容性的引用用于说明来源，应予保留。

## 前端依赖和字体

[licenses/npm-inventory.json](licenses/npm-inventory.json)记录安装版本及包声明的许可证；包中提供的许可、声明和 README 文件保存在 `licenses/npm`。`package-lock.json` 锁定依赖树，其中既有构建工具，也有浏览器资源，并非所有包都会发送给浏览器。

以下资源不能一律按 MIT 处理：

- Dave Gandy 的 Font Awesome 4.7.0：字体采用 SIL OFL 1.1，CSS/LESS/Sass 采用 MIT，文档采用 CC BY 3.0。见[上游政策](https://fontawesome.com/v4/license/)、`licenses/Font-Awesome-font-metadata.txt`、`licenses/Font-Awesome-OFL.txt` 和 `licenses/npm/font-awesome@4.7.0/README.md`。品牌图标不表示相关品牌的认可。
- Material Design Icons（mdi 1.9.33）：字体采用 SIL OFL 1.1，包元数据将代码声明为 MIT。版权归 Austin Andrews 和 Google；上游声明还引用 Google 图标的 Apache 2.0 许可。见 `licenses/mdi-LICENSE.txt` 和 `licenses/Apache-2.0.txt`。
- pixl-webapp 分发的 Lato 字体：版权归 tyPoland Lukasz Dziedzic，保留字体名称 Lato，采用 SIL OFL 1.1。见 `licenses/Lato-OFL.txt`。
- pixl-webapp 的 md5.js：署名 Paul Johnston（1999–2009）、Greg Holt、Andrew Kepert、Ydnar 和 Lostinet，采用 BSD 许可，见 `licenses/Paul-Johnston-BSD.html`。pixl-webapp 整体的 MIT 许可不会替代该文件的单独声明。

字体未被改名或重新许可，须保留其保留名称声明。

## Go 依赖

[licenses/go-inventory.json](licenses/go-inventory.json)列出 `go.mod` 显式声明的直接和间接依赖及版本。下载模块中的 LICENSE、NOTICE 和版权文件保存在 `licenses/go`，继续遵循上游 MIT、BSD、Apache 等条款，不重新许可为 OwlCron 的 MIT。Go 标准库和运行时使用 Go 项目的 BSD 风格许可，见 `licenses/Go-LICENSE`。

## 依赖更新和分发

修改 `go.mod`、`package-lock.json` 或随附资源时，应同步刷新版本清单和上游声明。发布压缩包及容器分发时，应包含 `LICENSE`、`THIRD_PARTY_NOTICES.md` 和 `licenses/`；压缩或合并资源时保留版权头。

当前清单覆盖仓库声明的 Go 模块和安装的 npm 依赖树，不覆盖部署方自行选择的外部插件或服务；这些组件需要单独维护声明。中文说明不替代 `licenses/` 中的上游许可原文。

## 浏览器测试工具

`tests/browser` 的独立开发工具不会进入运行时镜像。确切版本和上游许可见
[licenses/browser-inventory.json](licenses/browser-inventory.json)，许可和声明
副本保存在 `licenses/browser`。Playwright 安装的 Chromium 是独立测试前提，
保留其随附的许可声明。
