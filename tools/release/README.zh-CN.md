# OwlCron 二进制发行包

[English](README.md) | 简体中文

本压缩包包含 `owlcron-engine`、`owlcron-script`、Echo Plugin、前端资源、
示例配置、构建元数据及第三方许可。请在此目录中执行命令，确保服务可以找到
前端资源和插件。

```sh
./owlcron-engine version
./owlcron-engine --config config.json setup
OWLCRON_ADMIN_USERNAME=admin OWLCRON_ADMIN_PASSWORD='choose-a-strong-password' ./owlcron-engine --config config.json
```

上述密码是占位示例：部署时应生成强密码，并使用受保护的密钥注入方式。
数据和日志目录需要可写并限制访问，升级前做好备份，对外服务应限制访问并配置
HTTPS。示例配置不代表已经完成生产环境安全加固。

请阅读[部署文档](https://github.com/soulteary/owlcron/blob/main/docs/zh-CN/deployment.md)
和[发行政策](https://github.com/soulteary/owlcron/blob/main/docs/zh-CN/releases.md)。
项目仍处于实验阶段，不应假设可以跨版本回滚数据。下载后先验证压缩包旁的
`.sha256` 文件，再解压运行。`BUILD.json` 记录版本、源码提交、目标平台和 Go
工具链。分发时保留 [LICENSE](LICENSE)、[第三方声明](THIRD_PARTY_NOTICES.zh-CN.md)
及 `licenses/`。
