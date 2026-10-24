# 安全与漏洞报告

[English](SECURITY.md) | 简体中文

OwlCron 当前处于实验性阶段，还没有稳定版本支持周期。维护对象为主分支；
修复发布前应验证受影响的部署方式。已知可靠性限制见 [已知限制](docs/zh-CN/limitations.md)。

## 私下报告漏洞

请使用 GitHub 仓库 Security 页的 **Report a vulnerability** 功能：
https://github.com/soulteary/owlcron/security/advisories/new

此入口需要仓库维护者在 GitHub 设置中启用 Private vulnerability reporting。
如果入口不可用，请先提交不含敏感细节的 issue，请维护者提供私下联系渠道。
不要在公开 issue 中附上可直接利用的细节、真实令牌、密码或用户数据。

报告请包含受影响版本或提交号、运行平台、最小复现、影响范围和已尝试的缓解方法。
维护者确认影响与修复后再协调公开公告；目前不承诺固定响应 SLA。

## 部署边界

插件可以执行系统命令。拥有插件编辑或任务管理权限的用户，应视为拥有相应
服务账户的命令执行能力；OwlCron 不是不可信代码沙箱。使用专用非 root 用户，
限制任务账户可读写目录、网络和凭据，不要挂载 Docker socket 或宿主机根目录。

当前 HTTP 服务监听所有网卡。`base_app_url` 只设置应用公开地址，**不会限制监听地址**。
部署前必须用防火墙、安全组或容器端口映射限制 3012 端口，仅允许受信任的反向代理访问。
浏览器入口使用 HTTPS；不要将后台 HTTP 管理端口直接暴露到公网。

具体反向代理、Cookie、凭据和备份要求见 [部署指南](docs/zh-CN/deployment.md)。

## 依赖检查

CI 对锁定 npm 依赖执行 `bun audit`，对主模块和示例插件执行 `govulncheck`。
检查结果只覆盖扫描时的已知公告，不代表不存在未知漏洞。依赖升级后应重新
构建前端、运行安装冒烟测试，并用 `make licenses-update licenses-check` 更新许可材料。

扫描器可能报告依赖模块中未被导入的包。请区分可达调用、已导入包和仅模块级命中，核实调用路径并记录处理依据；不要通过隐藏输出代替评估。
