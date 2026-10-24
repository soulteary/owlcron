# 安全部署与首次运行

[English](../deployment.md) | 简体中文

目前建议用于开发、评估和受控环境。严格集群持续收敛问题尚未完成验收，
请先阅读 [已知限制](limitations.md)，不要把单机冒烟测试视为集群可靠性证明。

## 安装和启动

在 Linux 上使用压缩包部署还需要系统运行依赖：安装 CA 信任库以验证出站 TLS，
安装 IANA 时区数据以支持 UTC 以外的事件时区，并保证服务的 `PATH` 中能够找到
`procps` 提供的 `ps`。CPU/内存采样及相应任务限制依赖该命令；登录或普通任务成功
不代表资源监控可用。Debian/Ubuntu 可以执行：

```sh
sudo apt-get update
sudo apt-get install --no-install-recommends ca-certificates tzdata procps
```

项目容器已经提供这些依赖。二进制不需要 Go、Bun 或 Node.js，但不能替代上述
操作系统数据文件和工具。自定义插件还需要各自的解释器。

源码构建需要 Go、Bun、Python 和 make；版本及平台支持范围以[版本和发行](releases.md#支持的工具链和平台)为准。

使用 `curl -fsSL https://bun.sh/install | bash -s "bun-v1.4.2"` 安装固定版本的 Bun，再执行 `bun --version` 核实。前端和浏览器测试分别提交 `bun.lock`，安装时始终使用 `--frozen-lockfile` 保持可复现。`bunfig.toml` 保留静态资源路径和许可证校验所需的 hoisted 布局。这些构建和测试命令不需要 Node.js 或 npm。

```sh
git clone https://github.com/soulteary/owlcron.git
cd owlcron
bun install --cwd htdocs-deps --frozen-lockfile
make frontend build
make licenses-check smoke
cp config.json config.local.json
chmod 600 config.local.json
```

在 `config.local.json` 设置至少 32 字节的随机 `secret_key`，并将数据、队列和日志
目录放在服务账户独占的持久目录中。不要复用示例秘密。也可通过
`OWLCRON_SECRET_KEY` 注入秘密；密钥不要写进镜像、Git 或公开日志。

```sh
./owlcron-engine -config config.local.json setup
./owlcron-engine -config config.local.json
```

空用户数据库首次启动时，可通过 `OWLCRON_ADMIN_USERNAME` 和
`OWLCRON_ADMIN_PASSWORD` 注入管理员凭据，密码须为 12–72 字节。如果没有设置密码，
程序生成随机密码并写入存储目录的 `bootstrap-admin.json`，该文件含敏感凭据。
使用受控终端读取，确认可以登录后移除引导凭据文件并撤销环境注入。
已有用户数据库不会因重新设置环境变量而重置密码。

不要将密码直接写入 shell 命令行、服务启动参数或仓库。使用部署平台的 secret
管理或仅服务账户可读的环境文件；应用不自动读取 `.env` 文件。

## HTTPS 反向代理

以下片段用于 **同一宿主机上的 Nginx**；证书路径和域名需要替换。
先保证宿主机防火墙阻止外部直接访问 3012。容器部署可使用
`127.0.0.1:3012:3012` 端口映射；不要使用对所有网卡公开的映射。

在应用配置中合并：

```json
{
  "base_app_url": "https://cron.example.com",
  "http_port": 3012,
  "session_cookie_secure": true,
  "trusted_proxy_cidrs": ["127.0.0.1/32", "::1/128"]
}
```

只信任实际代理来源地址，不要设置 `0.0.0.0/0` 或宽泛的内网网段。
使用远端或容器代理时，应按实际网络配置替换上述回环地址。

```nginx
# 放在 http 上下文中
map $http_upgrade $owlcron_connection {
    default upgrade;
    ''      close;
}

server {
    listen 443 ssl;
    server_name cron.example.com;
    ssl_certificate     /etc/nginx/tls/fullchain.pem;
    ssl_certificate_key /etc/nginx/tls/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;

    location / {
        proxy_pass http://127.0.0.1:3012;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        # 单层代理覆盖客户端提交的转发头。
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $owlcron_connection;
        proxy_read_timeout 360s;
        proxy_buffering off;
    }
}
```

HTTP 重定向到 HTTPS 可另设 80 端口虚拟主机。多层代理需要逐跳维护可信来源，
不能直接照搬单层转发头示例。HTTPS 下检查登录 Cookie 包含 Secure、HttpOnly 和
SameSite=Lax。启用 Secure Cookie 后不要继续通过 HTTP 登录。

如果同时设置了 `cluster_tls_cert_file` 和 `cluster_tls_key_file`，应用的同一监听端口
会启用 TLS。此时代理上游也要改为 HTTPS，并配置受信任 CA、服务器名和证书校验，
不能继续使用上面面向明文后端的 `proxy_pass http://...`。不要关闭上游证书校验来绕过配置问题。

## 权限、网络和数据

- 使用专用非 root 服务账户，配置和数据目录仅该账户可读写。
- 只给可信管理员插件和任务管理权限。示例插件用于测试，不提供隔离能力。
- WebHook、SMTP、S3、Couchbase 等凭据按最小权限配置；验证 TLS，限制出站网络。
- 集群节点通过同一监听端口的 `/cluster/ws` 使用 WSS、节点认证和可选 mTLS；
  HTTP 反向代理不会替代这些认证，
  相关配置见 [配置文档](configuration.md)，端口只对集群成员开放。
- 无需自动发现时可设置 `udp_broadcast_port: -1` 禁用广播；不要在公网开放发现端口。
- 设置适合业务的并发、CPU、内存、日志大小和保留策略，并监控磁盘容量。
  单任务限制不等于整个节点磁盘配额。

## 备份、升级与回滚

升级前停止接受新任务并等待当前任务结束，再优雅停止服务，备份配置、完整后端数据、
队列和日志。备份包含密码哈希、会话、令牌和任务输出，需加密并限制访问。
本地 Filesystem 后端优先采用停机一致性快照；远程后端使用其一致性备份机制。
不要把运行中复制目录当作已验证的一致性备份。

先在隔离环境恢复备份并验证登录、任务、历史与日志，再升级正式环境。
回滚保留旧二进制、静态资源、插件及升级前快照；如数据格式发生变化，须配套恢复
对应快照，不能假设旧二进制可以读取新数据。恢复测试不得向真实收件人重复发通知。

[发行流程和版本政策](releases.md)定义了经过验证的发行包内容及支持的构建矩阵。
目前尚不保证数据迁移兼容性。正式升级流程、跨版本回滚和远程后端灾备
仍需按目标部署环境验收。

`export`、`import`、`storage-migrate` 应在全部节点停止后执行，以取得一致快照；具体参数见 `./owlcron-engine help`。备份包括规范记录、历史、日志、游标、队列和墓碑，排除初始明文密码及瞬态主租约/代次。import 会覆盖同名 key，但不删除目标中的额外记录；优先恢复到空命名空间。远程逐 key 恢复失败可用同一备份重试。

CLI 按记录流式写入已有的 `owlcron-storage-v1` 格式，不再将完整导出驻留在内存中。文件导出先写入私有临时文件，同步并完整写入后才重命名；重命名前发生错误会保留原目标文件；重命名后目录同步失败表示持久化未确认，此时新文件可能已经可见。键列表和最大的单条记录仍会占用内存。标准输出导出失败时可能留下部分内容，使用前必须检查退出状态。流式导出不会让运行中的后端自动具备一致快照。

## URL 子路径部署

配置 `base_path: "/tools/cron"`（末尾无斜线），并将 `base_app_url` 设为
`https://cron.example.com/tools/cron`。反向代理转发时保留此前缀。
界面入口为 `/tools/cron/`，API、浏览器 WebSocket 和日志地址共享前缀，
会话 Cookie 也限定在此前缀。路径段仅支持字母、数字、下划线和连字符。
空值保持根路径部署；不要让其他应用共享同一 Cookie 路径。
节点认证通道仍使用根路径 `/cluster/ws`，应限制为集群节点可达。
已有 TLS、同源检查和可信代理要求不变。

指标采集与故障处理请参阅[运维与指标](operations.md)。

## 候选包部署验收

[候选包检查](releases.md#准备候选包但不发布)通过独立 Nginx 进程代理解压后的发行包，
使用临时证书，并由测试客户端显式信任。它分别验证根路径和 `/tools/cron` 的前端
资源、Secure/HttpOnly/SameSite Cookie 属性及路径，认证 WSS 连接并接收实时任务日志，
拒绝跨来源写入及只读账号的运行请求，然后验证重启和离线恢复到空存储后的会话、
历史与日志。运行进程、证书和凭据都是临时生成的，不会连接已有安装实例。

实际部署时，还应通过公开 HTTPS 地址重新验证登录、代表性任务和实时日志，检查
证书链且不关闭验证，查看 Cookie 作用域，确认后端端口不能从公网访问，并验证
服务重启后的持久状态。在独立命名空间中演练备份恢复。确认任务在预期时区内按计划
执行，并验证业务需要的夏令时行为。如果启用账号邮件，使用独立地址通过目标 SMTP
服务验证邮箱和找回密码；本地 SMTP 夹具不能验证该服务。批准切换前，在私有记录中
保存产物摘要、源码提交和验收结果。

## 重置管理员密码

使用管理命令前先停止服务。从受保护的秘密文件读取密码，避免密码出现在进程参数或 Shell 历史中：

```sh
./owlcron-engine --config config.local.json admin admin --password-stdin < /run/secrets/owlcron-admin-password
```

输入应为一个 12–72 字节的密码，可带末尾换行；前后的空格会保留。为兼容旧调用，仍接受位置参数形式的密码，但推荐使用标准输入方式。

## 准备账号邮件

邮箱验证和邮件找回密码已可用，但默认关闭。启用 `account_email_enabled` 前，
配置 SMTP 服务和可信的公共地址，并用专用测试账号验证完整流程。以下配置片段
使用 STARTTLS；如果邮件服务要求认证，另行设置认证参数，并保护好包含凭据的配置：

```json
{
  "account_email_enabled": true,
  "base_app_url": "https://cron.example.com",
  "smtp_hostname": "smtp.example.com",
  "smtp_port": 587,
  "smtp_tls_mode": "starttls",
  "email_from": "OwlCron <noreply@example.com>"
}
```

使用 URL 前缀时，配置 `base_path` 并在 `base_app_url` 中保留相同路径。账号邮件
链接使用这个固定地址，不读取请求中的 Host。各节点保持配置一致，修改后重启。
既有用户未验证邮箱也可继续登录，但邮件找回要求账号已启用、邮箱已验证。
公开注册保持关闭。限额见[账号邮件策略](configuration.md#账号邮件策略)，
验证步骤、投递排障和管理员后备方案见[账号邮件与密码找回](operations.md#账号邮件与密码找回)。

邮箱验证和密码恢复链接包含持有者令牌，公共链接与 SMTP 传输都必须加密。`account_email_allow_insecure` 仅供隔离的本地测试使用明文 HTTP 和 SMTP，生产环境不得开启。
