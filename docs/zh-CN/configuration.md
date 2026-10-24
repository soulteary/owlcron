# 配置与运行机制

[English](../configuration.md) | 简体中文

示例配置见 [config.json](../../config.json)，完整字段定义见 [types.Config](../../types/types.go)。命令行使用 `-config` 指定配置文件，路径相对于启动目录解析。

部署步骤、代理示例和管理员初始化见[部署指南](deployment.md)。修改配置后需重启服务；不要假设运行中的节点会自动重载文件。

## 基础配置

- `hostname`：节点的唯一名称；集群中应显式配置，并与节点登记、公钥和证书保持一致。
- `base_app_url`：外部访问地址，用于链接和跨来源检查，不控制网络监听范围。
- `http_port`：HTTP 服务端口，默认 3012。服务监听所有网卡，外部访问控制由防火墙或代理负责。
- `secret_key`：至少 32 字节的随机秘密，也可由 `OWLCRON_SECRET_KEY` 提供。所有配置和密钥文件应限制访问。
- `storage_engine`：`Filesystem`、`S3` 或 `Couchbase`；本地默认使用 Filesystem。
- `storage_base_dir`：Filesystem 数据目录，默认 `data`。
- `log_dir`、`queue_dir`：本机任务日志和恢复队列目录，默认分别为 `logs`、`queue`。
- `udp_broadcast_port`：发现广播端口；设为 `-1` 禁用发现。发现不代替节点认证。

以下 JSON 块是可合并到配置中的片段，不是完整部署配置。

## 存储、备份与故障恢复

支持 Filesystem、S3 和 Couchbase：

- Filesystem 使用跨进程排他文件锁、同步后原子替换和多记录 redo journal。进程内锁等待遵守取消；已经开始提交的操作不能靠 HTTP 超时回滚。共享目录用于多节点时必须明确配置 `shared_storage`，挂载必须满足文件锁和原子替换语义；不能假设所有 NFS 实现均满足这些要求。
- S3 要求真正支持 `If-Match`、`If-None-Match` 及并发条件写入，首次修改前会探测，不支持的兼容服务会被拒绝。账户需要测试及实际命名空间的对象读写和前缀列举权限。
- Couchbase 使用 SDK CAS；启动恢复与完整枚举需要支持 range scan 的 7.6+ 服务及相应权限。远程后端不提供跨 key ACID 事务，应用采用可重试、幂等的规范记录与索引更新。

事件加载读取全部分页；失败保留原内存快照。遗留孤立事件记录通过前缀列举恢复，自定义存储适配器需实现 `storage.PrefixKeyLister`。列表分页仍可能解码整个索引，不能据此推导常量内存或最大事件容量。

执行中的任务遇到 daemon 强杀后，重启核对执行 token 和内核启动身份，再回收已验证的进程组并将任务标记为中断失败。不会自动续跑或重做插件。`OWLCRON_JOB_TOKEN` 是保留环境变量。无法验证的旧版存活 PID 会阻止启动，需人工核查；不要盲目杀死同号 PID。保持重启前后的 OS 身份和进程检查权限一致。主动脱离进程组或删除标记的插件不在该恢复约定内，detached 执行配置也不受支持。

完成写入失败时，本地已同步 spool 保留结果供重试及重启恢复；worker 的完成 outbox 在主节点可靠确认后删除。已写入本地磁盘的日志可恢复，纯内存缓冲中的尾部不作保证。外部命令及通知均不能保证恰好一次；插件应使用 Job ID，通知接收方应使用稳定消息标识实现幂等。

`export`、`import`、`storage-migrate` 应在全部节点停止后执行，以取得一致快照；具体参数见 `./owlcron-engine help`。备份包括规范记录、历史、日志、游标、队列和墓碑，排除初始明文密码及瞬态主租约/代次。import 会覆盖同名 key，但不删除目标中的额外记录；优先恢复到空命名空间。远程逐 key 恢复失败可用同一备份重试。

### 保留清理与日志限制

保留策略默认关闭，天数 0 表示不按时间删除。示例：

```json
{
  "job_data_expire_days": 180,
  "job_log_expire_days": 30,
  "history_cleanup_interval": 60,
  "history_cleanup_batch_size": 100,
  "job_log_hard_max_size": 67108864,
  "log_upload_concurrency": 4
}
```

历史过期依据已完成任务的 `time_end`；缺少完成时间的旧记录不猜测过期。历史到期删除正文、索引和日志，单独日志到期保留历史与详情。活动任务、待恢复 spool 和未确认远程完成受保护。持久删除意图可以重放；轻量墓碑长期保留，以阻止迟到完成恢复已删内容。不要手动清除这些墓碑或通知幂等标记。

历史完成按任务 ID 写小索引，避免全量重写。HTTP 历史查询仍会聚合保留任务；维护的前缀枚举、本机目录扫描和墓碑数量仍随数据量增长。保留策略不等同于全节点磁盘硬配额，需另外监控磁盘与清理积压。

日志使用 64KiB 块和 manifest，按持久偏移增量上传；失败保留本地内容。默认最多 4 个上传并行，单次同步超时 30 秒。单任务硬上限默认 64MiB，配置 0 仍使用默认，最大 1GiB；事件或分类设置不能超过硬上限。超限或磁盘写入失败会终止任务。

日志 API 支持字节 `offset`、`limit`，默认 64KiB、最大 1MiB。通过 `log_base64` 合并原始字节，依 `next_offset` 继续读取；`eof` 只表示当前快照末尾。GET `get_job_log`／`get_live_job_log` 使用 `stream=1&download=1` 可流式下载；界面仅保留最近约 1MiB 显示，不影响完整下载。旧 Filesystem/S3 日志支持有界读取；旧 Couchbase 单块二进制日志须离线迁移，不支持范围读取。

## 登录、代理与 HTTP 边界

会话随账户禁用、删除或改密失效，API Key 删除或禁用即时生效。普通用户不能自行提升权限。日志按纯文本展示，插件 HTML 报告仅保留允许的格式标签。

JSON 登录和 HTTP Basic 共用每进程限流，默认每 IP 20 次、每账户及 IP/账户组合 10 次，窗口 60 秒；成功尝试也计数。配置键为 `login_rate_limit`、`login_account_rate_limit`、`login_rate_window_seconds`、`login_max_concurrent`（默认 4）及 `login_limiter_max_entries`（默认 10000）。超限返回 429 和 Retry-After。重启清空本机计数，多节点入口需另设共享限流；频繁轮询使用会话或 API Key。

默认客户端 IP 来自直连 peer。只在 `trusted_proxy_cidrs` 中登记受控代理的精确网段；X-Forwarded-For 从右向左寻找首个不可信 hop。代理必须覆盖用户提供的转发头。Cookie 使用 HttpOnly、SameSite=Lax；直连 TLS、强制 `session_cookie_secure: true`，或可信 peer 提供唯一 `X-Forwarded-Proto: https` 时启用 Secure。HTTPS 部署建议强制 Secure，并限制应用只能通过预期代理访问。

HTTP 超时单位为秒，0 取默认，负值或超过 3600 拒绝：

```json
{
  "http_read_header_timeout_seconds": 10,
  "http_read_timeout_seconds": 30,
  "http_write_timeout_seconds": 35,
  "http_idle_timeout_seconds": 60,
  "http_request_timeout_seconds": 30,
  "http_stream_timeout_seconds": 300
}
```

普通请求总超时覆盖认证和处理，超时取消 context 并返回 503；写超时应长于请求超时以便返回结构化错误。日志流式下载同时受总时限和每块 30 秒写超时限制。后端必须配合 context 取消；已提交的修改不会回滚。WebSocket 使用自身协议生命周期，不继承短 API 总时限。

## 邮件、WebHook 与密钥

通知同步接纳到持久队列；共享存储不可用时先写 `queue_dir/notifications` 本地 outbox。默认 `notification_workers: 4`（1–64）、`notification_max_attempts: 8`（1–100），从 5 秒至 1 小时指数重试。永久失败或用尽尝试进入 dead，崩溃后的 claim 等待租约到期恢复。每条 payload 上限 256KiB；sent 保留幂等标记，dead 保留有界 payload 供重放。

SMTP 配置为 `smtp_hostname`、`smtp_port`、`email_from`、`smtp_username`、`smtp_password`、`smtp_tls_mode`、`smtp_timeout_seconds`，另支持 `smtp_tls_server_name`、`smtp_tls_ca_file`。TLS 模式默认 `starttls`，也支持 `implicit`；明文 relay 必须显式选择 `none`。认证要求 TLS 和完整用户名/密码，验证 TLS 1.2+ 证书；默认超时 30 秒，最大 300 秒。RFC 地址列表去重后各收件人独立入队。`max_emails_per_day` 是共享持久 UTC 日 SMTP 尝试限额，失败也计数，等待额度不消耗重试预算。

WebHook 请求超时 10 秒，不跟随重定向或环境代理；传输错误、429、5xx 重试并考虑有界 Retry-After，其他 4xx 永久失败。默认只允许公共 HTTPS。`web_hook_allowed_hosts` 接受精确主机名/IP，`web_hook_allowed_cidrs` 接受具体地址段，允许相应目标使用内部地址或 HTTP；它们不是公共 HTTPS 的全局白名单。元数据、link-local、多播和未指定地址仍禁止。DNS 全部应答及实际拨号地址均校验，禁止凭据式 URL userinfo、fragment 和 IPv6 zone。

请求签名需设置 `web_hook_signing_key_id`，并二选一设置 `web_hook_signing_secret` 或 `web_hook_signing_secret_file`。密钥须独立于集群和 SMTP 凭据，长度 32–4096 字节，不含内嵌换行或 NUL。优先使用 0600 普通文件，拒绝符号链接或宽松权限。不配置则保持无签名兼容，部分配置会拒绝启动。

每次尝试发送 `X-Owlcron-Key-ID`、`X-Owlcron-Timestamp` 和 `X-Owlcron-Signature: v1=<hex HMAC-SHA256>`。签名输入为以下字段以 LF 连接，最后一项是原始请求正文，不追加 LF：

```text
v1
<key ID>
<Unix timestamp>
<Idempotency-Key>
<HTTP method>
<URL scheme>
<URL host including explicit port>
<request URI including raw query>
<raw body bytes>
```

接收方应恒定时间比较签名、限定时间窗口并对稳定 `Idempotency-Key` 去重；SMTP 使用稳定 Message-ID。密钥轮换需先让接收方接受新旧 key ID，再更新发送节点并重启。共享队列各节点须使用一致目标策略与所需签名配置。

通知 payload 与错误会递归脱敏；保守匹配可能遮蔽同名业务字段。真实投递 URL 和消息正文仍是私有队列数据，旧备份和隔离记录不会批量改写。管理员 `get_notifications` 支持 `state`、`limit`（1–1000），不返回正文或 claim token；`POST retry_notification` 按 dead 通知 `id` 重置重试预算，均支持 `/v1`。清理任务历史时不要删除通知队列或幂等标记。

## 集群 TLS 与节点身份

节点使用 `/cluster/ws`，浏览器使用 `/ws`，两类凭据不可互换。节点连接默认要求原生 HTTPS/WSS 和 Ed25519 双向挑战；`cluster_allow_insecure: true` 仅用于明确开发测试，不应进入生产配置。

每个节点设置唯一 `hostname`；`node_auth_private_key` 或 `node_auth_key_file` 二选一，内容为标准 base64 的 32 字节 seed 或有效 64 字节 Ed25519 私钥。`node_auth_peers` 将精确 hostname 映射到标准 base64 的 32 字节公钥，含本节点匹配公钥。优先使用 0600 私钥文件，禁止共享私钥。双方还检查启用的规范 `servers:<hostname>` 登记；加入前先登记节点与公钥。

`cluster_tls_cert_file`、`cluster_tls_key_file` 必须成对配置，启用当前 HTTP 端口的 HTTPS/WSS。证书 SAN 包含节点 hostname，登记 IP 只控制拨号地址。默认使用系统 CA；配置 `cluster_tls_ca_file` 时改用指定 CA。私钥限普通文件、0400/0600、64KiB 以内，证书/CA 限普通文件、1MiB 以内；拒绝符号链接和替换竞态。设置真实 `base_app_url`，不要依赖推导地址。

mTLS 使用 `cluster_tls_require_client_cert: true`，要求显式 CA 和每节点证书/私钥，证书包含 serverAuth/clientAuth 用途及对应节点 SAN。HTTPS listener 校验提供的客户端证书，仅节点端点强制要求证书，普通浏览器仍可访问 HTTPS。

节点证明绑定角色、双方挑战、时间、nonce 和当前租约，允许时钟偏差不超过 60 秒；重放缓存上限 16384，耗尽时拒绝认证。保持时钟同步。WebSocket 最多 1024 连接，未认证窗口固定 10 秒。共享存储中的当前租约是接管与主节点命令的权威；旧 owner、旧连接代次或被禁用节点不能继续控制 worker 或确认完成。断连保留运行任务及持久完成，重连重新认证。

共享存储写权限等同于集群控制权。身份轮换需协调更新公钥登记并重启相关节点；目前每 hostname 只支持一把公钥，不应假设新旧 key ID 能重叠。CA 轮换使用规划好的信任 bundle。UDP 发现不提供身份授权。


## 每节点任务容量

服务器组的 `max_jobs` 限制每个匹配节点的运行中和已预留任务数，0 表示无限制。
多个匹配组采用最严格的正数限制，直接主机和 `local` 目标也受约束。
排队、延迟和多节点任务在执行前预留容量。调低上限不会终止已有任务。
节点满额时不再参与新任务选择；无可用容量的事件仅在启用排队时等待。
顶层 `max_jobs` 仍限制整个集群。
