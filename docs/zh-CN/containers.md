# 容器与 Kubernetes

[English](../containers.md) | 简体中文

示例仅运行单节点，适合评估和受控部署，不代表高可用认证，也不消除
[已知限制](limitations.md)。镜像包含引擎、`owlcron-script` 适配器、示例插件、静态界面与许可声明，
以 UID/GID 10001 运行，带有进程监控、CA 与时区数据。适配器可运行 `/bin/sh`
脚本，其他解释器需要安装到派生镜像，具体见[运行脚本文件](script-files.md)。
先本地构建：

```sh
docker build -t owlcron:local .
docker volume create owlcron-state
```

通过密钥管理系统或权限为 0600 的环境文件提供 `OWLCRON_SECRET_KEY`，
不要把真实密钥放进命令历史。初始化和启动必须使用相同密钥和状态卷：

```sh
docker run --rm --env-file /secure/owlcron.env \
  -v owlcron-state:/var/lib/owlcron owlcron:local setup
docker run -d --name owlcron --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m --cap-drop ALL \
  --security-opt no-new-privileges --env-file /secure/owlcron.env \
  -v owlcron-state:/var/lib/owlcron -p 127.0.0.1:3012:3012 owlcron:local
```

配置替换文件挂载到 `/etc/owlcron/config.json:ro`。绑定目录必须允许 UID 10001
写入，命名卷继承镜像目录所有权。持久化整个 `/var/lib/owlcron`，包括日志和队列。
使用 `docker stop -t 90 owlcron` 停止，停机后备份，不要随容器删除状态卷。
未提供初始密码时，在可信终端读取 `/var/lib/owlcron/data/bootstrap-admin.json`，
确认登录后删除该敏感文件，禁止上传支持日志或 CI 产物。公网部署前遵循
[管理员初始化和 HTTPS 部署](deployment.md)。

使用 Redis 后端时，单独运行 Redis 8.4+，为其配置持久数据卷、AOF 和 noeviction。向 OwlCron 提供连接凭据，只读挂载所需 TLS CA／客户端证书，并保留 OwlCron 日志、队列卷。见[Redis 配置](configuration.md#redis-连接与持久化)。应用镜像不会启动 Redis 或配置其持久化。

## Kubernetes

`deploy/kubernetes/owlcron.json` 是 kubectl 支持的 JSON 清单：使用 `apps/v1`
和核心 `v1`，单副本 Recreate Deployment、10 GiB ReadWriteOnce PVC、ClusterIP
服务。Pod 禁用 API Token、非 root、只读根文件系统、移除 capabilities，
包含启动/就绪/存活探针和 90 秒退出宽限。集群需要可用的默认 StorageClass。

1. 将镜像发布到集群可访问的仓库，固定版本或摘要，替换清单中两处
   `owlcron:local`。只有已将此镜像加载到开发集群时才能直接使用本地名称。
2. 在目标命名空间从受保护文件创建密钥：
   `kubectl create secret generic owlcron-secrets --from-file=secret-key=/secure/owlcron-secret-key`。
   文件包含至少 32 字节的随机值，清单不包含真实密钥。
3. 审核 ConfigMap、存储与资源配额后执行：

```sh
kubectl apply -f deploy/kubernetes/owlcron.json
kubectl rollout status deployment/owlcron
kubectl port-forward service/owlcron 3012:3012
```

init container 幂等初始化存储。用可信终端读取初始凭据并在登录后移除。
服务仅内部可达；添加 Ingress 前配置 HTTPS 和可信代理网段，按实际入口、DNS、
SMTP、WebHook 需求制定网络策略。示例不创建全放行策略，也不声称能自动隔离
默认允许流量的集群。此文件系统部署禁止扩展为多副本。集群化必须单独验证
共享存储、TLS、身份和故障恢复。

CI 构建镜像，验证只读非 root 启动、状态持久化与重启，并做离线清单安全检查。
这不等于真实 Kubernetes 认证，仍需在目标集群验证调度、卷权限和网络策略。

## 健康检查

`GET /api/app/ping` 用于存活检查；`GET /api/app/ready`（也支持 `/v1`）在
本地启动完成且存储能够响应只读存在性检查时返回 HTTP 200。启动中、退出中、
已关闭、检查失败或超过两秒时返回 HTTP 503。两个端点均无需认证，就绪响应
只包含通用结果，不暴露后端错误详情。健康工作节点不需要成为主节点才能就绪。
并发请求共享存储检查，后端卡住时不会不断创建新的检查任务。
启动时会在服务启动前向保留键 `global/readiness_probe` 写入一个字节的标记，
期限为两秒。存储权限必须允许读写该键。后续探针仅检查这个小标记，
不读取应用状态，也不写入数据；标记缺失视为不健康。所有节点共用相同的
固定标记，关闭时不删除。探针超过两秒后才返回的结果，即使后端报告成功，
也仍视为失败。

Kubernetes 示例将就绪检查用于启动和流量路由，将存活检查用于重启判断。
因此存储故障会将 Pod 从服务中移除，而不会触发反复重启。就绪检查仅验证连接
和读取能力，不保证写入权限、磁盘空间、集群仲裁、任务执行或
[已知限制](limitations.md)中描述的持久性语义。
