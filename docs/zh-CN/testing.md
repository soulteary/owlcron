# 测试与验证

[English](../testing.md) | 简体中文

所有命令默认从仓库根目录执行。Go 构建使用项目建议的安全工具链；Node.js 使用 22 系列。准备步骤和权限要求见[部署指南](deployment.md)。

## 常规检查

```sh
npm ci --prefix htdocs-deps
make frontend build
make fmt-check vet test frontend-test licenses-check docs-check smoke
make race
python3 -m unittest discover -s tests -p 'test_*.py'
(cd bin/echo-plugin && go vet ./... && go test ./...)
```

`make smoke` 会启动真实守护进程，在独立临时数据目录验证静态资源、登录、节点就绪、事件执行、示例插件、历史和日志，再优雅关闭。它需要本地临时端口权限，不读取现有运行数据。

`make docs-check` 离线检查维护文档的本地链接、章节锚点、中英文页面配对、双向切换链接和配置示例，也会检查两种语言的代码示例是否一致，避免引用未提交文件或本机目录。

`go test -short ./...` 可跳过独立进程 fixture，用于快速开发反馈，但不能代替常规回归。示例插件属于独立模块，根目录的 `go test ./...` 不包含它；该模块目前没有专门测试文件，仍需编译与静态检查。

## 漏洞与许可证

```sh
npm audit --prefix htdocs-deps --audit-level=moderate
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
(cd bin/echo-plugin && go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...)
```

扫描依赖于当时的公告数据库。分别记录可达调用、已导入包和仅模块级命中，不要把未使用包的公告描述为已确认可利用，也不要忽略实际可达漏洞。

npm 升级后运行 `make licenses-update licenses-check`，审阅许可变化。Go 依赖变化时，更新 `licenses/go-inventory.json` 及对应上游许可文件。许可证检查要求先执行 `npm ci`。

## 独立进程回归

```sh
make process-integration
make process-integration-repeat
```

这些入口使用实际 daemon 进程、Filesystem 后端、SIGKILL、网络分区和 TLS 节点连接，不需要外部 S3/Couchbase 服务。重复入口会运行三次所选用例。

## Linux 与远程存储故障

```sh
make linux-acceptance
```

需要 Docker、Python 3、可下载的镜像和 Go 依赖。脚本创建本次运行专属的 MinIO、Couchbase 和网络，不发布宿主端口，不向测试容器挂载 Docker socket。测试使用可丢弃的专用凭据，结束后清理自己创建的服务。

`OWLCRON_ACCEPTANCE_RESULTS` 指定外部结果目录；`OWLCRON_ACCEPTANCE_COUNT` 设置 1–3 次重复。镜像和构建缓存选项见 [run-linux-acceptance.sh](../../tests/run-linux-acceptance.sh)。不要让两次测试控制同一组服务。

已有专用测试服务时可使用 `make remote-storage-faults` 或 `make remote-process-integration`。前者要求 S3、Couchbase 端点及对应容器控制标识，后者要求后端列表和配置 JSON；完整变量名见 [Makefile](../../Makefile) 与脚本。未配置的远程用例会跳过，跳过不表示真实服务已通过。

## 负载与持续故障

```sh
make linux-load-acceptance
```

默认资源限制为 2 CPU、2 GiB 内存、512 个进程。入口依次运行 race、重复重载集群、容量、单机持续恢复、集群持续恢复和证据检查。`OWLCRON_LOAD_RESULTS` 必须指向空的结果目录，失败记录不能由旧结果补齐。

各阶段也可以单独运行：

```sh
make capacity-acceptance
make cluster-load-acceptance
OWLCRON_SOAK_DURATION=3m OWLCRON_PROCESS_SOAK_DURATION=3m \
  OWLCRON_LOAD_SOAK_TIMEOUT=15m make soak-acceptance
```

最后一项依次运行单机与集群场景，外层超时必须覆盖两者及准备过程。长时间测试应按目标时长重新设置超时。

资源配置、挂载类型、后端、数据量和日志/通知负载都会影响结果。测试中的 RTO、延迟分位数和采样 RSS 不是服务 SLA；短时间通过也不证明长期稳定性。持续集群场景的已知问题见[已知限制](limitations.md)。

## CI 和证据保存

[常规 CI](../../.github/workflows/ci.yml)验证 Go 1.27 系列、Node 22、构建、单元测试、安装冒烟、许可证和漏洞检查。[故障验收](../../.github/workflows/process-acceptance.yml)负责进程及 Linux 故障场景。

CI 在 PR 和 `main` 分支推送时运行；功能分支推送不再额外触发一套重复检查。新提交会取消旧任务。依赖和 Docker 镜像层使用缓存，但缓存命中不会跳过测试。

负载验收拆分为六个并行阶段：完整 Linux race 回归、重复集群恢复、启用和关闭 race 的容量测试、单机持续故障、集群持续故障。每个阶段检查本次运行的新鲜证据，最终汇总必须包含全部六个阶段及规定记录。资源限制、重复次数和两组各三分钟的持续测试时长保持不变。完整 race 与 vet 在回归阶段执行一次。进程恢复拆分为 SIGKILL、集群、负载集群三组，所有选中用例仍重复三次。远程存储故障保持独立任务。

复现单个负载阶段时，在 `make linux-load-acceptance` 前设置 `OWLCRON_LOAD_STAGE` 为 `regression`、`cluster`、`capacity-race`、`capacity-plain`、`daemon-soak` 或 `cluster-soak`；省略时仍串行执行完整矩阵。`OWLCRON_ACCEPTANCE_SKIP_BASELINE=1` 仅供同时要求 Linux 回归阶段通过的 CI 使用；独立运行远程验收默认保留基线测试。

工作流文件定义检查方式，实际状态以对应提交的 GitHub Actions 运行记录为准。记录测试结果时注明提交、工具链、资源、后端、命令、退出码和跳过项。

本机日志、失败堆栈、扫描结果和性能数据应放在仓库外，或保留为 CI 附件。仓库中的测试脚本和缺陷回归用例需要保留；会影响用户决策的限制维护在产品文档中，不随临时报告一起移除。
