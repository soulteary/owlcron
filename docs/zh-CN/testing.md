# 测试与验证

[English](../testing.md) | 简体中文

所有命令默认从仓库根目录执行。Go 构建使用项目建议的安全工具链；Bun 使用 1.4.2。准备步骤和权限要求见[部署指南](deployment.md)。

## 常规检查

```sh
bun install --cwd htdocs-deps --frozen-lockfile
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
bun audit --cwd htdocs-deps --audit-level=moderate
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
(cd bin/echo-plugin && go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...)
```

扫描依赖于当时的公告数据库。分别记录可达调用、已导入包和仅模块级命中，不要把未使用包的公告描述为已确认可利用，也不要忽略实际可达漏洞。

Bun 依赖升级后运行 `make licenses-update licenses-check`，审阅许可变化。Go 依赖变化时，更新 `licenses/go-inventory.json` 及对应上游许可文件。许可证检查要求先执行 `bun install --frozen-lockfile`。

## 独立进程回归

```sh
make process-integration
make process-integration-repeat
```

这些入口使用实际 daemon 进程、Filesystem 后端、SIGKILL、网络分区和 TLS 节点连接，不需要外部 S3/Couchbase/Redis 服务。重复入口会运行三次所选用例。

## Redis 集成、mTLS 和最小 ACL

```sh
OWLCRON_TEST_REDIS_ADDR=localhost:6379 \
  OWLCRON_TEST_REDIS_USERNAME=owlcron \
  OWLCRON_TEST_REDIS_PASSWORD=owlcron-test-secret make redis-integration
OWLCRON_TEST_REDIS_TLS_ADDR=localhost:6380 \
  OWLCRON_TEST_REDIS_TLS_CA_FILE=/path/to/redis-ca.crt \
  OWLCRON_TEST_REDIS_TLS_CERT_FILE=/path/to/client.crt \
  OWLCRON_TEST_REDIS_TLS_KEY_FILE=/path/to/client.key \
  OWLCRON_TEST_REDIS_TLS_BAD_CERT_FILE=/path/to/untrusted-client.crt \
  OWLCRON_TEST_REDIS_TLS_BAD_KEY_FILE=/path/to/untrusted-client.key \
  OWLCRON_TEST_REDIS_USERNAME=owlcron \
  OWLCRON_TEST_REDIS_PASSWORD=owlcron-test-secret make redis-mtls-integration
```

使用隔离的 Redis 8.4+ 实例。`make redis-integration` 要求设置 `OWLCRON_TEST_REDIS_ADDR`，覆盖存储契约和 CLI 备份恢复，并使用该服务运行 API `TestSingleNodeSystemLifecycle/Redis`，以及 engine `TestClusterAuthenticatedDispatchAndReconnect/Redis` 和 `TestPrimaryOutageAutomaticTakeoverPreservesCursor/Redis`。

`make redis-tls-integration` 保留服务端 TLS 验证入口，客户端证书/私钥变量可选。`make redis-mtls-integration` 是显式双向认证验收入口，服务端须配置 `tls-auth-clients yes`。必须设置 `OWLCRON_TEST_REDIS_TLS_ADDR`、`OWLCRON_TEST_REDIS_TLS_CA_FILE`、受信任客户端证书/私钥（`OWLCRON_TEST_REDIS_TLS_CERT_FILE`、`OWLCRON_TEST_REDIS_TLS_KEY_FILE`），以及由另一不受信任 CA 签发的有效客户端证书/私钥（`OWLCRON_TEST_REDIS_TLS_BAD_CERT_FILE`、`OWLCRON_TEST_REDIS_TLS_BAD_KEY_FILE`）。复用 `OWLCRON_TEST_REDIS_USERNAME`、`OWLCRON_TEST_REDIS_PASSWORD`。服务端证书的 Subject Alternative Name 必须包含 `localhost`，且用途为 serverAuth；客户端证书用途须为 clientAuth。测试覆盖可信身份读写，并拒绝缺少或不受信任的客户端证书、错误服务端名称/CA 和错误 Redis 凭据。

`make redis-acl-integration` 运行最小 ACL 用例及其 mTLS 组合检查，须同时提供上述 TLS 地址、CA、可信客户端证书和私钥。配置相应凭据后，该用例也会随 `make redis-integration` 运行。设置 `OWLCRON_TEST_REDIS_MINIMAL_USERNAME`、`OWLCRON_TEST_REDIS_MINIMAL_PASSWORD`、`OWLCRON_TEST_REDIS_MINIMAL_PREFIX`（如 `acl-test:`）和 `OWLCRON_TEST_REDIS_MINIMAL_DB=1`。账户使用 `reset on >A_DISPOSABLE_SECRET ~acl-test:* resetchannels -@all +ping +info|server +get +set +scan +unlink +select`，不显式授予 `HELLO` 或命令类别；Redis 仍允许认证握手。还需通过 `OWLCRON_TEST_REDIS_NOSELECT_USERNAME`、`OWLCRON_TEST_REDIS_NOSELECT_PASSWORD` 提供第二个账户，权限与 key 规则相同但不授予 `+select`；测试要求它连接 DB 1 时初始化失败，而 DB 0 仍可使用。它应与常规集成账户分开，因为其他用例会检查服务器状态，或使用该前缀外的测试命名空间。显式验收须提供全部身份与证书材料；材料缺失不能算作通过。

## Linux 与远程存储故障

```sh
make linux-acceptance
```

需要 Docker、Python 3、可下载的镜像和 Go 依赖。脚本创建本次运行专属的 MinIO、Couchbase、Redis 8.4+ 和网络，不发布宿主端口，不向测试容器挂载 Docker socket。测试使用可丢弃的专用凭据，结束后清理自己创建的服务。

同一个 Redis 实例同时监听明文端口 6379 和 TLS 端口 6380。每次运行生成有效期一天的自签 CA、服务端证书、可信客户端身份，以及由另一 CA 签发的客户端身份。服务端证书 SAN 包含 `localhost`，Redis 的 TLS 监听端口强制校验客户端证书。只将客户端测试材料与公开 CA 证书以只读方式挂载给 runner。Redis 使用 AOF、`appendfsync always` 和 `noeviction`；关闭默认 ACL 用户，常规命名测试用户禁止执行脚本。独立账户仅具有文档列出的最小命令白名单和受限 key 前缀，并使用 DB 1。完整 Linux 验收必须通过 Redis 集成、mTLS、最小 ACL 和故障用例，跳过用例会导致验收失败。

`OWLCRON_ACCEPTANCE_RESULTS` 指定外部结果目录；`OWLCRON_ACCEPTANCE_COUNT` 设置 1–3 次重复。镜像和构建缓存选项见 [run-linux-acceptance.sh](../../tests/run-linux-acceptance.sh)。不要让两次测试控制同一组服务。

已有专用测试服务时可使用 `make remote-storage-faults` 或 `make remote-process-integration`。前者要求 S3、Couchbase、Redis 端点及对应容器控制标识，后者要求后端列表和配置 JSON；完整变量名见 [Makefile](../../Makefile) 与脚本。直接运行 Go 测试时，未配置的远程用例会跳过；跳过不表示真实服务已通过。

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

[CI / Build and Test](../../.github/workflows/ci.yml)验证 Go 1.27 系列、Bun 1.4.2、构建、单元测试、安装冒烟、许可证和漏洞检查。[CI / Process Fault Acceptance](../../.github/workflows/process-acceptance.yml)在相关 PR 上执行真实 S3/Couchbase/Redis 恢复矩阵；主分支、定时和手动运行还覆盖重复进程故障与完整 Linux 负载矩阵。

CI 在 PR 和 `main` 分支推送时运行；功能分支推送不再额外触发一套重复检查。新提交会取消旧任务。依赖和 Docker 镜像层使用缓存，但缓存命中不会跳过测试。主分支的 Linux 故障矩阵只由 Go 代码及依赖、Makefile、验收与负载脚本和自身工作流触发；纯前端测试或容器配置检查走各自相关的 CI，不再重复运行无关的 Linux 故障场景。

PR 使用 `go test -short` 运行单元测试，并保留静态检查、安全扫描、构建、前端和安装冒烟测试；相关改动仍运行容器检查。目标是在缓存命中且 runner 无排队时约 1–2 分钟提供反馈，并非硬性执行时限。

完整故障验收在相关代码合入 `main` 后运行，并每天北京时间 03:17 定时覆盖最新主分支；也可手动触发。它包含完整 Linux race、真实远程存储、重复进程恢复和持续故障测试，不属于 PR 的快速反馈阶段。这会把长测发现的问题推迟到合并后；高风险改动可在合并前手动运行深度验收。

负载验收拆分为六个并行阶段：完整 Linux race 回归、重复集群恢复、启用和关闭 race 的容量测试、单机持续故障、集群持续故障。每个阶段检查本次运行的新鲜证据，最终汇总必须包含全部六个阶段及规定记录。资源限制、重复次数和两组各三分钟的持续测试时长保持不变。完整 race 与 vet 在回归阶段执行一次。进程恢复拆分为 SIGKILL、集群、负载集群三组，所有选中用例仍重复三次。远程存储故障保持独立任务。

复现单个负载阶段时，在 `make linux-load-acceptance` 前设置 `OWLCRON_LOAD_STAGE` 为 `regression`、`cluster`、`capacity-race`、`capacity-plain`、`daemon-soak` 或 `cluster-soak`；省略时仍串行执行完整矩阵。`OWLCRON_ACCEPTANCE_SKIP_BASELINE=1` 仅供同时要求 Linux 回归阶段通过的 CI 使用；独立运行远程验收默认保留基线测试。

工作流文件定义检查方式，实际状态以对应提交的 GitHub Actions 运行记录为准。记录测试结果时注明提交、工具链、资源、后端、命令、退出码和跳过项。

本机日志、失败堆栈、扫描结果和性能数据应放在仓库外，或保留为 CI 附件。仓库中的测试脚本和缺陷回归用例需要保留；会影响用户决策的限制维护在产品文档中，不随临时报告一起移除。

## Go 质量报告

Go 质量报告在推送标签、发布 Release 或手动触发时运行，包含竞态检测、覆盖率、独立示例插件检查和 Go Report Card。报告及原始测试记录可从工作流产物下载，保留 14 天。仅当被测提交仍与默认分支一致时才回写稳定报告，否则仅保存为产物。现有 CI 继续检查 PR。

[Reports / Go Tests and Report Card](https://github.com/soulteary/owlcron/actions/workflows/go-quality-reports.yml)

按路径筛选的 [CI / Browser Tests](../../.github/workflows/browser.yml) 工作流
运行[浏览器验收测试](../../tests/browser/README.zh-CN.md)：通用界面冒烟与隔离的
SMTP/账号邮件流程。后者验证邮箱确认、找回密码、链接隐私、拒绝令牌重放和会话
撤销。可按浏览器测试指南在本地运行两项测试，需要本地端口及 Playwright 锁定的浏览器，
不依赖外部 SMTP 服务。

工作流显示名称按 `CI /`、`Reports /` 和 `Release /` 分组；文件名保持不变，现有链接仍然有效。README badge 按测试状态、Go Report Card、版本、许可证排序。Go Tests badge 表示主分支构建与测试工作流状态，不是覆盖率百分比。Go Report Card badge 来自公共评估服务，可能与工作流生成的报告更新时间不同。工作流生成的覆盖率和质量报告分别保存在 `.github/coverage.svg` 与 `.github/goreportcard.svg`；首次成功回写前，通过工作流产物查看报告。

## 单节点界面发布门槛

浏览器冒烟测试使用临时 Filesystem 数据启动独立本地服务，通过可见控件操作并
检查保存结果，不连接已有部署。覆盖范围如下：

- **首页：**仪表盘和运行中任务展示；单实例任务再次执行进入队列，管理员清空
  队列，只读账号看不到终止任务或清空队列操作。
- **计划任务：**创建、编辑并重新加载标题和日志保留设置，保留 API 管理的任务链
  数组；验证时区、每日时间、超时、重试间隔、队列上限和两个任务链选择保存。
  任务链目标按需执行，其他定时测试任务保持禁用，避免依赖当前时钟触发。
- **任务详情：**手动执行、历史任务再次执行、终止运行任务、失败任务日志、进度
  和资源图表、一次有限重试、一秒超时、成功与失败任务链执行，以及只读操作可见性。只读账号直接调用修改 API 也必须被拒绝。
- **历史：**列表跳转详情、任务统计和图表提示。
- **个人账号与登录：**冒烟测试验证资料保存和关闭注册时的帮助说明；独立 SMTP
  测试验证邮箱确认、邮件找回密码和旧会话失效。
- **管理页面：**活动、API 密钥、分类、插件、服务器集群和用户页面展示；临时
  密钥、分类、插件和用户的增删改。非管理员访问管理页面返回首页，用户 API
  拒绝其请求。

保存定时参数并不代表浏览器验证了实际定时触发。引擎回归另行覆盖时区与夏令时
匹配、补跑、并发、终止后不重试、超时更新以及重启后的历史保存，见
`engine/scheduler_cluster_test.go` 和 `engine/job_integration_test.go`。
候选版本发布前还须运行完整 Go 测试和干净环境发行包安装检查。浏览器 CI 分别
运行 Chromium、Firefox、WebKit，包括隔离的账号邮件流程；验证登录及主要管理
页面在 320、375、768 CSS 像素下的布局、表格局部滚动和日期时间对话框。实际按键
测试覆盖 Tab/Shift+Tab、Enter/Space、修饰键快捷操作、离开脚本输入框、取消对话框
以及焦点恢复。

正式发布前仍需人工复核长日志可读性、通知内容，并在实际部署时区确认定时执行。
引擎矩阵不覆盖真实移动设备、读屏软件或每个 Chrome/Edge/Safari 版本。集群故障
测试属于独立能力门槛，不扩大首版单节点兼容承诺，也不代表完整 Cronicle API 或
插件兼容。
