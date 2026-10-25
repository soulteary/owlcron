# Redis 单实例存储支持计划

状态：已实现单实例 Redis 8.4+ 原生 CAS，保持既有数据与备份格式。kit 前置补丁已提交为草稿 PR；OwlCron 已固定该提交，不依赖本地 replace。

日期：2026-10-25。OwlCron 核查基线为 `ff5d6c22f4a854a7c28e1f0ed9a5ebf5d7156d69`。实现已同步至主分支 `693076c`。本文记录本次实现范围、依赖选择与验收要求；代码仍在开发分支，尚未合并或发布。

## 1. 已确定的范围

- 新增可选 `Redis` 存储引擎，首版只连接单个 Redis 8.4+ 实例，使用原生 CAS，不引入 Lua；允许多个 OwlCron 节点共享该实例。
- 保留现有业务 key、JSON 记录、JSON 数组列表、日志 manifest/chunk，以及 `remoteDocument{data, expires, deleted, revision}` 封装。Redis 中每个业务记录保存为一个 String。
- 保留 `owlcron-storage-v1` 备份格式，补齐 Redis 的导出、导入、跨后端迁移和恢复验收。
- 真实 Redis 服务、真实 daemon、进程强杀及网络故障测试是交付条件，mock 通过不能替代。
- 默认仍为 Filesystem。本期不引入 Sentinel、Cluster、原生 Redis List/Stream、分布式锁替换、缓存回退和调度算法调整。
- 保留已有远程存储边界：单 key 原子更新，不承诺跨 key ACID、在线一致性备份或业务任务恰好执行一次。

## 2. 三个 kit 的采用结论

核查了发布 tag 与默认分支源码，而非仅按 README 或库名判断。

### redis-kit：采用已补齐的客户端层

核查版本：[v1.7.3](https://github.com/soulteary/redis-kit/releases/tag/v1.7.3)，commit `4708fb0673af1e785323245e2de874ccf5504f8e`。main 为 `5b03fa3155d632f6496319428ee28faa4d06750e`，业务源码与发布版一致，差异为 CI/报告更新。

[`client.NewClient`](https://github.com/soulteary/redis-kit/blob/v1.7.3/client/client.go) 返回原生 `*redis.Client`，可以直接调用 `Get().Bytes()`、`Eval`、`Scan` 和 `Close`。已有连接池、密码、DB、拨号/读写/连接池超时和健康检查，无 Zap 耦合。OwlCron 特有的 CAS、key 枚举和 envelope 留在 OwlCron，不应扩展为 kit 的业务协议。

已在 kit 提供以下兼容性补丁：

1. `Config` 增加业务 Redis 的 `Username`、`TLSConfig`、`ContextTimeoutEnabled` 并透传到单实例构造器；现有 `SentinelUsername` 不能替代业务 ACL 用户名。`Username` 属于通用认证配置，应同步映射到 `NewClient` 和 `NewUniversalClient` 的底层选项。
2. 增加接受调用方 context 的构造入口，例如 `NewClientContext(ctx, cfg)`；现有构造器保留，避免破坏既有用户。连接探测应同时服从调用方取消与拨号预算。
3. 保留 `MaxRetries=-1` 禁用重试的能力，并添加覆盖上述映射和取消行为的测试；`0` 在底层并不表示禁用重试。
4. 保留既有连接池和重试默认值。硬连接数上限的额外 API 不属于本次补丁，不把 `PoolSize` 当作绝对上限。

以上补丁已提交为 [redis-kit PR #13](https://github.com/soulteary/redis-kit/pull/13)，提交 `973602e1662bc71af1edd6908394a4a2c33cd200`。OwlCron 固定 `v1.7.4-0.20261025032353-973602e1662b`，从公共模块源即可独立构建；未创建发布 tag，也未合并主分支。

ACL 用户名作为独立、范围很小的 kit 改进已完成：新增 `Username` 字段、可选的 `WithUsername` 和两处构造器透传，保留用户名为空时原有的默认用户认证行为。无需新增依赖或自行发送 AUTH，更不需要在 kit 实现 ACL 用户创建、权限管理、配置下发或管理员账户管理。验收覆盖旧密码模式、禁用 default 用户后的命名账户登录、错误凭据失败，以及业务用户名和 Sentinel 用户名分别映射。命令/key 权限仍由服务端管理员配置。[AUTH 说明](https://redis.io/docs/latest/commands/auth/)

Lua 也不是 kit 待补的客户端能力：`client` 只创建原生客户端并 PING；已有 `lock`、`ratelimit` 子包自身已使用 Eval。是否使用脚本由导入和调用这些功能决定，OwlCron 无需为 CAS 给 kit 新增通用脚本框架。[锁脚本](https://github.com/soulteary/redis-kit/blob/v1.7.3/lock/handle.go)、[限流脚本](https://github.com/soulteary/redis-kit/blob/v1.7.3/ratelimit/manager.go)

不采用 kit 的 `cache`、`lock` 和混合回退作为存储实现。其 cache 会再次 JSON 编码且不提供现有 CAS 协议；锁模块也不能替换 OwlCron 已有的租约与任务协调。`testutil.MockRedis` 仅识别有限的内置 Lua，不支持完整 SCAN/通用 Lua，不能用于证明此次适配正确。

来源：[配置](https://github.com/soulteary/redis-kit/blob/v1.7.3/client/config.go)、[缓存实现](https://github.com/soulteary/redis-kit/blob/v1.7.3/cache/redis.go)、[mock](https://github.com/soulteary/redis-kit/blob/v1.7.3/testutil/mock_redis.go)、[底层重试选项](https://github.com/redis/go-redis/blob/v9.23.0/options.go)、[EOF 重试](https://github.com/redis/go-redis/blob/v9.23.0/error.go)。

### parser-kit：未来多源数据导入可用，本期不引入

核查版本：[v3.0.2](https://github.com/soulteary/parser-kit/releases/tag/v3.0.2)，commit `0428276e81bca2cfc2e747c13bf7f8bdb584c4b7`，与 main 一致。

它是多源 JSON 数组 loader，提供 `NewLoader[T]`、`Load/LoadOne`、`File`、`BytesFetcher`、Redis/HTTP source、fallback 和 merge，并非 cron 表达式解析器。现有配置是单个 Config 对象，事件恢复还要核对分页索引与 canonical record；它不能替代这些流程，也没有写入或 CAS 能力。

以后新增“从文件、HTTP 或 Redis 导入事件模板、插件配置等 JSON 数组”时可以采用，先形成导入预览，再经过 OwlCron 的校验和保存接口。届时须明确：

- 合法空列表应设置 `AllowEmptyData=true`，避免默认 fallback 把已删除条目从备用源重新带回。
- merge 允许部分源失败，并且后处理源会覆盖同键值；这不是严格、完整的恢复协议。
- Redis source 的 `STRLEN` 和 `GET` 分两次执行，不应把它当作原子的读取内存上限。
- 用户可配置 HTTP 来源时，继续执行项目的网络访问限制，不能只依赖 URL 语法校验。

来源：[loader](https://github.com/soulteary/parser-kit/blob/v3.0.2/loader.go)、[选项](https://github.com/soulteary/parser-kit/blob/v3.0.2/types.go)、[Redis source](https://github.com/soulteary/parser-kit/blob/v3.0.2/redissource/redissource.go)、[HTTP source](https://github.com/soulteary/parser-kit/blob/v3.0.2/remotesource/remotesource.go)。

### cache-kit：非权威查询缓存可用，本期不引入

核查版本：[v2.0.3](https://github.com/soulteary/cache-kit/releases/tag/v2.0.3)，commit `ddc7d8e81005fe882fbefb1ebd7002ec5f77ee1b`。main 为 `aeed2e796c8bb7fca53da09c8331cf32c21df07a`，仅领先 CI/报告更新，功能源码一致。

`MemoryCache[V]` 的整体快照、多索引和内容 hash，可用于未来经测量确有需要的模板或插件展示查询。当前 scheduler 已有加锁 event map 和 `CloneEvent` 边界，引入会重复建模；缓存的值复制也不能代替包含 map/slice/pointer 的深复制。

Redis 子包不适合本项目的持久存储协议：

- 保存整组 `[]V` JSON，旁边有版本计数 key；`SET + INCR` 虽在事务内执行，但不检查 expected version，不是 CAS。
- 默认 TTL 为一小时，传入非正 TTL 仍会回退一小时；缺失 key 返回成功的空列表。均不符合现有永久记录与 `ErrNotFound` 约定。
- `Hybrid.Set` 先写内存，再写 Redis；失败后本机已改变，读取只看本机，且没有自动跨节点失效传播。
- 内存索引存在自动归一化、冲突覆盖和校验失败跳过条目的行为，不宜接管必须完整恢复的数据。

因此不放进主节点租约、调度状态、账号权限、session、API key 等权威链路。

来源：[内存实现](https://github.com/soulteary/cache-kit/blob/v2.0.3/memory.go)、[Redis 实现](https://github.com/soulteary/cache-kit/blob/v2.0.3/rediscache/rediscache.go)、[Hybrid](https://github.com/soulteary/cache-kit/blob/v2.0.3/rediscache/hybrid.go)、[现有 scheduler](../../engine/scheduler.go)、[深复制](../../types/clone.go)。

### 依赖与版本成本

三个 kit 的上述发布版均为 Apache-2.0，`go.mod` 均要求 Go 1.27.3；Redis 相关子包使用 `go-redis/v9 v9.23.0`。parser 的 HTTP 子包另用 `http-kit/v2`。parser/cache 的根包与 Redis/HTTP 子包分离，不必为了使用根包链接全部客户端。

核查时 OwlCron 声明 `go 1.26.0`、`toolchain go1.27.3`；本次已将最低 Go 版本提升到 1.27.3，并同步模块清单与上游许可证材料。

来源：[OwlCron go.mod](../../go.mod)、[redis-kit go.mod](https://github.com/soulteary/redis-kit/blob/v1.7.3/go.mod)、[parser-kit go.mod](https://github.com/soulteary/parser-kit/blob/v3.0.2/go.mod)、[cache-kit go.mod](https://github.com/soulteary/cache-kit/blob/v2.0.3/go.mod)。

## 3. 存储适配设计

### 原始格式与单 key CAS

新增 `storage/redis.go`，`RedisStorage` 嵌入 [`remoteStorage`](../../storage/remote.go)，实现 `versionedKV.read` 与 `compareAndSwap`，复用 Put/Get、JSON 列表、逻辑过期、墓碑和 Update 重试逻辑。

- `read` 原样获取 envelope 字节，返回 opaque version。优先用完整原始 envelope 的无歧义编码作为比较 token，避免另建版本 key，也避免 Lua 重新编码 JSON 改变字节。空 version 专门表示“原来不存在”。
- CAS 是必须满足的契约。仅支持 Redis 8.4.0+，用 `SET key new IFEQ old` 比较旧字节后写入，缺失时用 `SET NX`；不使用 Lua、WATCH 或原生 List，也不改变 envelope。
- 启动通过 `INFO server` 明确拒绝旧版本及非 standalone 服务，并验证两种条件 SET 的成功、失败及读取、扫描、删除能力。探针每次写入均带短期物理 TTL，健康路径删除；取消后依赖 TTL 回收，不另加同步清理预算。
- 只有服务端明确返回“版本比较失败”才映射 `ErrConflict`；不存在映射 `ErrNotFound`。认证、TLS、OOM、只读、协议损坏、连接错误和取消必须保留错误。
- 所有读写服从 context 和有界 socket/pool 超时。显式禁用客户端传输重试；仅确定的 CAS 冲突交由现有 `mutate` 重试。
- 已提交但响应丢失属于结果不确定，返回错误，不能映射冲突、假报成功或再次执行回调。后续若要确认提交，可独立设计基于唯一 revision 的核对，本期不隐式加入。
- 保持 `BeginTransaction -> ErrUnsupported`，与现有远程后端一致。

单 key CAS 是调度正确性的要求：[主租约](../../engine/leadership.go) 和 [`UpdateList`](../../storage/storage.go) 都依赖它。不能用本地互斥锁、无条件 SET 或 cache-kit 的版本递增替代。

依据：[原生 SET 条件比较与版本要求](https://redis.io/docs/latest/commands/set/)。原生 CAS 不能消除提交响应丢失，也不代替 AOF 耐久性配置。

### TTL、墓碑和枚举

- 继续使用 envelope 中的绝对 Unix 秒 `expires`；Put/Update 清除 TTL，过期记录不能通过延长 TTL 复活。首版不映射 Redis 的物理 TTL。
- 保留 `deleted` 和每次变更的新 revision；不顺手物理清除底层墓碑。主租约中的前代 owner 信息和应用永久栅栏也不得因缓存过期被回收。
- 实现 `KeyLister` 与 `PrefixKeyLister`。使用 SCAN 完整遍历游标，处理重复 key 和空页，去重、排序；MATCH 对 `*?[]\\` 做字面转义，取回后再次严格检查字面前缀，验证逻辑 key。
- 过滤逻辑过期、底层 deleted 记录；应用层日志删除 manifest、deleted_jobs 与通知幂等记录仍是有效业务数据，必须可枚举、可备份。
- 损坏 envelope 或不匹配的数据类型应报错，不能按“不存在”隐藏。扫描不读其他命名空间的 value，不使用 KEYS；SCAN MATCH 仍会遍历该 DB，容量成本应写入文档。
- `ListKeys` 接口本身需要收集 key，不能因 Redis 使用游标就声称全流程常量内存或在线快照。

依据：[前缀接口](../../storage/key_prefix.go)、[备份枚举](../../storage/backup.go)、[SCAN 的重复与并发边界](https://redis.io/docs/latest/commands/scan/)。

### 日志与连接配置

实现 `LegacyLogRemover`，校验 job ID 后直接删除本命名空间的 `job_logs:` 旧 payload，不为删除下载整个 value；复用现有 manifest 删除栅栏。首版支持现有 64 KiB 分块日志；旧单块二进制日志范围读取不纳入本期，明确返回不支持并记录离线迁移约束。

配置继续通过现有 `storage_engine`、`storage_options` 传递；新选项覆盖 addr、DB、非空命名空间 prefix、ACL 用户名/密码、TLS CA/server name，以及 dial/read/write/pool 超时。密码支持环境变量来源；配置和错误不得输出秘密。首版固定单实例构造器，拒绝误填 Cluster/Sentinel 选项，不静默忽略。

启动进行有界的连接、读写、CAS 和枚举能力探测，探测使用独立临时 key 并清理；权限不足或配置错误应明确失败。应用不要求 CONFIG 管理权限。持久化和淘汰策略由部署清单与隔离验收环境核验，不能凭 PING 成功宣称数据耐久。

生产基线：专用实例或受控 DB/命名空间，`maxmemory-policy noeviction`，启用 AOF；耐久验收使用 `appendfsync always` 和持久数据卷。`everysec` 是存在最近写入丢失窗口的可选部署折中，不能宣传为零丢失。Redis 单点故障期间停止依赖共享存储的派发，不切换本地权威状态。

日志也会占用 Redis 内存；继续使用现有保留期和单任务大小限制，并监控内存、AOF、扫描耗时和清理积压。逻辑过期与底层墓碑保留不会自动释放 Redis key，本期不附带未经验证的物理 GC。

依据：[日志删除接口](../../storage/log_legacy_range.go)、[分块日志](../../storage/job_logs.go)、[持久化](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)。

## 4. 备份、恢复与迁移

[`Export/ExportTo/Import`](../../storage/backup.go) 已通过 capability 工作，没有需要新增 Redis 的后端类型 switch。嵌入 remoteStorage 后，现有导出可以读取 expires。新增实现应复用这些能力，避免另建 Redis 专用备份格式。

恢复规则：

1. 停止全部源端写入后导出，恢复到离线、空白的目标命名空间；导入不会自动清空目标，禁止使用 FLUSHDB/FLUSHALL 简化流程。
2. 保留二进制数据、JSON 列表、绝对 expires、调度游标、日志 manifest/chunk、应用层删除栅栏与通知幂等记录。
3. 沿用既有过滤：不导出/恢复 `global/master_lease`，清除 `global/state._lease_owner`；不把旧 owner 带到恢复后的调度器。
4. 远程恢复依次写记录和索引、再恢复过期时间，不宣称整个导入原子。失败时保留错误，目标仍离线，修复后在可确认状态下重跑；完整校验通过前不启动 daemon。
5. `ExportTo` 继续用临时文件成功后原子替换；扫描或写盘中断不得覆盖已有成功备份。备份不包含远程实例的连接凭据。
6. 备份恢复不能替代本地 `queue_dir`、`log_dir` 的持久化；活跃任务和 completion spool 沿用既有进程恢复协议。

验收至少完成 Redis→Redis 新命名空间、Redis→Filesystem、Filesystem→Redis；已有 S3/Couchbase 隔离框架中再验证与 Redis 的双向逻辑备份兼容。比较逻辑业务数据与 TTL，不比较新生成的底层 revision。过期记录不复活，应用删除栅栏仍能阻止迟到完成恢复已删数据。

## 5. 实施顺序与文件范围

### 第一步：客户端前置能力

- redis-kit 补齐上面的配置与 context API，测试并提交审阅；OwlCron 固定公开可获取的不可变提交版本。
- 更新 OwlCron `go.mod/go.sum`、最低 Go 版本和许可证清单。`licenses-update` 不会自动生成 Go 清单，应核对 `licenses/go-inventory.json` 与 upstream notice。

完成条件：ACL/TLS/context/禁用重试均能通过实际调用验证，不能仅检查字段存在。

### 第二步：Redis 适配与契约测试

- 新增 `storage/redis.go` 及对应测试，实现 read/CAS、Close、KeyLister、PrefixKeyLister、LegacyLogRemover；用编译期断言检查接口完整。
- 扩展 `storage/storage.go` 的构造 switch 与说明。
- 复用 `storage/integration_test.go` 的 `exerciseStorage`，扩展缺配置拒绝列表，补充真实 Redis 的枚举、字节格式、TTL、墓碑、损坏数据及取消测试。
- `types.Config`、`engine.NewEngine`、`cli.openStorage` 已通用透传，不为新增后端重写配置或 CLI 架构。

完成条件：同一 Redis 上两个独立客户端、多个 goroutine 均能保持现有契约，现有后端测试不回归。

### 第三步：备份与恢复验收

- 扩展 `storage/backup_test.go`、`storage/backup_stream_test.go` 和 `cli` 的真实 Redis 集成测试，覆盖上述往返与失败条件。
- 检查 `ExportTo` 的 TTL 与流式输出、Import 的离线和部分完成行为；除发现共性缺陷外，不改备份格式和远程恢复承诺。

完成条件：空目标恢复后可读取配置、历史、日志与游标，可重新获取新租约；过期记录和旧 owner 不复活。

### 第四步：真实故障与 daemon 验收

- 新增 `storage/redis_fault_integration_test.go`，复用 `newRemoteContainerControl`；只操作显式登记、带测试标签的专用 Redis 容器。
- 现有控制器仅支持 pause/unpause/stop/start/restart。扩展 `tests/acceptance_control.py`、`storage/remote_container_test.go` 和 `process_remote_outage_test.go` 的白名单 action，提供显式 `kill/SIGKILL` 并补控制器测试；仍要求容器 ID 与测试标签校验。普通 stop/restart 不能替代强杀证据。
- 扩展 `api/system_integration_test.go`、`engine/scheduler_cluster_test.go` 的后端列表和配置 helper。
- 扩展 `process_remote_test.go`、`process_capacity_test.go`，新增 `OWLCRON_REMOTE_REDIS_CONFIG` 的配置读取。
- **修正 `process_remote_outage_test.go` 的后端选择为显式 switch。** 现有逻辑默认选择 S3 容器、只对 Couchbase 特判，直接增加 Redis 用例会操作错误容器。
- `tests/run-linux-acceptance.sh` 增加固定版本/摘要的 Redis 镜像、持久卷、隔离网络、清理、控制器 allow-list、环境变量和测试正则；不发布宿主端口、不把 Docker socket 挂给测试进程。
- `Makefile` 增加可独立执行的 Redis 真实存储/故障入口，并把 Redis 加入完整 integration、remote-storage-faults、remote-process-integration 门禁。普通开发测试允许未配置时跳过，显式验收入口缺少服务必须失败。
- 沿用 `.github/workflows/process-acceptance.yml` 的 Linux remote job，调整时限与证据收集；确保 Redis 用例实际运行，避免正则漏选或环境变量缺失导致全跳过。

完成条件：以下验收矩阵通过并保留本次运行证据。

### 第五步：文档与发布检查

更新中英文 configuration、architecture、compatibility、testing、deployment/containers/operations 中涉及后端支持的内容，确认 README 和容器示例保持一致。包含 ACL/TLS、单实例范围、AOF 与 noeviction、备份停写步骤、内存和逻辑墓碑增长、网络错误处理及真实测试入口。

执行 `make docs-check`、许可证检查、相关单元/集成测试、race、vet、完整回归与 Linux 故障验收。只报告实际跑过的范围，首版只声明经过固定镜像版本验收的 Redis 兼容范围。

## 6. 验收矩阵

以下为验收目标，不应将清单本身当作测试通过证据。本轮实际完成的验证记录见下一节。

- **格式兼容**：Redis 原始 String 可以解析为原 envelope；二进制往返无二次 JSON 编码；列表仍是原 JSON 数组；日志 chunk 与 manifest 无格式迁移。
- **跨客户端并发**：复用 40 并发 push 不丢项，shift/pop 无重复消费；首次创建竞争、旧版本、删除后重建、TTL 后重建均拒绝过时 CAS。
- **TTL**：绝对过去时间含 0/负数立即逻辑过期；过期不能延长复活；Put/Update 清除过期；备份往返保留未来到期时间，租约和永久栅栏无物理 TTL。
- **枚举**：多页、空页、重复 key、特殊字符前缀、跨租户前缀、过期与两类墓碑、损坏 JSON/错误类型、过程中取消；停写备份包含所有有效记录。
- **备份故障**：写盘错误、扫描断线、恢复中途断线、过期时间恢复失败均返回错误；原成功备份不被部分文件覆盖，部分恢复目标不能被当成成功启动。
- **暂停服务**：并发读写使用短 deadline；排队获取连接、socket 读写均有界退出；已取消请求不得转换成 NotFound 或 Conflict。
- **断连与重连**：真实连接拒绝和网络黑洞均返回错误；恢复网络后同一客户端与新客户端都能继续使用，未确认写入不被假报成功。
- **提交响应丢失**：代理确认一次 CAS 写命令已到服务端并执行后，丢弃返回；调用返回非冲突错误，断言只有一次实际变更，没有隐式重放。另测未提交即断线，区分错误与数据现状。
- **Redis 重启**：使用持久卷与 `appendfsync always`，真实 SIGKILL 后恢复已确认写入；保存旧 version，在新 owner 修改后确认旧 CAS 被拒绝；再跑跨客户端并发契约。
- **写入被拒**：受控 OOM/noeviction、只读/权限不足不能被当作不存在或成功；存储不可用时 readiness 和租约/派发沿用现有失败处理。
- **daemon 故障**：Redis 后端上实际运行任务，强杀 daemon，验证活跃进程核对、日志及 completion spool 恢复、通知租约恢复、主节点接管、旧主停止派发、删除栅栏有效。

故障证据记录实际镜像 digest、Redis 持久化/淘汰配置、用例名称、时间范围、退出状态和日志；严格区分 pass、fail、skip。进程 SIGKILL 与网络故障验收不能宣传为已验证宿主断电、磁盘损坏或主从故障切换。

## 7. 本轮验证记录

使用 Go 1.27.3 和真实 Redis 8.4.7；AOF `appendfsync always`、持久数据卷、noeviction，默认账户关闭，命名业务账户不允许脚本及管理命令。

- redis-kit：全量测试、全量 race、vet，以及重复三次的客户端 TCP/TLS 生命周期测试通过。
- OwlCron：从固定 kit 提交独立构建，全量 `go test -race ./...` 与 vet 通过；34 项 Python 测试、文档、格式、模块校验和许可证检查通过。
- 真实 Redis：原 envelope/二进制/列表、跨客户端 40 并发、TTL、前缀隔离、Redis 与 Filesystem 备份往返、CLI 导出导入及迁移通过。
- 真实网络故障：提交前断连、网络黑洞、提交后响应丢失、导出/导入中断与恢复重跑通过；对未知提交断言发送、回调和实际提交均只有一次。
- 真实 TLS：可信 CA 正常读写；错误主机名、不受信任 CA、错误命名账户均被拒绝。启动探针超时与途中取消均在预算内返回。
- Redis 进程：真实 SIGKILL 后 AOF 恢复已确认数据，旧 CAS 被拒绝；测试容器白名单与标签验证通过。
- Linux 存储：最终版本的 11 项真实 Redis 存储、TLS、备份、网络和强杀用例全部通过，无 skip。套接字 deadline 与 context timer 的错误归一化竞态已修正，确定性回归及真实启动预算测试通过。
- Linux daemon：race 模式下 ActiveJobAndChunkedLog、MailAndWebHookQueue、ClusterPartitionAndFailover、StorageOutageCompletionSpool 四项全部通过，无 skip，包含真实 daemon 强杀、Redis 暂停/强杀和完成 spool 恢复。

CI 已接入固定 Redis 镜像及 TLS 服务，逐项核对存储、CLI、API/集群及 daemon 用例 PASS，拒绝遗漏或跳过。整套 MinIO/Couchbase/Redis 联合脚本尚待远程 CI 执行；本轮本机真实后端验证聚焦 Redis，并未重跑 S3/Couchbase 服务或验证跨它们的实际迁移。宿主断电、磁盘损坏、Redis 主从切换、硬容量认证不在已验证范围。

## 8. 后续扩展边界

单实例意味着 Redis 本身仍是可用性单点。Sentinel/Cluster 另行设计和验收；异步复制可能在切换时丢失已确认写入，不能只把构造器替换为 UniversalClient 就宣称保持租约安全。[Redis 复制说明](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)

parser-kit 的外部数据导入、cache-kit 的非权威查询缓存、原生 Redis 数据结构优化与物理墓碑 GC 都作为独立后续需求，不扩大本次存储首版范围。
