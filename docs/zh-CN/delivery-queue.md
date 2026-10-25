# 持久投递队列契约

[English](../delivery-queue.md) | 简体中文

本文说明 OwlCron 内部持久通知队列的当前契约。队列仍位于 `engine`，没有新增独立包或
发布 `delivery-kit`。实现见[队列源码](../../engine/notification_queue.go)、
[队列回归测试](../../engine/notification_queue_test.go)和
[应用集成](../../engine/notification_integration.go)。
此次收窄还包括[存储契约测试](../../engine/notification_store_contract_test.go)和
[存储适配器测试](../../engine/notification_storage_test.go)。

## 接口边界

[`engine.NotificationStore`](../../engine/notification_contract.go) 只包含队列实际使用的三个操作：

```go
Get(context.Context, string) ([]byte, error)
Update(context.Context, string, func([]byte) ([]byte, error)) error
ListKeysPrefix(context.Context, string) ([]string, error)
```

`NewNotificationQueue` 接受这个接口，以及具名的 `NotificationDeliverer` 回调，签名为
`func(context.Context, *Notification) error`。队列解释投递时机和执行结果，回调解释
`Kind` 与 `Payload`。引擎的[存储适配器](../../engine/notification_storage.go)检查后端能力，不支持时保留
`storage.ErrUnsupported`。实现三个方法，并不自动证明自定义后端具备所需的原子性和持久性。

Go 调用处若将存储值声明为 `storage.Storage`，需要先检查能力，取得 `NotificationStore`；
完整接口没有声明 `Update` 和 `ListKeysPrefix`。OwlCron 启动时通过
[存储适配器](../../engine/notification_storage.go)执行这项检查。

队列不需要[完整存储 API](../../storage/storage.go)中的通用列表修改、流、过期、事务或删除
操作；它仍沿用 OwlCron 的存储错误和现有的无显式版本号 JSON 记录格式，
没有建立独立的错误契约或格式版本契约。

## 存储要求

- `Get` 读取记录，缺失时可通过 `errors.Is(err, storage.ErrNotFound)` 识别。
- `Update` 必须在共享命名空间的多个进程、存储实例之间串行化同一 key 的读改写。
  远端后端仅有进程内互斥锁不够。缺失或过期记录以空输入传给回调；回调返回错误时，
  不得提交该次候选修改；后端包装错误后，仍必须能通过 `errors.Is` 识别原回调错误。
  返回 `nil, nil` 表示要写入的值，不是通用的“不修改”指令。
- CAS 冲突后，更新回调可能执行多次。它必须根据每次输入重新计算结果，不能发送消息或执行
  其他外部副作用。后端必须配合 context 取消，并限制自身的冲突重试次数。
  领取结果在每次回调开始时重置，只有更新返回成功后才调用投递回调。
  见[远端 CAS](../../storage/remote.go)与[Filesystem 锁](../../storage/filesystem.go)。
- 更新成功沿用后端现有的提交确认保证，不增加副本或节点断电持久性承诺；当前
  [Couchbase Insert/Replace](../../storage/couchbase_impl.go) 没有指定显式持久性级别。
  超时、取消、连接故障，以及 Filesystem 在替换或同步阶段报错，都可能使写入结果不确定：
  返回错误不代表一定没有提交。同一稳定 ID 的重复准入，以及租约过期后的领取恢复，
  都用于处理这种不确定性。
- `ListKeysPrefix` 列举指定命名空间中的有效 key，可能读取记录正文检查过期；它不是所有记录的
  事务一致快照。列举后记录可能被删除，`List` 容忍后续 `Get` 返回记录不存在。
  列举失败必须返回错误，不能伪装成空队列。见[前缀列举](../../storage/key_prefix.go)。

队列不提供跨 key 事务，也不与外部副作用组成事务。协作节点必须一致使用
`notifications/` 命名空间。租约时间采用墙上时钟的 Unix 纳秒值，时钟同步与后端可用性
仍是部署条件。

## 准入与持久身份

记录 key 为 `notifications/` 加通知 ID 的 SHA-256 摘要。准入接受 1–1024 字节的 ID、
1–64 字节的 kind，以及 JSON 编码后不超过 256KiB 的 payload。大小限制作用于编码结果，
不是调用方输入对象在内存中的大小。

**同一 ID、同一 kind 采用首次写入为准。** 后续使用不同 payload 重复准入仍会成功，
但不会覆盖原记录，也不会产生另一条投递。相同 key 下记录的 ID 或 kind 不同时返回
`ErrNotificationConflict`。`ContentHash` 保存首次 kind 与编码后 payload 的摘要，
当前不用于严格的内容冲突校验。不能根据这个字段或错误名称推导更强的去重保证。

现有 JSON 字段名和状态字符串保持为持久格式契约。`sent` 记录清除 payload，但保留身份
和内容摘要作为去重标记，队列不会自动使其过期。dead 记录保留 payload 供人工重放。
删除这些记录会改变去重与恢复行为。

## 领取、结果与重试预算

- 新记录进入 `pending`。已到期的记录，或租约已过期的 `inflight` 记录，可以被原子领取。
  领取增加 `Attempts`，写入随机租约 token 和到期时间，并将状态改为 `inflight`。
- 未过期租约不会被接管，当前没有租约续期。完成更新必须仍匹配存储中的 `inflight` 状态和租约 token。
  旧执行者不能确认新领取，也不能覆盖新领取的结果。这约束的是存储更新，不是接收方的
  外部操作。只要记录仍由同一 token 持有，租约时间到期本身不会使其确认失效。
- 投递成功变为 `sent`，清除 payload 和诊断。普通投递错误重新进入 `pending`；
  尝试预算耗尽后变为 `dead`。已过期的领取若已达到尝试上限，会直接变为 `dead`，不再投递。
- `NotificationDeliveryError.Permanent` 表示永久失败。`Deferred` 在非永久失败时使记录
  回到 `pending`，并归还该次领取消耗的尝试预算。其他错误在预算耗尽前允许重试。
  分类通过 `errors.As` 识别被包装的错误。
- 普通重试从 `RetryBase` 开始指数倍增，较大的 `RetryAfter` 可以增加等待时间，最终受
  `RetryMax` 限制。延期等待为 `min(24 小时, max(RetryBase, RetryAfter))`，采用独立上限。
- `RetryDead` 只接受 dead 记录，重置尝试次数、下次时间、领取状态和最后错误，保留原身份
  与 payload。

投递语义为**至少一次**。接收方可能已经接受消息，而发送方在持久确认前崩溃或写入失败；
租约恢复后可能再次投递。回调必须将记录视为只读、能处理重复执行，向支持去重的接收方传递稳定 ID，并遵守
取消信号。投递超时是 context 截止时间，不会强制终止回调代码。忽视截止时间的回调可能持续
占用 worker，并与后续领取发生重叠。

领取后父 context 被取消时，队列保留持久 claim，等待租约恢复。投递 context 过期后回调
才返回成功，会按失败处理。投递诊断先经过应用配置的脱敏函数，在 JSON 编码前按原始字节
截断至 1024 字节。如果截断切开 UTF-8 字符，JSON 对无效字节的替换可能使读回的诊断略超
1024 字节；这里不保证严格的持久字节长度上限。

`RunOnce` 返回 nil 不表示每个接收方都已接受消息：投递失败通常会转为持久的重试或死信
状态。存储和处理错误会返回；一次执行返回最先观察到的错误。实际投递结果应读取队列记录。

## 限额与列举成本

配置零值选择默认值：4 个 worker、每批 100 条、8 次尝试、30 秒投递超时、60 秒租约、
5 秒重试基数和 1 小时重试上限。校验允许 1–64 个 worker、每批 1–10000 条和 1–100 次
尝试；时长必须为正，租约长于投递超时，重试上限不小于基数。

`RunOnce` 先列举 `notifications/` 下的**全部** key，排序后再选择轮转批次。
`BatchSize` 限制选中的记录数，不限制列举时间、内存、队列记录总量或后端读取量。
游标只属于本地队列实例，不是持久顺序保证。`Workers` 限制该实例内的投递回调并发，
不是集群所有节点的总并发。

`List` 同样先列举命名空间，再读取记录并按状态筛选。结果 limit 不在 1–1000 时使用默认
100；它不限制读取多少条不匹配状态的记录。HTTP API 另行执行更严格的参数校验与访问控制。

## 留在应用中的职责

引擎保留通知目标选择、稳定业务 ID 生成、payload 准备、本地 spool 回退与重放、维护及
关闭协调、账户邮件规则、凭据、错误脱敏，以及 SMTP/WebHook 传输。这些路径不属于此次
收窄的存储接口。队列记录可能包含私有正文和领取 token；管理员鉴权与 API 字段移除仍由
[通知处理器](../../api/notifications.go)负责。相关运维行为见
[通知配置](configuration.md#邮件webhook-与密钥)和
[外部副作用限制](limitations.md#外部副作用与通知)。

## 后续评估 delivery-kit 的条件

单 key 的领取与重试状态机、窄存储接口和投递回调，是可能的抽取候选；接口变小并不表示
已经可以独立发布。新建库之前至少需要：

- 出现语义相同的真实第二个应用；OwlCron 内多个通知渠道本身不能证明独立需求。
- 确定独立错误契约、持久格式版本与兼容规划，以及现有 key、去重标记的迁移路径。
- 建立可复用的远端后端契约测试，覆盖原子更新回调重试、结果不确定的写入、跨实例领取、
  旧领取确认和恢复。现有 Filesystem 队列回归及后端 CAS 测试是证据的一部分，
  不能据此承诺所有部署和适配器都已经验证。
- 明确共享命名空间、时钟、回调配合、租约重叠和扫描规模的部署边界，并按目标队列规模
  测量。不能仅根据当前批次和租约配置宣传有界读取或恰好一次投递。

本轮继续在 OwlCron 内维护这些决策，让现有队列契约便于测试与审查。
