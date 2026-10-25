# 不可变对象存储

[English](../blob-storage.md) | 简体中文

## 与调度状态分离

`blob` 包独立于 `storage.Storage`，保存不可变的日志块和索引页。调度状态继续依赖
`AtomicStorage.Update`；对象适配器不提供 CAS、追加、事务或过期接口。

基础层提供适配器、存储注册表，以及布局初始化和校验函数。日志仓库、守护进程、离线
CLI、完整备份和迁移由拆分存储系列的后续改动接入。仅加入基础层不会切换已有日志
格式，原有 S3 和 Redis 状态存储行为保持不变。

调用方必须在上传前分配对象 key，并且只能用完全相同的字节重试同一个 key。对象写入
成功不表示日志已经发布；发布由元数据根记录的原子更新决定。内容身份由 SHA-256 和
精确长度确定，ETag 不参与协议。这是应用协议管理的不可变性，不是服务端 WORM 保证。

## 适配器契约

`blob.Store` 提供 `Put`、`Open`、`Stat` 和 `Delete`。首版每个对象最多 64 KiB，索引页
也遵守此限制。`Put` 在任何后端写入前核对声明长度和 SHA-256，更大的内容由所属业务分块。

S3 适配器使用 OtterIO SDK 发送普通、长度已知的对象请求，不添加 `If-Match` 或
`If-None-Match`，也不构造 S3 状态适配器或执行条件写探针。请求保留原始字节，从请求
开始到响应体读取结束受 30 秒超时约束，同时遵守调用方更短的截止时间。写入结果不确定
时，将错误交给日志仓库，按原 key 核实。权限失败和 Bucket 缺失不会转换为对象不存在。

Filesystem 适配器先写入并同步私有临时文件，再以不替换已有 key 的方式发布。相同内容
可幂等重试，内容冲突返回完整性错误，并同步父目录。对象路径拒绝目录穿越和符号链接。
后续库存扫描可以清理超过一小时、从未发布的临时文件。

Filesystem 对象目录与元数据目录必须分开。不同主机使用相同路径字符串，并不意味着
它们访问同一份共享存储。

## 库存扫描

`blob.Inventory.Scan` 接受前缀、不透明游标和 1–1000 的数量限制。扫描不提供一致性
快照，正常读取也不依赖枚举结果。

S3 使用有界的 ListObjects 请求。Filesystem 最多保留八个目录遍历器，每次检查最多
`max(256, 2 * limit)` 个目录条目，不会加载或排序整个目录。Filesystem 返回顺序不固定，
空页也可能带有非空游标。该游标只可使用一次，闲置三十分钟后失效，进程重启后也不能继续
使用。遇到 `blob.ErrInvalidCursor` 应从头重扫。周期扫描同样必须重新开始，以发现迟到
写入以及并发变化期间遗漏的对象。

## 注册表与存储绑定

`BlobStorageConfig` 包含 `write_store` 和 1–16 个命名 `stores`。Store ID 使用字母、数字、
`_` 和 `-`，各 ID 必须对应不同物理位置。Filesystem 需要 `engine: Filesystem` 和
`base_dir`。S3 需要 `engine: S3`、`bucket` 和凭据；自定义 OtterIO 端点应设置
`path_style: true`。可选项包括 `endpoint`、默认值为 `us-east-1` 的 `region` 和 `prefix`。

S3 凭据可以来自 `access_key` / `secret_key`，或 `access_key_file` / `secret_key_file`。
同一凭据不能同时设置值和文件。均未设置时，注册表读取 `OWLCRON_BLOB_ACCESS_KEY_ID` 和
`OWLCRON_BLOB_SECRET_ACCESS_KEY`。`session_token` 可以回退到
`OWLCRON_BLOB_SESSION_TOKEN`。不要把凭据文件加入版本控制。

绑定指纹包含 Bucket 和前缀，或解析后的 Filesystem 目录。凭据和端点不进入指纹，允许
轮换，但只读命名空间标记检查仍须证明客户端访问同一数据集。修改端点不允许自动用空
Bucket 替代原数据。

## 显式初始化布局

`InitializeStorageLayout` 是面向全新元数据目标的离线函数。它先在
`global/storage_layout` 中原子预留随机命名空间，再检查普通 PUT/HEAD/GET/DELETE，写入
各存储的身份标记，最后进入 `ready`、`restoring` 或 `migrating` 阶段。初始化中断后，只有
相同绑定和目标阶段才能恢复同一个命名空间。

`ValidateStorageLayout` 只接受 `ready`；指定阶段的校验函数用于离线恢复。`Readiness`
只读取很小的身份标记，不创建缺失标记、不替换绑定，也不修改业务数据。已初始化布局的
绑定集合固定；在其中切换新日志写入存储，不会改变旧引用的 Store ID。

所有写入者必须在修改数据前校验布局。旧二进制不认识此标记，必须停止旧进程，或通过
新的元数据目录／前缀及部署凭据隔离旧写入者。恢复旧快照必须使用新命名空间，不能在
原命名空间内回拨上传序号。

共享的 `OpenConfiguredMetadata` 函数保留旧配置的默认行为。配置对象存储后，首版支持
Filesystem 或 Couchbase 元数据。Couchbase 必须设置 `durability: persist_to_majority`，
即便新日志写 v1，已有 v2 根仍然需要持久的序号与删除历史。普通 Couchbase 配置可显式
选择 `none`、`majority`、`majority_and_persist_to_active` 或 `persist_to_majority`；不设置
时保留此前 SDK 默认值。不支持的持久性或拓扑条件会返回错误，不会自动降级。
