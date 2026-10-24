# 检查 Cronicle 迁移

[English](../migration.md) | 简体中文

OwlCron 的目标是替代 Cronicle，并提供选定范围内的兼容。转换数据前可先运行只读预检；它不会导入记录、执行插件、初始化存储、创建凭据或连接集群。

## 运行预检

使用 Cronicle 自带的 `bin/control.sh export cronicle-export.txt` 导出。支持的输入是 **Cronicle Data Export v1.0**：注释头加逐行 `KEY - JSON`，包含列表头和分页记录，不是 JSON 数组或 OwlCron 备份。原始导出可能含密码哈希、API key、脚本和环境变量，应妥善保管。该导出不包含已完成任务历史、日志、会话及应用配置。

在源码目录构建并运行独立检查工具：

```sh
go build -o owlcron-migrate-check ./cmd/owlcron-migrate-check
./owlcron-migrate-check --export cronicle-export.txt
./owlcron-migrate-check --export cronicle-export.txt --config cronicle-config.json
```

可选的 `--config` 只检查现有 Cronicle JSON 配置，不会用它初始化 OwlCron。工具没有写入或导入模式，每个输入文件上限为 32 MiB；更大的安装需单独评估。检查正常完成时退出码为 0，即使报告包含待处理项；输入无效、不完整或不可读时退出码为 1。自动化流程可按 JSON `findings` 自行设置迁移门槛。

## 阅读报告

- `supported`：某项行为符合当前数据结构，例如事件时间规则，不表示整个事件或部署已经获准迁移。
- `requires_conversion`：需要转换存储格式、重建凭据或节点身份、映射目标，或复核插件命令和参数。
- `unsupported`：detached 执行、无效时区或时间规则及其他无效事件数据、缺失引用，或 OwlCron 数据结构之外的配置。

`item` 是按分页顺序还原的源列表位置，从 1 开始。计数和报告顺序固定，不输出源 ID、标题、命令、秘密或配置值。结构错误仅定位列表或行号，不回显原始内容。虽然报告不含这些值，导出和可选配置文件仍应作为敏感文件处理。

成功链（`chain`）和失败链（`chain_error`）会按照运行时支持的字符串或数组形式，检查目标是否存在于导出的事件列表。缺失目标分别产生 `missing_chain` 或 `missing_chain_error`，不输出目标 ID。这仅校验引用，不执行链式任务，也不验证运行时行为。

Cronicle 内联 `shell-plugin.js` 需要改为审核后的固定脚本文件和 `owlcron-script` 插件配置。自定义插件须核实运行时、命令解析、uid/gid、环境变量及逐行 JSON 完成协议。工具不会执行命令，不能验证业务行为。参阅[脚本文件](script-files.md)和[兼容性](compatibility.md)。

## 完成迁移

逐项处理报告，准备新的 OwlCron 配置和节点身份，只将已审核的兼容记录转换为 OwlCron 格式。不要把原始 Cronicle 导出直接交给 `owlcron-engine import`。在独立部署中使用合成或脱敏数据，核实时区、事件目标、权限、通知、脚本退出行为及备份，再安排切换和回滚。阅读[部署](deployment.md)和[已知限制](limitations.md)，尤其是远程派发未确认及外部副作用的边界。

可复用的合成样例位于 `migration/testdata` 和 `plugin/testdata`。导出结构依据 [Cronicle 导出实现](https://github.com/jhuckaby/Cronicle/blob/master/bin/storage-cli.js)，样例为测试编写，不含安装数据。测试覆盖分页输入、无效输入、报告确定性、只读行为及分块传输的逐行 JSON 插件输出；这些测试只确认选定契约，不代表兼容所有 Cronicle 版本或插件。
