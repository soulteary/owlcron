# 运行版本管理的脚本文件

[English](../script-files.md) | 简体中文

`make build` 会生成 `owlcron-script`。适配器执行管理员配置的固定命令，
向标准输入传递一行任务 JSON，把脚本的标准输出和错误作为日志，并根据退出码
生成完成消息。脚本不需要实现插件协议，也不能通过输出 JSON 伪造成功。

```json
{
  "id": "daily-report",
  "title": "Daily report script",
  "enabled": 1,
  "command": "/opt/owlcron/owlcron-script -- /bin/sh /opt/jobs/report.sh",
  "cwd": "/opt/jobs",
  "params": [
    {"id": "args", "type": "text", "title": "Arguments (JSON array)", "value": "[]"}
  ]
}
```

事件可传入 `"params": {"args": "[\"daily\",\"two words\",\"\"]"}`。
参数直接传递，不做 Shell 展开。`args` 最多编码 256 个字符串、64 KiB；任务输入
限制为 1 MiB。可执行文件必须使用绝对路径。任务参数不能替换命令、解释器、
脚本路径、工作目录、环境或系统身份。需要原生进度或报表协议的插件应直接执行。

插件命令仅由可信管理员管理，脚本拥有服务账号权限且没有沙箱。仍须审查脚本
自己的参数解析逻辑。所有候选节点都应部署同一经审核的脚本版本，不要在任务
执行时自动拉取未审核代码。适配器不另建进程组，引擎取消会回收适配器及普通
后代；主动脱离进程组的进程不在保证范围内。
