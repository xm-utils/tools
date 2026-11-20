# logging — 5 分钟上手

## 1. 初始化（main 启动时一次）

```go
import "github.com/xm-utils/tools/logging"

func main() {
    if err := logging.Init(logging.Config{
        Service:    "order-svc",
        Level:      "info",
        OutputFile: "/var/log/order/app.log", // 可选：文件输出 + 按大小轮转
    }); err != nil {
        panic(err) // 启动期配置错误直接退出属于业务选择，组件本身不 panic
    }
    defer logging.Close() // 关闭文件句柄（未配 OutputFile 时为空操作）
}
```

## 2. 打带 traceId 的日志

```go
// 请求链路内（ctx 已被 trace/grpcx/httpclient/mq 贯通）：
logging.Context(ctx).Info("order created")
// 输出：{"level":"info","msg":"order created","time":"...","service":"order-svc","trace_id":"4bf92f..."}

// 组件/模块内部按注入模式使用：
log := logging.For("deadletter")
log.Warnf("recover failed: %v", err)
// 输出：{...,"service":"order-svc","module":"deadletter","level":"warning",...}
```

无链路上下文的 ctx 不会附加 trace_id 字段，无需判空。

## 3. 接入动态调级（可选，装配层实现）

logging 只提供键名约定 `LevelWatchKey` 与应用端点 `SetLevel`，监听循环写在应用装配层（完整参考实现见 README「运行期动态调级」一节，约 40 行）：

```go
// 装配层核心就三行：监听 → 变更 → 应用
changes, _ := cfgSrc.Watch(ctx, logging.LevelWatchKey("order-svc"))
for ch := range changes {
    if err := logging.SetLevel(string(ch.Value)); err != nil {
        log.Warnf("忽略非法日志级别 %q: %v", ch.Value, err)
    }
}
```

## 4. 运行期改级验证

```bash
# 运维侧（consul KV 示例）：
consul kv put /log/level/order-svc debug

# 应用侧立即生效，无需重启：
curl ... ; grep '"level":"debug"' app.log | head
consul kv del /log/level/order-svc   # 删除键 → 回退首次启动监听的级别基线
```

## 5. 全链路 grep 排障

```bash
# 拿到一个 trace_id 后：
grep '"trace_id":"4bf92f..."' order-svc.log payment-svc.log | sort -k time
```

## 常见错误

| 现象 | 原因 |
|---|---|
| 日志没有 trace_id | ctx 未经过 trace 贯通的入口（grpcx/httpclient/mq 拦截器），或 trace 未 Init（noop 无有效 ID） |
| `logging: 非法日志级别` | 级别拼写错误；合法值 panic/fatal/error/warn/info/debug/trace |
| 调级不生效 | 键名不是 `logging.LevelWatchKey(service)` 拼出的 `/log/level/<service>`；或推送值非法（`SetLevel` 报错被忽略维持现级别）；或装配层根本没启动监听循环 |
| 重复 Init 后 service 消失 | Init 是整体覆盖语义，需带上全部期望字段 |
| 日志文件没有生成 | lumberjack 首次写入才建文件；确认 Init 未因路径不可用报错（报错时保持原配置不切换） |
| 文件有内容但控制台没了 | AlsoStdout 被显式设为 false；默认双写 |
