# logging — 统一日志出口组件

基于 logrus 的全仓库统一日志封装：**JSON 结构化输出 + traceId 自动附加 + 文件轮转输出 + 可调级标准抽象**。对应架构路线图 3.1.3。

- 模块路径：`github.com/xm-utils/tools/logging`
- 技术选型：`logrus`（与现仓库一致，不引入新日志链）+ `lumberjack`（文件按大小轮转）；traceId 提取依赖 `trace` 组件
- **依赖收敛**：本包不依赖任何配置源/注册中心抽象；动态调级只提供 `SetLevel` 应用端点与键名约定，具体监听由使用方（装配层）实现，依赖方向永远是装配层 → logging，不存在反向环的可能
- 生效范围：装配在 **logrus 标准 logger** 上，`pipeline` 的 `ExecContext.Logger()` 等既有 `logrus.StandardLogger()` 使用点自动受益

## 目录结构

```
logging/
├── logging.go        # Config / Init / contextFormatter / 文件输出与轮转 / Context / For / SetLevel / Close / LevelWatchKey
└── config.example.yaml
```

## API 参考

### 初始化

```go
import "github.com/xm-utils/tools/logging"

err := logging.Init(logging.Config{
    Service: "order-svc", // 每条日志附加 service 字段
    Level:   "info",
    Caller:  false,
})
```

- `JSON *bool` 缺省 true（结构化输出），显式 false 走 TextFormatter；
- 非法 level / 含空白的 trace_key 返回 error 且**保持原配置不变**，不 panic；
- 不调 `Init` 也可直接用本包 API（默认 info、无 service 字段、`trace_id` 键名）。

### 文件输出与轮转

```go
err := logging.Init(logging.Config{
    Service:    "order-svc",
    OutputFile: "/var/log/order/app.log", // 留空则仅输出 os.Stderr
    AlsoStdout: nil,                      // nil 默认 true：控制台与文件双写（io.MultiWriter）
    MaxSizeMB:  100,                      // 单文件超阈自动改名轮转，0 沿用 lumberjack 默认
    MaxBackups: 7,                        // 保留旧文件数，0 不限
    MaxAgeDays: 30,                       // 按天清理，0 不按时间清理
    Compress:   true,                     // 旧文件 gzip
})
defer logging.Close() // 幂等；关闭文件句柄并把输出恢复为 stderr
```

- 路径不可用（目录不存在/无权限）时 `Init` 返回 error 且**不改任何现有配置**（试探创建，lumberjack 本身惰性建文）；
- 轮转/清理行为完全委托 lumberjack，备份命名如 `app.log-2026-11-21T00-00-00.123`；
- 重复 `Init` 切换文件时先切 output 再关旧句柄，在途写入不丢失、不泄漏 fd。

### traceId 自动附加

`Format` 阶段从 `entry.Context` 调 `trace.TraceIDFromCtx` 提取，非空才注入；实现上是在基础 formatter 序列化产物上做 JSON 字段合并（业务自设的同名字段优先不被覆盖），绝不重写调用方 Entry：

```go
// 方式一：本包入口
logging.Context(ctx).Info("order created")   // {"level":"info","msg":"order created","trace_id":"4bf9..."}

// 方式二：logrus 原生 WithContext 亦可（formatter 挂在全局）
logrus.WithContext(ctx).Info("order created")
```

未初始化 trace（noop provider）或 ctx 无链路上下文时输出**不含** trace_id 字段——零污染，接入代码可无条件常驻。

> 实现约束：不可改用在 `Formatter.Format` 内调 `Entry.WithFields` 注入——logrus v1.10 的 `dup()` 不复制 `Message`/`Level`，派生 Entry 的 Level 零值即 `PanicLevel(0)`，会把每条日志变成空 msg 的 panic 记录。

### 注入模式（沿用 pipeline 约定）

组件与业务代码不直接打全局日志，按模块注入 Entry：

```go
log := logging.For("deadletter")   // 每条附加 "module":"deadletter"
log.Warnf("recover failed: %v", err)
```

### 运行期动态调级（使用方装配）

logging 只提供两个标准抽象：`LevelWatchKey(service)` 键名约定（`/log/level/<service>`）与 `SetLevel(name)` 应用端点。**监听配置源属于装配职责，由使用方实现**，参考实现（可直接复制到应用仓库，约 40 行）：

```go
// levelwatch.go（应用装配层）：监听配置中心调级键，交给 logging.SetLevel 应用。
func StartLevelWatch(ctx context.Context, src config.ConfigSource, service string) error {
	key := logging.LevelWatchKey(service)
	changes, err := src.Watch(ctx, key)
	if err != nil {
		return err
	}
	baseline := logging.CurrentLevel() // 键删除时的回退目标
	if v, err := src.Get(ctx, key); err == nil { // 键不存在则保持当前级别
		_ = logging.SetLevel(string(v))
	} else if !errors.Is(err, config.ErrKeyNotFound) {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ch, ok := <-changes:
				if !ok || ctx.Err() != nil { // 退场丢弃在途事件，防 select 双就绪误应用
					return
				}
				if len(ch.Value) == 0 { // 键删除：回退基线
					_ = logging.SetLevel(baseline)
					continue
				}
				_ = logging.SetLevel(string(ch.Value)) // 非法值报错忽略，维持现级别
			}
		}
	}()
	return nil
}
```

键值约定与语义（`panic|fatal|error|warn|info|debug|trace`，`SetLevel` 对大小写与首尾空白宽容）：

| 事件 | 行为（由使用方实现，以上是参考语义） |
|---|---|
| 启动时键存在 | 立即应用键值级别 |
| 运行期推送合法值 | 实时生效，不重启 |
| 运行期推送非法值 | `SetLevel` 返回 error，忽略并维持现级别 |
| 键被删除 | 回退启动时记录的基线 |

运维侧改级只需写配置中心：`consul kv put /log/level/order-svc debug`。若同一应用可能重复启动监听，装配层自行保证幂等（每服务只调一次，或加互斥替换旧 goroutine）。

### 级别操作

```go
err := logging.SetLevel("debug")   // 手动调级（非法级别返回 error），也是动态调级的应用端点
lvl := logging.CurrentLevel()      // "debug"
svc := logging.Service()           // Init 配置的服务名
key := logging.LevelWatchKey(svc)  // "/log/level/order-svc"（监听键约定）
err = logging.Close()              // 关闭文件句柄恢复 stderr（幂等）
```

## 设计说明

1. **标准 logger 装配点**：`Init` 只配置 `logrus.StandardLogger()` 的 Formatter/级别/Caller，不新建 Logger 实例——保证 pipeline `ExecContext.Logger()`、各组件内部 logrus 用法统一出口；代价是全局单例，多引擎隔离场景请业务自备 logger。
2. **动态读取配置**：`service`/`trace_key` 存于 `atomic.Pointer` 状态，formatter 每次格式化实时读取，`Init` 重建后已分发的 Entry 自动跟随。
3. **依赖收敛**：traceId 提取只依赖 `trace.TraceIDFromCtx`（会传递引入 otel 链，trace 固有代价，见架构决策）；除 trace 外零仓内依赖——不引用 `registry/config`，避免 logging↔registry 的模块互依赖与潜在环，监听策略（重试/回退/幂等）归装配层自定。
4. **panic 转 error**：`Init` / `SetLevel` 全部返回 error；日志写入链（formatter 合并失败降级原样输出）无 panic 面。

## 开发状态

- 已发布 `logging/v0.1.0`。对 `trace` 的引用按 main commit pseudo-version，无 replace，可独立 `go get github.com/xm-utils/tools/logging`；不依赖 registry。

## 测试

```bash
go test -tags test -count=1 -race ./...   # traceId 贯通基于 otel sdk 真实 span；调级基于内存 fake ConfigSource；文件轮转落盘验证
```

> 测试文件带 `//go:build test` 约束，命令行需 `-tags test`；GoLand 默认已在运行配置中启用该 tag。
