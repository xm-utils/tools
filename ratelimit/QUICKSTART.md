# ratelimit — 5 分钟上手

完整 API 与算法选型见 [README.md](README.md)，配置键位见 [config.example.yaml](config.example.yaml)。

## 1. 引入与最小可用

```go
import "github.com/xm-utils/tools/ratelimit"

// 100 QPS，允许 200 个突发额度
tb, err := ratelimit.NewTokenBucket(ratelimit.TokenBucketConfig{Rate: 100, Burst: 200})
if err != nil {
    return err // 构造期只校验配置，组件自身不 panic
}
defer tb.Close()

if err := tb.Allow(ctx); err != nil {
    // 被限流：errors.Is(err, ratelimit.ErrRateLimited)
    // 消息形如 "ratelimit: rate limited: retry after 100ms"
    return err
}
handle()
```

零依赖：`go get github.com/xm-utils/tools/ratelimit` 只拉标准库；Redis 与 gRPC 依赖分别只在 `redisstore` / `grpcmiddleware` 子包。

## 2. 换算法就换构造器（契约完全一致）

```go
// 精确计数：任意 1 分钟内不超过 100 次
sw, _ := ratelimit.NewSlidingWindow(ratelimit.SlidingWindowConfig{Limit: 100, Window: time.Minute})

// 匀速出水：每 10ms 一个，最多排 2 秒（200 个）——保护自己调用的下游
lb, _ := ratelimit.NewLeakyBucket(ratelimit.LeakyBucketConfig{Rate: 100, Queue: 200})

var l ratelimit.Limiter = sw // 三者都实现 Limiter，可互相替换注入
_ = l
```

漏桶多两个整形入口（出口自我限速场景常用）：

```go
delay, err := lb.Reserve(ctx, 10) // 拿到"本次应排队多久"，自行决定 Sleep 还是放弃
err = lb.Wait(ctx, 10)            // 直接阻塞到出水时刻；ctx 取消立即返回，额度不回滚
```

批量消耗统一走 `AllowN`，**被拒不报错误**：

```go
ok, err := tb.AllowN(ctx, 10)
switch {
case err != nil:      // n<=0、已 Close、ctx 取消等真实故障
case !ok:             // 额度不足，正常拒绝
default:              // 放行
}
```

## 3. 多维限流：一个 Local 管所有 Key

```go
local, _ := ratelimit.NewLocal(ratelimit.LocalConfig{Rate: 100, Burst: 200})
defer local.Close()

_ = local.Allow(ctx, "api:order:create")   // 接口级
_ = local.Allow(ctx, "tenant:10086")       // 租户级，Key 首次出现时惰性创建
res := local.Stats("api:order:create")     // 观测：Remaining / RetryAfter / Limit

local.Update("api:order:create", 50, 100)  // 热更新该 Key 的速率与容量
local.Remove("api:order:create")           // 规则下线
```

Key 传空串会报 `ErrInvalidConfig`；入口中间件把"KeyFunc 返回空串"当作**直通**约定，用于公开端点豁免。

## 4. 挂到入口

**HTTP（net/http）**

```go
import "github.com/xm-utils/tools/ratelimit/httpmiddleware"

handler, err := httpmiddleware.Wrap(tb, mux)     // 服务级：整站共享一份额额（任何 Limiter 均可）
http.Handle("/", handler)

// 按路径/租户分维，故障时放行
mw, _ := httpmiddleware.Middleware(httpmiddleware.Config{
    Local:    local,                              // 与 Limiter 互斥，二选一
    KeyFunc:  func(r *http.Request) (string, error) { return r.URL.Path, nil },
    FailOpen: true,
})
handler = mw(mux)                                 // Gin：engine.Use(gin.WrapH(handler))
```

被拒 → `429` + `Retry-After: <秒>` + `{"code":429,"message":"too many requests"}`。

**gRPC**

```go
import (
    "google.golang.org/grpc"
    "github.com/xm-utils/tools/ratelimit/grpcmiddleware"
)

itc, err := grpcmiddleware.NewUnaryServerInterceptor(grpcmiddleware.Config{
    Local: local,                                 // 缺省按 fullMethod 分维
})
server := grpc.NewServer(grpc.UnaryInterceptor(itc))
```

被拒 → `codes.ResourceExhausted`，trailer `retry-after-ms` 给出建议等待毫秒。流拦截器（`NewStreamServerInterceptor`）只在建流时判定一次。

## 5. 多实例共享份额：换 redisstore 后端

```go
import (
    "github.com/go-redis/redis/v8"
    "github.com/xm-utils/tools/ratelimit/redisstore"
)

client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 0})

rl, err := redisstore.New(redisstore.Config{
    Client: client,
    Key:    "api:order:create",    // 实际落库 xm_ratelimit:{api:order:create}，Cluster 下单 slot
    Rate:   100,
    Burst:   200,
})
// rl 满足 ratelimit.Limiter，可直接替换上面所有装配点的 Limiter 字段
```

10 个副本共用这 100 QPS，不再各算各的；扣减在 Lua 脚本内原子完成，Redis 故障时返回 error（由中间件 `FailOpen` 决定放行还是 500）。

常见组合：入口用 `redisstore` 控全局总额，本地再挂一层 `Local`（`Rate ≈ 全局/副本数×1.5`）兜 Redis 抖动。

## 6. 规则热更新

组件只提供应用端点，不含配置源监听（保持零依赖）：

```go
// 由使用方经 registry/config.Watch 收到变更后回调
func onRuleChanged(key string, rate, burst float64) error {
    return local.Update(key, rate, burst)   // 或 tb.SetParams(rate, burst)
}
```

## 常见错误

| 现象 | 原因 |
|---|---|
| `ratelimit: invalid config: Limiter 与 Local 必须二选一` | 中间件 `Config` 同时填了两者或都不填 |
| `ratelimit: invalid config: Queue 必须不小于 1` | 漏桶 `Queue` 留空或写 0（纯匀速应写 `Queue: 1`） |
| `n=… 超出单次判定上限 9223372036, 请拆批消耗` | `redisstore.AllowN` 的定点乘法保护，拆批调用 |
| `AllowN` 返回 `(false, nil)` 但拿不到退避时长 | 设计如此：被拒是正常结果而非错误，需等待时长时另取 `Peek().RetryAfter`（或走 `Allow`，错误消息自带） |
| 明明配了 100 QPS，线上像 1000 | 多实例各自用 `Local`（进程内份额），需换 `redisstore` |
| `ratelimit: closed` | 限流器已 `Close`；`redisstore.Close` 只停本实例，不动 Redis 共享状态 |
| 单测里放行数量飘忽 | 未注入假时钟 `Clock`；并发用例应冻结时钟后严格断言放行数 = `Burst`/`Limit`/`Queue` |
