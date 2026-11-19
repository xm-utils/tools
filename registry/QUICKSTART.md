# registry 快速开始（5 分钟上手）

目标：不关心底层是 Consul 还是 Nacos，走通 **注册 → 发现 → 监听实例 → 配置热更新**。

## 1. 引入

```bash
go get github.com/xm-utils/tools/registry
```

## 2. 选择后端（得到 registry.Backend，业务代码与后端无关）

```go
import (
    xmconsul "github.com/xm-utils/tools/consul"
    xmnacos  "github.com/xm-utils/tools/nacos"
    "github.com/xm-utils/tools/registry"
    regconsul "github.com/xm-utils/tools/registry/consul"
    regnacos "github.com/xm-utils/tools/registry/nacos"
)

// 方式 A：Consul
backend, err := regconsul.New(&xmconsul.Config{Address: "127.0.0.1:8500"})

// 方式 B：Nacos（与 A 二选一，后续代码完全相同）
backend, err = regnacos.New(&xmnacos.Config{
    Enabled:      true, // 必须为 true
    ServerConfig: []xmnacos.ServerConfig{{IpAddr: "127.0.0.1", Port: 8848}},
    ClientConfig: xmnacos.ClientConfig{NamespaceId: "dev", GroupName: "DEFAULT_GROUP"},
})
```

> 存量代码已持有 `consul.Client` / `NamingClientWrapper` 时用 `NewFromClient` / `NewFromNamingClient` 复用连接。
> 配置样例见 [config.example.yaml](config.example.yaml)。

## 3. 注册自己

```go
inst := registry.ServiceInstance{
    Name: "order-svc", Host: "10.0.0.1", Port: 8080,
    Tags: []string{"v1"}, Meta: map[string]string{"zone": "az1"},
}
err = backend.Register(ctx, &registry.RegisterRequest{
    Instance: inst,
    // 仅 Consul 生效；Nacos 为临时实例，SDK 自动心跳，Checks 声明被忽略
    Checks: []registry.HealthCheckSpec{
        {HTTP: "http://10.0.0.1:8080/health", Interval: 10 * time.Second, Timeout: 2 * time.Second},
    },
})

// 停机时注销（Nacos 需要 Host+Port；Consul 用 ID，ID 为空自动回退 name-host-port）
defer backend.Deregister(context.Background(), inst)
```

## 4. 发现下游

```go
instances, err := backend.Services(ctx, registry.DiscoverRequest{
    Name:        "user-svc",
    Tags:        []string{"v2"}, // Consul 原生过滤；Nacos 按 Metadata["tags"] 结果内过滤
    HealthyOnly: true,
})
for _, i := range instances {
    call(i.Address()) // "10.0.0.2:8080"
}

names, err := backend.AllServices(ctx) // 所有已注册服务名（已排序）
```

## 5. 监听实例变化

```go
ch, err := registry.Watch(ctx, backend, "user-svc", registry.WatchConfig{
    Interval:    10 * time.Second, // 轮询间隔，默认 10s
    HealthyOnly: true,
}, func(err error) { logrus.Warnf("watch user-svc: %v", err) }) // 查询失败不中断轮询

go func() {
    for snapshot := range ch { // 启动即推初始快照，此后仅集合变化时推送；ctx 取消后关闭（Backend.Close 不会终止本监听）
        refreshLocalCache(snapshot)
    }
}()
```

## 6. 配置中心：读写 + 热更新（consul KV / nacos config 同一接口）

```go
import "github.com/xm-utils/tools/registry/config"

var src config.ConfigSource
src, err = regconsul.NewConfigAdapter(&xmconsul.Config{Address: "127.0.0.1:8500"})
// src, err = regnacos.NewConfigAdapter(nacosCfg)   // key 即 dataId

// 写与读
src.Put(ctx, "config/order/ratelimit", []byte(`{"qps":100}`))
data, err := src.Get(ctx, "config/order/ratelimit")
if errors.Is(err, config.ErrKeyNotFound) {
    data = defaultCfg // 键不存在统一此错误，两个后端语义一致
}

// 热更新：Watch 不推当前值，只推后续变更；键被删除时 len(c.Value)==0
changes, _ := src.Watch(ctx, "config/order/ratelimit")
go func() {
    for c := range changes {
        reloadOrDefault(c.Value)
    }
}()

src.Close() // 幂等；终止所有 Watch
```

## 7. 常见问题

- **Nacos 场景 Tags 去哪了？** 存入 `Metadata["tags"]`（逗号分隔），查询时自动拆回 `Tags`。
- **Nacos 注销报错 host/port required？** 正常，Nacos 按 Ip+Port 注销，传入注册时的同一 `ServiceInstance`。
- **Consul Watch 配置取消后 channel 多久关闭？** 最长约 10s（blocking query 单次阻塞时长）。
- **Nacos 同一 key 重复 Watch？** 覆盖语义：旧 channel 关闭，新监听生效。
- **怎么做单元测试？** 面向 `registry.Backend` / `config.ConfigSource` 写 fake，无需真实中间件（参考 `watch_test.go`）。
- **切换注册中心要改几行？** 只改第 2 步构造函数一行，业务代码零改动。

## 8. 验证

```bash
cd registry && go test -race ./...   # 38 用例全绿
```

下一步阅读：[README.md](README.md) —— API 参考、双后端差异语义矩阵、依赖与 replace 说明。
