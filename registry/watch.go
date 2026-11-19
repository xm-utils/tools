package registry

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// WatchConfig Watch 轮询配置。
type WatchConfig struct {
	Interval    time.Duration // 轮询间隔，默认 10s
	HealthyOnly bool          // 仅推送健康实例
	Tags        []string      // 标签过滤
}

// Watch 基于 Discovery 的通用轮询监听：周期性查询服务实例，
// 仅当实例集合（ID/地址/健康状态）发生变化时向返回的 channel 推送全量快照。
//
// 首个快照为启动时的初始实例列表（即便无变化也会推送一次）。
// ctx 取消后 channel 关闭；期间查询失败不中断轮询，失败错误通过 onError 回调（可为 nil）。
//
// 该实现只依赖 Discovery 接口，consul/nacos 适配器无需各自实现 Watch。
func Watch(ctx context.Context, d Discovery, service string, cfg WatchConfig, onError func(error)) (<-chan []*ServiceInstance, error) {
	if d == nil {
		return nil, ErrUnsupported
	}
	if service == "" {
		return nil, ErrServiceEmpty
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}

	ch := make(chan []*ServiceInstance, 1)
	go func() {
		defer close(ch)

		var lastSig string
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()

		for {
			instances, err := d.Services(ctx, DiscoverRequest{
				Name:        service,
				Tags:        cfg.Tags,
				HealthyOnly: cfg.HealthyOnly,
			})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if onError != nil {
					onError(err)
				}
			} else {
				sig := signature(instances)
				if sig != lastSig {
					lastSig = sig
					select {
					case ch <- instances:
					case <-ctx.Done():
						return
					}
				}
			}

			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// signature 计算实例列表的稳定指纹，用于变更检测。
// 排序消除后端返回顺序不稳定的影响。
func signature(instances []*ServiceInstance) string {
	items := make([]string, 0, len(instances))
	for _, inst := range instances {
		if inst == nil {
			continue
		}
		items = append(items, fmt.Sprintf("%s|%s|%t", inst.ID, inst.Address(), inst.Healthy))
	}
	sort.Strings(items)
	return fmt.Sprintf("%v", items)
}

// joinHostPort 拼接 host:port。
func joinHostPort(host string, port int) string {
	return host + ":" + strconv.Itoa(port)
}
