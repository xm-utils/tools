package pipeline

import (
	"context"
	"time"
)

// Queue 消息队列抽象：Submit 入队（可携带延迟时长），Receive 阻塞领取一条消息。
//
// 实现契约：
//   - Receive 返回 ok=false 仅表示 ctx 已结束（停机），此时保证未领取任何消息；
//     Receive 在 handle 兜底范围之外被调用，实现内不得 panic（panic 将击穿 worker 进程）；
//   - Submit 会被多个 worker 并发调用，必须线程安全，且**不得无限阻塞**
//     （建议带超时/非阻塞发送）：停机流程中引擎仍可能调用 Submit，
//     若 Submit 阻塞会卡死 worker 导致 Stop() 的 wg.Wait 永不返回；
//   - 停机期间 Submit 允许返回 error，引擎按「留待外部兜底」处理，不会误判终态。
type Queue[M any] interface {
	Submit(msg M, delay time.Duration) error
	Receive(ctx context.Context) (msg M, ok bool)
}
