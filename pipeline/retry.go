package pipeline

import (
	"math"
	"time"
)

// Strategy 重试策略接口
type Strategy interface {
	// Delay 获取下次重试的延迟时间
	// attempt: 当前重试次数(从0开始)
	Delay(attempt int) time.Duration
}

// RetryPolicy 失败重试策略：指数退避 base×2^attempt，MaxAttempt 为最大重试次数（不含首次执行），
// MaxDelay 为单次退避封顶（0 表示不设封顶）；溢出饱和且未设 MaxDelay 时引擎按额度耗尽处理。
type RetryPolicy struct {
	MaxAttempt int           // 最大重试次数
	BaseDelay  time.Duration // 退避基数
	MaxDelay   time.Duration // 单次退避封顶；0 表示不设封顶
}

// Delay 第 attempt 次失败后的等待时长：base × 2^attempt，经 MaxDelay 封顶。
// 溢出防护：负数轮次按首次处理（避免负移位 panic）；移位前先预判 base > MaxInt64>>attempt
// 即饱和为 time.Duration 最大可表示值（含移位≥63）；饱和值同样受 MaxDelay 封顶。
// 杜绝回绕出负值或错误小值 delay 入队引发热循环重试风暴。
func (p *RetryPolicy) Delay(attempt int) time.Duration {
	const maxDur = time.Duration(math.MaxInt64)
	if p.BaseDelay <= 0 {
		return 0
	}
	if attempt < 0 {
		attempt = 0
	}
	d := maxDur
	if attempt < 63 && p.BaseDelay <= maxDur>>attempt {
		d = p.BaseDelay << attempt
	}
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay
	}
	return d
}

// Retryer 重试策略抽象接口：失败消息是否重试、等待多久、重投消息如何变换，
// 具体决策由使用方实现，引擎不预设「计数+指数退避」形态。典型自定义形态：
// 按错误类别分档（网络抖动多重试/校验错误立即终态）、重试状态外置（按 msgID
// 到 Redis/GORM 计数而非编码进消息体）、固定延迟阶梯队列、动态额度。
// 契约：
//   - 实现必须线程安全（多个 worker 并发调用）；实现内 panic 由 handle 兜底转 OnFatal；
//   - retry=true 时 delay 直接交给 Queue.Submit，引擎不再做饱和防护，如需溢出保护
//     请复用 RetryPolicy.Backoff；轮次状态由实现自行存取，引擎不介入；
//   - 含 ErrNonRetryable 的错误不会到达本接口（引擎短路直达终态，全局语义统一）。
type Retryer[M any] interface {
	// Retry 对失败消息作出重试决策：retry=true 时以 next/delay 重投，false 时转 OnExhausted。
	Retry(msg M, failedStep string, err error) (next M, delay time.Duration, retry bool)
}

// PolicyRetryer Retryer 的内置默认实现：指数退避策略 + Attempt/Next 回调组合。
// 供使用方在自己的 Retryer 内委托复用（如：先按错误类别覆写部分决策，其余回落默认退避）。
type PolicyRetryer[M any] struct {
	Policy  Strategy        // 退避策略（nil 视为不重试）
	Attempt func(msg M) int // 读当前重试轮次；nil 按 0
	Next    func(msg M) M   // 生成重投消息；retry=true 时必须提供
}

var _ Retryer[struct{}] = (*PolicyRetryer[struct{}])(nil)

// Retry 按「额度 + 退避」决策；饱和封顶视同耗尽转终态，与引擎内置路径语义一致。
func (r *PolicyRetryer[M]) Retry(msg M, failedStep string, err error) (M, time.Duration, bool) {
	var zero M
	attempt := 0
	if r.Attempt != nil {
		attempt = r.Attempt(msg)
	}
	if r.Policy == nil {
		return zero, 0, false
	}
	delay := r.Policy.Delay(attempt)
	if delay >= time.Duration(math.MaxInt64) {
		return zero, 0, false // 饱和且未封顶：等待已无实际意义，按耗尽处理，不留僵尸消息
	}
	if r.Next == nil {
		return zero, 0, false
	}
	return r.Next(msg), delay, true
}
