package pipeline

import (
	"math"
	"time"
)

// Strategy 重试退避策略接口：退避形状与重试额度由实现自行维护和裁决。
// 契约：Delay 返回 0 表示「不需要重试」（典型场景：额度耗尽、未配退避基数）；
// 正数表示等待该时长后重试。实现必须并发安全（同一实例可被多 worker 共享调用）。
type Strategy interface {
	// Delay 给出第 attempt 次失败后（从 0 计）下一次重试的等待时长；返回 0 表示不需要重试。
	Delay(attempt int) time.Duration
}

// RetryPolicy 失败重试策略：指数退避 base×2^attempt，MaxDelay 为单次退避封顶
// （0 表示不设封顶）；MaxAttempt 为最大重试次数（不含首次执行），由 Delay 自检，
// 超额或 BaseDelay<=0 时返回 0（Strategy 契约的「不需要重试」）；溢出饱和且未设
// MaxDelay 时 PolicyRetryer 按耗尽处理转终态。
type RetryPolicy struct {
	MaxAttempt int           // 最大重试次数
	BaseDelay  time.Duration // 退避基数
	MaxDelay   time.Duration // 单次退避封顶；0 表示不设封顶
}

// Delay 第 attempt 次失败后第 attempt+1 次重试的等待时长：base × 2^attempt，经 MaxDelay 封顶。
// 额度自检（Strategy 契约）：attempt+1 > MaxAttempt 或 BaseDelay<=0 时返回 0 表示不需要重试。
// 溢出防护：负数轮次按首次处理（避免负移位 panic）；移位前先预判 base > MaxInt64>>attempt
// 即饱和为 time.Duration 最大可表示值（含移位≥63）；饱和值同样受 MaxDelay 封顶。
// 杜绝回绕出负值或错误小值 delay 入队引发热循环重试风暴。
func (p *RetryPolicy) Delay(attempt int) time.Duration {
	const maxDur = time.Duration(math.MaxInt64)
	if attempt < 0 {
		attempt = 0
	}
	if attempt+1 > p.MaxAttempt || p.BaseDelay <= 0 {
		return 0 // 额度耗尽或未配退避：Strategy 契约中 0 即「不需要重试」
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
//   - retry=true 时 delay 直接交给 Queue.Submit（含 delay=0 的立即重投，与 Strategy 层的
//     0 契约无关），引擎不再做饱和防护，如需溢出保护请复用 RetryPolicy.Delay；
//     轮次状态由实现自行存取，引擎不介入；
//   - 含 ErrNonRetryable 的错误不会到达本接口（引擎短路直达终态，全局语义统一）。
type Retryer[M any] interface {
	// Retry 对失败消息作出重试决策：retry=true 时以 next/delay 重投，false 时转 OnExhausted。
	Retry(msg M, failedStep string, err error) (next M, delay time.Duration, retry bool)
}

// PolicyRetryer Retryer 的内置默认实现：裁决委托给 Strategy 退避策略（典型为 RetryPolicy，
// 额度在其 Delay 内自检：返回 0 即不重试，见 Strategy 契约）+ Attempt/Next 回调组合。
// 开箱即用，也供使用方在自己的 Retryer 内委托复用（如：先按错误类别覆写部分决策，其余回落默认退避）。
type PolicyRetryer[M any] struct {
	Policy  Strategy        // 退避策略（nil 视为不重试）
	Attempt func(msg M) int // 读当前重试轮次；nil 按 0
	Next    func(msg M) M   // 生成重投消息；retry=true 时必须提供
}

var _ Retryer[struct{}] = (*PolicyRetryer[struct{}])(nil)

// Retry 裁决：缺策略/缺 Next/Strategy.Delay 返回 0（额度耗尽或未配退避）/饱和封顶 → 不重试；
// 否则以 Next(msg)/delay 重投。饱和封顶视同耗尽：等待已无实际意义，转终态不留僵尸消息。
func (r *PolicyRetryer[M]) Retry(msg M, failedStep string, err error) (M, time.Duration, bool) {
	var zero M
	if r.Policy == nil || r.Next == nil {
		return zero, 0, false
	}
	attempt := 0
	if r.Attempt != nil {
		attempt = r.Attempt(msg)
	}
	delay := r.Policy.Delay(attempt)
	if delay <= 0 || delay >= time.Duration(math.MaxInt64) {
		return zero, 0, false // 0=策略裁决不需重试；饱和=按耗尽处理
	}
	return r.Next(msg), delay, true
}
