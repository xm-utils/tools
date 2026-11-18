package pipeline

import (
	"time"

	"github.com/sirupsen/logrus"
)

// Config 引擎装配配置。消费-执行-重试-终态的全部生命周期钩子集中在此声明，
// 业务侧通过钩子注入领域语义（状态回写/死信落库/重试计数），引擎本身不触达任何业务表。
type Config[M any, C Context] struct {
	Queue   Queue[M]
	Workers int

	// Resolve 解析消息为执行计划：定位 Step 链并构造上下文。
	// 返回的 C 只需填业务领域字段：消息级 context 与统一日志出口由引擎在步骤执行前自动注入（setContext/setLogger），使用方无需处理。
	// 硬性契约：Workers>1 时必须为每条消息返回独立的 C 实例（共享单例/池化对象会导致
	// 注入覆盖与并发写 Data map 竞争）。steps 为空链时视为空成功（引擎告警后走 OnSuccess）。
	// 返回 error 表示该消息不可恢复（数据缺失/类型未知等），引擎不重试、直接回调 OnFatal。
	// Resolve 内 panic 由 handle 兜底捕获，按不可恢复转 OnFatal。
	Resolve func(msg M) (steps []Step[C], ctx C, err error)

	// OnFatal 不可恢复消息处置（Resolve 失败后调用），如：终态回写+落死信。
	OnFatal func(msg M, reason string)

	// OnSuccess 全部步骤执行成功后调用。
	OnSuccess func(msg M, ctx C)

	// OnRetried 失败且 Retryer 裁决重投、入队成功后调用（msg 为本轮消息、next 为重投消息），
	// 如：重试计数 +1。重投入队失败时不调用，消息留在途由外部兜底（如补偿任务）。
	OnRetried func(msg M, next M, delay time.Duration)

	// OnExhausted 失败且重试裁决不重试后调用（Retryer 为 nil 时首次失败即触发；
	// ErrNonRetryable 短路与退避饱和封顶同样命中），
	// 如：终态回写 + 落死信。退避饱和封顶（等待时长已无实际意义）按耗尽处理，
	// 避免消息陷入「永不入队也永不死信」的僵尸态。
	OnExhausted func(msg M, failedStep string, err error)

	// OnInterrupted 停机中断时调用（可选，仅观测用途）：步骤因 Stop 取消而返回错误时，
	// 引擎判定为停机中断而非业务失败——不重投、不落终态、不计重试额度，
	// 消息由上游重投或补偿通道兜底。
	OnInterrupted func(msg M, err error)

	// Retryer 重试策略抽象接口（可选）：失败后由它裁决「是否重试/等待多久/重投什么」，
	// 接口与实现契约见 Retryer；内置默认实现 PolicyRetryer（额度+指数退避）开箱即用。
	// nil 表示不重试：首次失败直接转 OnExhausted 终态。
	Retryer Retryer[M]

	// MsgKeyFunc 返回消息的有序键（可选）：配置后引擎切换为「单接收器 + 哈希分片」模式，
	// 同一 Key 的消息严格路由到同一 worker 串行执行（保序），不同 Key 间仍并发。
	// 返回空串表示该消息无有序要求，按轮询分发。分片通道无缓冲：worker 忙时转发阻塞
	// 即天然背压（领取停滞，积压留在 MQ 侧）。注意：同 Key 串行会牺牲该 Key 的吞吐。
	MsgKeyFunc func(msg M) string

	Log *logrus.Entry
}
