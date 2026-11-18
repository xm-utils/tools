package pipeline

import (
	"fmt"
	"time"
)

// Step 泛型执行步骤：C 受密封接口 Context 约束（业务 embed 内置 ExecContext 即满足）。
// 执行前引擎已自动注入消息级 context 与统一日志出口，
// 步骤内用 ctx.Done()/Err() 感知停机取消，ctx.Logger() 取统一日志。
type Step[C Context] interface {
	Name() string
	Execute(ctx C) error
}

// TimeoutDecliner 可选接口：声明步骤执行上限时长。步骤自实现或经 WithTimeout 包装均可，
// 引擎在 runStep 中识别：执行前以从消息级 context 派生的超时 ctx 替换注入，
// 执行后恢复——后续步骤不受前序超时影响。仅对遵守 ctx.Done() 的操作生效。
type TimeoutDecliner interface {
	Timeout() time.Duration
}

// timeoutStep 步骤级超时包装器（配合 engine.runStep 生效）。
type timeoutStep[C Context] struct {
	Step[C]
	timeout time.Duration
}

// WithTimeout 包装步骤限制其执行时长上限 d：超时后步骤内经 ctx.Done()/ ctx.Err()
// 的操作收到 context.DeadlineExceeded，按普通步骤失败进入重试/终态分派；
// 消息级 context 不受影响，同一消息的后续步骤照常获得完整退避预算。
// 注意：超时以步骤整体 Execute 为界（含组合子内部链），且只约束遵守 ctx 协作的代码。
func WithTimeout[C Context](s Step[C], d time.Duration) Step[C] {
	return &timeoutStep[C]{Step: s, timeout: d}
}

func (t *timeoutStep[C]) Timeout() time.Duration { return t.timeout }

// ConditionFunc 条件谓词：判定是否执行组合步骤的内部 Step 链。
type ConditionFunc[C Context] func(ctx C) bool

// ConditionalStep 条件步骤组合器（泛型组合子，不感知业务上下文）：
// Condition 返回 false 时静默跳过（跳过视为成功，不中断上层 Pipeline），
// 否则顺序执行内部 Step 链，任一步失败即中断并向上传播错误，
// 错误经包装携带组合步骤名与内部失败步骤名（%w 保留原始 error 链供 errors.Is/As）。
type ConditionalStep[C Context] struct {
	StepName  string
	Step      []Step[C]
	Condition ConditionFunc[C]
}

func NewConditionalStep[C Context](name string, fn ConditionFunc[C], steps ...Step[C]) *ConditionalStep[C] {
	return &ConditionalStep[C]{
		StepName:  name,
		Step:      steps,
		Condition: fn,
	}
}

func (c *ConditionalStep[C]) Name() string {
	return c.StepName
}

func (c *ConditionalStep[C]) Execute(ctx C) error {
	if c.Condition != nil && !c.Condition(ctx) {
		return nil // 条件不满足，静默跳过
	}
	for _, step := range c.Step {
		if err := step.Execute(ctx); err != nil {
			// 包装保留失败定位链路：引擎层只记录组合步骤名，不包装则内部步骤名丢失
			return fmt.Errorf("conditional[%s]: step[%s]: %w", c.StepName, step.Name(), err)
		}
	}
	return nil
}

// BestEffortStep 非关键步骤组合器（容错语义）：
// 执行内部 Step 链，任一步骤**失败或 panic** 时中断内部剩余步骤，
// 但不向上传播错误/panic，保证主流程其他步骤继续执行、消息不因非关键 bug 落死信。
// 日志出口优先级：OnIgnoredError 钩子 > ctx.Logger()（引擎注入的统一出口）。
type BestEffortStep[C Context] struct {
	*ConditionalStep[C]

	// OnIgnoredError 容错日志钩子：stepName/err 为失败步骤信息，ctx 供取领域上下文字段。
	// 配置后不再经 ctx.Logger() 重复输出，二选一。
	OnIgnoredError func(ctx C, stepName string, err error)
}

func NewBestEffortStep[C Context](name string, fn ConditionFunc[C], steps ...Step[C]) *BestEffortStep[C] {
	return &BestEffortStep[C]{
		ConditionalStep: NewConditionalStep[C](name, fn, steps...),
	}
}

func (b *BestEffortStep[C]) Execute(ctx C) error {
	if b.Condition != nil && !b.Condition(ctx) {
		return nil // 条件不满足，静默跳过
	}

	for _, step := range b.Step {
		if err := b.safeExec(step, ctx); err != nil {
			if b.OnIgnoredError != nil {
				b.OnIgnoredError(ctx, step.Name(), err)
			} else {
				ctx.Logger().Warnf("非关键步骤执行失败(已忽略,不中断主流程): step=%s, err=%v", step.Name(), err)
			}
			return nil
		}
	}
	return nil
}

// safeExec 执行内部步骤并捕获 panic：与非关键失败走同一「忽略」分支——
// 否则内部 bug 会冒泡至引擎 runStep 被当作整条消息失败，违背容错语义。
func (b *BestEffortStep[C]) safeExec(step Step[C], ctx C) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("step panic: %v", r)
		}
	}()
	return step.Execute(ctx)
}
