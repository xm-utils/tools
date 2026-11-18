package pipeline_test

import (
	"context"
	"time"

	"github.com/xm-utils/tools/pipeline"
)

// 跨包接入形态验证：Context 为密封接口（含未导出 setContext/setLogger），
// 业务包只需 embed 内置 ExecContext，全部方法经提升自动满足，零实现。
type borrowOpContext struct {
	pipeline.ExecContext
	OpNo string
}

var (
	_ pipeline.Context                = (*borrowOpContext)(nil)
	_ pipeline.Step[*borrowOpContext] = noopExtStep{}
	_ pipeline.Queue[string]          = (*noopExtQueue)(nil)
)

type noopExtStep struct{}

func (noopExtStep) Name() string { return "noop" }

func (noopExtStep) Execute(ctx *borrowOpContext) error {
	// 步骤内可直接使用引擎注入的能力
	_ = ctx.Done()
	_ = ctx.Logger()
	ctx.SetData("k", "v")
	_, _ = ctx.GetData("k")
	return nil
}

type noopExtQueue struct{}

func (noopExtQueue) Submit(msg string, delay time.Duration) error { return nil }

func (noopExtQueue) Receive(ctx context.Context) (string, bool) {
	return "", false
}
