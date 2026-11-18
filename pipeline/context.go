package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
)

// Context 执行上下文约束（密封接口）：setLogger/setContext 为未导出方法，
// 业务上下文结构体只需 embed 内置 ExecContext 即可跨包满足全部方法，无需自行实现。
// 注意：ExecContext 方法均为指针接收者，**C 必须用业务结构体的指针类型**
// （如 Config[M, *BizCtx]），值类型实例化编译失败。
// 消息级 context 与统一日志出口均由引擎在步骤执行前自动注入（见 Engine.process），
// 使用方在 Resolve 中只填业务领域字段，零处理。
type Context interface {
	context.Context
	Logger() *logrus.Entry
	setLogger(*logrus.Entry)
	setContext(gctx context.Context)
	GetData(key string) (interface{}, bool)
	SetData(key string, value interface{})
}

// DataKey 类型安全的步骤间数据键，替代 map[string]interface{} 的字符串索引。
// 泛型参数 T 保证 Get/Set 操作的编译期类型检查，key 拼写错误在编译阶段即可发现。
type DataKey[T any] struct{ Name string }

// 数据键读取的两类失败哨兵错误，调用方可用 errors.Is 精确区分：
//   - ErrKeyNotFound：键从未写入（流程分支未执行/步骤顺序错误）
//   - ErrKeyTypeMismatch：键存在但写入类型与读取声明不符（上游写错了类型）
var (
	ErrKeyNotFound     = errors.New("pipeline: 数据键不存在")
	ErrKeyTypeMismatch = errors.New("pipeline: 数据键类型不匹配")
)

// Get 读取键值：键存在且类型匹配返回 (v, nil)；不存在返回零值 + 包装 ErrKeyNotFound
// 的错误（含键名与期望类型）；类型不符返回零值 + 包装 ErrKeyTypeMismatch 的错误
// （含期望与实际存储类型），不再混淆两种失败。
func (k DataKey[T]) Get(ctx Context) (T, error) {
	v, ok := ctx.GetData(k.Name)
	var zero T
	if !ok {
		return zero, fmt.Errorf("%w: key=%q, want %T", ErrKeyNotFound, k.Name, zero)
	}
	typed, ok := v.(T)
	if !ok {
		return zero, fmt.Errorf("%w: key=%q, want %T, got %T", ErrKeyTypeMismatch, k.Name, zero, v)
	}
	return typed, nil
}

func (k DataKey[T]) Set(ctx Context, v T) {
	ctx.SetData(k.Name, v)
}

// ExecContext 内置 Context 实现：消息级执行上下文 + 步骤间数据 + 原始 Payload。
// 业务上下文 **embed 值形态本结构体**（推荐）后，全部接口方法自动继承；
// 若 embed 指针 *ExecContext，实例未初始化时引擎注入将空指针 panic
// （由 handle 兜底转 OnFatal，但应避免）。
// 引擎路径下 context/logger 由 setContext/setLogger 强制注入，Resolve 无需处理。
type ExecContext struct {
	// 引擎注入的消息级上下文（setContext），取消/超时/取值均由此继承
	context.Context
	// Log 业务日志出口，未设置时回落 logrus 标准 logger
	Log *logrus.Entry
	// 中间结果（步骤间传递）
	Data map[string]interface{}
	// 原始 Payload 字节，由业务在 Resolve 中自行填充（引擎不触达消息体字节）
	RawPayload []byte
}

// NewExecContext 构造内置上下文
// gctx 为 nil 时回落 context.Background()，避免继承 nil 导致 Done()/Err() panic。
func NewExecContext(gctx context.Context, log *logrus.Entry) *ExecContext {
	if gctx == nil {
		gctx = context.Background()
	}
	return &ExecContext{Context: gctx, Log: log, Data: make(map[string]interface{})}
}

func (c *ExecContext) GetData(key string) (interface{}, bool) {
	v, ok := c.Data[key]
	return v, ok
}

func (c *ExecContext) SetData(key string, value interface{}) {
	if c.Data == nil {
		c.Data = make(map[string]interface{})
	}
	c.Data[key] = value
}

// setLogger 实现 Context 接口（未导出）：引擎在步骤执行前注入统一日志出口，nil 不覆盖现值。
func (c *ExecContext) setLogger(log *logrus.Entry) {
	if log != nil {
		c.Log = log
	}
}

// setContext 实现 Context 接口（未导出）：引擎在步骤执行前注入消息级 context，nil 不覆盖现值。
func (c *ExecContext) setContext(gctx context.Context) {
	if gctx != nil {
		c.Context = gctx
	}
}

// Logger 实现 Context 接口，Log 未配置时回落标准 logger（非引擎路径兜底）。
func (c *ExecContext) Logger() *logrus.Entry {
	if c.Log != nil {
		return c.Log
	}
	return logrus.NewEntry(logrus.StandardLogger())
}

// 编译期断言：内置实现必须满足密封约束接口
var _ Context = (*ExecContext)(nil)

// GetPayload 辅助方法：从 ctx 反序列化 Payload 到目标结构体。
// 不在 Context 接口内，仅具体上下文类型在步骤内直接调用（RawPayload 由 Resolve 填充）。
func (c *ExecContext) GetPayload(v interface{}) error {
	if len(c.RawPayload) == 0 {
		return nil
	}
	return json.Unmarshal(c.RawPayload, v)
}
