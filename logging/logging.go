// Package logging 提供全仓库统一的日志出口：logrus JSON 结构化封装、
// traceId 自动附加与文件轮转输出。
//
// 设计要点（对应架构路线图 3.1.3）：
//   - 基于 logrus（与现仓库一致），不引入新日志链；Init 配置标准 logger，
//     pipeline ExecContext.Logger() 等既有 logrus.StandardLogger() 使用点自动受益；
//   - traceId 自动附加：Format 阶段从 entry.Context 提取 trace.TraceIDFromCtx，
//     业务只需 logrus 常规用法 + WithContext/Context(ctx) 即带上 trace_id；
//     未初始化 trace 时提取结果为空串，零行为、零污染；
//   - 动态调级只提供标准抽象 SetLevel：配置中心监听由使用方（装配层）
//     实现，键约定 /log/level/<service>（LevelWatchKey），logging 不依赖任何配置源；
//   - 文件输出：OutputFile 指定路径后经 lumberjack 按大小轮转，可选同步保留控制台；
//   - panic 安全：对外 API 不向调用方抛 panic，非法配置一律返回 error。
//
// 约定：组件与业务代码不直接打全局日志，经 For(module) / Context(ctx) 注入
// Entry 后使用（沿用 pipeline ExecContext.Logger() 注入模式）。
package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/xm-utils/tools/trace"
	"gopkg.in/natefinch/lumberjack.v2"
)

// 默认值与字段名约定
const (
	// DefaultTraceKey traceId 字段的默认键名
	DefaultTraceKey = "trace_id"
	// ServiceField 服务名字段固定键名
	ServiceField = "service"
	// ModuleField 模块名字段固定键名（For 注入）
	ModuleField  = "module"
	defaultLevel = "info"
)

// Config 日志组件配置，字段与 config.example.yaml 一一对应。
type Config struct {
	// Service 服务名，非空时每条日志自动附加 service 字段，
	// 也是动态调级键 /log/level/<service> 的默认来源。
	Service string `yaml:"service" json:"service"`
	// Level 初始日志级别（panic/fatal/error/warn/info/debug/trace），留空默认 info。
	Level string `yaml:"level" json:"level"`
	// JSON 是否 JSON 结构化输出，默认 true；显式置 false 使用文本格式。
	JSON *bool `yaml:"json" json:"json"`
	// Caller 是否输出调用方文件与行号。
	Caller bool `yaml:"caller" json:"caller"`
	// TraceKey traceId 字段键名，留空默认 trace_id。
	TraceKey string `yaml:"trace_key" json:"trace_key"`
	// OutputFile 日志文件路径；留空仅输出控制台（os.Stderr，logrus 默认）。
	// 写入文件时按 MaxSizeMB 大小轮转（lumberjack），备份命名如 xxx-2026-11-21T00-00-00.000。
	OutputFile string `yaml:"output_file" json:"output_file"`
	// AlsoStdout 写文件时是否同时保留控制台输出，默认 true（io.MultiWriter）。
	AlsoStdout *bool `yaml:"also_stdout" json:"also_stdout"`
	// MaxSizeMB 单个日志文件轮转阈值（MB），0 沿用 lumberjack 默认 100；不得为负。
	MaxSizeMB int `yaml:"max_size_mb" json:"max_size_mb"`
	// MaxBackups 保留的旧日志文件数，0 为不限（lumberjack 默认）；不得为负。
	MaxBackups int `yaml:"max_backups" json:"max_backups"`
	// MaxAgeDays 按天数清理旧日志，0 为不按时间清理（lumberjack 默认）；不得为负。
	MaxAgeDays int `yaml:"max_age_days" json:"max_age_days"`
	// Compress 是否 gzip 压缩轮转后的旧日志文件。
	Compress bool `yaml:"compress" json:"compress"`
}

// runtimeState Init 后生效的可变设置，formatter 每次格式化时实时读取。
type runtimeState struct {
	traceKey string
	service  string
}

var state atomic.Pointer[runtimeState]

// initMu 串行化 Init/Close 的输出口轮换，防并发时文件句柄泄漏；
// openedSink 为最近一次 Init 打开的文件输出句柄（nil 表示无需关闭）。
var (
	initMu     sync.Mutex
	openedSink io.Closer
)

func init() {
	// 未 Init 时的缺省状态：不附加 service，trace 键名取默认。
	state.Store(&runtimeState{traceKey: DefaultTraceKey})
}

// Init 按配置装配 logrus 标准 logger（Formatter/级别/Caller/输出目的地）。
// 可重复调用，整体覆盖前一次设置（含输出回到 stderr/新文件）；非法 level、
// 含空白的 trace_key、负数轮转参数或日志文件打不开，均返回 error 并保持原配置不变，不 panic。
func Init(cfg Config) error {
	lvl := strings.TrimSpace(cfg.Level)
	if lvl == "" {
		lvl = defaultLevel
	}
	parsed, err := logrus.ParseLevel(lvl)
	if err != nil {
		return fmt.Errorf("logging: 非法日志级别 %q: %w", cfg.Level, err)
	}
	traceKey := strings.TrimSpace(cfg.TraceKey)
	if traceKey == "" {
		traceKey = DefaultTraceKey
	} else if strings.ContainsAny(traceKey, " \t") {
		return fmt.Errorf("logging: trace_key %q 不能含空白字符", cfg.TraceKey)
	}
	if cfg.MaxSizeMB < 0 || cfg.MaxBackups < 0 || cfg.MaxAgeDays < 0 {
		return fmt.Errorf("logging: 轮转参数不得为负 (max_size_mb=%d max_backups=%d max_age_days=%d)",
			cfg.MaxSizeMB, cfg.MaxBackups, cfg.MaxAgeDays)
	}

	var base logrus.Formatter
	jsonMode := true
	if cfg.JSON != nil && !*cfg.JSON {
		base = &logrus.TextFormatter{FullTimestamp: true}
		jsonMode = false
	} else {
		base = &logrus.JSONFormatter{}
	}

	// 装配输出 sink：lumberjack 惰性建文，先行试探创建以做到路径不可用即时失败
	var (
		sink      io.Writer = os.Stderr
		newCloser io.Closer
	)
	if path := strings.TrimSpace(cfg.OutputFile); path != "" {
		f, oerr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if oerr != nil {
			return fmt.Errorf("logging: 无法打开日志文件 %q: %w", path, oerr)
		}
		_ = f.Close()
		lj := &lumberjack.Logger{
			Filename:   path,
			MaxSize:    cfg.MaxSizeMB,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAgeDays,
			Compress:   cfg.Compress,
		}
		newCloser = lj
		if cfg.AlsoStdout == nil || *cfg.AlsoStdout {
			sink = io.MultiWriter(os.Stdout, lj)
		} else {
			sink = lj
		}
	}

	// 一切校验通过，加锁换装；先切 output 再关旧句柄，避免在途写入丢失
	initMu.Lock()
	defer initMu.Unlock()
	logrus.SetFormatter(&contextFormatter{base: base, json: jsonMode})
	logrus.SetLevel(parsed)
	logrus.SetReportCaller(cfg.Caller)
	logrus.SetOutput(sink)
	if openedSink != nil {
		_ = openedSink.Close()
	}
	openedSink = newCloser
	state.Store(&runtimeState{
		traceKey: traceKey,
		service:  strings.TrimSpace(cfg.Service),
	})
	return nil
}

// Close 关闭 Init 打开的日志文件句柄（若有）并把输出恢复为 os.Stderr，
// 级别、格式等其余设置不变。幂等；未配置 OutputFile 时为空操作。
// 进程退出前调用可确保尾部日志落盘（lumberjack 无用户态缓冲，通常无需）。
func Close() error {
	initMu.Lock()
	defer initMu.Unlock()
	if openedSink == nil {
		return nil
	}
	err := openedSink.Close()
	openedSink = nil
	logrus.SetOutput(os.Stderr)
	return err
}

// contextFormatter 在基础 formatter 序列化产物上追加 service 与 trace_id，
// 绝不重写调用方的 Entry。
//
// 注意不可改用 Entry.WithFields 在 Format 内注入：logrus v1.10 的 dup()
// 只复制 Logger/Time/Caller/Context，不复制 Message 与 Level，派生 Entry 的
// Level 零值即 PanicLevel(0)，会把每条日志变成空 msg 的 panic 记录。
type contextFormatter struct {
	base logrus.Formatter
	json bool // base 是否 JSON 序列化（文本模式降级为行尾追加 k=v）
}

func (f *contextFormatter) Format(e *logrus.Entry) ([]byte, error) {
	st := state.Load()
	var extra map[string]any
	if st.service != "" {
		extra = map[string]any{ServiceField: st.service}
	}
	if e.Context != nil {
		if tid := trace.TraceIDFromCtx(e.Context); tid != "" {
			if extra == nil {
				extra = make(map[string]any, 1)
			}
			extra[st.traceKey] = tid
		}
	}

	data, err := f.base.Format(e)
	if err != nil || len(extra) == 0 {
		return data, err
	}
	if f.json {
		return mergeJSON(data, extra)
	}
	return appendText(data, extra), nil
}

// mergeJSON 把附加字段合并进 JSONFormatter 输出的单行对象；业务已自设的同名字段
// 优先（不覆盖）。解析失败时原样返回——宁可少一个字段，不破坏日志行。
func mergeJSON(data []byte, extra map[string]any) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil || m == nil {
		return data, nil
	}
	for k, v := range extra {
		if _, ok := m[k]; !ok {
			m[k] = v
		}
	}
	out, err := json.Marshal(m)
	if err != nil {
		return data, nil
	}
	return append(out, '\n'), nil
}

// appendText 在 TextFormatter 输出的换行符前追加 k=v 字段（按 key 排序保证稳定）。
func appendText(data []byte, extra map[string]any) []byte {
	keys := make([]string, 0, len(extra))
	for k := range extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, " %s=%v", k, extra[k])
	}
	if n := len(data); n > 0 && data[n-1] == '\n' {
		return append(append(data[:n-1], sb.String()...), '\n')
	}
	return append(data, sb.String()...)
}

// 编译期断言：contextFormatter 满足 logrus.Formatter。
var _ logrus.Formatter = (*contextFormatter)(nil)

// Context 返回携带上下文的 Entry，日志输出自动附加当前 traceId：
//
//	logging.Context(ctx).Info("order created")
//
// 等价于 logrus.WithContext(ctx)，提供入口收敛。
func Context(ctx context.Context) *logrus.Entry {
	return logrus.WithContext(ctx)
}

// For 返回绑定模块名的 Entry，沿用 pipeline ExecContext.Logger() 的注入模式：
//
//	log := logging.For("deadletter")
//	log.Warnf("recover failed: %v", err)
func For(module string) *logrus.Entry {
	return logrus.WithField(ModuleField, module)
}

// SetLevel 运行期调整全局日志级别（大小写不敏感），非法级别返回 error。
// 动态调级场景请优先使用 StartLevelWatch，本函数用于本地/临时调整。
func SetLevel(name string) error {
	parsed, err := logrus.ParseLevel(strings.TrimSpace(name))
	if err != nil {
		return fmt.Errorf("logging: 非法日志级别 %q: %w", name, err)
	}
	logrus.SetLevel(parsed)
	return nil
}

// CurrentLevel 返回当前全局日志级别名。
func CurrentLevel() string {
	return logrus.GetLevel().String()
}

// Service 返回 Init 配置的服务名（未 Init 或未配置返回空串）。
func Service() string {
	return state.Load().service
}

// LevelWatchKey 按仓库约定拼装动态调级配置键：/log/level/<service>。
// 本包只提供键名约定与 SetLevel 应用端点，不监听任何配置源；
// 使用方（装配层）监听该键并把变更值交给 SetLevel 应用，参考 README 示例。
func LevelWatchKey(service string) string {
	return "/log/level/" + strings.TrimSpace(service)
}
