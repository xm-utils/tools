package database

import "github.com/sirupsen/logrus"

// LogrusWriter 是一个实现了 io.Writer 接口的结构体
type LogrusWriter struct {
	logger *logrus.Logger
	level  logrus.Level
}

// NewLogrusWriter 创建一个新的 LogrusWriter
func NewLogrusWriter(logger *logrus.Logger, level logrus.Level) *LogrusWriter {
	return &LogrusWriter{
		logger: logger,
		level:  level,
	}
}

// Write 实现 io.Writer 接口
func (w *LogrusWriter) Write(p []byte) (n int, err error) {
	// Logrus 的 Entry 方法会自动处理换行和格式化
	// 注意：ORM 传来的日志通常已经包含换行符，logrus 可能会再添加一个，可根据需要修剪
	w.logger.Log(w.level, string(p))
	return len(p), nil
}
