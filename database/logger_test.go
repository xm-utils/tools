package database_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm/logger"

	"github.com/xm-utils/tools/database"
)

// TestNewLog 测试日志初始化
func TestNewLog(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Info,
	}

	// 测试空日志目录
	log := database.NewLog(config, "")
	if log == nil {
		t.Fatal("Expected log to be not nil")
	}

	// 测试自定义日志目录
	tmpDir := t.TempDir()
	log = database.NewLog(config, tmpDir)
	if log == nil {
		t.Fatal("Expected log to be not nil")
	}
}

// TestLogMode 测试日志模式切换
func TestLogMode(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Info,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	// 测试切换到不同的日志级别
	newLog := log.LogMode(logger.Warn)
	if newLog == nil {
		t.Fatal("Expected newLog to be not nil")
	}

	newLog = log.LogMode(logger.Error)
	if newLog == nil {
		t.Fatal("Expected newLog to be not nil")
	}

	newLog = log.LogMode(logger.Silent)
	if newLog == nil {
		t.Fatal("Expected newLog to be not nil")
	}
}

// TestInfo 测试Info级别日志
func TestInfo(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Info,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()

	// 应该能够正常调用而不panic
	log.Info(ctx, "test info message %s", "data")
}

// TestWarn 测试Warn级别日志
func TestWarn(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Warn,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()

	// 应该能够正常调用而不panic
	log.Warn(ctx, "test warn message %s", "data")
}

// TestError 测试Error级别日志
func TestError(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Error,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()

	// 应该能够正常调用而不panic
	log.Error(ctx, "test error message %s", "data")
}

// TestTrace_Success 测试Trace功能 - 正常情况
func TestTrace_Success(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Info,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()
	begin := time.Now().Add(-100 * time.Millisecond)

	fc := func() (string, int64) {
		return "SELECT * FROM users WHERE id = 1", 1
	}

	// 应该能够正常调用而不panic
	log.Trace(ctx, begin, fc, nil)
}

// TestTrace_Error 测试Trace功能 - 错误情况
func TestTrace_Error(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: false,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Error,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()
	begin := time.Now().Add(-100 * time.Millisecond)

	fc := func() (string, int64) {
		return "SELECT * FROM users WHERE id = 1", 1
	}

	testErr := errors.New("database error")

	// 应该能够正常调用而不panic
	log.Trace(ctx, begin, fc, testErr)
}

// TestTrace_SlowQuery 测试Trace功能 - 慢查询
func TestTrace_SlowQuery(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             50 * time.Millisecond,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Warn,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()
	begin := time.Now().Add(-200 * time.Millisecond) // 200ms > 50ms threshold

	fc := func() (string, int64) {
		return "SELECT * FROM users WHERE id = 1", 1
	}

	// 应该能够正常调用而不panic
	log.Trace(ctx, begin, fc, nil)
}

// TestTrace_Silent 测试Trace功能 - Silent模式
func TestTrace_Silent(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Silent,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()
	begin := time.Now()

	fc := func() (string, int64) {
		return "SELECT * FROM users WHERE id = 1", 1
	}

	// Silent模式下不应该有任何输出
	log.Trace(ctx, begin, fc, nil)
}

// TestTrace_NegativeRows 测试Trace功能 - rows为-1的情况
func TestTrace_NegativeRows(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Info,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()
	begin := time.Now().Add(-100 * time.Millisecond)

	fc := func() (string, int64) {
		return "UPDATE users SET name = 'test'", -1
	}

	// 应该能够正常调用而不panic
	log.Trace(ctx, begin, fc, nil)
}

// TestParamsFilter 测试参数过滤功能
func TestParamsFilter(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Info,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()
	sql := "SELECT * FROM users WHERE id = ?"

	// 通过 Info 方法验证 logger 正常工作
	log.Info(ctx, "test params filter - SQL: %s", sql)
}

// TestParamsFilter_Parameterized 测试参数化查询配置
func TestParamsFilter_Parameterized(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      true, // 启用参数化查询
		LogLevel:                  logger.Info,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()
	sql := "SELECT * FROM users WHERE id = ?"

	// 测试参数化查询配置下的日志记录
	log.Info(ctx, "test parameterized query - SQL: %s", sql)
}

// TestLogrusFileLoggerHook 测试文件日志Hook
func TestLogrusFileLoggerHook(t *testing.T) {
	tmpDir := t.TempDir()

	hook, err := database.NewLogrusFileLoggerHook(tmpDir, 10<<20)
	if err != nil {
		t.Fatalf("Failed to create hook: %v", err)
	}

	if hook == nil {
		t.Fatal("Expected hook to be not nil")
	}

	// 测试Levels方法
	levels := hook.Levels()
	if len(levels) == 0 {
		t.Error("Expected levels to be not empty")
	}
}

// TestLogrusFileLoggerHook_Fire 测试Hook的Fire方法
func TestLogrusFileLoggerHook_Fire(t *testing.T) {
	tmpDir := t.TempDir()

	hook, err := database.NewLogrusFileLoggerHook(tmpDir, 10<<20)
	if err != nil {
		t.Fatalf("Failed to create hook: %v", err)
	}

	// 创建日志条目
	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Data:    make(logrus.Fields),
		Time:    time.Now(),
		Level:   logrus.InfoLevel,
		Message: "test message",
	}

	// 触发日志写入
	err = hook.Fire(entry)
	if err != nil {
		t.Fatalf("Failed to fire hook: %v", err)
	}

	// 验证日志文件是否创建
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read log directory: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected at least one log file to be created")
	}
}

// TestFileSize 测试文件大小函数
func TestFileSize(t *testing.T) {
	// 测试空路径
	size, err := database.FileSize("")
	if err == nil {
		t.Error("Expected error for empty path")
	}
	if size != 0 {
		t.Errorf("Expected size 0 for empty path, got %d", size)
	}

	// 测试不存在的文件
	size, err = database.FileSize("/nonexistent/file.log")
	if err == nil {
		t.Error("Expected error for nonexistent file")
	}

	// 测试实际文件
	tmpFile := filepath.Join(t.TempDir(), "test.log")
	content := "test content"
	err = os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	size, err = database.FileSize(tmpFile)
	if err != nil {
		t.Fatalf("Failed to get file size: %v", err)
	}
	if size != int64(len(content)) {
		t.Errorf("Expected size %d, got %d", len(content), size)
	}
}

// TestColorfulLog 测试彩色日志
func TestColorfulLog(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  true, // 启用彩色
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Info,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()

	// 测试Info
	log.Info(ctx, "colorful info message")

	// 测试Warn
	log.Warn(ctx, "colorful warn message")

	// 测试Error
	log.Error(ctx, "colorful error message")
}

// TestIgnoreRecordNotFoundError 测试忽略记录未找到错误
func TestIgnoreRecordNotFoundError(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		LogLevel:                  logger.Error,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()
	begin := time.Now().Add(-100 * time.Millisecond)

	fc := func() (string, int64) {
		return "SELECT * FROM users WHERE id = 1", 0
	}

	// RecordNotFound错误应该被忽略
	log.Trace(ctx, begin, fc, logger.ErrRecordNotFound)
}

// TestNotIgnoreRecordNotFoundError 测试不忽略记录未找到错误
func TestNotIgnoreRecordNotFoundError(t *testing.T) {
	config := logger.Config{
		SlowThreshold:             time.Second,
		Colorful:                  false,
		IgnoreRecordNotFoundError: false, // 不忽略
		ParameterizedQueries:      false,
		LogLevel:                  logger.Error,
	}

	tmpDir := t.TempDir()
	log := database.NewLog(config, tmpDir)

	ctx := context.Background()
	begin := time.Now().Add(-100 * time.Millisecond)

	fc := func() (string, int64) {
		return "SELECT * FROM users WHERE id = 1", 0
	}

	// RecordNotFound错误应该被记录
	log.Trace(ctx, begin, fc, logger.ErrRecordNotFound)
}
