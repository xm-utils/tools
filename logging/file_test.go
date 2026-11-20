//go:build test

package logging_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xm-utils/tools/logging"
)

// fileSetup 初始化文件输出用例：确保结束后关闭文件句柄并恢复默认输出。
func fileSetup(t *testing.T) string {
	t.Helper()
	resetLogger(t)
	t.Cleanup(func() { _ = logging.Close() })
	return t.TempDir()
}

// readAll 读取文件全文（不存在返回空串）。
func readAll(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		t.Fatalf("读取 %s 失败: %v", path, err)
	}
	return string(b)
}

// redirectStd 临时替换 os.Stdout / os.Stderr 到临时文件，返回读取函数与还原动作。
func redirectStd(t *testing.T, isOut bool) func() string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	target := &os.Stderr
	if isOut {
		target = &os.Stdout
	}
	old := *target
	*target = w
	t.Cleanup(func() { *target = old })
	done := make(chan string, 1)
	go func() {
		var sb strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			sb.Write(buf[:n])
			if err != nil {
				break
			}
		}
		done <- sb.String()
	}()
	t.Cleanup(func() {
		_ = w.Close()
	})
	return func() string {
		_ = w.Close() // 触发读 goroutine 结束
		select {
		case s := <-done:
			return s
		case <-time.After(2 * time.Second):
			t.Fatal("读取重定向输出超时")
			return ""
		}
	}
}

func TestFileOutputWithStdoutMirror(t *testing.T) {
	dir := fileSetup(t)
	path := filepath.Join(dir, "app.log")

	readOut := redirectStd(t, true)
	if err := logging.Init(logging.Config{Service: "file-svc", OutputFile: path}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	logging.For("m").Info("hello file")

	got := readAll(t, path)
	if !strings.Contains(got, "hello file") || !strings.Contains(got, "file-svc") {
		t.Errorf("文件内容缺少消息或 service: %q", got)
	}
	if console := readOut(); !strings.Contains(console, "hello file") {
		t.Errorf("默认应同步保留控制台输出, 得 %q", console)
	}
}

func TestFileOutputOnlyWhenAlsoStdoutFalse(t *testing.T) {
	dir := fileSetup(t)
	path := filepath.Join(dir, "only.log")
	only := false

	readOut := redirectStd(t, true)
	if err := logging.Init(logging.Config{OutputFile: path, AlsoStdout: &only}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	logging.For("m").Info("to file only")

	if !strings.Contains(readAll(t, path), "to file only") {
		t.Error("文件应有内容")
	}
	if console := readOut(); strings.Contains(console, "to file only") {
		t.Errorf("AlsoStdout=false 时控制台不应有输出: %q", console)
	}
}

func TestInitBadFilePathKeepsPreviousConfig(t *testing.T) {
	fileSetup(t)
	if err := logging.Init(logging.Config{Level: "debug", Service: "keep"}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}

	bad := filepath.Join(t.TempDir(), "no-such-dir", "x.log")
	if err := logging.Init(logging.Config{Level: "error", OutputFile: bad}); err == nil {
		t.Fatal("不可用路径应返回 error")
	}
	if _, err := os.Stat(bad); !os.IsNotExist(err) {
		t.Error("失败的 Init 不应创建文件")
	}
	// 原配置保持不变
	if logging.CurrentLevel() != "debug" || logging.Service() != "keep" {
		t.Errorf("失败 Init 篡改了配置: level=%s service=%s", logging.CurrentLevel(), logging.Service())
	}
}

func TestInitNegativeRotationParams(t *testing.T) {
	fileSetup(t)
	if err := logging.Init(logging.Config{OutputFile: filepath.Join(t.TempDir(), "a.log"), MaxSizeMB: -1}); err == nil {
		t.Error("负 MaxSizeMB 应报错")
	}
	if err := logging.Init(logging.Config{MaxBackups: -2}); err == nil {
		t.Error("负 MaxBackups 应报错")
	}
}

func TestReinitSwitchesFileAndClosesOld(t *testing.T) {
	dir := fileSetup(t)
	pathA := filepath.Join(dir, "a.log")
	pathB := filepath.Join(dir, "b.log")

	if err := logging.Init(logging.Config{OutputFile: pathA, AlsoStdout: ptr(false)}); err != nil {
		t.Fatalf("Init A 失败: %v", err)
	}
	logging.For("m").Info("first")

	if err := logging.Init(logging.Config{OutputFile: pathB, AlsoStdout: ptr(false)}); err != nil {
		t.Fatalf("Init B 失败: %v", err)
	}
	logging.For("m").Info("second")

	if got := readAll(t, pathB); !strings.Contains(got, "second") {
		t.Errorf("新文件应含 second: %q", got)
	}
	if got := readAll(t, pathA); strings.Contains(got, "second") {
		t.Error("切换后旧文件不应再被写入")
	}
}

func TestCloseRevertsToStderr(t *testing.T) {
	dir := fileSetup(t)
	path := filepath.Join(dir, "close.log")

	if err := logging.Init(logging.Config{OutputFile: path, AlsoStdout: ptr(false)}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	logging.For("m").Info("in file")

	readErr := redirectStd(t, false)
	if err := logging.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if err := logging.Close(); err != nil {
		t.Fatalf("重复 Close 应幂等: %v", err)
	}
	logging.For("m").Info("after close")

	if console := readErr(); !strings.Contains(console, "after close") {
		t.Errorf("Close 后应回到 stderr: %q", console)
	}
	got := readAll(t, path)
	if !strings.Contains(got, "in file") || strings.Contains(got, "after close") {
		t.Errorf("Close 后文件不应再增长: %q", got)
	}
}

func TestRotationProducesBackup(t *testing.T) {
	dir := fileSetup(t)
	path := filepath.Join(dir, "rotate.log")

	// 1MB 阈值：写入约 2MB 触发至少一次轮转
	if err := logging.Init(logging.Config{OutputFile: path, AlsoStdout: ptr(false), MaxSizeMB: 1}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	t.Cleanup(func() { _ = logging.Close() })
	payload := strings.Repeat("x", 2048)
	for i := 0; i < 1200; i++ {
		logging.For("r").Info(fmt.Sprintf("%04d %s", i, payload))
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("超过 MaxSize 后应产生备份文件, 目录仅: %v", names)
	}
}

func ptr[T any](v T) *T { return &v }
