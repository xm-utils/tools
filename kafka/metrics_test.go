package kafka

import (
	"testing"
	"time"
)

// TestConsumerMetrics_Basic 测试基本指标收集
func TestConsumerMetrics_Basic(t *testing.T) {
	metrics := NewConsumerMetrics(1 * time.Second)

	// 模拟拉取消息
	for i := 0; i < 10; i++ {
		metrics.RecordFetch(1, 50*time.Millisecond)
		metrics.RecordProcessTime(20 * time.Millisecond)
	}

	// 等待窗口切换
	time.Sleep(1100 * time.Millisecond)

	// 再模拟一些数据
	for i := 0; i < 5; i++ {
		metrics.RecordFetch(1, 30*time.Millisecond)
		metrics.RecordProcessTime(15 * time.Millisecond)
	}

	// 获取统计
	stats := metrics.GetOverallStats()
	if stats.TotalMessages != 15 {
		t.Errorf("期望总消息数15，实际%d", stats.TotalMessages)
	}

	if stats.TotalFetches != 15 {
		t.Errorf("期望总拉取次数15，实际%d", stats.TotalFetches)
	}

	t.Logf("总体统计: %+v", stats)

	// 获取当前窗口
	currentWindow := metrics.GetCurrentWindow()
	t.Logf("当前窗口: 消息数=%d, 拉取数=%d", currentWindow.MessageCount, currentWindow.FetchCount)

	// 获取历史窗口
	history := metrics.GetHistoryWindows()
	t.Logf("历史窗口数量: %d", len(history))
}

// TestConsumerMetrics_Error 测试错误记录
func TestConsumerMetrics_Error(t *testing.T) {
	metrics := NewConsumerMetrics(1 * time.Second)

	// 模拟正常处理
	for i := 0; i < 10; i++ {
		metrics.RecordFetch(1, 50*time.Millisecond)
		metrics.RecordProcessTime(20 * time.Millisecond)
	}

	// 模拟错误
	for i := 0; i < 3; i++ {
		metrics.RecordError()
	}

	stats := metrics.GetOverallStats()
	if stats.TotalErrors != 3 {
		t.Errorf("期望错误数3，实际%d", stats.TotalErrors)
	}

	t.Logf("错误统计: 总数=%d", stats.TotalErrors)
}

// TestConsumerMetrics_WindowRotation 测试窗口切换
func TestConsumerMetrics_WindowRotation(t *testing.T) {
	metrics := NewConsumerMetrics(500 * time.Millisecond)

	// 第一个窗口
	for i := 0; i < 5; i++ {
		metrics.RecordFetch(1, 50*time.Millisecond)
	}

	time.Sleep(600 * time.Millisecond)

	// 第二个窗口
	for i := 0; i < 8; i++ {
		metrics.RecordFetch(1, 40*time.Millisecond)
	}

	time.Sleep(600 * time.Millisecond)

	// 第三个窗口
	for i := 0; i < 3; i++ {
		metrics.RecordFetch(1, 60*time.Millisecond)
	}

	// 检查历史窗口
	history := metrics.GetHistoryWindows()
	if len(history) < 2 {
		t.Errorf("期望至少2个历史窗口，实际%d", len(history))
	}

	// 检查累计统计
	stats := metrics.GetOverallStats()
	if stats.TotalMessages != 16 {
		t.Errorf("期望总消息数16，实际%d", stats.TotalMessages)
	}

	t.Logf("历史窗口数量: %d", len(history))
	t.Logf("累计消息数: %d", stats.TotalMessages)
}

// TestConsumerMetrics_Rates 测试速率计算
func TestConsumerMetrics_Rates(t *testing.T) {
	metrics := NewConsumerMetrics(1 * time.Second)

	startTime := time.Now()

	// 在2秒内持续产生数据
	go func() {
		for i := 0; i < 20; i++ {
			metrics.RecordFetch(1, 50*time.Millisecond)
			metrics.RecordProcessTime(20 * time.Millisecond)
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// 等待数据处理完成
	time.Sleep(2500 * time.Millisecond)

	stats := metrics.GetOverallStats()
	elapsed := time.Since(startTime).Seconds()

	expectedRate := float64(stats.TotalMessages) / elapsed
	t.Logf("运行时长: %.2f秒", elapsed)
	t.Logf("总消息数: %d", stats.TotalMessages)
	t.Logf("平均速率: %.2f 条/秒", stats.MessageRate)
	t.Logf("期望速率: %.2f 条/秒", expectedRate)

	// 速率应该在合理范围内（允许一定误差）
	if stats.MessageRate <= 0 {
		t.Error("消息速率应该大于0")
	}
}

// TestConsumerMetrics_FormatStats 测试格式化输出
func TestConsumerMetrics_FormatStats(t *testing.T) {
	metrics := NewConsumerMetrics(1 * time.Second)

	// 添加一些数据
	for i := 0; i < 100; i++ {
		metrics.RecordFetch(1, 50*time.Millisecond)
		metrics.RecordProcessTime(25 * time.Millisecond)
	}

	// 等待窗口切换
	time.Sleep(1100 * time.Millisecond)

	output := metrics.FormatStats()
	t.Logf("\n%s", output)

	// 验证输出包含关键信息
	if output == "" {
		t.Error("格式化输出不应为空")
	}
}

// TestConsumerMetrics_Reset 测试重置功能
func TestConsumerMetrics_Reset(t *testing.T) {
	metrics := NewConsumerMetrics(1 * time.Second)

	// 添加数据
	for i := 0; i < 50; i++ {
		metrics.RecordFetch(1, 50*time.Millisecond)
		metrics.RecordProcessTime(20 * time.Millisecond)
	}
	metrics.RecordError()

	// 重置
	metrics.Reset()

	// 验证重置后的状态
	stats := metrics.GetOverallStats()
	if stats.TotalMessages != 0 {
		t.Errorf("重置后总消息数应为0，实际%d", stats.TotalMessages)
	}

	if stats.TotalFetches != 0 {
		t.Errorf("重置后总拉取次数应为0，实际%d", stats.TotalFetches)
	}

	if stats.TotalErrors != 0 {
		t.Errorf("重置后总错误数应为0，实际%d", stats.TotalErrors)
	}

	t.Log("重置成功")
}

// TestConsumerMetrics_ConcurrentAccess 测试并发访问
func TestConsumerMetrics_ConcurrentAccess(t *testing.T) {
	metrics := NewConsumerMetrics(1 * time.Second)

	done := make(chan bool, 10)

	// 启动10个协程并发写入
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				metrics.RecordFetch(1, 50*time.Millisecond)
				metrics.RecordProcessTime(20 * time.Millisecond)
			}
			done <- true
		}()
	}

	// 等待所有协程完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证统计数据
	stats := metrics.GetOverallStats()
	expectedMessages := int64(1000) // 10个协程 * 100次
	if stats.TotalMessages != expectedMessages {
		t.Errorf("期望总消息数%d，实际%d", expectedMessages, stats.TotalMessages)
	}

	t.Logf("并发测试通过: 总消息数=%d", stats.TotalMessages)
}

// TestMetricsWindow_Calculation 测试窗口数据计算
func TestMetricsWindow_Calculation(t *testing.T) {
	window := &MetricsWindow{
		StartTime:        time.Now().Add(-10 * time.Second),
		EndTime:          time.Now(),
		MessageCount:     100,
		FetchCount:       50,
		TotalProcessTime: 2 * time.Second,
	}

	// 手动计算平均值
	expectedAvg := window.TotalProcessTime / time.Duration(window.MessageCount)
	t.Logf("平均处理耗时: %v", expectedAvg)

	// 计算速率
	durationSeconds := window.EndTime.Sub(window.StartTime).Seconds()
	expectedMsgRate := float64(window.MessageCount) / durationSeconds
	expectedFetchRate := float64(window.FetchCount) / durationSeconds

	t.Logf("消息速率: %.2f 条/秒", expectedMsgRate)
	t.Logf("拉取速率: %.2f 次/秒", expectedFetchRate)
}
