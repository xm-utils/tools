// Package metrics 提供全仓库统一的指标出口：集中注册表、命名规范与 /metrics HTTP 暴露。
//
// 设计要点（对应架构路线图 3.1.2）：
//   - 统一注册表：所有组件指标注册进同一个 prometheus.Registry，Handler() 一站式暴露；
//   - 命名规范：指标全名一律 <namespace>_<component>_<name>，默认前缀 xmt，如 xmt_breaker_requests_total；
//   - 兼容策略：不删除、不修改各组件既有 metrics.go 的私有指标实现，跨组件接入一律走
//     metrics/adapter/* 薄适配器（拉取式 prometheus.Collector），组件本体零依赖本包；
//   - panic 安全：对外 API 不向调用方抛 panic，重复注册等错误一律以 error 返回。
//
// 未调用 Init 时使用默认配置（namespace=xmt、启用 Go collector），
// 所有函数可直接使用；Init 应在进程启动、注册任何指标之前调用一次。
package metrics

import (
	"fmt"
	"strings"
)

// DefaultNamespace 默认指标名前缀，形成 xmt_<component>_<name> 约定。
const DefaultNamespace = "xmt"

// QualifiedName 按 <namespace>_<component>_<name> 规范拼装指标全名。
// component 与 name 均不得为空；拼装结果须符合 Prometheus 指标名字符集，
// 否则返回 error（不 panic）。
func QualifiedName(component, name string) (string, error) {
	ns := Namespace()
	if component == "" {
		return "", fmt.Errorf("metrics: component 不能为空")
	}
	if name == "" {
		return "", fmt.Errorf("metrics: 指标名不能为空")
	}
	fq := ns + "_" + component + "_" + name
	if !isValidMetricName(fq) {
		return "", fmt.Errorf("metrics: 非法指标全名 %q（仅允许字母、数字、下划线、冒号，且不以数字开头）", fq)
	}
	return fq, nil
}

// isValidMetricName 校验 Prometheus 指标名字符集：[a-zA-Z_:][a-zA-Z0-9_:]*。
func isValidMetricName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if i == 0 {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c == ':') {
				return false
			}
			continue
		}
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == ':') {
			return false
		}
	}
	return true
}

// normalizeLabels 去空白、保序去重 label 名，空输入返回 nil（Vec 构造器不接受零 label）。
func normalizeLabels(labelNames []string) ([]string, error) {
	seen := make(map[string]bool, len(labelNames))
	out := make([]string, 0, len(labelNames))
	for _, l := range labelNames {
		l = strings.TrimSpace(l)
		if l == "" {
			return nil, fmt.Errorf("metrics: label 名不能为空")
		}
		if seen[l] {
			return nil, fmt.Errorf("metrics: label 名重复: %s", l)
		}
		seen[l] = true
		out = append(out, l)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("metrics: 至少需要一个 label 名")
	}
	return out, nil
}
