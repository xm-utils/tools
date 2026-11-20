module github.com/xm-utils/tools/mq/pipequeue

go 1.24.2

require (
	github.com/sirupsen/logrus v1.10.2
	github.com/xm-utils/tools/mq/core v0.0.0-00010101000000-000000000000
	github.com/xm-utils/tools/pipeline v0.0.1
)

require golang.org/x/sys v0.13.0 // indirect

// SPLIT-PLACEHOLDER: 门面提交推送后替换为真实 pseudo-version 并删除本 replace。
replace github.com/xm-utils/tools/mq/core => ../core
