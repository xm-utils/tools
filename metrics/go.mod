module github.com/xm-utils/tools/metrics

go 1.24.2

require (
	github.com/alicebob/miniredis/v2 v2.40.0
	github.com/prometheus/client_golang v1.22.0
	github.com/prometheus/client_model v0.6.1
	github.com/xm-utils/tools/breaker v0.0.0-20261120164222-657347a6a8b5
	github.com/xm-utils/tools/deadletter v0.0.0-20261120164222-657347a6a8b5
	github.com/xm-utils/tools/mq/kafka v0.0.0-20261120164222-657347a6a8b5
	github.com/xm-utils/tools/redis v1.0.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-redis/redis/v8 v8.11.5 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/segmentio/kafka-go v0.4.51 // indirect
	github.com/sirupsen/logrus v1.10.2 // indirect
	github.com/xm-utils/tools/retry v1.0.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
	gorm.io/gorm v1.31.2 // indirect
)

// trace 尚未发布 tag，经 mq/kafka 间接依赖时以本地路径临时替代；trace 发版后删除本行并转 pseudo-version。
replace github.com/xm-utils/tools/trace => ../trace
