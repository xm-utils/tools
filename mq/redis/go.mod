module github.com/xm-utils/tools/mq/redis

go 1.24.2

require (
	github.com/alicebob/miniredis/v2 v2.40.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/panjf2000/ants/v2 v2.12.1
	github.com/sirupsen/logrus v1.10.2
	github.com/xm-utils/tools/database v1.1.0
	github.com/xm-utils/tools/deadletter v1.0.2
	github.com/xm-utils/tools/mq/core v0.0.0-00010101000000-000000000000
	github.com/xm-utils/tools/redis v1.0.0
	github.com/xm-utils/tools/retry v1.0.2
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	github.com/google/go-querystring v1.2.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.20.0 // indirect
	gorm.io/driver/mysql v1.6.0 // indirect
	gorm.io/gorm v1.31.2 // indirect
)

// SPLIT-PLACEHOLDER: 门面提交推送后替换为真实 pseudo-version 并删除本 replace。
replace github.com/xm-utils/tools/mq/core => ../core
