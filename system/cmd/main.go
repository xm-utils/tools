package main

import (
	"github.com/xm-utils/tools/beego/database"
	"github.com/xm-utils/tools/grpcx"
	"github.com/xm-utils/tools/redis"
	"github.com/xm-utils/tools/system"
)

func main() {

	err := database.InitMysql(&database.MysqlConfig{
		Alias:       "default",
		Name:        "system",
		User:        "xm-pay",
		Password:    "c8}kCrE]jxp0df.R53,Z",
		Host:        "127.0.0.1",
		Port:        "3306",
		Debug:       "true",
		TablePrefix: "",
		Charset:     "utf8",
		Location:    "Local",
	})
	if err != nil {
		panic(err)
	}

	err = redis.InitRedisCache(&redis.Config{
		Prefix: "xm-system",
		Host:   "127.0.0.1:6379",
	})
	if err != nil {
		panic(err)
	}
	system.RegisterModel()

	httpServer := grpcx.NewHTTPServer("manager-http", ":9009", system.Init)

	go httpServer.Start()

	grpcx.NewShutdown().
		InstallShutdownHook(httpServer).
		WaitShutdown()
}
