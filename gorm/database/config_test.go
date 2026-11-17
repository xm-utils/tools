package database

import (
	"strings"
	"testing"
)

// TestMysqlConfigValidate 验证 P3：openDB 前置的必填字段校验，缺失时返回列出缺失项的清晰 error。
func TestMysqlConfigValidate(t *testing.T) {
	valid := func() *MysqlConfig {
		return &MysqlConfig{
			User: "root",
			Host: "127.0.0.1",
			Port: "3306",
			Name: "demo",
		}
	}

	t.Run("完整配置通过校验", func(t *testing.T) {
		if err := valid().validate(); err != nil {
			t.Fatalf("合法配置不应报错，实际=%v", err)
		}
	})

	t.Run("密码为空允许", func(t *testing.T) {
		c := valid()
		c.Password = ""
		if err := c.validate(); err != nil {
			t.Fatalf("Password 允许为空，实际报错=%v", err)
		}
	})

	t.Run("缺失字段被逐一列出", func(t *testing.T) {
		c := valid()
		c.User = ""
		c.Host = ""
		c.Port = ""
		c.Name = ""
		err := c.validate()
		if err == nil {
			t.Fatal("四个必填项全空应报错")
		}
		for _, field := range []string{"User", "Host", "Port", "Name"} {
			if !strings.Contains(err.Error(), field) {
				t.Errorf("错误信息应包含缺失字段 %q，实际=%q", field, err.Error())
			}
		}
	})

	t.Run("单个缺失字段仅列该字段", func(t *testing.T) {
		c := valid()
		c.Name = ""
		err := c.validate()
		if err == nil {
			t.Fatal("Name 为空应报错")
		}
		if !strings.Contains(err.Error(), "Name") {
			t.Errorf("错误应包含 Name，实际=%q", err.Error())
		}
		if strings.Contains(err.Error(), "User") {
			t.Errorf("User 非空不应被列出，实际=%q", err.Error())
		}
	})

	t.Run("从库节点缺 Host 或 Port 报错", func(t *testing.T) {
		c := valid()
		c.Slaves = []MysqlNodeConfig{
			{Host: "10.0.0.2", Port: "3306"},
			{Host: "", Port: "3307"}, // 缺 Host
		}
		err := c.validate()
		if err == nil {
			t.Fatal("从库缺 Host 应报错")
		}
		if !strings.Contains(err.Error(), "slaves[1]") {
			t.Errorf("错误应指明从库索引 slaves[1]，实际=%q", err.Error())
		}
	})

	t.Run("从库节点齐全通过校验", func(t *testing.T) {
		c := valid()
		c.Slaves = []MysqlNodeConfig{
			{Host: "10.0.0.2", Port: "3306"},
			{Host: "10.0.0.3", Port: "3306"},
		}
		if err := c.validate(); err != nil {
			t.Fatalf("从库配置齐全不应报错，实际=%v", err)
		}
	})
}

// TestMysqlNodeUrlInherit 顺带验证从库 DSN 生成对空字段继承主库、非空覆盖的基本语义。
func TestMysqlNodeUrlInherit(t *testing.T) {
	global := &MysqlConfig{User: "root", Password: "pwd", Name: "demo", Charset: "utf8mb4", Location: "Local"}
	node := MysqlNodeConfig{Host: "10.0.0.2", Port: "3306"}
	dsn := node.Url(global)
	// 应继承主库 user/pwd/database
	if !strings.Contains(dsn, "root:pwd@tcp(10.0.0.2:3306)/demo") {
		t.Fatalf("从库 DSN 应继承主库连接信息，实际=%q", dsn)
	}
	// 覆盖 user/pwd/database 后应使用节点自身值
	node2 := MysqlNodeConfig{Host: "10.0.0.9", Port: "3306", User: "ro", Password: "ropwd", Database: "repl"}
	dsn2 := node2.Url(global)
	if !strings.Contains(dsn2, "ro:ropwd@tcp(10.0.0.9:3306)/repl") {
		t.Fatalf("从库显式字段应覆盖主库继承，实际=%q", dsn2)
	}
}
