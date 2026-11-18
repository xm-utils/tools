package database

import (
	"strings"
	"testing"
)

// TestMysqlNodeValidate 验证节点级必填校验：openDB 对 config.Master 调 validate，
// User/Host/Port/Database 缺失时返回列出缺失项的清晰 error，Password 允许为空。
func TestMysqlNodeValidate(t *testing.T) {
	valid := func() *MysqlNodeConfig {
		return &MysqlNodeConfig{
			User:     "root",
			Host:     "127.0.0.1",
			Port:     "3306",
			Database: "demo",
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
		c := &MysqlNodeConfig{}
		err := c.validate()
		if err == nil {
			t.Fatal("四个必填项全空应报错")
		}
		for _, field := range []string{"User", "Host", "Port", "Database"} {
			if !strings.Contains(err.Error(), field) {
				t.Errorf("错误信息应包含缺失字段 %q，实际=%q", field, err.Error())
			}
		}
	})

	t.Run("单个缺失字段仅列该字段", func(t *testing.T) {
		c := valid()
		c.Database = ""
		err := c.validate()
		if err == nil {
			t.Fatal("Database 为空应报错")
		}
		if !strings.Contains(err.Error(), "Database") {
			t.Errorf("错误应包含 Database，实际=%q", err.Error())
		}
		if strings.Contains(err.Error(), "User") {
			t.Errorf("User 非空不应被列出，实际=%q", err.Error())
		}
	})
}

// TestMysqlNodeUrl 验证节点 DSN 生成：使用自身字段（不再继承主库），
// Charset/Location 留空时回落默认 utf8mb4/Local，显式配置时生效。
func TestMysqlNodeUrl(t *testing.T) {
	node := MysqlNodeConfig{
		Host:     "10.0.0.2",
		Port:     "3306",
		User:     "ro",
		Password: "ropwd",
		Database: "repl",
	}
	dsn := node.Url()
	if !strings.Contains(dsn, "ro:ropwd@tcp(10.0.0.2:3306)/repl") {
		t.Fatalf("DSN 应使用节点自身连接信息，实际=%q", dsn)
	}
	// charset/location 留空使用默认值
	if !strings.Contains(dsn, "charset=utf8mb4") || !strings.Contains(dsn, "loc=Local") {
		t.Fatalf("留空的 charset/location 应回落默认值 utf8mb4/Local，实际=%q", dsn)
	}

	// 显式 charset 应生效
	node2 := MysqlNodeConfig{
		Host: "10.0.0.9", Port: "3306", User: "root", Password: "pwd", Database: "demo",
		Charset: "utf8",
	}
	dsn2 := node2.Url()
	if !strings.Contains(dsn2, "root:pwd@tcp(10.0.0.9:3306)/demo") {
		t.Fatalf("DSN 基础段不正确，实际=%q", dsn2)
	}
	if !strings.Contains(dsn2, "charset=utf8") {
		t.Fatalf("显式 charset 应生效，实际=%q", dsn2)
	}
}
