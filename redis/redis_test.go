package redis

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func init() {
	err := InitRedisCache(&Config{
		Prefix:   "aaaa",
		Host:     "192.168.3.87:6379",
		Password: "",
		DbNum:    0,
	})
	if err != nil {
		panic(err)
	}
}

type args struct {
	Name  string `json:"name,omitempty"`
	Age   int    `json:"age,omitempty"`
	Phone string `json:"phone,omitempty"`
}

func TestHGetAll(t *testing.T) {
	ctx := context.Background()

	exist2 := IsExist(ctx, "test_users2")
	fmt.Println("test_users2 IsExist", exist2)

	if err := Delete(ctx, "test_users"); err != nil {
		t.Error(err)
		return
	}

	exist := IsExist(ctx, "test_users")
	fmt.Println("test_users IsExist", exist)
	if exist {
		t.Log("HLen", HLen(ctx, "test_users"))
	}

	fmt.Println("HSet。。。。。。。。。。。。。。。")
	if err := HSet(ctx, "test_users", "user_11", &args{
		Name:  "name_11",
		Age:   11,
		Phone: "phone_11",
	}); err != nil {
		t.Error("HSet error:", err)
		return
	}
	fmt.Println("HGet。。。。。。。。。。。。。。。")

	if arg, err := HGet[args](ctx, "test_users", "user_11"); err != nil {
		t.Error("HGet error:", err)
		return
	} else {
		t.Log(arg)
	}

	fmt.Println("HMSet。。。。。。。。。。。。。。。")
	data := make(map[string]interface{})
	for i := 0; i < 10; i++ {
		field := fmt.Sprintf("user_%d", i)

		data[field] = &args{
			Name:  fmt.Sprintf("name_%d", i),
			Age:   20,
			Phone: fmt.Sprintf("phone_%d", i),
		}
	}

	if err := HMSet(ctx, "test_users", data); err != nil {
		t.Error("HSetAll error:", err)
		return
	}

	t.Log("HLen", HLen(ctx, "test_users"))

	fmt.Println("HKeys。。。。。。。。。。。。。。。")
	if keys, err := HKeys(ctx, "test_users"); err != nil {
		t.Error("HKeys error:", err)
		return
	} else {
		t.Log(keys)
	}

	fmt.Println("HMGet。。。。。。。。。。。。。。。")
	hmGet := HMGet[args](ctx, "test_users", "user_1", "user_2", "user_3")
	for k, v := range hmGet {
		fmt.Println(k, v)
	}

	fmt.Println("HGetAll。。。。。。。。。。。。。。。")
	err, m := HGetAll[args](ctx, "test_users")
	if err != nil {
		t.Error("HGetAll error", err)
		return
	}
	for k, v := range m {
		fmt.Println(k, v)
	}

	fmt.Println("HVals。。。。。。。。。。。。。。。。。")
	vals, err := HVals[args](ctx, "test_users")
	if err != nil {
		t.Error("HVals error", err)
		return
	}
	for _, v := range vals {
		fmt.Println(v)
	}

}

func TestHSet(t *testing.T) {
	interval := time.Tick(time.Second * 30)
	for range interval {

		fmt.Println(time.Now().Unix())
	}

}

func TestList(t *testing.T) {
	ctx := context.Background()

	fmt.Println("LPush。。。。。。。。。。。。。。。")
	if err := LPush(ctx, listKey, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"); err != nil {
		t.Error(err)
		return
	}

	fmt.Println("获取列表长度")
	if lenth, err := LLen(ctx, listKey); err != nil {
		t.Error(err)
		return
	} else {
		t.Log("列表长度：", lenth)
	}

	fmt.Println("通过索引获取列表中的元素")
	if v, err := LIndex[string](ctx, listKey, 2); err != nil {
		t.Error(err)
		return
	} else {
		t.Log(v)
	}

	fmt.Println("LPop。。。。。。。。。。。。。。。")
	if v, err := LPop[string](ctx, listKey); err != nil {
		t.Error(err)
		return
	} else {
		t.Log(v)
	}

	// Lindex 4	LINDEX key index 通过索引获取列表中的元素

	// Linsert 5	LINSERT key BEFORE|AFTER pivot value 在列表的元素前或者后插入元素
	fmt.Println("Linsert。。。。。。。。。。。。。。。")
	if err := LInsert(ctx, listKey, "BEFORE", 2, 11); err != nil {
		t.Error(err)
		return
	}

	// LPush 8	LPUSH key value1 [value2] 将一个或多个值插入到列表头部
	fmt.Println("LPush。。。。。。。。。。。。。。。")
	if err := LPush(ctx, listKey, 20, 21, 22, 23, 24, 25); err != nil {
		t.Error(err)
		return
	}

	// LPushX 9	LPUSHX key value 将一个值插入到已存在的列表头部
	fmt.Println("LPushX。。。。。。。。。。。。。。。")
	if err := LPushX(ctx, listKey, 26, 27, 28, 29, 30); err != nil {
		t.Error(err)
		return
	}

	// Lrange 10	LRANGE key start stop 获取列表指定范围内的元素
	fmt.Println("Lrange。。。。。。。。。。。。。。。")
	if v, err := LRange[string](ctx, listKey, 0, 10); err != nil {
		t.Error(err)
		return
	} else {
		t.Log(v)
	}

	// LRem 11	LREM key count value 移除列表元素
	fmt.Println("LRem。。。。。。。。。。。。。。。")
	if v, err := LRem(ctx, listKey, 2, 27); err != nil {
		t.Error(err)
		return
	} else {
		t.Log(v)
	}

	// Lset 12	LSET key index value 通过索引设置列表元素的值
	fmt.Println("Lset。。。。。。。。。。。。。。。")
	if err := LSet(ctx, listKey, 5, 40); err != nil {
		t.Error(err)
		return
	}

	// Ltrim 13	LTRIM key start stop 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
	fmt.Println("Ltrim。。。。。。。。。。。。。。。")
	if err := Ltrim(ctx, listKey, 5, 10); err != nil {
		t.Error(err)
		return
	}

	// RPop 14	RPOP key 移除列表的最后一个元素，返回值为移除的元素。
	fmt.Println("RPop。。。。。。。。。。。。。。。")
	if v, err := RPop[string](ctx, listKey); err != nil {
		t.Error(err)
		return
	} else {
		t.Log(v)
	}

	// RPopLPush 15	RPOPLPUSH source destination 移除列表的最后一个元素，并将该元素添加到另一个列表并返回
	fmt.Println("RPopLPush。。。。。。。。。。。。。。。")
	if v, err := RPopLPush[string](ctx, listKey, listKey2); err != nil {
		t.Error(err)
		return
	} else {
		t.Log(v)
	}

	// Rpush 16	RPUSH key value1 [value2] 在列表中添加一个或多个值到列表尾部
	fmt.Println("Rpush。。。。。。。。。。。。。。。")
	if err := RPush(ctx, listKey, 1, 2, 3, 4, 5); err != nil {
		t.Error(err)
		return
	}

	// RPushX 17 RPUSHX key value 为已存在的列表添加值
	fmt.Println("RPushX。。。。。。。。。。。。。。。")
	if err := RPushX(ctx, listKey, 6, 7, 8, 9); err != nil {
		t.Error(err)
		return
	}

	fmt.Println("获取列表长度")
	if lenth, err := LLen(ctx, listKey); err != nil {
		t.Error(err)
		return
	} else {
		res, err := LRange[string](ctx, listKey, 0, lenth)
		t.Log("列表长度：", lenth, "列表内容：", res)
		if err != nil {
			t.Error(err)
			return
		}
		t.Log("列表内容：", res)
	}
}

const (
	listKey  = "test_list"
	listKey2 = "test_list2"
)

func TestBLPop(t *testing.T) {
	ctx := context.Background()
	for {
		k, v, err := BLPop[args](ctx, 0, listKey, listKey2)
		if err != nil {
			t.Error(err)
			return
		}
		t.Log(k, "====>", v)
	}
}

func TestRPush(t *testing.T) {
	ctx := context.Background()
	index := 0
	for {

		val := &args{
			Name:  fmt.Sprintf("name_%d", index),
			Age:   20,
			Phone: fmt.Sprintf("phone_%d", index),
		}

		err := RPush(ctx, listKey, val)
		if err != nil {
			t.Error(err)
			return
		}
		time.Sleep(10 * time.Second)
		index++
	}
}

func TestRPush2(t *testing.T) {
	ctx := context.Background()
	index := 0
	for {

		val := &args{
			Name:  fmt.Sprintf("k2_name_%d", index),
			Age:   20,
			Phone: fmt.Sprintf("k2_phone_%d", index),
		}

		err := RPush(ctx, listKey2, val)
		if err != nil {
			t.Error(err)
			return
		}
		time.Sleep(5 * time.Second)
		index++
	}
}

func TestFe(t *testing.T) {
	ctx := context.Background()

	key := "test_fe"
	err := Set(ctx, key, 1000, 0)
	if err != nil {
		t.Error(err)
		return
	}

	in, err := IncrBy(ctx, key, 1000)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(in)

	for i := 0; i < 22; i++ {
		de, err := DecrBy(ctx, key, 100)
		if err != nil {
			t.Error(err)
			return
		}
		t.Log(i, "========", de)
	}
}

func TestEval(t *testing.T) {

	ctx := context.Background()

	key := "test_fe"
	err := Set(ctx, key, 1000, 0)
	if err != nil {
		t.Error(err)
		return
	}

	in, err := IncrBy(ctx, key, 1000)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("初始余额:", in)

	script := `
		local key = KEYS[1]
		local amount = tonumber(ARGV[1])
		local val = redis.call('GET', key)
		local balance = tonumber(val)
		if balance >= amount then
			redis.call('SET', key, balance - amount)
			return balance - amount
		else
			return -1
		end
		`

	for i := 0; i < 22; i++ {
		de, err1 := Eval[int64](ctx, script, []string{key}, 100)

		if err1 != nil {
			t.Error(err1)
			return
		}
		t.Log(i, "========", de)
	}

}

// TestEvalReturnInt64 测试 Lua 脚本返回 int64 类型
func TestEvalReturnInt64(t *testing.T) {
	ctx := context.Background()

	script := `return 42`
	result, err := Eval[int64](ctx, script, nil)
	if err != nil {
		t.Fatalf("Eval int64 error: %v", err)
	}
	if result != 42 {
		t.Errorf("expected 42, got %d", result)
	}
	t.Log("int64 result:", result)
}

// TestEvalReturnString 测试 Lua 脚本返回 string 类型
func TestEvalReturnString(t *testing.T) {
	ctx := context.Background()

	script := `return "hello from lua"`
	result, err := Eval[string](ctx, script, nil)
	if err != nil {
		t.Fatalf("Eval string error: %v", err)
	}
	if result != "hello from lua" {
		t.Errorf("expected 'hello from lua', got '%s'", result)
	}
	t.Log("string result:", result)
}

// TestEvalReturnBool 测试 Lua 脚本返回布尔类型
func TestEvalReturnBool(t *testing.T) {
	ctx := context.Background()

	// Lua 返回 true
	scriptTrue := `return true`
	result, err := Eval[bool](ctx, scriptTrue, nil)
	if err != nil {
		t.Fatalf("Eval bool error: %v", err)
	}
	r, ok := result.(bool)
	if !ok {
		t.Error("expected true, got false")
	}
	t.Log("bool result (true):", r)

	// Lua 返回 false -> go-redis 会返回 redis.Nil
	scriptFalse := `return false`
	result2, err := Eval[bool](ctx, scriptFalse, nil)
	if err != nil {
		t.Fatalf("Eval bool false error: %v", err)
	}
	t.Log("bool result (false):", result2)
}

// TestEvalKeyNotExist 测试访问不存在的 key 时 nil 安全处理
func TestEvalKeyNotExist(t *testing.T) {
	ctx := context.Background()

	script := `
		local val = redis.call('GET', KEYS[1])
		if val == false then
			return -1
		end
		return tonumber(val)
	`
	result, err := Eval[int64](ctx, script, []string{"non_existent_key_12345"})
	if err != nil {
		t.Fatalf("Eval key not exist error: %v", err)
	}
	if result != -1 {
		t.Errorf("expected -1 for non-existent key, got %d", result)
	}
	t.Log("non-existent key result:", result)
}

// TestEvalDeduction 测试扣减场景：余额充足返回剩余，不足返回 -1
func TestEvalDeduction(t *testing.T) {
	ctx := context.Background()
	key := "test_eval_deduct"

	// 初始化余额 500
	if err := Set(ctx, key, 500, 60); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	defer Delete(ctx, key)

	script := `
		local key = KEYS[1]
		local amount = tonumber(ARGV[1])
		local val = redis.call('GET', key)
		if val == false then
			return -1
		end
		local balance = tonumber(val)
		if balance >= amount then
			redis.call('SET', key, balance - amount)
			return balance - amount
		else
			return -1
		end
	`

	// 第一次扣减 200，应剩余 300
	r1, err := Eval[int64](ctx, script, []string{key}, 200)
	if err != nil {
		t.Fatalf("deduct 200 error: %v", err)
	}
	if r1 != 300 {
		t.Errorf("expected 300, got %d", r1)
	}
	t.Log("after deduct 200:", r1)

	// 第二次扣减 300，应剩余 0
	r2, err := Eval[int64](ctx, script, []string{key}, 300)
	if err != nil {
		t.Fatalf("deduct 300 error: %v", err)
	}
	if r2 != 0 {
		t.Errorf("expected 0, got %d", r2)
	}
	t.Log("after deduct 300:", r2)

	// 第三次扣减 100，余额不足应返回 -1
	r3, err := Eval[int64](ctx, script, []string{key}, 100)
	if err != nil {
		t.Fatalf("deduct 100 error: %v", err)
	}
	if r3 != -1 {
		t.Errorf("expected -1 (insufficient), got %d", r3)
	}
	t.Log("after deduct 100 (insufficient):", r3)
}

// TestEvalMultiKeys 测试多 key 操作
func TestEvalMultiKeys(t *testing.T) {
	ctx := context.Background()
	key1 := "test_eval_src"
	key2 := "test_eval_dst"

	// 初始化
	Set(ctx, key1, 1000, 60)
	Set(ctx, key2, 500, 60)
	defer Delete(ctx, key1)
	defer Delete(ctx, key2)

	// Lua 脚本：将 key1 的值转移到 key2
	script := `
		local src = KEYS[1]
		local dst = KEYS[2]
		local amount = tonumber(ARGV[1])

		local srcVal = redis.call('GET', src)
		if srcVal == false then
			return 0
		end
		local srcBalance = tonumber(srcVal)
		if srcBalance < amount then
			return 0
		end

		local dstVal = redis.call('GET', dst)
		local dstBalance = 0
		if dstVal ~= false then
			dstBalance = tonumber(dstVal)
		end

		redis.call('SET', src, srcBalance - amount)
		redis.call('SET', dst, dstBalance + amount)
		return 1
	`

	result, err := Eval[int64](ctx, script, []string{key1, key2}, 300)
	if err != nil {
		t.Fatalf("Eval multi keys error: %v", err)
	}
	if result != 1 {
		t.Errorf("expected 1 (success), got %d", result)
	}

	// 验证转移结果
	srcAfter, _ := Get[int64](ctx, key1)
	dstAfter, _ := Get[int64](ctx, key2)
	t.Logf("src after: %d, dst after: %d", srcAfter, dstAfter)

	if srcAfter != 700 {
		t.Errorf("expected src=700, got %d", srcAfter)
	}
	if dstAfter != 800 {
		t.Errorf("expected dst=800, got %d", dstAfter)
	}
}

// TestEvalReturnSlice 测试 Lua 脚本返回数组类型
func TestEvalReturnSlice(t *testing.T) {
	ctx := context.Background()

	script := `return {1, 2, 3}`
	result, err := Eval(ctx, script, nil)
	if err != nil {
		t.Fatalf("Eval slice error: %v", err)
	}
	if result == nil {
		t.Error("expected a slice, got nil")
		return
	}
	r, ok := result.([]int64)
	if ok {
		t.Errorf("expected 1, got %d", r[0])
	}
	if len(r) != 3 {
		t.Errorf("expected 3 elements, got %d", len(r))
	}
	t.Log("slice result:", result)
}
