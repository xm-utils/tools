package zset

// Lua 脚本清单：Redis 单线程执行保证原子性，各脚本键/参含义见调用点注释。
const (
	// claimZScript 领取一条到期消息：ready 中 score<=ARGV[1]（当前毫秒）的最小成员
	// 挪入 processing 并把 score 重置为领取时刻（认领时间，供可见性回收判断）。
	// 无可领取消息时返回 nil（客户端侧表现为 redis.Nil）。ARGV[1]=now(ms)。
	claimZScript = `
local due = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 1)
if #due == 0 then
  return nil
end
local m = due[1]
redis.call("ZREM", KEYS[1], m)
redis.call("ZADD", KEYS[2], ARGV[1], m)
return m
`

	// requeueZScript 原子回队：从 processing 移除原成员，把 attempt+1 的新成员
	// 按新可见时间 ZADD 回 ready。
	// KEYS[1]=processing, KEYS[2]=ready;
	// ARGV[1]=原成员, ARGV[2]=重投成员, ARGV[3]=重投可见时间(ms)。
	requeueZScript = `
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
return 1
`

	// reclaimZScript 可见性回收：processing 中认领时间早于 deadline 的成员挪回 ready
	// （score 重置为当前时间，attempt 不计数——崩溃恢复与业务失败重投是两条路径）。
	// KEYS[1]=processing, KEYS[2]=ready;
	// ARGV[1]=deadline(now-timeout, ms), ARGV[2]=now(ms), ARGV[3]=单批上限。
	reclaimZScript = `
local stale = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, ARGV[3])
for i, m in ipairs(stale) do
  redis.call("ZREM", KEYS[1], m)
  redis.call("ZADD", KEYS[2], ARGV[2], m)
end
return #stale
`
)
