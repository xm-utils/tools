package list

// Lua 脚本清单：Redis 单线程执行保证原子性，各脚本键/参含义见调用点注释。
const (
	// claimScript 领取一条消息：ready 尾端 RPOP → processing 头部 LPUSH。
	// 无可领消息时返回 nil（客户端侧表现为 redis.Nil）。
	claimScript = `
local v = redis.call("RPOP", KEYS[1])
if v then
  redis.call("LPUSH", KEYS[2], v)
end
return v
`

	// requeueScript 原子回队：从 processing 移除原消息，把 attempt+1 的新信封 LPUSH 回 ready。
	// ARGV[1]=原消息 payload，ARGV[2]=重投 payload。
	requeueScript = `
redis.call("LREM", KEYS[1], 1, ARGV[1])
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
`

	// recoverScript 把 processing 残留回补 ready（processing 尾端 → ready 头部，
	// 回补后不保证原始 FIFO 顺序，语义见包注释）。
	// ARGV[1]=单次脚本搬运上限（防超长脚本阻塞 Redis），调用方循环执行直至搬空。
	recoverScript = `
local limit = tonumber(ARGV[1])
local n = 0
while n < limit do
  local v = redis.call("RPOP", KEYS[1])
  if not v then break end
  redis.call("LPUSH", KEYS[2], v)
  n = n + 1
end
return n
`
)
