// Package redisstore 提供 ratelimit 的 Redis 后端(ROADMAP 3.2):
// 令牌扣减经 Lua 脚本在服务端原子执行,多实例共享同一份额额,消除本地
// Local 后端"每进程各算各的"放大问题。
//
// 复用仓库 redis 组件的方式:连接管理沿用其 InitRedisCache(配置/连接池/健康
// 检查归组件),本包接收其管理的 *redis.Client 直接执行脚本——组件全局 Eval
// 存在 KEYS 双前缀且 client 未导出,故不走全局函数。
//
// 集群友好:状态收敛在单 Hash Key 内并以 {tag} 包裹,Redis Cluster 下
// EVALSHA/脚本内命令全部命中同一 slot。
package redisstore

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/xm-utils/tools/ratelimit"
)

var (
	// ErrInvalidConfig 配置非法,构造时返回(与 ratelimit 根包哨兵语义一致)
	ErrInvalidConfig = ratelimit.ErrInvalidConfig
	// ErrClosed 限流器已关闭
	ErrClosed = ratelimit.ErrClosed
)

// nanoScale 令牌定点放大倍数:Lua 5.1 数字精度依赖 double,往返传输统一按 1e9 放大为整数微令牌,规避分数解析歧义
const nanoScale = 1e9

// maxJudgementN 单次判定可承受的令牌数上限:n×nanoScale 需落在 int64 内,
// 超出后定点乘法溢出,判定前自检拦截并报 ErrInvalidConfig。
// 取 int 型便于与入参 n 直接比较;32 位平台 int 容纳不下会在编译期报错
const maxJudgementN = int(math.MaxInt64 / int64(nanoScale))

// bucketScript 令牌桶原子判定脚本。
// KEYS[1]=桶 Hash;ARGV: 1=速率(纳令牌/秒) 2=容量(纳令牌) 3=消耗(纳令牌)
// 4=当前时间(微秒) 5=状态 TTL(毫秒)。状态字段:last(微秒)/tokens(浮点,纳令牌)。
// 返回 {放行0/1, 判定后剩余(纳令牌), 距下一可用微令牌等待(微秒)}。
// 时钟回拨(now<last)按 elapsed=0 处理,只重置基准不倒扣令牌。
const bucketScript = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local need = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])

local state = redis.call('HMGET', key, 'last', 'tokens')
local last = tonumber(state[1])
local tokens = tonumber(state[2])

if last == nil or tokens == nil then
  last = now
  tokens = burst
end

local elapsed = now - last
if elapsed < 0 then
  elapsed = 0
end
tokens = math.min(burst, tokens + elapsed / 1000000 * rate)

local allowed = 0
local wait = 0
if tokens >= need then
  allowed = 1
  tokens = tokens - need
else
  local deficit = need - tokens
  wait = math.ceil(deficit / rate * 1000000)
  -- 被拒时等待上取整到至少 1 微秒,避免调用方拿到 0 误判可立即重试
  if wait <= 0 then
    wait = 1
  end
end

redis.call('HSET', key, 'last', now, 'tokens', tokens)
redis.call('PEXPIRE', key, ttl_ms)

local remaining = math.floor(tokens)
return {allowed, remaining, wait}
`

// bucket 脚本单例:Run 乐观走 EVALSHA,NOSCRIPT 时自动回落 EVAL(v8 未导出
// NoScriptError,不手写回落判定)
var bucket = redis.NewScript(bucketScript)

// Config Redis 令牌桶配置
type Config struct {
	// Client 由 redis 组件 InitRedisCache 建立(或同等来源)的客户端,必填
	Client *redis.Client
	// Key 限流维度标识,如 "api:order:create";实际存储 Key 为
	// xm_ratelimit:{<Key>}——花括号 tag 保证 Cluster 下单 slot
	Key string
	// Rate 回填速率(个/秒),必须 > 0
	Rate float64
	// Burst 桶容量,必须 > 0
	Burst float64
	// TTL 空闲状态过期时间,桶长期无请求时自动清理;<=0 时取默认 10 分钟
	TTL time.Duration
	// Clock 时间源,nil 时使用 SystemClock;测试可注入假时钟(经参数入脚本,
	// 不依赖服务端 TIME,规避脚本非确定性限制)
	Clock ratelimit.Clock
}

// Limiter Redis 后端令牌桶限流器,实现 ratelimit.Limiter。
// 无本地状态,每次判定执行一段原子脚本;多实例并发安全由服务端保证。
type Limiter struct {
	client    *redis.Client
	key       string
	rateNano  float64 // 纳令牌/秒
	burstNano float64 // 纳令牌(截断取整)
	ttlMs     int64
	clock     ratelimit.Clock

	mu     sync.Mutex
	closed bool
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New 创建 Redis 令牌桶;配置非法返回包装 ErrInvalidConfig 的 error,不 panic
func New(cfg Config) (*Limiter, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("%w: Client 不能为 nil", ErrInvalidConfig)
	}
	if cfg.Key == "" {
		return nil, fmt.Errorf("%w: Key 不能为空", ErrInvalidConfig)
	}
	if cfg.Rate <= 0 || math.IsNaN(cfg.Rate) || math.IsInf(cfg.Rate, 0) {
		return nil, fmt.Errorf("%w: Rate 必须为正数, 当前 %v", ErrInvalidConfig, cfg.Rate)
	}
	if cfg.Burst <= 0 || math.IsNaN(cfg.Burst) || math.IsInf(cfg.Burst, 0) {
		return nil, fmt.Errorf("%w: Burst 必须为正数, 当前 %v", ErrInvalidConfig, cfg.Burst)
	}
	if cfg.Clock == nil {
		cfg.Clock = ratelimit.SystemClock{}
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}

	return &Limiter{
		client:    cfg.Client,
		key:       "xm_ratelimit:{" + cfg.Key + "}",
		rateNano:  cfg.Rate * nanoScale,
		burstNano: math.Floor(cfg.Burst * nanoScale),
		ttlMs:     ttl.Milliseconds(),
		clock:     cfg.Clock,
	}, nil
}

// Allow 判定单次请求是否放行;被拒时返回包装 ErrRateLimited 的 error(含等待时长)
func (l *Limiter) Allow(ctx context.Context) error {
	return l.allowN(ctx, 1)
}

// AllowN 判定一次消耗 n 个令牌;n <= 0 或超出定点上限(约 9.2e9)时
// 返回包装 ErrInvalidConfig 的 error;
// 被拒时仅返回 (false, nil),与根包 Local.AllowN 语义一致
func (l *Limiter) AllowN(ctx context.Context, n int) (bool, error) {
	if n <= 0 {
		return false, fmt.Errorf("%w: n 必须为正数, 当前 %d", ErrInvalidConfig, n)
	}
	if n > maxJudgementN {
		return false, fmt.Errorf("%w: n=%d 超出单次判定上限 %d, 请拆批消耗",
			ErrInvalidConfig, n, maxJudgementN)
	}
	res, err := l.judge(ctx, n)
	if err != nil {
		return false, err
	}
	return res.OK, nil
}

// allowN 供 Allow 使用:被拒时把判定同批返回的 RetryAfter 组装进错误
func (l *Limiter) allowN(ctx context.Context, n int) error {
	res, err := l.judge(ctx, n)
	if err != nil {
		return err
	}
	if res.OK {
		return nil
	}
	return fmt.Errorf("%w: retry after %s", ratelimit.ErrRateLimited, res.RetryAfter)
}

// Peek 只读计算当前可用令牌(不下发脚本、不消耗),用于观测与预估
func (l *Limiter) Peek() ratelimit.Result {
	l.mu.Lock()
	burst := l.burstNano
	l.mu.Unlock()

	fields, err := l.client.HGetAll(context.Background(), l.key).Result()
	if err != nil {
		return ratelimit.Result{}
	}
	last, _ := strconv.ParseFloat(fields["last"], 64)
	tokens, _ := strconv.ParseFloat(fields["tokens"], 64)
	if fields["last"] == "" || fields["tokens"] == "" {
		// 桶未初始化:视为满桶(与脚本首判行为一致)
		return ratelimit.Result{OK: burst >= nanoScale, Limit: burst / nanoScale, Remaining: burst / nanoScale}
	}
	elapsedUs := float64(l.clock.Now().UnixMicro()) - last
	if elapsedUs < 0 {
		elapsedUs = 0
	}
	rate := l.rateNano
	refilled := math.Min(burst, tokens+elapsedUs/1_000_000*rate)
	return ratelimit.Result{
		OK:         refilled >= nanoScale,
		RetryAfter: waitDuration(refilled, nanoScale, rate),
		Limit:      rate / nanoScale,
		Remaining:  refilled / nanoScale,
	}
}

// Close 标记限流器关闭(仅阻断本实例后续判定,不影响 Redis 中共享状态),幂等
func (l *Limiter) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	return nil
}

// judge 执行原子脚本判定消耗 n 个令牌,返回与判定同批的完整 Result——
// RetryAfter 取自脚本返回值,不另发 Peek,避免跨调用间隙回填导致的误判
func (l *Limiter) judge(ctx context.Context, n int) (ratelimit.Result, error) {
	if err := ctx.Err(); err != nil {
		return ratelimit.Result{}, err
	}
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return ratelimit.Result{}, ErrClosed
	}

	nanoNeed := int64(n) * int64(nanoScale)
	nowUs := l.clock.Now().UnixMicro()
	arr, err := l.eval(ctx, nanoNeed, nowUs)
	if err != nil {
		return ratelimit.Result{}, fmt.Errorf("ratelimit/redisstore: 执行限流脚本失败: %w", err)
	}
	if len(arr) < 3 {
		return ratelimit.Result{}, fmt.Errorf("ratelimit/redisstore: 脚本返回值格式异常: %#v", arr)
	}
	allowed, err := toInt64(arr[0])
	if err != nil {
		return ratelimit.Result{}, err
	}
	remaining, err := toInt64(arr[1])
	if err != nil {
		return ratelimit.Result{}, err
	}
	waitUs, err := toInt64(arr[2])
	if err != nil {
		return ratelimit.Result{}, err
	}
	return ratelimit.Result{
		OK:         allowed == 1,
		RetryAfter: time.Duration(waitUs) * time.Microsecond,
		Limit:      l.rateNano / nanoScale,
		Remaining:  float64(remaining) / nanoScale,
	}, nil
}

// eval 执行限流脚本并取回三元数组返回值
func (l *Limiter) eval(ctx context.Context, nanoNeed, nowUs int64) ([]interface{}, error) {
	args := []interface{}{
		strconv.FormatFloat(l.rateNano, 'f', -1, 64),
		strconv.FormatFloat(l.burstNano, 'f', -1, 64),
		nanoNeed,
		nowUs,
		l.ttlMs,
	}
	return bucket.Run(ctx, l.client, []string{l.key}, args...).Slice()
}

func toInt64(v interface{}) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	default:
		return 0, fmt.Errorf("ratelimit/redisstore: 脚本返回元素非整数: %#v", v)
	}
}

// waitDuration 按 deficit/rate 计算等待时长(向上取整,至少 1µs)
func waitDuration(tokens, need, rateNanoPerSec float64) time.Duration {
	deficit := need - tokens
	if deficit <= 0 {
		return 0
	}
	us := math.Ceil(deficit / rateNanoPerSec * 1_000_000)
	return time.Duration(us) * time.Microsecond
}
