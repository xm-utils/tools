package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

// StreamMessage 表示流中的消息
type StreamMessage struct {
	ID     string                 `json:"id"`
	Values map[string]interface{} `json:"values"`
}

// StreamConsumer 表示消费者信息
type StreamConsumer struct {
	Name    string        `json:"name"`
	Pending int64         `json:"pending"`
	Idle    time.Duration `json:"idle"`
}

// StreamGroup 表示消费者组信息
type StreamGroup struct {
	Name      string `json:"name"`
	Consumers int64  `json:"consumers"`
	Pending   int64  `json:"pending"`
	LastID    string `json:"last_id"`
}

// XAddArgs XAdd 命令的参数
type XAddArgs struct {
	Stream string      // 流名称
	MaxLen int64       // 最大长度（用于修剪）
	MinID  string      // 最小ID（用于修剪）
	Limit  int64       // 限制数量
	ID     string      // 消息ID，空则自动生成
	Values interface{} // 消息内容
}

// XReadArgs XRead 命令的参数
type XReadArgs struct {
	Streams []string      // 流名称列表
	IDs     []string      // ID列表，对应每个流的起始ID
	Count   int64         // 返回的最大消息数
	Block   time.Duration // 阻塞时间
}

// XReadGroupArgs XReadGroup 命令的参数
type XReadGroupArgs struct {
	Group    string        // 消费者组名称
	Consumer string        // 消费者名称
	Streams  []string      // 流名称列表
	IDs      []string      // ID列表，对应每个流的起始ID
	Count    int64         // 返回的最大消息数
	Block    time.Duration // 阻塞时间
	NoAck    bool          // 是否不自动确认
}

// XPendingArgs XPending 命令的参数
type XPendingArgs struct {
	Stream   string // 流名称
	Group    string // 消费者组名称
	Start    string // 起始ID
	End      string // 结束ID
	Count    int64  // 返回的最大消息数
	Consumer string // 可选的消费者名称
}

// XAutoClaimArgs XAutoClaim 命令的参数
type XAutoClaimArgs struct {
	Stream   string        // 流名称
	Group    string        // 消费者组名称
	Consumer string        // 消费者名称
	MinIdle  time.Duration // 最小空闲时间
	Start    string        // 起始ID
	Count    int64         // 返回的最大消息数
}

type XPendingExt struct {
	redis.XPendingExt
}
type XStreamInfo struct {
	*redis.XInfoStream
}

// XAdd 向流中添加消息
func XAdd(ctx context.Context, args *XAddArgs) (string, error) {
	// 将值转换为map[string]interface{}格式
	var valueMap map[string]interface{}
	if m, ok := args.Values.(map[string]interface{}); ok {
		valueMap = m
	} else if m, ok := args.Values.(map[string]string); ok {
		valueMap = make(map[string]interface{})
		for k, v := range m {
			valueMap[k] = v
		}
	} else {
		// 如果不是map类型，尝试编码后作为单个值处理
		values, err := encode(args.Values)
		if err != nil {
			return "", err
		}
		valueMap = map[string]interface{}{
			"data": values,
		}
	}

	cmd := client.XAdd(ctx, &redis.XAddArgs{
		Stream: associate(args.Stream),
		MaxLen: args.MaxLen,
		MinID:  args.MinID,
		Limit:  args.Limit,
		ID:     args.ID,
		Values: valueMap,
	})

	if cmd.Err() != nil {
		return "", cmd.Err()
	}

	return cmd.Val(), nil
}

// XDel 从流中删除消息
func XDel(ctx context.Context, stream string, ids ...string) (int64, error) {
	cmd := client.XDel(ctx, associate(stream), ids...)
	return cmd.Val(), cmd.Err()
}

// XLen 获取流的长度
func XLen(ctx context.Context, stream string) (int64, error) {
	cmd := client.XLen(ctx, associate(stream))
	return cmd.Val(), cmd.Err()
}

// XRange 获取指定范围内的消息
func XRange(ctx context.Context, stream, start, stop string, count int64) ([]StreamMessage, error) {
	cmd := client.XRangeN(ctx, associate(stream), start, stop, count)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	return convertToStreamMessages(cmd.Val()), nil
}

// XRevRange 反向获取指定范围内的消息
func XRevRange(ctx context.Context, stream, start, stop string, count int64) ([]StreamMessage, error) {
	cmd := client.XRevRangeN(ctx, associate(stream), start, stop, count)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	return convertToStreamMessages(cmd.Val()), nil
}

// XRead 读取流中的消息
func XRead(ctx context.Context, args *XReadArgs) (map[string][]StreamMessage, error) {
	streams := make([]string, len(args.Streams))
	for i, s := range args.Streams {
		streams[i] = associate(s)
	}

	cmd := client.XRead(ctx, &redis.XReadArgs{
		Streams: append(streams, args.IDs...),
		Count:   args.Count,
		Block:   args.Block,
	})

	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	result := make(map[string][]StreamMessage)
	for _, stream := range cmd.Val() {
		messages := make([]StreamMessage, len(stream.Messages))
		for i, msg := range stream.Messages {
			valuesMap := make(map[string]interface{})
			for k, v := range msg.Values {
				valuesMap[k] = v
			}
			messages[i] = StreamMessage{
				ID:     msg.ID,
				Values: valuesMap,
			}
		}
		result[stream.Stream] = messages
	}

	return result, nil
}

// XTrim 修剪流
func XTrim(ctx context.Context, stream string, maxLen int64) (int64, error) {
	cmd := client.XTrim(ctx, associate(stream), maxLen)
	return cmd.Val(), cmd.Err()
}

// XTrimApprox 近似修剪流
func XTrimApprox(ctx context.Context, stream string, maxLen int64) (int64, error) {
	cmd := client.XTrimApprox(ctx, associate(stream), maxLen)
	return cmd.Val(), cmd.Err()
}

// XGroupCreate 创建消费者组
func XGroupCreate(ctx context.Context, stream, group, id string) error {
	cmd := client.XGroupCreate(ctx, associate(stream), group, id)
	return cmd.Err()
}
func XGroupCreateMkStream(ctx context.Context, stream, group, id string) error {
	cmd := client.XGroupCreateMkStream(ctx, associate(stream), group, id)
	return cmd.Err()
}

// XGroupDestroy 销毁消费者组
func XGroupDestroy(ctx context.Context, stream, group string) (int64, error) {
	cmd := client.XGroupDestroy(ctx, associate(stream), group)
	return cmd.Val(), cmd.Err()
}

// XGroupDelConsumer 删除消费者组中的消费者
func XGroupDelConsumer(ctx context.Context, stream, group, consumer string) (int64, error) {
	cmd := client.XGroupDelConsumer(ctx, associate(stream), group, consumer)
	return cmd.Val(), cmd.Err()
}

// XGroupSetID 设置消费者组的ID
func XGroupSetID(ctx context.Context, stream, group, id string) error {
	cmd := client.XGroupSetID(ctx, associate(stream), group, id)
	return cmd.Err()
}

// XReadGroup 从消费者组中读取消息
func XReadGroup(ctx context.Context, args *XReadGroupArgs) (map[string][]StreamMessage, error) {
	streams := make([]string, len(args.Streams))
	for i, s := range args.Streams {
		streams[i] = associate(s)
	}

	readArgs := &redis.XReadGroupArgs{
		Group:    args.Group,
		Consumer: args.Consumer,
		Streams:  append(streams, args.IDs...),
		Count:    args.Count,
		Block:    args.Block,
		NoAck:    args.NoAck,
	}

	cmd := client.XReadGroup(ctx, readArgs)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	result := make(map[string][]StreamMessage)
	for _, stream := range cmd.Val() {
		messages := make([]StreamMessage, len(stream.Messages))
		for i, msg := range stream.Messages {
			valuesMap := make(map[string]interface{})
			for k, v := range msg.Values {
				valuesMap[k] = v
			}
			messages[i] = StreamMessage{
				ID:     msg.ID,
				Values: valuesMap,
			}
		}
		result[stream.Stream] = messages
	}

	return result, nil
}

// XAck 确认消息
func XAck(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	cmd := client.XAck(ctx, associate(stream), group, ids...)
	return cmd.Val(), cmd.Err()
}

// XPending 查看待处理的消息
func XPending(ctx context.Context, args *XPendingArgs) ([]XPendingExt, error) {
	cmd := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   associate(args.Stream),
		Group:    args.Group,
		Start:    args.Start,
		End:      args.End,
		Count:    args.Count,
		Consumer: args.Consumer,
	})

	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	exts := make([]XPendingExt, len(cmd.Val()))
	for i, ext := range cmd.Val() {
		exts[i] = XPendingExt{
			XPendingExt: ext,
		}
	}
	return exts, nil
}

// XClaim 认领消息
func XClaim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, ids ...string) ([]StreamMessage, error) {
	cmd := client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   associate(stream),
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Messages: ids,
	})

	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	return convertToStreamMessages(cmd.Val()), nil
}

// XAutoClaim 自动认领消息
func XAutoClaim(ctx context.Context, args *XAutoClaimArgs) (string, []StreamMessage, error) {
	cmd := client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   associate(args.Stream),
		Group:    args.Group,
		Consumer: args.Consumer,
		MinIdle:  args.MinIdle,
		Start:    args.Start,
		Count:    args.Count,
	})

	if cmd.Err() != nil {
		return "", nil, cmd.Err()
	}

	// XAutoClaim 返回 (messages []XMessage, nextID string)
	messages, nextID := cmd.Val()
	streamMessages := convertToStreamMessages(messages)
	return nextID, streamMessages, nil
}

// XInfoGroups 获取消费者组信息
func XInfoGroups(ctx context.Context, stream string) ([]StreamGroup, error) {
	cmd := client.XInfoGroups(ctx, associate(stream))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	groups := make([]StreamGroup, len(cmd.Val()))
	for i, g := range cmd.Val() {
		groups[i] = StreamGroup{
			Name:      g.Name,
			Consumers: g.Consumers,
			Pending:   g.Pending,
			LastID:    g.LastDeliveredID,
		}
	}

	return groups, nil
}

// XInfoConsumers 获取消费者信息
func XInfoConsumers(ctx context.Context, stream, group string) ([]StreamConsumer, error) {
	cmd := client.XInfoConsumers(ctx, associate(stream), group)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	consumers := make([]StreamConsumer, len(cmd.Val()))
	for i, c := range cmd.Val() {
		consumers[i] = StreamConsumer{
			Name:    c.Name,
			Pending: c.Pending,
			Idle:    time.Duration(c.Idle) * time.Millisecond,
		}
	}

	return consumers, nil
}

// XInfoStream 获取流信息
func XInfoStream(ctx context.Context, stream string) (*XStreamInfo, error) {
	cmd := client.XInfoStream(ctx, associate(stream))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	return &XStreamInfo{
		cmd.Val(),
	}, nil
}

// convertToStreamMessages 将redis.XMessage切片转换为StreamMessage切片
func convertToStreamMessages(messages []redis.XMessage) []StreamMessage {
	result := make([]StreamMessage, len(messages))
	for i, msg := range messages {
		valuesMap := make(map[string]interface{})
		for k, v := range msg.Values {
			valuesMap[k] = v
		}
		result[i] = StreamMessage{
			ID:     msg.ID,
			Values: valuesMap,
		}
	}
	return result
}
