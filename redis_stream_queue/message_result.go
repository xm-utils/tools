package redis_stream

// ==================== 消息处理结果 ====================

// MessageResult 消息处理结果（通用响应结构）
// 结合 retry.Result 的设计理念，提供统一的消息处理返回格式
type MessageResult struct {
	Success bool        `json:"success"` // 是否成功
	Data    interface{} `json:"data"`    // 返回数据
	Error   string      `json:"error"`   // 错误信息
}

// NewSuccessResult 创建成功结果
func NewSuccessResult(data interface{}) *MessageResult {
	return &MessageResult{
		Success: true,
		Data:    data,
	}
}

// NewErrorResult 创建错误结果
func NewErrorResult(err error) *MessageResult {
	return &MessageResult{
		Success: false,
		Error:   err.Error(),
	}
}

// NewErrorResultWithMsg 创建错误结果（带错误消息）
func NewErrorResultWithMsg(errMsg string) *MessageResult {
	return &MessageResult{
		Success: false,
		Error:   errMsg,
	}
}

// ToRetryResult 转换为 retry.Result 格式
func (r *MessageResult) ToRetryResult() interface{} {
	return r.Data
}

// IsSuccess 判断是否成功
func (r *MessageResult) IsSuccess() bool {
	return r != nil && r.Success
}

// GetError 获取错误信息
func (r *MessageResult) GetError() string {
	if r == nil {
		return ""
	}
	return r.Error
}

// GetData 获取返回数据
func (r *MessageResult) GetData() interface{} {
	if r == nil {
		return nil
	}
	return r.Data
}
