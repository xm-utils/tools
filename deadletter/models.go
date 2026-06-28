package deadletter

import (
	"time"
)

// QueueStatus 死信队列消息状态
type QueueStatus int8

const (
	DLQStatusPending    QueueStatus = 1 // 待处理
	DLQStatusProcessing QueueStatus = 2 // 处理中
	DLQStatusProcessed  QueueStatus = 3 // 已处理
	DLQStatusAbandoned  QueueStatus = 4 // 已放弃
)

// QueueMsgRecord 死信队列表模型
type QueueMsgRecord struct {
	ID            uint64      `json:"id" comment:"主键ID"`
	QueueKey      string      `json:"queueKey" comment:"队列Key标识"`
	MessageID     string      `json:"messageId" comment:"消息唯一ID"`
	MessageData   string      `json:"messageData" comment:"消息内容(JSON格式)"`
	ErrorMessage  string      `json:"errorMessage" comment:"失败原因"`
	RetryCount    int         `json:"retryCount" comment:"重试次数"`
	MaxRetry      int         `json:"maxRetry" comment:"最大重试次数"`
	Status        QueueStatus `json:"status" comment:"状态: 1-待处理, 2-处理中, 3-已处理, 4-已放弃"`
	Operator      string      `json:"operator" comment:"操作人"`
	OperatorId    uint64      `json:"operatorId" comment:"操作人ID"`
	NextRetryTime *time.Time  `json:"nextRetryTime" comment:"下次重试时间"`
	LastErrorTime *time.Time  `json:"lastErrorTime" comment:"最后错误时间"`
	ProcessedTime *time.Time  `json:"processedTime" comment:"处理完成时间"`
	CreatedTime   time.Time   `json:"createdTime" comment:"创建时间"`
	UpdatedTime   time.Time   `json:"updatedTime" comment:"更新时间"`
}
