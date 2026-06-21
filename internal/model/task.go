package model

import "time"

// TaskStatus 任务状态
type TaskStatus string

const (
	// TaskStatusQueued 排队中
	TaskStatusQueued TaskStatus = "queued"
	// TaskStatusRunning 执行中
	TaskStatusRunning TaskStatus = "running"
	// TaskStatusCompleted 已完成
	TaskStatusCompleted TaskStatus = "completed"
	// TaskStatusFailed 失败
	TaskStatusFailed TaskStatus = "failed"
	// TaskStatusCancelled 已取消
	TaskStatusCancelled TaskStatus = "cancelled"
)

const (
	TaskCancelReasonUserCancelled = "user_cancelled"
	TaskCancelReasonCancelled     = "cancelled"
)

// TaskInfo 任务信息
type TaskInfo struct {
	// ID 任务唯一标识
	ID string `json:"id"`
	// Status 任务状态
	Status TaskStatus `json:"status"`
	// Model 模型名称
	Model string `json:"model"`
	// Requester 请求用户
	Requester string `json:"requester"`
	// QueuePosition 队列位置（可选）
	QueuePosition int `json:"queue_position,omitempty"`
	// CancelReason 取消原因
	CancelReason string `json:"cancel_reason,omitempty"`
	// Result 结果数据（完成时可用）
	Result []byte `json:"result,omitempty"`
	// Error 错误信息（失败时可用）
	Error string `json:"error,omitempty"`
	// CreatedAt 创建时间
	CreatedAt time.Time `json:"created_at"`
	// StartedAt 开始执行时间
	StartedAt *time.Time `json:"started_at,omitempty"`
	// CompletedAt 完成时间
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	// CallbackURL Webhook回调URL（可选）
	CallbackURL string `json:"-"`
}
