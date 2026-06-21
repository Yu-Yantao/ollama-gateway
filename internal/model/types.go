package model

import (
	"context"
	"time"
)

// 任务优先级常量，数值越小优先级越高
const (
	// PriorityVIP 是 VIP 任务的优先级，最高优先级
	PriorityVIP = 0
	// PriorityBatch 是批量任务的优先级，低优先级
	PriorityBatch = 10
)

// Worker 代表一个 Ollama 计算资源节点
type Worker struct {
	// ID 是节点唯一标识，如 "Ollama-01"
	ID string
	// URL 是节点服务地址，如 "http://192.168.1.10:11434"
	URL string
	// Owner 是资源归属用户，用于亲和性调度
	Owner string
	// SupportedModels 是该节点支持的模型列表
	SupportedModels []string
	// Busy 表示是否正在执行任务
	Busy bool
}

// Job 代表一个待执行的推理任务
type Job struct {
	// ID 是任务唯一标识
	ID string
	// Model 是任务请求的目标模型名称
	Model string
	// Requester 是请求发起用户
	Requester string
	// SourceAddr 是请求来源地址（客户端IP）
	SourceAddr string
	// Priority 是任务优先级，数值越小越优先
	Priority int
	// Payload 是请求体原始数据
	Payload []byte
	// ResultCh 是结果回传通道，调度器完成后写入。
	// 必须使用容量 1 的 buffer：execute 写一次必成功，
	// waitForAsyncResult 不再关闭 channel，由 GC 自然回收，
	// 避免「send on closed channel」竞态。
	ResultCh chan JobResult
	// Ctx 用于取消传播和超时控制
	Ctx context.Context
	// CancelFunc 是取消函数，调用后终止任务
	CancelFunc context.CancelFunc
	// Index 是堆中索引，用于 O(log n) 删除
	Index int
	// AddedAt 是入队时间，同优先级按 FIFO 排序
	AddedAt time.Time
}

// JobResult 定义任务执行结果
type JobResult struct {
	// Data 是响应数据
	Data []byte
	// Err 是错误信息，成功时为 nil
	Err error
}
