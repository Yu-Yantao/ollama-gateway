package gateway

import (
	"context"
	"ollama-gateway/internal/logger"
	"ollama-gateway/internal/model"
	"sync"
	"time"
)

// TaskManager 任务管理器，负责管理异步任务的状态和结果
type TaskManager struct {
	tasks map[string]*model.TaskInfo
	mu    sync.RWMutex
	// 结果保留时间
	ttl time.Duration
	// 清理间隔
	cleanInterval time.Duration
	ctx           context.Context
}

// NewTaskManager 创建新的任务管理器
func NewTaskManager(ctx context.Context, ttl time.Duration, cleanInterval time.Duration) *TaskManager {
	if ttl == 0 {
		// 默认保留1小时
		ttl = 1 * time.Hour
	}
	if cleanInterval == 0 {
		// 默认10分钟清理一次
		cleanInterval = 10 * time.Minute
	}

	tm := &TaskManager{
		tasks:         make(map[string]*model.TaskInfo),
		ttl:           ttl,
		cleanInterval: cleanInterval,
		ctx:           ctx,
	}

	// 启动定期清理协程
	go tm.cleanupExpiredTasks()

	return tm
}

// CreateTask 创建新任务，如果 ID 已存在则返回 false
func (tm *TaskManager) CreateTask(id string, info *model.TaskInfo) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if _, exists := tm.tasks[id]; exists {
		return false
	}
	tm.tasks[id] = info
	return true
}

// GetTask 获取任务信息
func (tm *TaskManager) GetTask(id string) (*model.TaskInfo, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	task, ok := tm.tasks[id]
	return task, ok
}

// UpdateStatus 更新任务状态
func (tm *TaskManager) UpdateStatus(id string, status model.TaskStatus) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if task, ok := tm.tasks[id]; ok {
		task.Status = status
		now := time.Now()

		switch status {
		case model.TaskStatusRunning:
			task.StartedAt = &now
		case model.TaskStatusCompleted, model.TaskStatusFailed, model.TaskStatusCancelled:
			task.CompletedAt = &now
		}
	}
}

// SetResult 设置任务结果
func (tm *TaskManager) SetResult(id string, result []byte, err error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if task, ok := tm.tasks[id]; ok {
		now := time.Now()
		task.CompletedAt = &now

		if err != nil {
			task.Status = model.TaskStatusFailed
			task.Error = err.Error()
		} else {
			task.Status = model.TaskStatusCompleted
			task.Result = result
		}
	}
}

// UpdateQueuePosition 更新队列位置
func (tm *TaskManager) UpdateQueuePosition(id string, position int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if task, ok := tm.tasks[id]; ok {
		task.QueuePosition = position
	}
}

// CancelTask 取消任务并记录原因
func (tm *TaskManager) CancelTask(id string, reason string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if task, ok := tm.tasks[id]; ok {
		if task.Status == model.TaskStatusCancelled && task.CancelReason != "" {
			return
		}
		task.Status = model.TaskStatusCancelled
		task.CancelReason = reason
		now := time.Now()
		task.CompletedAt = &now
	}
}

// DeleteTask 删除任务
func (tm *TaskManager) DeleteTask(id string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.tasks, id)
}

// ListTasks 列出所有任务（可选：按状态过滤）
func (tm *TaskManager) ListTasks(status model.TaskStatus) []*model.TaskInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var tasks []*model.TaskInfo
	for _, task := range tm.tasks {
		if status == "" || task.Status == status {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// GetTaskCount 获取任务数量统计
func (tm *TaskManager) GetTaskCount() map[model.TaskStatus]int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	counts := make(map[model.TaskStatus]int)
	for _, task := range tm.tasks {
		counts[task.Status]++
	}
	return counts
}

// cleanupExpiredTasks 定期触发清理过期的已完成任务
func (tm *TaskManager) cleanupExpiredTasks() {
	ticker := time.NewTicker(tm.cleanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tm.ctx.Done():
			logger.InfoEvent("task_cleanup_stopped")
			return
		case <-ticker.C:
			if cleaned, remaining := tm.cleanupOnce(time.Now()); cleaned > 0 {
				logger.InfoEvent("task_cleanup_completed",
					"cleaned_count", cleaned,
					"remaining_count", remaining,
				)
			}
		}
	}
}

// cleanupOnce 执行一次清理：删除完成时间已超过 TTL 的任务，返回 (清理数, 剩余数)。
// 抽成独立方法便于单元测试（无需依赖 ticker / goroutine）。
func (tm *TaskManager) cleanupOnce(now time.Time) (cleaned, remaining int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for id, task := range tm.tasks {
		if task.CompletedAt != nil && now.Sub(*task.CompletedAt) > tm.ttl {
			delete(tm.tasks, id)
			cleaned++
		}
	}
	return cleaned, len(tm.tasks)
}
