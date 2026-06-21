package policy

import "ollama-gateway/internal/model"

// SchedulerPolicy 是调度策略的抽象接口
type SchedulerPolicy interface {
	// Name 返回策略名称
	Name() string

	// Push 将任务加入调度池
	Push(job *model.Job)

	// Schedule 根据策略选出最佳 Worker 和 Job
	// workers: 当前所有 Worker 列表
	// 返回: 选中的 Worker 和 Job (无匹配返回 nil)
	Schedule(workers []*model.Worker) (*model.Worker, *model.Job)

	// RemoveByID 根据 ID 移除任务
	RemoveByID(id string) *model.Job

	// RemoveByIDs 批量移除指定ID的任务
	RemoveByIDs(ids []string) []*model.Job

	// Len 返回排队任务总数
	Len() int

	// PositionOf 返回指定任务在队列中的位置（从 1 开始），不存在返回 0
	PositionOf(id string) int
}

func supportsModel(w *model.Worker, modelName string) bool {
	if modelName == "" {
		return true
	}
	for _, m := range w.SupportedModels {
		if m == modelName {
			return true
		}
	}
	return false
}
