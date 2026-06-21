package policy

import (
	"container/heap"

	"ollama-gateway/internal/model"
)

// GlobalGreedyPolicy 全局贪心：吞吐量优先。
// 每个模型一条独立优先级队列，避免「队头是忙碌模型的任务挡住其它空闲模型」的队头阻塞。
// 同一模型内部仍是 优先级 + FIFO（见 PriorityQueue.Less）。
type GlobalGreedyPolicy struct {
	// queues 按模型名分桶：模型名 -> 该模型的优先级队列
	queues map[string]*PriorityQueue
	// jobMap 用于 O(1) 按 ID 定位任务（删除、查询队列位置）
	jobMap map[string]*model.Job
}

// NewGlobalGreedyPolicy 初始化
func NewGlobalGreedyPolicy() *GlobalGreedyPolicy {
	return &GlobalGreedyPolicy{
		queues: make(map[string]*PriorityQueue),
		jobMap: make(map[string]*model.Job),
	}
}

func (p *GlobalGreedyPolicy) Name() string { return "Global-Greedy" }

// Len 返回所有模型队列里的任务总数
func (p *GlobalGreedyPolicy) Len() int { return len(p.jobMap) }

func (p *GlobalGreedyPolicy) Push(job *model.Job) {
	q, ok := p.queues[job.Model]
	if !ok {
		pq := make(PriorityQueue, 0)
		heap.Init(&pq)
		q = &pq
		p.queues[job.Model] = q
	}
	p.jobMap[job.ID] = job
	heap.Push(q, job)
}

// Schedule 为某个空闲 Worker 选出可运行的最高优先级任务。
// 关键点：按 Worker 能支持的模型去各自队列取队头，模型之间互不阻塞；
// 同一 Worker 支持多个模型时，在这些模型的队头里挑全局优先级最高的那个。
func (p *GlobalGreedyPolicy) Schedule(workers []*model.Worker) (*model.Worker, *model.Job) {
	for _, w := range workers {
		if w.Busy {
			continue
		}

		var bestQueue *PriorityQueue
		for modelName, q := range p.queues {
			if q.Len() == 0 || !supportsModel(w, modelName) {
				continue
			}
			// 只看队头：保证同模型内严格的 优先级 + FIFO 顺序。
			if bestQueue == nil || jobHigher((*q)[0], (*bestQueue)[0]) {
				bestQueue = q
			}
		}

		if bestQueue != nil {
			job := heap.Pop(bestQueue).(*model.Job)
			delete(p.jobMap, job.ID)
			return w, job
		}
	}

	return nil, nil
}

// PositionOf 返回任务在其所属模型队列中的位置（从 1 开始），不存在返回 0
func (p *GlobalGreedyPolicy) PositionOf(id string) int {
	job, ok := p.jobMap[id]
	if !ok || job.Index == -1 {
		return 0
	}
	q, ok := p.queues[job.Model]
	if !ok {
		return 0
	}
	pos := 0
	for i := 0; i < q.Len(); i++ {
		item := (*q)[i]
		if item.Priority < job.Priority ||
			(item.Priority == job.Priority && item.AddedAt.Before(job.AddedAt)) {
			pos++
		}
	}
	return pos + 1
}

func (p *GlobalGreedyPolicy) RemoveByID(id string) *model.Job {
	job, ok := p.jobMap[id]
	if !ok {
		return nil
	}
	if q, ok := p.queues[job.Model]; ok && job.Index != -1 {
		heap.Remove(q, job.Index)
	}
	delete(p.jobMap, id)
	return job
}

func (p *GlobalGreedyPolicy) RemoveByIDs(ids []string) []*model.Job {
	var removed []*model.Job
	for _, id := range ids {
		if job := p.RemoveByID(id); job != nil {
			removed = append(removed, job)
		}
	}
	return removed
}
