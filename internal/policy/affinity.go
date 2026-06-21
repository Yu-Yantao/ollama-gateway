package policy

import (
	"container/heap"

	"ollama-gateway/internal/model"
)

// sharedOwner 是公共（业务无关）资源的 owner 取值。
const sharedOwner = "shared"

// queueKey 唯一标识一条队列：归属 + 模型。
type queueKey struct {
	owner string
	model string
}

// AffinityStealingPolicy 亲和性窃取调度。
//
// 队列按 (归属, 模型) 划分，并在启动时根据 worker 配置预先创建：
//   - 公共模型（有 owner=shared 的 worker）：所有人共用一条 (shared, 模型) 队列；
//   - 业务专属模型（owner 为某业务线）：每个业务线一条 (业务线, 模型) 队列。
//
// 调度遵循「自家优先，空闲窃取」；亲和模式不区分 VIP/Batch，队列内按 FIFO（入队时间）。
type AffinityStealingPolicy struct {
	queues map[queueKey]*PriorityQueue
	jobMap map[string]*model.Job

	// 以下为不可变拓扑（构造后只读），用于入队路由与准入判断，读取无需加锁。
	publicModels map[string]bool            // 公共模型集合（有 shared worker）
	ownerModels  map[string]map[string]bool // 业务线 -> 它拥有 worker 的模型集合
}

// NewAffinityStealingPolicy 根据 worker 配置构建拓扑并预创建队列。
func NewAffinityStealingPolicy(workers []*model.Worker) *AffinityStealingPolicy {
	p := &AffinityStealingPolicy{
		queues:       make(map[queueKey]*PriorityQueue),
		jobMap:       make(map[string]*model.Job),
		publicModels: make(map[string]bool),
		ownerModels:  make(map[string]map[string]bool),
	}

	for _, w := range workers {
		for _, m := range w.SupportedModels {
			// 预创建队列
			k := queueKey{owner: w.Owner, model: m}
			if _, ok := p.queues[k]; !ok {
				pq := make(PriorityQueue, 0)
				heap.Init(&pq)
				p.queues[k] = &pq
			}
			// 记录拓扑
			if w.Owner == sharedOwner {
				p.publicModels[m] = true
			} else {
				if p.ownerModels[w.Owner] == nil {
					p.ownerModels[w.Owner] = make(map[string]bool)
				}
				p.ownerModels[w.Owner][m] = true
			}
		}
	}

	return p
}

func (p *AffinityStealingPolicy) Name() string { return "Affinity-Stealing" }

func (p *AffinityStealingPolicy) Len() int { return len(p.jobMap) }

// route 计算任务应进入的队列 key；第二个返回值表示是否可接收。
//
//  1. 发起者拥有该模型的 worker            -> (发起者, 模型)
//  2. 否则该模型是公共模型                 -> (shared, 模型)
//  3. 否则不可接收（模型不存在，或为他人的业务专属模型）
func (p *AffinityStealingPolicy) route(requester, modelName string) (queueKey, bool) {
	if models, ok := p.ownerModels[requester]; ok && models[modelName] {
		return queueKey{owner: requester, model: modelName}, true
	}
	if p.publicModels[modelName] {
		return queueKey{owner: sharedOwner, model: modelName}, true
	}
	return queueKey{}, false
}

// CanRoute 供准入阶段判断 (用户, 模型) 是否可被接收。只读不可变拓扑，无需加锁。
func (p *AffinityStealingPolicy) CanRoute(requester, modelName string) bool {
	_, ok := p.route(requester, modelName)
	return ok
}

func (p *AffinityStealingPolicy) Push(job *model.Job) {
	k, ok := p.route(job.Requester, job.Model)
	if !ok {
		// 正常情况下 handler 已在准入阶段拦截；这里兜底，避免无队列可入。
		return
	}
	// 亲和模式不区分优先级：统一优先级，使优先级队列退化为按入队时间的 FIFO。
	job.Priority = model.PriorityBatch
	p.jobMap[job.ID] = job
	heap.Push(p.queues[k], job)
}

// Schedule 两步：先让每个空闲 worker 清自己的队列，全都没有自家任务时才进入窃取。
func (p *AffinityStealingPolicy) Schedule(workers []*model.Worker) (*model.Worker, *model.Job) {
	// 步骤 1：自家优先。shared worker 的「自家」就是公共队列。
	for _, w := range workers {
		if w.Busy {
			continue
		}
		if q := p.bestOwnQueue(w); q != nil {
			return w, p.pop(q)
		}
	}

	// 步骤 2：窃取。自家都空了，才去允许窃取的队列里捞等待最久的任务。
	for _, w := range workers {
		if w.Busy {
			continue
		}
		if q := p.bestStealQueue(w); q != nil {
			return w, p.pop(q)
		}
	}

	return nil, nil
}

func (p *AffinityStealingPolicy) pop(q *PriorityQueue) *model.Job {
	job := heap.Pop(q).(*model.Job)
	delete(p.jobMap, job.ID)
	return job
}

// bestOwnQueue 在 worker 自家 (owner, 它支持的模型) 队列中，选队头等待最久的一条。
func (p *AffinityStealingPolicy) bestOwnQueue(w *model.Worker) *PriorityQueue {
	var best *PriorityQueue
	for _, m := range w.SupportedModels {
		q := p.queues[queueKey{owner: w.Owner, model: m}]
		if q == nil || q.Len() == 0 {
			continue
		}
		if best == nil || olderHead(q, best) {
			best = q
		}
	}
	return best
}

// bestStealQueue 在 worker 允许窃取的他人队列中，选队头等待最久的一条。
func (p *AffinityStealingPolicy) bestStealQueue(w *model.Worker) *PriorityQueue {
	var best *PriorityQueue
	for k, q := range p.queues {
		if q.Len() == 0 || !p.canSteal(w, k) {
			continue
		}
		if best == nil || olderHead(q, best) {
			best = q
		}
	}
	return best
}

// canSteal 判定 worker w 是否允许从队列 k 窃取（k 必须不是 w 的自家队列）。
//
//	公共 worker  -> 可帮业务线（k 必为业务队列）
//	业务 worker  -> 可偷其他业务线的同模型队列；默认不帮公共池
//
// 注：业务 -> 公共 这条边目前硬编码为关闭；将来要开放可加每 worker 开关。
func (p *AffinityStealingPolicy) canSteal(w *model.Worker, k queueKey) bool {
	if k.owner == w.Owner {
		return false // 自家队列，不算窃取
	}
	if !supportsModel(w, k.model) {
		return false // 这个 worker 跑不了该模型
	}
	if w.Owner == sharedOwner {
		return true // 公共 worker 帮业务线
	}
	if k.owner == sharedOwner {
		return false // 业务 worker 默认不帮公共池
	}
	return true // 业务线之间互相窃取
}

// olderHead 返回 a 的队头是否比 b 的队头等待更久（FIFO 比较，两条队列均非空）。
func olderHead(a, b *PriorityQueue) bool {
	return (*a)[0].AddedAt.Before((*b)[0].AddedAt)
}

// PositionOf 返回任务在其所属队列中的位置（从 1 开始），不存在返回 0。
func (p *AffinityStealingPolicy) PositionOf(id string) int {
	job, ok := p.jobMap[id]
	if !ok || job.Index == -1 {
		return 0
	}
	k, ok := p.route(job.Requester, job.Model)
	if !ok {
		return 0
	}
	q := p.queues[k]
	if q == nil {
		return 0
	}
	pos := 0
	for i := 0; i < q.Len(); i++ {
		if (*q)[i].AddedAt.Before(job.AddedAt) {
			pos++
		}
	}
	return pos + 1
}

func (p *AffinityStealingPolicy) RemoveByID(id string) *model.Job {
	job, ok := p.jobMap[id]
	if !ok {
		return nil
	}
	if k, ok := p.route(job.Requester, job.Model); ok {
		if q := p.queues[k]; q != nil && job.Index != -1 {
			heap.Remove(q, job.Index)
		}
	}
	delete(p.jobMap, id)
	return job
}

func (p *AffinityStealingPolicy) RemoveByIDs(ids []string) []*model.Job {
	var removed []*model.Job
	for _, id := range ids {
		if job := p.RemoveByID(id); job != nil {
			removed = append(removed, job)
		}
	}
	return removed
}
