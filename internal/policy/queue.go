package policy

import (
	"ollama-gateway/internal/model"
)

// PriorityQueue 实现了 container/heap 接口
type PriorityQueue []*model.Job

// Len 返回队列长度
func (pq *PriorityQueue) Len() int {
	return len(*pq)
}

// Less 定义排序逻辑：优先级数值越小越高；同优先级先来后到
func (pq *PriorityQueue) Less(i, j int) bool {
	if (*pq)[i].Priority != (*pq)[j].Priority {
		return (*pq)[i].Priority < (*pq)[j].Priority
	}
	return (*pq)[i].AddedAt.Before((*pq)[j].AddedAt)
}

// Swap 交换元素位置并更新索引
func (pq *PriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
	(*pq)[i].Index = i
	(*pq)[j].Index = j
}

// Push 入堆
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*model.Job)
	item.Index = n
	*pq = append(*pq, item)
}

// Pop 出堆
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.Index = -1
	*pq = old[0 : n-1]
	return item
}

// jobHigher 返回 a 是否比 b 优先级更高（数值更小=更高；同优先级按 FIFO）。
// 供 global 模式跨模型比较队头使用。
func jobHigher(a, b *model.Job) bool {
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	return a.AddedAt.Before(b.AddedAt)
}
