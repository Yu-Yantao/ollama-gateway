package policy

import (
	"testing"
	"time"

	"ollama-gateway/internal/model"
)

// t0 是测试用的基准时间，配合偏移量控制 FIFO 顺序。
var t0 = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func mkJob(id, requester, modelName string, priority int, addedOffset time.Duration) *model.Job {
	return &model.Job{
		ID:        id,
		Requester: requester,
		Model:     modelName,
		Priority:  priority,
		AddedAt:   t0.Add(addedOffset),
	}
}

func mkWorker(id, owner string, busy bool, models ...string) *model.Worker {
	return &model.Worker{ID: id, Owner: owner, Busy: busy, SupportedModels: models}
}

// 核心回归用例：队头是忙碌模型的任务，不应阻塞另一个有空闲 worker 的模型。
func TestGlobalNoHeadOfLineBlocking(t *testing.T) {
	p := NewGlobalGreedyPolicy()
	p.Push(mkJob("a1", "", "A", model.PriorityBatch, 0))             // 队头：模型 A
	p.Push(mkJob("b1", "", "B", model.PriorityBatch, 1*time.Second)) // 其后：模型 B

	workers := []*model.Worker{
		mkWorker("wA", "shared", true, "A"),  // A 的 worker 忙
		mkWorker("wB", "shared", false, "B"), // B 的 worker 闲
	}

	w, j := p.Schedule(workers)
	if w == nil || j == nil {
		t.Fatal("模型 B 应可调度，不应被队头的 A 阻塞")
	}
	if w.ID != "wB" || j.ID != "b1" {
		t.Fatalf("期望 (wB, b1)，实际 (%s, %s)", w.ID, j.ID)
	}
}

func TestGlobalVIPBeforeBatchSameModel(t *testing.T) {
	p := NewGlobalGreedyPolicy()
	p.Push(mkJob("batch", "", "A", model.PriorityBatch, 0))         // 先到的 Batch
	p.Push(mkJob("vip", "", "A", model.PriorityVIP, 1*time.Second)) // 后到的 VIP

	w, j := p.Schedule([]*model.Worker{mkWorker("w", "shared", false, "A")})
	if w == nil || j == nil || j.ID != "vip" {
		t.Fatalf("同模型内 VIP 应先于 Batch，得到 %v", j)
	}
}

func TestGlobalFIFOSamePriority(t *testing.T) {
	p := NewGlobalGreedyPolicy()
	p.Push(mkJob("j1", "", "A", model.PriorityBatch, 0))
	p.Push(mkJob("j2", "", "A", model.PriorityBatch, 1*time.Second))

	_, j := p.Schedule([]*model.Worker{mkWorker("w", "shared", false, "A")})
	if j == nil || j.ID != "j1" {
		t.Fatalf("同优先级应 FIFO，先调度 j1，得到 %v", j)
	}
}

func TestGlobalMultiModelWorkerPicksHighest(t *testing.T) {
	p := NewGlobalGreedyPolicy()
	p.Push(mkJob("a", "", "A", model.PriorityBatch, 0))
	p.Push(mkJob("b", "", "B", model.PriorityVIP, 1*time.Second))

	// 一个 worker 同时支持 A、B，应在两个队头里挑优先级最高的（B 的 VIP）。
	_, j := p.Schedule([]*model.Worker{mkWorker("w", "shared", false, "A", "B")})
	if j == nil || j.ID != "b" {
		t.Fatalf("多模型 worker 应挑最高优先级队头，期望 b，得到 %v", j)
	}
}

func TestGlobalNoIdleWorker(t *testing.T) {
	p := NewGlobalGreedyPolicy()
	p.Push(mkJob("a1", "", "A", model.PriorityBatch, 0))
	w, j := p.Schedule([]*model.Worker{mkWorker("wA", "shared", true, "A")})
	if w != nil || j != nil {
		t.Fatalf("无空闲 worker 应返回 nil,nil，得到 (%v,%v)", w, j)
	}
}

func TestGlobalEmptyQueue(t *testing.T) {
	p := NewGlobalGreedyPolicy()
	w, j := p.Schedule([]*model.Worker{mkWorker("w", "shared", false, "A")})
	if w != nil || j != nil {
		t.Fatalf("空队列应返回 nil,nil，得到 (%v,%v)", w, j)
	}
}

func TestGlobalLenRemovePosition(t *testing.T) {
	p := NewGlobalGreedyPolicy()
	p.Push(mkJob("a1", "", "A", model.PriorityBatch, 0))
	p.Push(mkJob("a2", "", "A", model.PriorityBatch, 1*time.Second))
	p.Push(mkJob("b1", "", "B", model.PriorityBatch, 2*time.Second))

	if p.Len() != 3 {
		t.Fatalf("Len 应为 3，得到 %d", p.Len())
	}
	if pos := p.PositionOf("a2"); pos != 2 {
		t.Fatalf("a2 在 A 队列位置应为 2，得到 %d", pos)
	}
	if pos := p.PositionOf("b1"); pos != 1 {
		t.Fatalf("b1 在 B 队列位置应为 1，得到 %d", pos)
	}

	removed := p.RemoveByID("a1")
	if removed == nil || removed.ID != "a1" {
		t.Fatalf("RemoveByID 应返回 a1，得到 %v", removed)
	}
	if p.Len() != 2 {
		t.Fatalf("移除后 Len 应为 2，得到 %d", p.Len())
	}
	if pos := p.PositionOf("a2"); pos != 1 {
		t.Fatalf("移除 a1 后 a2 位置应为 1，得到 %d", pos)
	}
	if p.RemoveByID("nope") != nil {
		t.Fatal("移除不存在的任务应返回 nil")
	}
	if got := p.PositionOf("nope"); got != 0 {
		t.Fatalf("不存在任务的位置应为 0，得到 %d", got)
	}
}

func TestGlobalRemoveByIDs(t *testing.T) {
	p := NewGlobalGreedyPolicy()
	p.Push(mkJob("a1", "", "A", model.PriorityBatch, 0))
	p.Push(mkJob("a2", "", "A", model.PriorityBatch, 1*time.Second))
	p.Push(mkJob("b1", "", "B", model.PriorityBatch, 2*time.Second))

	removed := p.RemoveByIDs([]string{"a1", "b1", "ghost"})
	if len(removed) != 2 {
		t.Fatalf("应移除 2 个，得到 %d", len(removed))
	}
	if p.Len() != 1 {
		t.Fatalf("剩余应为 1，得到 %d", p.Len())
	}
}
