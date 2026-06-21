package policy

import (
	"container/heap"
	"testing"
	"time"

	"ollama-gateway/internal/model"
)

func TestPriorityQueueOrdering(t *testing.T) {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	heap.Push(&pq, mkJob("batch", "", "A", model.PriorityBatch, 1*time.Second))
	heap.Push(&pq, mkJob("vip", "", "A", model.PriorityVIP, 2*time.Second))
	heap.Push(&pq, mkJob("batchEarly", "", "A", model.PriorityBatch, 0))

	// 出堆顺序：VIP（优先级最高）→ 同为 Batch 时按 FIFO（先到的 batchEarly）→ batch。
	want := []string{"vip", "batchEarly", "batch"}
	for _, id := range want {
		got := heap.Pop(&pq).(*model.Job)
		if got.ID != id {
			t.Fatalf("出堆顺序错误：期望 %s，得到 %s", id, got.ID)
		}
	}
}

func TestPriorityQueueIndexAndRemove(t *testing.T) {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	j1 := mkJob("j1", "", "A", model.PriorityBatch, 0)
	j2 := mkJob("j2", "", "A", model.PriorityBatch, 1*time.Second)
	j3 := mkJob("j3", "", "A", model.PriorityBatch, 2*time.Second)
	heap.Push(&pq, j1)
	heap.Push(&pq, j2)
	heap.Push(&pq, j3)

	// 每个元素的 Index 应被正确维护，可用于 O(log n) 删除。
	for _, j := range []*model.Job{j1, j2, j3} {
		if j.Index < 0 || j.Index >= pq.Len() {
			t.Fatalf("%s 的 Index 非法：%d", j.ID, j.Index)
		}
		if pq[j.Index] != j {
			t.Fatalf("%s 的 Index 指向错误元素", j.ID)
		}
	}

	heap.Remove(&pq, j2.Index)
	if pq.Len() != 2 {
		t.Fatalf("移除后长度应为 2，得到 %d", pq.Len())
	}
	for i := 0; i < pq.Len(); i++ {
		if pq[i].ID == "j2" {
			t.Fatal("j2 应已被移除")
		}
		if pq[i].Index != i {
			t.Fatalf("移除后 Index 未更新：位置 %d 的元素 Index=%d", i, pq[i].Index)
		}
	}
}

func TestJobHigher(t *testing.T) {
	vip := mkJob("vip", "", "A", model.PriorityVIP, 0)
	batch := mkJob("batch", "", "A", model.PriorityBatch, 0)
	if !jobHigher(vip, batch) {
		t.Error("VIP 应比 Batch 优先级高")
	}
	if jobHigher(batch, vip) {
		t.Error("Batch 不应比 VIP 高")
	}

	early := mkJob("early", "", "A", model.PriorityBatch, 0)
	late := mkJob("late", "", "A", model.PriorityBatch, 1*time.Second)
	if !jobHigher(early, late) {
		t.Error("同优先级时先到的应更高（FIFO）")
	}
	if jobHigher(late, early) {
		t.Error("同优先级时后到的不应更高")
	}
}
