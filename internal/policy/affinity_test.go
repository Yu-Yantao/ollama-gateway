package policy

import (
	"testing"
	"time"

	"ollama-gateway/internal/model"
)

// 标准拓扑：14b 公共（1 台），thinking 业务专属（userA/userB/userC 各 1 台）。
func standardWorkers() []*model.Worker {
	return []*model.Worker{
		mkWorker("w14b", "shared", false, "14b"),
		mkWorker("wA", "userA", false, "think"),
		mkWorker("wB", "userB", false, "think"),
		mkWorker("wC", "userC", false, "think"),
	}
}

func TestAffinityRouting(t *testing.T) {
	p := NewAffinityStealingPolicy(standardWorkers())

	cases := []struct {
		user, model string
		wantOK      bool
		wantOwner   string
	}{
		{"userA", "think", true, "userA"}, // 自家业务模型
		{"userB", "think", true, "userB"}, // 各自业务线
		{"userZ", "think", false, ""},     // 越权：非本业务线请求业务专属模型 -> 拒绝
		{"userA", "14b", true, "shared"},  // 业务线用公共模型 -> 公共池
		{"anon", "14b", true, "shared"},   // 任何人用公共模型 -> 公共池
		{"userA", "nope", false, ""},      // 不存在的模型 -> 拒绝
	}

	for _, c := range cases {
		if got := p.CanRoute(c.user, c.model); got != c.wantOK {
			t.Errorf("CanRoute(%s,%s)=%v，期望 %v", c.user, c.model, got, c.wantOK)
		}
		k, ok := p.route(c.user, c.model)
		if ok != c.wantOK {
			t.Errorf("route(%s,%s) ok=%v，期望 %v", c.user, c.model, ok, c.wantOK)
		}
		if ok && k.owner != c.wantOwner {
			t.Errorf("route(%s,%s) owner=%s，期望 %s", c.user, c.model, k.owner, c.wantOwner)
		}
	}
}

func TestAffinityPreCreatedQueues(t *testing.T) {
	p := NewAffinityStealingPolicy(standardWorkers())
	want := []queueKey{
		{"shared", "14b"},
		{"userA", "think"},
		{"userB", "think"},
		{"userC", "think"},
	}
	if len(p.queues) != len(want) {
		t.Fatalf("队列数应为 %d，得到 %d", len(want), len(p.queues))
	}
	for _, k := range want {
		if p.queues[k] == nil {
			t.Errorf("缺少预创建队列 %+v", k)
		}
	}
}

// 自家有活儿时优先跑自家，即使别人的任务更老。
func TestAffinityOwnFirstBeatsSteal(t *testing.T) {
	p := NewAffinityStealingPolicy(standardWorkers())
	p.Push(mkJob("ub", "userB", "think", model.PriorityBatch, 0))             // userB 的，更老
	p.Push(mkJob("ua", "userA", "think", model.PriorityBatch, 5*time.Second)) // userA 自家，较新

	// 只有 userA 的 worker 空闲。
	workers := []*model.Worker{
		mkWorker("w14b", "shared", true, "14b"),
		mkWorker("wA", "userA", false, "think"),
		mkWorker("wB", "userB", true, "think"),
		mkWorker("wC", "userC", true, "think"),
	}
	w, j := p.Schedule(workers)
	if w == nil || w.ID != "wA" || j.ID != "ua" {
		t.Fatalf("应自家优先跑 ua，得到 (%v,%v)", w, j)
	}
}

// 业务线之间互相窃取：userA 空闲且自家无活儿时，偷 userB 的同模型任务。
func TestAffinityStealBusinessToBusiness(t *testing.T) {
	p := NewAffinityStealingPolicy(standardWorkers())
	p.Push(mkJob("ub", "userB", "think", model.PriorityBatch, 0))

	workers := []*model.Worker{
		mkWorker("w14b", "shared", true, "14b"),
		mkWorker("wA", "userA", false, "think"), // 空闲，自家 (userA,think) 为空
		mkWorker("wB", "userB", true, "think"),
		mkWorker("wC", "userC", true, "think"),
	}
	w, j := p.Schedule(workers)
	if w == nil || w.ID != "wA" || j.ID != "ub" {
		t.Fatalf("userA 的 worker 应窃取 userB 的任务，得到 (%v,%v)", w, j)
	}
}

// 公共 worker 帮业务线：shared 也能跑 think 时，空闲就替业务线消化。
func TestAffinitySharedHelpsBusiness(t *testing.T) {
	workers := []*model.Worker{
		mkWorker("wShared", "shared", false, "think"), // 公共 worker 支持 think
		mkWorker("wA", "userA", true, "think"),        // 业务 worker 忙
	}
	p := NewAffinityStealingPolicy(workers)
	p.Push(mkJob("ua", "userA", "think", model.PriorityBatch, 0)) // -> (userA, think)

	w, j := p.Schedule(workers)
	if w == nil || w.ID != "wShared" || j.ID != "ua" {
		t.Fatalf("公共 worker 应帮业务线跑 ua，得到 (%v,%v)", w, j)
	}
}

// 业务 worker 默认不帮公共池：公共队列有任务，业务 worker 空闲也不碰。
func TestAffinityBusinessDoesNotHelpShared(t *testing.T) {
	workers := []*model.Worker{
		mkWorker("wShared", "shared", true, "think"), // 公共 worker 忙
		mkWorker("wA", "userA", false, "think"),      // 业务 worker 闲
	}
	p := NewAffinityStealingPolicy(workers)
	// userZ 非 owner，think 因有 shared worker 而成为公共模型 -> 进 (shared, think)
	p.Push(mkJob("z", "userZ", "think", model.PriorityBatch, 0))

	w, j := p.Schedule(workers)
	if w != nil || j != nil {
		t.Fatalf("业务 worker 不应帮公共池，应返回 nil,nil，得到 (%v,%v)", w, j)
	}
}

// 亲和模式无优先级：先到先跑，忽略 X-Task-Type 带来的优先级差异。
func TestAffinityFIFOIgnoresPriority(t *testing.T) {
	p := NewAffinityStealingPolicy(standardWorkers())
	p.Push(mkJob("early", "userA", "think", model.PriorityBatch, 0))             // 先到（Batch）
	p.Push(mkJob("lateVIP", "userA", "think", model.PriorityVIP, 1*time.Second)) // 后到（VIP）

	workers := []*model.Worker{mkWorker("wA", "userA", false, "think")}
	_, j := p.Schedule(workers)
	if j == nil || j.ID != "early" {
		t.Fatalf("亲和模式应 FIFO（忽略优先级），先跑 early，得到 %v", j)
	}
}

func TestAffinityRejectedJobNotQueued(t *testing.T) {
	// 拓扑里没有 shared 的 think，think 只属于 userA。
	workers := []*model.Worker{mkWorker("wA", "userA", false, "think")}
	p := NewAffinityStealingPolicy(workers)
	p.Push(mkJob("bad", "userZ", "think", model.PriorityBatch, 0)) // 越权，route 失败
	if p.Len() != 0 {
		t.Fatalf("被拒绝的任务不应入队，Len 应为 0，得到 %d", p.Len())
	}
}

func TestAffinityCanStealMatrix(t *testing.T) {
	p := &AffinityStealingPolicy{}
	wShared := mkWorker("s", "shared", false, "think")
	wA := mkWorker("a", "userA", false, "think")
	wNo := mkWorker("n", "userA", false, "other")

	cases := []struct {
		name   string
		w      *model.Worker
		k      queueKey
		expect bool
	}{
		{"公共偷业务", wShared, queueKey{"userA", "think"}, true},
		{"业务偷业务", wA, queueKey{"userB", "think"}, true},
		{"业务偷公共(关)", wA, queueKey{"shared", "think"}, false},
		{"自家队列非窃取", wA, queueKey{"userA", "think"}, false},
		{"不支持该模型", wNo, queueKey{"userB", "think"}, false},
	}
	for _, c := range cases {
		if got := p.canSteal(c.w, c.k); got != c.expect {
			t.Errorf("%s: canSteal=%v，期望 %v", c.name, got, c.expect)
		}
	}
}

func TestAffinityLenRemovePosition(t *testing.T) {
	p := NewAffinityStealingPolicy(standardWorkers())
	p.Push(mkJob("a1", "userA", "think", model.PriorityVIP, 0))
	p.Push(mkJob("a2", "userA", "think", model.PriorityVIP, 1*time.Second))

	if p.Len() != 2 {
		t.Fatalf("Len 应为 2，得到 %d", p.Len())
	}
	if pos := p.PositionOf("a1"); pos != 1 {
		t.Fatalf("a1 位置应为 1，得到 %d", pos)
	}
	if pos := p.PositionOf("a2"); pos != 2 {
		t.Fatalf("a2 位置应为 2，得到 %d", pos)
	}

	if removed := p.RemoveByID("a1"); removed == nil || removed.ID != "a1" {
		t.Fatalf("RemoveByID 应返回 a1，得到 %v", removed)
	}
	if p.Len() != 1 {
		t.Fatalf("移除后 Len 应为 1，得到 %d", p.Len())
	}
	if pos := p.PositionOf("a2"); pos != 1 {
		t.Fatalf("移除 a1 后 a2 位置应为 1，得到 %d", pos)
	}
	if p.RemoveByID("ghost") != nil {
		t.Fatal("移除不存在的任务应返回 nil")
	}
}
