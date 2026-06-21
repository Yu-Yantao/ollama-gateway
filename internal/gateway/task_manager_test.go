package gateway

import (
	"errors"
	"testing"
	"time"

	"ollama-gateway/internal/model"
)

// newTestTM 构造一个不启动清理 goroutine 的 TaskManager，便于纯逻辑测试。
func newTestTM(ttl time.Duration) *TaskManager {
	return &TaskManager{
		tasks: make(map[string]*model.TaskInfo),
		ttl:   ttl,
	}
}

func TestCreateTaskRejectsDuplicate(t *testing.T) {
	tm := newTestTM(time.Hour)
	if !tm.CreateTask("t1", &model.TaskInfo{ID: "t1", Status: model.TaskStatusQueued}) {
		t.Fatal("首次创建应成功")
	}
	if tm.CreateTask("t1", &model.TaskInfo{ID: "t1"}) {
		t.Fatal("重复 ID 应创建失败")
	}
}

func TestGetTask(t *testing.T) {
	tm := newTestTM(time.Hour)
	tm.CreateTask("t1", &model.TaskInfo{ID: "t1"})
	if _, ok := tm.GetTask("t1"); !ok {
		t.Fatal("应能取到已创建任务")
	}
	if _, ok := tm.GetTask("ghost"); ok {
		t.Fatal("不存在的任务应返回 false")
	}
}

func TestUpdateStatusTimestamps(t *testing.T) {
	tm := newTestTM(time.Hour)
	tm.CreateTask("t1", &model.TaskInfo{ID: "t1", Status: model.TaskStatusQueued})

	tm.UpdateStatus("t1", model.TaskStatusRunning)
	task, _ := tm.GetTask("t1")
	if task.Status != model.TaskStatusRunning || task.StartedAt == nil {
		t.Fatalf("running 应设置 StartedAt，得到 status=%s started=%v", task.Status, task.StartedAt)
	}

	tm.UpdateStatus("t1", model.TaskStatusCompleted)
	if task.Status != model.TaskStatusCompleted || task.CompletedAt == nil {
		t.Fatalf("completed 应设置 CompletedAt，得到 status=%s completed=%v", task.Status, task.CompletedAt)
	}
}

func TestSetResultSuccess(t *testing.T) {
	tm := newTestTM(time.Hour)
	tm.CreateTask("t1", &model.TaskInfo{ID: "t1", Status: model.TaskStatusRunning})

	tm.SetResult("t1", []byte("hello"), nil)
	task, _ := tm.GetTask("t1")
	if task.Status != model.TaskStatusCompleted {
		t.Fatalf("成功应为 completed，得到 %s", task.Status)
	}
	if string(task.Result) != "hello" {
		t.Fatalf("结果应为 hello，得到 %q", task.Result)
	}
	if task.CompletedAt == nil {
		t.Fatal("应设置 CompletedAt")
	}
}

func TestSetResultFailure(t *testing.T) {
	tm := newTestTM(time.Hour)
	tm.CreateTask("t1", &model.TaskInfo{ID: "t1", Status: model.TaskStatusRunning})

	tm.SetResult("t1", nil, errors.New("boom"))
	task, _ := tm.GetTask("t1")
	if task.Status != model.TaskStatusFailed {
		t.Fatalf("失败应为 failed，得到 %s", task.Status)
	}
	if task.Error != "boom" {
		t.Fatalf("错误信息应为 boom，得到 %q", task.Error)
	}
}

func TestCancelTaskIdempotent(t *testing.T) {
	tm := newTestTM(time.Hour)
	tm.CreateTask("t1", &model.TaskInfo{ID: "t1", Status: model.TaskStatusQueued})

	tm.CancelTask("t1", model.TaskCancelReasonUserCancelled)
	task, _ := tm.GetTask("t1")
	if task.Status != model.TaskStatusCancelled || task.CancelReason != model.TaskCancelReasonUserCancelled {
		t.Fatalf("首次取消应记录原因，得到 status=%s reason=%s", task.Status, task.CancelReason)
	}

	// 已取消且已有原因时，再次取消不应覆盖原因。
	tm.CancelTask("t1", model.TaskCancelReasonCancelled)
	if task.CancelReason != model.TaskCancelReasonUserCancelled {
		t.Fatalf("重复取消不应覆盖原因，得到 %s", task.CancelReason)
	}
}

func TestDeleteTask(t *testing.T) {
	tm := newTestTM(time.Hour)
	tm.CreateTask("t1", &model.TaskInfo{ID: "t1"})
	tm.DeleteTask("t1")
	if _, ok := tm.GetTask("t1"); ok {
		t.Fatal("删除后不应再取到")
	}
}

func TestUpdateQueuePosition(t *testing.T) {
	tm := newTestTM(time.Hour)
	tm.CreateTask("t1", &model.TaskInfo{ID: "t1", Status: model.TaskStatusQueued})
	tm.UpdateQueuePosition("t1", 5)
	task, _ := tm.GetTask("t1")
	if task.QueuePosition != 5 {
		t.Fatalf("队列位置应为 5，得到 %d", task.QueuePosition)
	}
}

func TestListTasksFilter(t *testing.T) {
	tm := newTestTM(time.Hour)
	tm.CreateTask("t1", &model.TaskInfo{ID: "t1", Status: model.TaskStatusQueued})
	tm.CreateTask("t2", &model.TaskInfo{ID: "t2", Status: model.TaskStatusRunning})
	tm.CreateTask("t3", &model.TaskInfo{ID: "t3", Status: model.TaskStatusQueued})

	if got := len(tm.ListTasks("")); got != 3 {
		t.Fatalf("无过滤应返回 3，得到 %d", got)
	}
	if got := len(tm.ListTasks(model.TaskStatusQueued)); got != 2 {
		t.Fatalf("queued 应返回 2，得到 %d", got)
	}
	if got := len(tm.ListTasks(model.TaskStatusRunning)); got != 1 {
		t.Fatalf("running 应返回 1，得到 %d", got)
	}
}

func TestGetTaskCount(t *testing.T) {
	tm := newTestTM(time.Hour)
	tm.CreateTask("t1", &model.TaskInfo{ID: "t1", Status: model.TaskStatusQueued})
	tm.CreateTask("t2", &model.TaskInfo{ID: "t2", Status: model.TaskStatusQueued})
	tm.CreateTask("t3", &model.TaskInfo{ID: "t3", Status: model.TaskStatusRunning})

	counts := tm.GetTaskCount()
	if counts[model.TaskStatusQueued] != 2 || counts[model.TaskStatusRunning] != 1 {
		t.Fatalf("计数应为 queued=2 running=1，得到 %v", counts)
	}
}

func TestCleanupOnce(t *testing.T) {
	tm := newTestTM(1 * time.Hour)
	now := time.Now()
	old := now.Add(-2 * time.Hour)      // 已过 TTL
	recent := now.Add(-1 * time.Minute) // 未过 TTL

	tm.CreateTask("expired", &model.TaskInfo{ID: "expired", Status: model.TaskStatusCompleted, CompletedAt: &old})
	tm.CreateTask("recent", &model.TaskInfo{ID: "recent", Status: model.TaskStatusCompleted, CompletedAt: &recent})
	tm.CreateTask("running", &model.TaskInfo{ID: "running", Status: model.TaskStatusRunning}) // 未完成，永不清理

	cleaned, remaining := tm.cleanupOnce(now)
	if cleaned != 1 {
		t.Fatalf("应只清理 1 个过期任务，得到 %d", cleaned)
	}
	if remaining != 2 {
		t.Fatalf("应剩余 2 个，得到 %d", remaining)
	}
	if _, ok := tm.GetTask("expired"); ok {
		t.Error("过期任务应被删除")
	}
	if _, ok := tm.GetTask("recent"); !ok {
		t.Error("未过期任务应保留")
	}
	if _, ok := tm.GetTask("running"); !ok {
		t.Error("未完成任务应保留")
	}
}
