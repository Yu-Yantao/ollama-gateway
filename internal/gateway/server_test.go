package gateway

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ollama-gateway/config"
	"ollama-gateway/internal/model"
	"ollama-gateway/internal/policy"
)

func gwWorker(id, owner string, models ...string) *model.Worker {
	return &model.Worker{ID: id, Owner: owner, SupportedModels: models}
}

func TestCalculateRetryAfter(t *testing.T) {
	s := &Server{}
	cases := []struct {
		name                           string
		queueLen, maxQueue, idle, want int
	}{
		{"无上限", 0, 0, 1, 120},
		{"95%+", 10, 10, 1, 180},
		{"85-95%", 9, 10, 1, 120},
		{"70-85%", 8, 10, 1, 90},
		{"<70%", 5, 10, 1, 60},
		{"全忙加成", 5, 10, 0, 90}, // 60 * 1.5
	}
	for _, c := range cases {
		if got := s.calculateRetryAfter(c.queueLen, c.maxQueue, c.idle); got != c.want {
			t.Errorf("%s: calculateRetryAfter=%d，期望 %d", c.name, got, c.want)
		}
	}
}

func TestGenerateRequiresModelHeader(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Port:             ":1103",
			Mode:             "affinity",
			Mock:             true,
			MaxQueueSize:     100,
			MaxRequestBodyMB: 10,
		},
		Workers: []config.WorkerConfig{
			{ID: "w1", URL: "http://127.0.0.1:11434", Owner: "shared", Models: []string{"m"}},
		},
	}
	s := NewServer(cfg)
	defer s.Shutdown()

	req := httptest.NewRequest(http.MethodPost, "/api/generate", bytes.NewBufferString(`{"prompt":"hello"}`))
	req.Header.Set("X-Request-ID", "missing-model")
	rr := httptest.NewRecorder()

	s.HandleGenerateAsync(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("缺少 X-Model 应返回 400，状态码=%d body=%s", rr.Code, rr.Body.String())
	}
	if _, ok := s.taskManager.GetTask("missing-model"); ok {
		t.Fatal("缺少 X-Model 的请求不应创建任务")
	}
}

func TestGenerateReturnsPositionWithinRoutedQueue(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Port:             ":1103",
			Mode:             "affinity",
			Mock:             true,
			MaxQueueSize:     100,
			MaxRequestBodyMB: 10,
		},
		Workers: []config.WorkerConfig{
			{ID: "wA", URL: "http://127.0.0.1:11434", Owner: "userA", Models: []string{"m"}},
			{ID: "wB", URL: "http://127.0.0.1:11435", Owner: "userB", Models: []string{"m"}},
		},
	}
	s := NewServer(cfg)
	defer s.Shutdown()

	submit := func(id, user string) int {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/generate", bytes.NewBufferString(`{"model":"m","prompt":"hello"}`))
		req.Header.Set("X-User", user)
		req.Header.Set("X-Model", "m")
		req.Header.Set("X-Request-ID", id)
		rr := httptest.NewRecorder()

		s.HandleGenerateAsync(rr, req)
		if rr.Code != http.StatusAccepted {
			t.Fatalf("任务提交应成功，状态码=%d body=%s", rr.Code, rr.Body.String())
		}

		var resp struct {
			QueuePosition int `json:"queue_position"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("响应应为 JSON: %v", err)
		}
		return resp.QueuePosition
	}

	if got := submit("task-a", "userA"); got != 1 {
		t.Fatalf("userA 队列首个任务位置应为 1，得到 %d", got)
	}
	if got := submit("task-b", "userB"); got != 1 {
		t.Fatalf("userB 独立队列首个任务位置应为 1，得到 %d", got)
	}

	s.mu.Lock()
	jobs := s.policy.RemoveByIDs([]string{"task-a", "task-b"})
	s.mu.Unlock()
	for _, job := range jobs {
		job.CancelFunc()
	}
}

func TestGenerateIgnoresTimeoutAndEstimateHeaders(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Port:             ":1103",
			Mode:             "global",
			Mock:             true,
			MaxQueueSize:     100,
			MaxRequestBodyMB: 10,
		},
		Workers: []config.WorkerConfig{
			{ID: "w1", URL: "http://127.0.0.1:11434", Owner: "shared", Models: []string{"m"}},
		},
	}
	s := NewServer(cfg)
	defer s.Shutdown()

	req := httptest.NewRequest(http.MethodPost, "/api/generate", bytes.NewBufferString(`{"model":"m","prompt":"hello"}`))
	req.Header.Set("X-User", "userA")
	req.Header.Set("X-Model", "m")
	req.Header.Set("X-Request-ID", "timeout-header-is-ignored")
	req.Header.Set("X-Client-Timeout", "not-a-number")
	req.Header.Set("X-Estimated-Duration", "not-a-number")
	rr := httptest.NewRecorder()

	s.HandleGenerateAsync(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("timeout/estimate headers 不应影响异步任务提交，状态码=%d body=%s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("响应应为 JSON: %v", err)
	}
	if _, ok := resp["client_timeout_seconds"]; ok {
		t.Fatal("提交响应不应再返回 client_timeout_seconds")
	}
	if _, ok := resp["timeout_at"]; ok {
		t.Fatal("提交响应不应再返回 timeout_at")
	}
	if _, ok := resp["estimated_wait_seconds"]; ok {
		t.Fatal("提交响应不应再返回 estimated_wait_seconds")
	}
	if _, ok := resp["estimated_task_duration_seconds"]; ok {
		t.Fatal("提交响应不应再返回 estimated_task_duration_seconds")
	}
}

func TestHandleBatchTaskStatus(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{Port: ":1103", Mode: "global"},
		Workers: []config.WorkerConfig{
			{ID: "w1", URL: "http://127.0.0.1:11434", Owner: "shared", Models: []string{"m"}},
		},
	}
	s := NewServer(cfg)
	defer s.Shutdown()

	now := time.Now()
	s.taskManager.CreateTask("t1", &model.TaskInfo{ID: "t1", Status: model.TaskStatusCompleted, Model: "m", Requester: "u", CreatedAt: now})
	s.taskManager.CreateTask("t2", &model.TaskInfo{ID: "t2", Status: model.TaskStatusQueued, Model: "m", Requester: "u", CreatedAt: now, QueuePosition: 3})

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/status", bytes.NewBufferString(`{"ids":["t2","missing","t1"]}`))
	rr := httptest.NewRecorder()

	s.HandleBatchTaskStatus(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("批量查询应成功，状态码=%d body=%s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Tasks   []model.TaskInfo `json:"tasks"`
		Missing []string         `json:"missing"`
		Total   int              `json:"total"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("响应应为 JSON: %v", err)
	}
	if resp.Total != 2 || len(resp.Tasks) != 2 {
		t.Fatalf("应返回 2 个已存在任务，得到 total=%d tasks=%d", resp.Total, len(resp.Tasks))
	}
	if resp.Tasks[0].ID != "t2" || resp.Tasks[1].ID != "t1" {
		t.Fatalf("任务顺序应跟请求 ID 顺序一致，得到 %+v", resp.Tasks)
	}
	if len(resp.Missing) != 1 || resp.Missing[0] != "missing" {
		t.Fatalf("应返回 missing ID，得到 %+v", resp.Missing)
	}
}

func TestCanAccept(t *testing.T) {
	workers := []*model.Worker{
		gwWorker("w14b", "shared", "14b"),
		gwWorker("wA", "userA", "think"),
		gwWorker("wB", "userB", "think"),
	}

	// affinity：按路由规则
	aff := &Server{policy: policy.NewAffinityStealingPolicy(workers), workers: workers}
	affCases := []struct {
		user, model string
		want        bool
	}{
		{"userA", "think", true},  // 自家
		{"userZ", "think", false}, // 越权 -> 拒绝
		{"anyone", "14b", true},   // 公共
		{"x", "nope", false},      // 不存在
		{"x", "", true},           // 空模型放行
	}
	for _, c := range affCases {
		if got := aff.canAccept(c.user, c.model); got != c.want {
			t.Errorf("affinity canAccept(%s,%s)=%v，期望 %v", c.user, c.model, got, c.want)
		}
	}

	// global：只看是否有 worker 支持该模型，不看归属
	glob := &Server{policy: policy.NewGlobalGreedyPolicy(), workers: workers}
	if !glob.canAccept("userZ", "think") {
		t.Error("global 模式下任何人请求受支持的模型都应放行")
	}
	if glob.canAccept("x", "bogus") {
		t.Error("global 模式下不存在的模型应被拒绝")
	}
}
