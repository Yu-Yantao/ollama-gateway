package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"ollama-gateway/internal/logger"
	"ollama-gateway/internal/model"
	"strings"
	"time"
)

// HandleGenerateAsync 异步提交任务
func (s *Server) HandleGenerateAsync(w http.ResponseWriter, r *http.Request) {
	user := r.Header.Get("X-User")
	if user == "" {
		user = "default"
	}

	priority := model.PriorityVIP
	if r.Header.Get("X-Task-Type") == "batch" {
		priority = model.PriorityBatch
	}

	modelName := strings.TrimSpace(r.Header.Get("X-Model"))
	// 可选的 Webhook 回调URL
	callbackURL := r.Header.Get("X-Callback-URL")

	taskID := r.Header.Get("X-Request-ID")
	if taskID == "" {
		taskID = fmt.Sprintf("task_%d", time.Now().UnixNano()%100000000)
	}
	if modelName == "" {
		logger.WarnEvent("task_rejected",
			"task_id", taskID,
			"user", user,
			"reason", "missing_model",
		)
		http.Error(w, "X-Model header is required", http.StatusBadRequest)
		return
	}

	// 校验模型是否可被接收（affinity 模式下还会校验该用户是否有权使用该模型）
	if !s.canAccept(user, modelName) {
		logger.WarnEvent("task_rejected",
			"task_id", taskID,
			"user", user,
			"model", modelName,
			"reason", "unsupported_model",
		)
		http.Error(w, fmt.Sprintf("Model [%s] is not supported by any workers in the cluster", modelName), http.StatusBadRequest)
		return
	}

	// 读取请求体并限制大小
	maxBodySize := s.cfg.Server.GetMaxRequestBodySize()
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(maxBodySize)))
	if err != nil {
		logger.ErrorEvent("request_body_read_failed",
			"task_id", taskID,
			"user", user,
			"model", modelName,
			"error", err,
		)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	if len(body) >= maxBodySize {
		logger.WarnEvent("task_rejected",
			"task_id", taskID,
			"user", user,
			"model", modelName,
			"reason", "request_body_too_large",
			"body_bytes", len(body),
			"max_body_bytes", maxBodySize,
		)
		http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	// 检查队列容量
	s.mu.Lock()
	qLen := s.policy.Len()
	maxQueueSize := s.cfg.Server.GetMaxQueueSize()
	idleWorkers := s.countIdleWorkersLocked()
	s.mu.Unlock()

	if qLen >= maxQueueSize {
		// 计算建议的重试时间
		retryAfter := s.calculateRetryAfter(qLen, maxQueueSize, idleWorkers)

		w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":          "queue_full",
			"message":        fmt.Sprintf("任务队列已满，建议 %d 秒后重试", retryAfter),
			"queue_length":   qLen,
			"max_queue_size": maxQueueSize,
			"retry_after":    retryAfter,
		})

		logger.WarnEvent("task_rejected",
			"task_id", taskID,
			"user", user,
			"model", modelName,
			"reason", "queue_full",
			"queue_len", qLen,
			"max_queue_size", maxQueueSize,
			"retry_after_s", retryAfter,
		)
		return
	}

	createdAt := time.Now()

	// 创建任务信息
	taskInfo := &model.TaskInfo{
		ID:          taskID,
		Status:      model.TaskStatusQueued,
		Model:       modelName,
		Requester:   user,
		CreatedAt:   createdAt,
		CallbackURL: callbackURL,
	}
	if !s.taskManager.CreateTask(taskID, taskInfo) {
		logger.WarnEvent("task_rejected",
			"task_id", taskID,
			"user", user,
			"model", modelName,
			"reason", "duplicate_task_id",
		)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   "duplicate_task_id",
			"message": fmt.Sprintf("任务ID [%s] 已存在", taskID),
			"task_id": taskID,
		})
		return
	}

	// 创建Job并加入队列
	ctx, cancel := context.WithCancel(context.Background())
	job := &model.Job{
		ID:         taskID,
		Model:      modelName,
		Requester:  user,
		SourceAddr: r.RemoteAddr,
		Priority:   priority,
		Payload:    body,
		// 缓冲 1 个结果，避免等待方超时退出后执行方阻塞或触发关闭竞态。
		ResultCh:   make(chan model.JobResult, 1),
		Ctx:        ctx,
		CancelFunc: cancel,
		AddedAt:    createdAt,
	}

	s.mu.Lock()
	s.policy.Push(job)
	s.cond.Signal()
	queuePos := s.policy.PositionOf(taskID)
	s.mu.Unlock()

	// 更新队列位置
	s.taskManager.UpdateQueuePosition(taskID, queuePos)

	// 异步等待结果
	go s.waitForAsyncResult(job, taskInfo)

	priorityTag := "VIP"
	if priority > 0 {
		priorityTag = "Batch"
	}

	logger.InfoEvent("task_created",
		"task_id", taskID,
		"user", user,
		"model", modelName,
		"priority", priorityTag,
		"source", r.RemoteAddr,
		"queue_pos", queuePos,
	)

	// 立即返回task_id和状态
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"task_id":        taskID,
		"status":         taskInfo.Status,
		"queue_position": queuePos,
		"created_at":     taskInfo.CreatedAt,
	}

	if callbackURL != "" {
		response["callback_url"] = callbackURL
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(response)
}

// waitForAsyncResult 等待异步任务结果
func (s *Server) waitForAsyncResult(job *model.Job, taskInfo *model.TaskInfo) {
	select {
	case res := <-job.ResultCh:
		// 设置任务结果
		s.taskManager.SetResult(job.ID, res.Data, res.Err)

		if res.Err != nil {
			logger.ErrorEvent("task_failed",
				"task_id", job.ID,
				"user", job.Requester,
				"model", job.Model,
				"error", res.Err,
			)
		} else {
			logger.InfoEvent("task_completed",
				"task_id", job.ID,
				"user", job.Requester,
				"model", job.Model,
			)
		}

		// 如果有回调URL，发送webhook
		if taskInfo.CallbackURL != "" {
			go s.sendWebhook(taskInfo)
		}

	case <-job.Ctx.Done():
		reason := model.TaskCancelReasonCancelled
		s.taskManager.CancelTask(job.ID, reason)
		if reason != model.TaskCancelReasonUserCancelled {
			logger.WarnEvent("task_cancelled",
				"task_id", job.ID,
				"user", job.Requester,
				"model", job.Model,
				"reason", reason,
			)
		}
	}
}

// HandleGetTask 查询任务状态
func (s *Server) HandleGetTask(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("id")
	if taskID == "" {
		http.Error(w, `{"error":"missing task id"}`, http.StatusBadRequest)
		return
	}

	task, ok := s.taskManager.GetTask(taskID)
	if !ok {
		http.Error(w, `{"error":"task not found"}`, http.StatusNotFound)
		return
	}

	// 如果任务还在队列中，更新队列位置
	if task.Status == model.TaskStatusQueued {
		s.mu.Lock()
		task.QueuePosition = s.policy.PositionOf(taskID)
		s.mu.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	// 不返回Result字段（太大），只返回状态信息
	response := map[string]interface{}{
		"id":         task.ID,
		"status":     task.Status,
		"model":      task.Model,
		"requester":  task.Requester,
		"created_at": task.CreatedAt,
	}

	if task.QueuePosition > 0 {
		response["queue_position"] = task.QueuePosition
	}
	if task.StartedAt != nil {
		response["started_at"] = task.StartedAt
	}
	if task.CompletedAt != nil {
		response["completed_at"] = task.CompletedAt
	}
	if task.CancelReason != "" {
		response["cancel_reason"] = task.CancelReason
	}
	if task.Error != "" {
		response["error"] = task.Error
	}

	json.NewEncoder(w).Encode(response)
}

// HandleGetTaskResult 获取任务结果
func (s *Server) HandleGetTaskResult(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("id")
	if taskID == "" {
		http.Error(w, `{"error":"missing task id"}`, http.StatusBadRequest)
		return
	}

	task, ok := s.taskManager.GetTask(taskID)
	if !ok {
		http.Error(w, `{"error":"task not found"}`, http.StatusNotFound)
		return
	}

	switch task.Status {
	case model.TaskStatusCompleted:
		// 返回结果
		w.Header().Set("Content-Type", "application/json")
		w.Write(task.Result)

	case model.TaskStatusFailed:
		// 返回错误
		http.Error(w, fmt.Sprintf(`{"error":"task failed","message":"%s"}`, task.Error), http.StatusInternalServerError)

	case model.TaskStatusQueued:
		// 还在排队
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":         task.Status,
			"message":        "Task is still queued",
			"queue_position": task.QueuePosition,
		})

	case model.TaskStatusRunning:
		// 正在执行
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  task.Status,
			"message": "Task is running",
		})

	case model.TaskStatusCancelled:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGone)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":         "task cancelled",
			"cancel_reason": task.CancelReason,
		})
	}
}

// HandleCancelAsync 取消异步任务
func (s *Server) HandleCancelAsync(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("id")
	if taskID == "" {
		http.Error(w, `{"error":"missing task id"}`, http.StatusBadRequest)
		return
	}

	// 检查任务是否存在
	task, ok := s.taskManager.GetTask(taskID)
	if !ok {
		http.Error(w, `{"error":"task not found"}`, http.StatusNotFound)
		return
	}

	// 只能取消排队中或执行中的任务
	if task.Status != model.TaskStatusQueued && task.Status != model.TaskStatusRunning {
		http.Error(w, fmt.Sprintf(`{"error":"cannot cancel task in status %s"}`, task.Status), http.StatusBadRequest)
		return
	}

	// 尝试从队列中移除
	s.mu.Lock()
	job := s.policy.RemoveByID(taskID)
	s.cond.Signal()
	s.mu.Unlock()

	if job != nil {
		job.CancelFunc()
		s.taskManager.CancelTask(taskID, model.TaskCancelReasonUserCancelled)
		logger.InfoEvent("task_cancelled",
			"task_id", taskID,
			"user", task.Requester,
			"model", task.Model,
			"reason", model.TaskCancelReasonUserCancelled,
			"status", task.Status,
		)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Task cancelled successfully",
			"task_id": taskID,
		})
	} else {
		// 任务可能正在执行中，无法从队列移除
		http.Error(w, `{"error":"task is running and cannot be cancelled"}`, http.StatusConflict)
	}
}

// HandleListTasks 列出所有任务
func (s *Server) HandleListTasks(w http.ResponseWriter, r *http.Request) {
	statusFilter := r.URL.Query().Get("status")
	user := r.Header.Get("X-User")

	allTasks := s.taskManager.ListTasks(model.TaskStatus(statusFilter))

	// 过滤出当前用户的任务（如果指定了用户）
	var tasks []*model.TaskInfo
	for _, task := range allTasks {
		if user == "" || task.Requester == user {
			tasks = append(tasks, task)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tasks": tasks,
		"total": len(tasks),
	})
}

// HandleBatchTaskStatus 批量查询任务状态，不返回任务结果内容。
func (s *Server) HandleBatchTaskStatus(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}
	if len(req.IDs) == 0 {
		http.Error(w, `{"error":"ids is required"}`, http.StatusBadRequest)
		return
	}

	var tasks []map[string]interface{}
	var missing []string
	for _, id := range req.IDs {
		task, ok := s.taskManager.GetTask(id)
		if !ok {
			missing = append(missing, id)
			continue
		}
		tasks = append(tasks, s.taskStatusPayload(task))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tasks":   tasks,
		"missing": missing,
		"total":   len(tasks),
	})
}

func (s *Server) taskStatusPayload(task *model.TaskInfo) map[string]interface{} {
	if task.Status == model.TaskStatusQueued {
		s.mu.Lock()
		if pos := s.policy.PositionOf(task.ID); pos > 0 {
			task.QueuePosition = pos
		}
		s.mu.Unlock()
	}

	response := map[string]interface{}{
		"id":         task.ID,
		"status":     task.Status,
		"model":      task.Model,
		"requester":  task.Requester,
		"created_at": task.CreatedAt,
	}
	if task.QueuePosition > 0 {
		response["queue_position"] = task.QueuePosition
	}
	if task.StartedAt != nil {
		response["started_at"] = task.StartedAt
	}
	if task.CompletedAt != nil {
		response["completed_at"] = task.CompletedAt
	}
	if task.CancelReason != "" {
		response["cancel_reason"] = task.CancelReason
	}
	if task.Error != "" {
		response["error"] = task.Error
	}
	return response
}

// HandleGetQueueStatus 获取队列状态
func (s *Server) HandleGetQueueStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	queueLen := s.policy.Len()
	workerCount := len(s.workers)
	idleWorkers := s.countIdleWorkersLocked()
	s.mu.Unlock()

	// 获取任务统计
	taskCounts := s.taskManager.GetTaskCount()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"queue_length":  queueLen,
		"total_workers": workerCount,
		"idle_workers":  idleWorkers,
		"busy_workers":  workerCount - idleWorkers,
		"task_counts":   taskCounts,
		"policy":        s.policy.Name(),
		"timestamp":     time.Now(),
	})
}

// sendWebhook 发送Webhook回调
func (s *Server) sendWebhook(taskInfo *model.TaskInfo) {
	if taskInfo.CallbackURL == "" {
		return
	}

	// 构造回调payload（不包含大的结果数据）
	payload := map[string]interface{}{
		"task_id":      taskInfo.ID,
		"status":       taskInfo.Status,
		"model":        taskInfo.Model,
		"requester":    taskInfo.Requester,
		"created_at":   taskInfo.CreatedAt,
		"completed_at": taskInfo.CompletedAt,
	}

	if taskInfo.CancelReason != "" {
		payload["cancel_reason"] = taskInfo.CancelReason
	}
	if taskInfo.Error != "" {
		payload["error"] = taskInfo.Error
	}

	payloadBytes, _ := json.Marshal(payload)

	// 发送POST请求
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(taskInfo.CallbackURL, "application/json", bytes.NewBuffer(payloadBytes))

	if err != nil {
		logger.ErrorEvent("webhook_failed",
			"task_id", taskInfo.ID,
			"url", taskInfo.CallbackURL,
			"error", err,
		)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		logger.DebugEvent("webhook_sent",
			"task_id", taskInfo.ID,
			"url", taskInfo.CallbackURL,
			"status_code", resp.StatusCode,
		)
	} else {
		logger.WarnEvent("webhook_unexpected_status",
			"task_id", taskInfo.ID,
			"url", taskInfo.CallbackURL,
			"status_code", resp.StatusCode,
		)
	}
}

// calculateRetryAfter 计算建议的重试时间（秒）
// 基于队列饱和度返回不同的建议值，针对任务时长1-2分钟的场景
func (s *Server) calculateRetryAfter(queueLen, maxQueueSize, idleWorkers int) int {
	if maxQueueSize == 0 {
		return 120 // 默认2分钟
	}

	// 计算队列饱和度
	saturation := float64(queueLen) / float64(maxQueueSize)

	// 基础策略：根据饱和度分级
	var baseRetryAfter int

	switch {
	case saturation >= 0.95: // 95-100%满
		baseRetryAfter = 180 // 3分钟（队列几乎满，建议多等一会）

	case saturation >= 0.85: // 85-95%满
		baseRetryAfter = 120 // 2分钟

	case saturation >= 0.70: // 70-85%满
		baseRetryAfter = 90 // 1.5分钟

	default: // < 70%满
		baseRetryAfter = 60 // 1分钟
	}

	// 如果所有Worker都在忙，建议时间加长50%
	// 因为至少要等一个任务完成才会有空位
	if idleWorkers == 0 {
		baseRetryAfter = int(float64(baseRetryAfter) * 1.5)
	}

	return baseRetryAfter
}
