package gateway

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"ollama-gateway/internal/logger"
	"ollama-gateway/internal/model"
	"time"
)

// RunScheduler 启动后台调度 Goroutine
func (s *Server) RunScheduler() {
	logger.InfoEvent("scheduler_started",
		"policy", s.policy.Name(),
		"mock", s.cfg.Server.Mock,
		"worker_count", len(s.workers),
	)

	for {
		// 检查是否收到关闭信号
		select {
		case <-s.ctx.Done():
			logger.InfoEvent("scheduler_stopped")
			return
		default:
		}

		s.mu.Lock()

		// 1. 核心调度逻辑 (通过接口调用)
		worker, job := s.policy.Schedule(s.workers)

		// 2. 无任务或无资源，等待
		if worker == nil || job == nil {
			s.cond.Wait()
			s.mu.Unlock()
			// Wait 被唤醒后再次检查是否关闭
			if s.ctx.Err() != nil {
				logger.InfoEvent("scheduler_stopped")
				return
			}
			continue
		}

		// 3. 锁定资源
		worker.Busy = true
		s.mu.Unlock()

		// 4. 检查任务是否已过期/取消
		select {
		case <-job.Ctx.Done():
			logger.WarnEvent("task_skipped",
				"task_id", job.ID,
				"worker", worker.ID,
				"user", job.Requester,
				"source", job.SourceAddr,
				"reason", "context_done_before_dispatch",
			)
			s.releaseWorker(worker)
			continue
		default:
		}

		// 5. 异步执行
		go s.execute(worker, job)
	}
}

// execute 执行 HTTP 转发
func (s *Server) execute(w *model.Worker, j *model.Job) {
	defer s.releaseWorker(w)

	// 更新任务状态为执行中
	s.taskManager.UpdateStatus(j.ID, model.TaskStatusRunning)

	// 根据策略类型输出不同的日志
	priorityTag := "VIP"
	if j.Priority > 0 {
		priorityTag = "Batch"
	}

	dispatchType := "global"
	if s.cfg.Server.Mode == "affinity" {
		if w.Owner == "shared" {
			dispatchType = "shared"
		} else if w.Owner != "" && w.Owner != j.Requester {
			dispatchType = "steal"
		} else {
			dispatchType = "affinity"
		}
	}
	logger.InfoEvent("task_started",
		"task_id", j.ID,
		"worker", w.ID,
		"worker_owner", w.Owner,
		"user", j.Requester,
		"model", j.Model,
		"priority", priorityTag,
		"dispatch", dispatchType,
		"source", j.SourceAddr,
		"target", fmt.Sprintf("%s/%s", w.URL, j.Model),
	)

	// 只统计真正转发到 worker 的执行耗时，便于日志排查。
	startedAt := time.Now()
	resp, err := s.performRequest(w, j)
	duration := time.Since(startedAt)

	if j.Ctx.Err() != nil {
		logger.WarnEvent("task_execution_cancelled",
			"task_id", j.ID,
			"worker", w.ID,
			"user", j.Requester,
			"model", j.Model,
			"source", j.SourceAddr,
			"target", fmt.Sprintf("%s/%s", w.URL, j.Model),
		)
		err = fmt.Errorf("canceled")
	} else if err != nil {
		logger.ErrorEvent("task_execution_failed",
			"task_id", j.ID,
			"worker", w.ID,
			"user", j.Requester,
			"model", j.Model,
			"source", j.SourceAddr,
			"target", fmt.Sprintf("%s/%s", w.URL, j.Model),
			"error", err,
		)
	} else {
		logger.InfoEvent("task_execution_completed",
			"task_id", j.ID,
			"worker", w.ID,
			"user", j.Requester,
			"model", j.Model,
			"source", j.SourceAddr,
			"target", fmt.Sprintf("%s/%s", w.URL, j.Model),
			"duration_ms", duration.Milliseconds(),
		)
	}

	// 回传结果
	select {
	case j.ResultCh <- model.JobResult{Data: resp, Err: err}:
	case <-j.Ctx.Done():
	}
}

func (s *Server) releaseWorker(w *model.Worker) {
	s.mu.Lock()
	w.Busy = false
	s.cond.Signal()
	s.mu.Unlock()
}

func (s *Server) performRequest(w *model.Worker, j *model.Job) ([]byte, error) {
	if s.cfg.Server.Mock {
		// 使用配置的Mock任务执行时间
		select {
		case <-time.After(s.cfg.Server.GetMockTaskDuration()):
			return []byte(fmt.Sprintf(`{"mock_result": "from %s"}`, w.ID)), nil
		case <-j.Ctx.Done():
			return nil, j.Ctx.Err()
		}
	}

	// 真实请求
	req, err := http.NewRequestWithContext(j.Ctx, "POST", w.URL+"/api/generate", bytes.NewBuffer(j.Payload))
	if err != nil {
		return nil, err
	}
	// 使用配置的超时时间，避免goroutine泄露
	client := &http.Client{
		Timeout: s.cfg.Server.GetRequestTimeout(),
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
