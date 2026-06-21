package gateway

import (
	"context"
	"ollama-gateway/config"
	"ollama-gateway/internal/model"
	"ollama-gateway/internal/policy"
	"strings"
	"sync"
)

// Server 网关核心对象
type Server struct {
	cfg         *config.Config
	policy      policy.SchedulerPolicy
	workers     []*model.Worker
	taskManager *TaskManager
	mu          sync.Mutex
	cond        *sync.Cond
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewServer 构造函数
func NewServer(cfg *config.Config) *Server {
	var workers []*model.Worker
	// 初始化所有 Worker 对象
	for _, wc := range cfg.Workers {
		workers = append(workers, &model.Worker{
			ID:              wc.ID,
			URL:             wc.URL,
			Owner:           wc.Owner,
			SupportedModels: wc.Models,
			Busy:            false,
		})
	}

	// 选择策略
	var pol policy.SchedulerPolicy
	if cfg.Server.Mode == "global" {
		pol = policy.NewGlobalGreedyPolicy()
	} else {
		pol = policy.NewAffinityStealingPolicy(workers)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// 创建任务管理器，使用配置的结果保留时间和清理间隔
	taskManager := NewTaskManager(ctx, cfg.Server.GetTaskResultTTL(), cfg.Server.GetTaskCleanInterval())

	s := &Server{
		cfg:         cfg,
		policy:      pol,
		workers:     workers,
		taskManager: taskManager,
		ctx:         ctx,
		cancel:      cancel,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Shutdown 优雅关闭调度器和后台协程
func (s *Server) Shutdown() {
	s.cancel()
	s.cond.Broadcast()
}

// IsModelSupported 检查系统中是否有任何 Worker 支持该模型
func (s *Server) IsModelSupported(modelName string) bool {
	// 如果请求没指定模型，默认允许（兼容性处理）
	if modelName == "" {
		return true
	}

	// 加锁保护并发访问
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, w := range s.workers {
		for _, m := range w.SupportedModels {
			if strings.EqualFold(m, modelName) {
				return true
			}
		}
	}
	return false
}

// canAccept 判断 (用户, 模型) 是否可被系统接收（准入）。
// affinity 模式按路由规则判断（自家拥有该模型，或该模型为公共模型）；
// 其它模式按「集群中是否有任意 worker 支持该模型」。
func (s *Server) canAccept(user, modelName string) bool {
	if modelName == "" {
		return true
	}
	if ap, ok := s.policy.(*policy.AffinityStealingPolicy); ok {
		return ap.CanRoute(user, modelName)
	}
	return s.IsModelSupported(modelName)
}

// countIdleWorkersLocked 统计空闲的Worker数量（需要在调用前已持有锁）
func (s *Server) countIdleWorkersLocked() int {
	count := 0
	for _, w := range s.workers {
		if !w.Busy {
			count++
		}
	}
	return count
}

// countModelWorkersLocked 统计支持指定模型的 worker 总数和空闲数（需持有锁）
func (s *Server) countModelWorkersLocked(modelName string) (total, idle int) {
	for _, w := range s.workers {
		if supportsModelCI(w, modelName) {
			total++
			if !w.Busy {
				idle++
			}
		}
	}
	return
}

func supportsModelCI(w *model.Worker, modelName string) bool {
	if modelName == "" {
		return true
	}
	for _, m := range w.SupportedModels {
		if strings.EqualFold(m, modelName) {
			return true
		}
	}
	return false
}
