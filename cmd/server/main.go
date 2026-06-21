package main

import (
	"context"
	"net/http"
	"ollama-gateway/config"
	"ollama-gateway/internal/gateway"
	"ollama-gateway/internal/logger"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// 1. 加载配置
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		logger.FatalEvent("config_load_failed", "path", "config.yaml", "error", err)
	}

	// 2. 初始化网关
	gw := gateway.NewServer(cfg)

	// 3. 启动调度器
	go gw.RunScheduler()

	// 4. 注册路由
	// 异步任务提交
	http.HandleFunc("/api/generate", gw.HandleGenerateAsync)
	// 查询任务状态
	http.HandleFunc("/api/task", gw.HandleGetTask)
	// 获取任务结果
	http.HandleFunc("/api/task/result", gw.HandleGetTaskResult)
	// 取消任务
	http.HandleFunc("/api/task/cancel", gw.HandleCancelAsync)
	// 列出所有任务
	http.HandleFunc("/api/tasks", gw.HandleListTasks)
	// 批量查询任务状态
	http.HandleFunc("/api/tasks/status", gw.HandleBatchTaskStatus)
	// 队列状态
	http.HandleFunc("/api/queue/status", gw.HandleGetQueueStatus)

	// 5. 创建 HTTP 服务器
	server := &http.Server{
		Addr:        cfg.Server.Port,
		ReadTimeout: 30 * time.Second,
		// 略大于请求超时
		WriteTimeout: cfg.Server.GetRequestTimeout() + 10*time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// 6. 在 goroutine 中启动服务器
	go func() {
		logger.InfoEvent("server_started", "addr", cfg.Server.Port, "mode", cfg.Server.Mode)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.FatalEvent("server_start_failed", "addr", cfg.Server.Port, "error", err)
		}
	}()

	// 7. 监听系统信号，实现优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	sig := <-sigCh

	logger.InfoEvent("server_stopping", "signal", sig.String())

	// 8. 关闭调度器和后台协程
	gw.Shutdown()

	// 9. 设置关闭超时（30秒）
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 10. 优雅关闭 HTTP 服务器（等待进行中的请求完成）
	if err := server.Shutdown(ctx); err != nil {
		logger.ErrorEvent("server_shutdown_failed", "error", err)
	} else {
		logger.InfoEvent("server_stopped")
	}
}
