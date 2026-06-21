package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

// ServerConfig 定义服务器参数
type ServerConfig struct {
	Port string `yaml:"port"`
	Mode string `yaml:"mode"`
	Mock bool   `yaml:"mock"`
	// Mock模式下任务执行时间（秒），默认30秒
	MockTaskDuration int `yaml:"mock_task_duration"`
	// HTTP请求超时时间（秒），默认300秒
	RequestTimeout int `yaml:"request_timeout"`
	// 最大请求体大小（MB），默认10MB
	MaxRequestBodyMB int `yaml:"max_request_body_mb"`
	// 任务结果保留时间（秒），默认3600秒
	TaskResultTTL int `yaml:"task_result_ttl"`
	// 任务清理间隔（秒），默认600秒
	TaskCleanInterval int `yaml:"task_clean_interval"`
	// 最大队列长度，默认100
	MaxQueueSize int `yaml:"max_queue_size"`
}

// GetRequestTimeout 获取请求超时时间，如果未配置则返回默认值300秒
func (sc *ServerConfig) GetRequestTimeout() time.Duration {
	if sc.RequestTimeout <= 0 {
		// 默认5分钟
		return 300 * time.Second
	}
	return time.Duration(sc.RequestTimeout) * time.Second
}

// GetMockTaskDuration 获取Mock任务执行时间，如果未配置则返回默认值30秒
func (sc *ServerConfig) GetMockTaskDuration() time.Duration {
	if sc.MockTaskDuration <= 0 {
		// 默认30秒
		return 30 * time.Second
	}
	return time.Duration(sc.MockTaskDuration) * time.Second
}

// GetMaxRequestBodySize 获取最大请求体大小（字节），如果未配置则返回默认值10MB
func (sc *ServerConfig) GetMaxRequestBodySize() int {
	if sc.MaxRequestBodyMB <= 0 {
		// 默认10MB
		return 10 * 1024 * 1024
	}
	return sc.MaxRequestBodyMB * 1024 * 1024
}

// GetTaskResultTTL 获取任务结果保留时间，如果未配置则返回默认值1小时
func (sc *ServerConfig) GetTaskResultTTL() time.Duration {
	if sc.TaskResultTTL <= 0 {
		// 默认1小时
		return 3600 * time.Second
	}
	return time.Duration(sc.TaskResultTTL) * time.Second
}

// GetTaskCleanInterval 获取任务清理间隔，如果未配置则返回默认值10分钟
func (sc *ServerConfig) GetTaskCleanInterval() time.Duration {
	if sc.TaskCleanInterval <= 0 {
		// 默认10分钟
		return 600 * time.Second
	}
	return time.Duration(sc.TaskCleanInterval) * time.Second
}

// GetMaxQueueSize 获取最大队列长度，如果未配置则返回默认值100
func (sc *ServerConfig) GetMaxQueueSize() int {
	if sc.MaxQueueSize <= 0 {
		return 100 // 默认100
	}
	return sc.MaxQueueSize
}

// WorkerConfig 定义显卡节点参数
type WorkerConfig struct {
	ID    string `yaml:"id"`
	URL   string `yaml:"url"`
	Owner string `yaml:"owner"`
	// 节点支持的模型列表
	Models []string `yaml:"models"`
}

// Config 总配置结构
type Config struct {
	Server  ServerConfig   `yaml:"server"`
	Workers []WorkerConfig `yaml:"workers"`
}

// LoadConfig 读取并解析配置文件
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	// 验证配置
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("配置验证失败: %w", err)
	}
	return &cfg, nil
}

// Validate 验证配置的有效性
func (cfg *Config) Validate() error {
	// 验证服务器配置
	if err := cfg.Server.Validate(); err != nil {
		return fmt.Errorf("服务器配置错误: %w", err)
	}

	// 验证Worker配置
	if len(cfg.Workers) == 0 {
		return fmt.Errorf("至少需要配置一个Worker节点")
	}

	seen := make(map[string]bool)
	for i, w := range cfg.Workers {
		if err := w.Validate(); err != nil {
			return fmt.Errorf("Worker[%d] 配置错误: %w", i, err)
		}
		// 检查Worker ID是否重复
		if seen[w.ID] {
			return fmt.Errorf("Worker ID 重复: %s", w.ID)
		}
		seen[w.ID] = true
	}

	return nil
}

// Validate 验证服务器配置
func (sc *ServerConfig) Validate() error {
	// 验证端口格式
	if sc.Port == "" {
		return fmt.Errorf("端口不能为空")
	}
	if !strings.HasPrefix(sc.Port, ":") {
		return fmt.Errorf("端口格式错误: %s (应该以 : 开头，如 :8000)", sc.Port)
	}

	// 验证调度模式
	if sc.Mode != "global" && sc.Mode != "affinity" {
		return fmt.Errorf("调度模式错误: %s (只支持 'global' 或 'affinity')", sc.Mode)
	}
	if sc.MaxQueueSize < 0 {
		return fmt.Errorf("max_queue_size 不能小于 0")
	}

	return nil
}

// Validate 验证Worker配置
func (wc *WorkerConfig) Validate() error {
	// 验证ID
	if wc.ID == "" {
		return fmt.Errorf("Worker ID 不能为空")
	}

	// 验证URL格式
	if wc.URL == "" {
		return fmt.Errorf("Worker URL 不能为空")
	}
	if _, err := url.Parse(wc.URL); err != nil {
		return fmt.Errorf("Worker URL 格式错误: %w", err)
	}

	// 验证Owner
	if wc.Owner == "" {
		return fmt.Errorf("Worker Owner 不能为空")
	}

	// 验证模型列表
	if len(wc.Models) == 0 {
		return fmt.Errorf("Worker 至少需要支持一个模型")
	}

	return nil
}
