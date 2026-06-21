# Ollama Gateway

中文 | [English](README.md)

Ollama Gateway 是一个面向 Ollama 兼容模型服务的异步推理网关。它接收推理请求，将请求保存为任务，并根据模型能力、队列状态和资源归属，将任务调度到配置好的 Ollama Worker。

本项目主要面向自建 GPU 场景：多个团队或用户共享一组有限的 Ollama Worker，其中部分资源归业务方专用，部分资源作为公共能力提供。网关围绕任务提交、状态轮询、结果获取、取消、准入控制和调度策略提供一层 HTTP 控制面。

## 核心能力

- **异步任务生命周期**：提交请求后返回任务 ID，支持查询状态、轮询结果、取消排队任务，并按 TTL 清理终态任务记录。
- **模型感知调度**：Worker 显式声明可服务的模型，调度器只会选择与目标模型匹配的 Worker。
- **两种调度策略**：全局贪心适合吞吐优先场景，亲和窃取适合有资源归属要求的共享场景。
- **准入控制**：拒绝重复任务 ID、不可路由模型、超大请求体和满队列。
- **Mock 执行模式**：无需真实 Ollama 后端即可验证完整网关流程。
- **结构化日志**：输出 `event=... key=value` 格式日志，便于检索和排查。

## 适用场景

当多个业务方在自建 GPU 上运行不同模型，并希望同时满足“资源归属优先”和“空闲资源可复用”时，可以使用该网关。典型配置是：业务方各自拥有一部分 Worker，同时存在若干 `shared` Worker 提供公共模型能力。

本仓库聚焦调度和任务生命周期管理，不覆盖完整推理平台所需的所有能力。鉴权、持久化、指标采集和 GPU 显存规划目前不属于内置范围。

## 架构

```text
Client
  |
  | POST /api/generate
  v
Gateway Handler
  |  校验请求、创建 TaskInfo、将 Job 入队
  v
Scheduler
  |  选择一个排队任务和一个可执行该任务的空闲 Worker
  v
Execution Goroutine
  |  POST {worker.url}/api/generate
  v
Ollama Worker
  |  响应通过 ResultCh 返回
  v
Task Manager
  |  保存状态、时间戳、结果、错误和取消原因
  v
Client
  |  GET /api/task, GET /api/task/result, DELETE /api/task/cancel
```

## 调度策略

### Global Greedy

在 `server.mode: "global"` 下启用。

该策略按模型维护独立优先级队列。Worker 空闲时，只检查自身支持的模型队列，并从可执行的队头任务中选择优先级最高的任务。这样可以避免不同模型之间的队头阻塞，同时保留同一模型内的稳定顺序。

同一模型内的排序规则为：

1. VIP 任务优先于 batch 任务。
2. 同优先级内按 FIFO 执行。

请求头 `X-Task-Type: batch` 会将任务标记为低优先级。未设置或设置为其他值时按 VIP 处理。

### Affinity Stealing

在 `server.mode: "affinity"` 下启用。

该策略根据 Worker 拓扑创建队列：

- `owner` 为 `shared` 的 Worker 提供公共模型能力。
- 其他 `owner` 表示业务方或用户专属资源。
- 队列按 `(owner, model)` 划分。

任务路由规则如下：

1. 如果请求方拥有目标模型的 Worker，任务进入 `(requester, model)` 队列。
2. 否则，如果目标模型存在 `shared` Worker，任务进入 `(shared, model)` 队列。
3. 以上都不满足时，拒绝请求。

Worker 选择遵循“归属优先”：

- Worker 优先消费自己的 `(owner, model)` 队列。
- `shared` Worker 可以执行各业务队列中的任务。
- 业务 Worker 可以执行其他业务队列中相同模型的任务。
- 业务 Worker 默认不执行公共队列中的任务。

亲和模式不使用 `X-Task-Type` 区分优先级。每个 `(owner, model)` 队列内部按入队时间处理，跨队列仍以归属优先和 Work-Stealing 规则为准。已经开始执行的任务不会被抢占。

## 配置

服务启动时从当前工作目录读取 `config.yaml`。

```yaml
server:
  port: ":1103"
  mode: "affinity"              # "global" 或 "affinity"
  mock: true                    # true 表示不请求真实 Ollama
  mock_task_duration: 30        # 秒
  request_timeout: 30           # 请求上游 Ollama 的超时，秒
  max_request_body_mb: 10
  task_result_ttl: 3600
  task_clean_interval: 600
  max_queue_size: 100

workers:
  - id: "qwen3:14b"
    url: "http://127.0.0.1:21434"
    owner: "shared"
    models: [ "qwen3:14b" ]

  - id: "thinking-userA"
    url: "http://127.0.0.1:11410"
    owner: "userA"
    models: [ "Qwen3:30b-a3b-thinking-2507-q4_K_M" ]
```

`workers[].url` 是 Ollama 服务基础地址。网关会将请求转发到 `{url}/api/generate`，请求体保持原样转发。

提交任务时，`X-Model` 应使用 `workers[].models` 中配置的模型名称。

## 快速开始

### 1. 启动网关

```bash
go run ./cmd/server
```

当 `mock: true` 时，不需要真实 Ollama 后端。接入真实 Worker 时，将 `mock` 改为 `false`。

### 2. 提交任务

```bash
curl -X POST "http://localhost:1103/api/generate" \
  -H "Content-Type: application/json" \
  -H "X-User: userA" \
  -H "X-Model: Qwen3:30b-a3b-thinking-2507-q4_K_M" \
  -H "X-Request-ID: demo-001" \
  -d '{"model":"Qwen3:30b-a3b-thinking-2507-q4_K_M","prompt":"hello"}'
```

示例响应：

```json
{
  "task_id": "demo-001",
  "status": "queued",
  "queue_position": 1
}
```

### 3. 查询状态和结果

```bash
curl "http://localhost:1103/api/task?id=demo-001"
curl "http://localhost:1103/api/task/result?id=demo-001"
```

任务排队或执行中时，`/api/task/result` 返回 `202 Accepted`；任务完成后返回上游响应体。

任务较多时，可以批量查询状态：

```bash
curl -X POST "http://localhost:1103/api/tasks/status" \
  -H "Content-Type: application/json" \
  -d '{"ids":["demo-001","demo-002"]}'
```

## API

| 方法和路径 | 说明 |
| --- | --- |
| `POST /api/generate` | 提交异步推理任务，返回 `202 Accepted` 和任务元数据。 |
| `GET /api/task?id=<task_id>` | 查询任务状态、队列位置、时间字段，以及错误或取消原因。 |
| `GET /api/task/result?id=<task_id>` | 获取已完成任务的结果；任务未完成时返回 `202`。 |
| `DELETE /api/task/cancel?id=<task_id>` | 取消排队中的任务。任务一旦派发到 Worker，当前实现不再从 Worker 移除。 |
| `GET /api/tasks` | 列出任务。支持 `?status=` 过滤；可用 `X-User` 只查看该请求方任务。 |
| `POST /api/tasks/status` | 批量查询任务状态元数据，不返回结果正文。 |
| `GET /api/queue/status` | 返回队列长度、Worker 数量、任务统计和当前策略。 |

### 提交任务请求头

| 请求头 | 必填 | 说明 |
| --- | --- | --- |
| `X-User` | 否 | 请求归属，默认 `default`。用于亲和路由和任务列表过滤。 |
| `X-Model` | 是 | 调度器使用的目标模型名称，应与配置中的模型名称一致。 |
| `X-Task-Type` | 否 | 在全局模式中，`batch` 表示低优先级；亲和模式忽略该字段。 |
| `X-Request-ID` | 否 | 客户端指定的任务 ID。未设置时由网关生成；重复 ID 返回 `409`。 |
| `X-Callback-URL` | 否 | 可选 Webhook 地址，任务完成或失败后回调。 |

## 准入控制

任务入队前，网关会检查路由是否可达、请求体大小、任务 ID 是否唯一和队列容量。因队列已满被拒绝时，响应状态为 `503`，并带有 `Retry-After` 头。

## 开发

```bash
go test ./...
go test ./... -cover
```

项目结构：

```text
cmd/server/          进程入口和 HTTP 路由注册
config/              YAML 配置加载与校验
internal/gateway/    HTTP Handler、调度循环、任务管理器
internal/model/      Task、Worker、Job 等数据结构
internal/policy/     全局贪心和亲和窃取调度策略
internal/logger/     结构化日志工具
```

## 当前限制

- 任务状态和结果保存在内存中；进程重启会丢失全部任务记录，包括排队、执行中和各类终态任务。
- 未内置认证和授权；需要时应部署在内部网关或鉴权层之后。
- 未提供持久化、主备切换或高可用协调。
- 调度器将每个 Worker 视为一个忙闲槽位，不建模 GPU 显存、模型驻留和模型加载/卸载成本。
- 网关保存并返回完整响应体，不是流式透传代理。
- 暂无 metrics 接口；当前主要依赖结构化日志进行运行观测。

## 许可证

本项目采用 MIT License。详情见 [LICENSE](LICENSE)。
