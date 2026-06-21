# Ollama Gateway

[中文](README.zh-CN.md) | English

Ollama Gateway is an asynchronous gateway for Ollama-compatible model services. It accepts inference requests, stores them as tasks, and dispatches them to configured Ollama workers according to model capability, queue state, and resource ownership.

The project is intended for self-hosted GPU environments where multiple teams share a limited set of Ollama workers. It provides a small HTTP control plane around task submission, polling, cancellation, admission control, and scheduling.

## Core Capabilities

- **Asynchronous task lifecycle**: submit a request, receive a task ID, poll status or result, cancel queued tasks, and expire terminal task records by TTL.
- **Model-aware dispatch**: workers declare the models they serve, and the scheduler only selects workers compatible with the requested model.
- **Two scheduling policies**: global greedy scheduling for throughput-oriented queues, and affinity stealing for owner-aware resource sharing.
- **Admission control**: rejects duplicate task IDs, unsupported routes, oversized bodies, and full queues.
- **Mock execution mode**: runs the full gateway flow without real Ollama backends.
- **Structured logs**: writes `event=... key=value` logs for filtering and troubleshooting.

## Typical Use Case

The gateway is useful when several business groups or users run their own models on dedicated GPUs, while some models are provided as shared capacity. Dedicated resources should serve their owner first, but idle capacity should still be available to other queues when policy allows it.

This repository focuses on scheduling and task lifecycle management. It is not a complete inference platform: authentication, persistence, metrics, and GPU memory planning are outside the current scope.

## Architecture

```text
Client
  |
  | POST /api/generate
  v
Gateway Handler
  |  validate the request, create TaskInfo, and enqueue Job
  v
Scheduler
  |  select a queued task and an eligible idle worker
  v
Execution Goroutine
  |  POST {worker.url}/api/generate
  v
Ollama Worker
  |  return the response through ResultCh
  v
Task Manager
  |  store status, timestamps, result, error, cancellation reason
  v
Client
  |  GET /api/task, GET /api/task/result, DELETE /api/task/cancel
```

## Scheduling Policies

### Global Greedy

Global greedy mode is selected with `server.mode: "global"`.

The policy keeps one priority queue per model. When a worker is idle, it checks the queues for models it supports and takes the best queue head. This avoids head-of-line blocking between models while preserving ordering within each model queue.

Within one model, ordering is:

1. VIP tasks before batch tasks.
2. FIFO order within the same priority.

`X-Task-Type: batch` marks a request as low priority. Any other value, including an omitted header, is treated as VIP.

### Affinity Stealing

Affinity stealing mode is selected with `server.mode: "affinity"`.

The policy builds queues from the worker topology:

- Workers whose `owner` is `shared` provide public model capacity.
- Workers with any other `owner` provide owner-specific capacity.
- Queues are keyed by `(owner, model)`.

Task routing is deterministic:

1. If the requester owns a worker for the requested model, the task enters `(requester, model)`.
2. Otherwise, if the requested model has a `shared` worker, the task enters `(shared, model)`.
3. Otherwise, the request is rejected.

Worker selection is owner-first:

- A worker first serves its own `(owner, model)` queues.
- A `shared` worker may execute tasks from business-owned queues.
- A business-owned worker may execute tasks from other business-owned queues for the same model.
- A business-owned worker does not execute tasks from the shared queue by default.

Affinity mode ignores `X-Task-Type`. Each `(owner, model)` queue is ordered by enqueue time, while owner-first and work-stealing rules take precedence across queues. Running tasks are not preempted.

## Configuration

The server reads `config.yaml` from the process working directory.

```yaml
server:
  port: ":1103"
  mode: "affinity"              # "global" or "affinity"
  mock: true                    # true: simulate execution without Ollama
  mock_task_duration: 30        # seconds
  request_timeout: 30           # upstream Ollama request timeout, seconds
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

`workers[].url` is the base Ollama service address. The gateway forwards requests to `{url}/api/generate` and sends the original request body unchanged.

Model names used in `X-Model` should match the names configured under `workers[].models`.

## Quick Start

### 1. Start the Gateway

```bash
go run ./cmd/server
```

With `mock: true`, no Ollama backend is required. Set `mock: false` when the configured workers are available.

### 2. Submit a Task

```bash
curl -X POST "http://localhost:1103/api/generate" \
  -H "Content-Type: application/json" \
  -H "X-User: userA" \
  -H "X-Model: Qwen3:30b-a3b-thinking-2507-q4_K_M" \
  -H "X-Request-ID: demo-001" \
  -d '{"model":"Qwen3:30b-a3b-thinking-2507-q4_K_M","prompt":"hello"}'
```

Example response:

```json
{
  "task_id": "demo-001",
  "status": "queued",
  "queue_position": 1
}
```

### 3. Query Status and Result

```bash
curl "http://localhost:1103/api/task?id=demo-001"
curl "http://localhost:1103/api/task/result?id=demo-001"
```

`/api/task/result` returns `202 Accepted` while a task is queued or running. It returns the upstream response body after completion.

For many tasks, query status in batches:

```bash
curl -X POST "http://localhost:1103/api/tasks/status" \
  -H "Content-Type: application/json" \
  -d '{"ids":["demo-001","demo-002"]}'
```

## API

| Method and path | Description |
| --- | --- |
| `POST /api/generate` | Submit an asynchronous inference task. Returns `202 Accepted` with task metadata. |
| `GET /api/task?id=<task_id>` | Return task status, queue position, timing fields, and error or cancellation reason when present. |
| `GET /api/task/result?id=<task_id>` | Return the completed result, or `202` while the task is pending. |
| `DELETE /api/task/cancel?id=<task_id>` | Cancel a queued task. Running tasks cannot be removed from the worker once dispatched. |
| `GET /api/tasks` | List tasks. Optional `?status=` filter; optional `X-User` header limits results to that requester. |
| `POST /api/tasks/status` | Return status metadata for a batch of task IDs. Does not include result bodies. |
| `GET /api/queue/status` | Return queue length, worker counts, task counts, and active policy. |

### Submission Headers

| Header | Required | Description |
| --- | --- | --- |
| `X-User` | No | Request owner. Defaults to `default`. Used by affinity routing and task listing. |
| `X-Model` | Yes | Model name used by the scheduler. Use the configured model name. |
| `X-Task-Type` | No | `batch` lowers priority in global mode. Ignored in affinity mode. |
| `X-Request-ID` | No | Client-provided task ID. A generated ID is used when absent. Duplicate IDs return `409`. |
| `X-Callback-URL` | No | Optional webhook URL called after task completion or failure. |

## Admission Control

Before enqueueing a request, the gateway checks route eligibility, request body size, task ID uniqueness, and queue capacity. Rejections caused by a full queue return `503` with a `Retry-After` header.

## Development

```bash
go test ./...
go test ./... -cover
```

Project layout:

```text
cmd/server/          process entry point and HTTP route registration
config/              YAML loading and validation
internal/gateway/    HTTP handlers, scheduler loop, task manager
internal/model/      task, worker, and job data structures
internal/policy/     global greedy and affinity stealing policies
internal/logger/     structured log helpers
```

## Current Limitations

- Task state and results are stored in memory; restart drops all task records, including queued, running, and terminal tasks.
- There is no built-in authentication or authorization. Deploy behind an internal gateway or auth layer when needed.
- There is no persistence, leader election, or high-availability coordination.
- The scheduler treats each worker as one busy or idle slot; it does not model GPU memory, model residency, or model load/unload cost.
- The gateway stores and returns complete response bodies. It is not a streaming passthrough proxy.
- There is no metrics endpoint yet; operational visibility currently depends on logs.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
