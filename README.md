<p align="center">
  <h1 align="center">forxi-mq</h1>
  <p align="center">基于 Redis Stream 的轻量级消息队列框架</p>
</p>

<p align="center">
  <a href="https://github.com/zengzhifei/forxi-mq/releases"><img src="https://img.shields.io/github/v/release/zengzhifei/forxi-mq" alt="Release"></a>
  <a href="https://pkg.go.dev/github.com/zengzhifei/forxi-mq"><img src="https://pkg.go.dev/badge/github.com/zengzhifei/forxi-mq.svg" alt="Go Reference"></a>
  <a href="https://goreportcard.com/report/github.com/zengzhifei/forxi-mq"><img src="https://goreportcard.com/badge/github.com/zengzhifei/forxi-mq" alt="Go Report Card"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/zengzhifei/forxi-mq" alt="License"></a>
</p>

---

## 为什么选择 forxi-mq

- **轻量** — 不依赖额外 Broker，Redis 即消息服务端
- **开箱即用** — 两行代码接入，所有配置都有合理默认值
- **功能完整** — 延迟队列、重试、死信队列、优雅退出
- **可观测** — 内置 Web Dashboard，实时监控队列状态
- **生产可靠** — Consumer Group 保证消息不丢，XAUTOCLAIM 自动恢复超时消息

适用于中小型 Go 项目中需要异步解耦的场景：发通知、异步任务、事件驱动等。

## 环境要求

- Go 1.24+
- Redis 6.2+

## 安装

```bash
go get github.com/zengzhifei/forxi-mq
```

## 快速开始

```go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"
    "time"

    fxmq "github.com/zengzhifei/forxi-mq"
)

func main() {
    engine, err := fxmq.NewEngine("localhost:6379", "my-service")
    if err != nil {
        panic(err)
    }

    ctx := context.Background()

    // 订阅 topic
    engine.Subscribe(ctx, "order.created", func(ctx context.Context, msg *fxmq.Message) error {
        var order map[string]any
        msg.Decode(&order)
        fmt.Println("处理订单:", order)
        return nil
    })

    engine.Start(ctx)

    // 发布消息
    msg, _ := fxmq.NewMessage("order.created", map[string]any{"order_id": "12345"})
    engine.Publish(ctx, msg)

    // 等待退出信号
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit

    engine.Shutdown()
}
```

## 核心功能

### 发布消息

```go
msg, _ := fxmq.NewMessage("topic.name", payload)
msg.SetMeta("trace_id", "abc123")  // 可选 metadata
engine.Publish(ctx, msg)
```

### 延迟消息

消息在指定时间后才会投递给消费者：

```go
msg, _ := fxmq.NewMessage("order.timeout", orderData)
engine.DelayPublish(ctx, msg, 30*time.Minute)
```

### 订阅消费

```go
engine.Subscribe(ctx, "topic.name", func(ctx context.Context, msg *fxmq.Message) error {
    var data MyStruct
    if err := msg.Decode(&data); err != nil {
        return err  // 返回 error 触发自动重试
    }
    // 业务逻辑...
    return nil  // 成功后自动 ACK
})
```

### 死信队列

超过最大重试次数的消息自动进入死信队列：

```go
// 查看死信消息
messages, _ := engine.DLQ.List(ctx, "topic.name", 100)
```

也可以通过 Dashboard 一键重新投递。

### Web Dashboard

内置可视化监控面板，一行开启：

```go
engine, _ := fxmq.NewEngine("localhost:6379", "my-service",
    fxmq.WithDashboard(":9090"),
)
```

访问 `http://localhost:9090` 查看：

- 所有 Topic 概览（消息量、Pending、Dead Letter）
- Consumer 列表及状态
- 死信消息查看与重新投递
- 延迟队列待投递消息

## 配置

### 必传参数

```go
engine, err := fxmq.NewEngine(redisAddr, group, opts...)
```

| 参数 | 说明 | 示例 |
|------|------|------|
| `redisAddr` | Redis 地址 | `"localhost:6379"` |
| `group` | 消费者组 | `"order-service"` |

### 可选配置

通过 `WithXxx` 按需设置，全部有合理默认值：

```go
engine, _ := fxmq.NewEngine("localhost:6379", "my-service",
    fxmq.WithConcurrency(16),                   // 并发 worker 数（默认 8）
    fxmq.WithMaxRetry(5),                       // 最大重试次数（默认 3）
    fxmq.WithRetryBackoff(2*time.Second),       // 重试退避基数（默认 2s）
    fxmq.WithAckTimeout(60*time.Second),        // 消息超时（默认 60s）
    fxmq.WithRetention(7*24*time.Hour),         // 消息保留时间（默认不限）
    fxmq.WithStreamMaxLen(50000),               // Stream 最大条数（默认不限）
    fxmq.WithDashboard(":9090"),                // 开启 Dashboard
    fxmq.WithRedisPassword("secret"),           // Redis 密码
    fxmq.WithRedisDB(1),                        // Redis DB
    fxmq.WithLogger(customLogger),              // 自定义日志
    fxmq.WithRedisClient(existingClient),       // 复用 Redis 连接
)
```

### 默认值一览

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| Concurrency | 8 | 每个 Topic 的消费 worker 数 |
| MaxRetry | 3 | 超过后进入死信队列 |
| RetryBackoff | 2s | 指数退避：2s → 4s → 8s |
| AckTimeout | 60s | 超时未 ACK 触发恢复 |
| Retention | 0 | 0 表示永久保留 |
| StreamMaxLen | 0 | 0 表示不裁剪 |
| Consumer | $HOSTNAME | 自动取主机名 |

## 部署

### 单实例

```go
engine, _ := fxmq.NewEngine("redis:6379", "my-service")
```

### 多实例（水平扩展）

多个 Pod 代码完全相同，Consumer 自动取 HOSTNAME 保证唯一：

```go
// 所有 Pod 相同代码，自动竞争消费
engine, _ := fxmq.NewEngine("redis:6379", "order-service")
engine.Subscribe(ctx, "order.created", handleOrder)
```

### 多服务订阅同一 Topic（广播）

不同 Group 各自独立收到全量消息：

```go
// 服务 A：处理订单
engine, _ := fxmq.NewEngine("redis:6379", "order-service")
engine.Subscribe(ctx, "order.created", processOrder)

// 服务 B：发通知
engine, _ := fxmq.NewEngine("redis:6379", "notify-service")
engine.Subscribe(ctx, "order.created", sendNotification)
```

## 消息保留策略

支持按条数和按时间两种策略，可单独使用或组合：

```go
// 按条数
fxmq.WithStreamMaxLen(10000)

// 按时间
fxmq.WithRetention(7 * 24 * time.Hour)

// 组合（任一条件触发即裁剪）
fxmq.WithStreamMaxLen(50000)
fxmq.WithRetention(7 * 24 * time.Hour)
```

## 消息可靠性

forxi-mq 提供 **at-least-once** 语义：

| 保障 | 机制 |
|------|------|
| 不丢消息 | Consumer Group + Pending List + XAUTOCLAIM 恢复 |
| 自动重试 | handler 返回 error 触发指数退避重试 |
| 死信兜底 | 超过重试次数进入 DLQ，不会无限循环 |
| 优雅退出 | Context 取消 + WaitGroup 等待所有 worker 完成 |

> **注意**：不重复需要业务侧保证幂等（通过 `msg.ID` 或业务唯一键去重）。

## 自定义日志

实现 `log.Logger` 接口即可替换内置日志：

```go
type Logger interface {
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
    Debug(msg string, args ...any)
}
```

兼容 `slog`、`zap`（需简单封装）、`zerolog` 等主流日志库。

## 项目结构

```
forxi-mq/
├── engine.go              # 入口、Options、Engine
├── mq/                    # 核心类型（Config、Message）
├── producer/              # 消息发布
├── consumer/              # 消费者 + Worker Pool
├── retry/                 # 重试策略（指数退避）
├── deadletter/            # 死信队列
├── delay/                 # 延迟队列（ZSET + Lua）
├── recovery/              # 超时消息恢复（XAUTOCLAIM）
├── dashboard/             # Web Dashboard（Vue 3 + Element Plus）
├── log/                   # 日志接口
├── internal/              # Redis Key 命名
└── examples/              # 使用示例
```

## Contributing

欢迎提交 Issue 和 Pull Request。

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/xxx`)
3. 提交更改 (`git commit -m 'feat: add xxx'`)
4. 推送分支 (`git push origin feature/xxx`)
5. 创建 Pull Request

## License

[MIT](LICENSE)
