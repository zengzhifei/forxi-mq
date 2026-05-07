# forxi-mq

基于 Redis Stream 的轻量级消息队列框架，适用于 Go 项目。

## 环境要求

- Go 1.24+
- Redis 6.2+

## 安装

```bash
go get github.com/zengzhifei/forxi-mq
```

## 快速开始

```go
engine, err := fxmq.NewEngine("localhost:6379", "my-service")

// 订阅
engine.Subscribe(ctx, "order.created", func(ctx context.Context, msg *fxmq.Message) error {
    var order Order
    msg.Decode(&order)
    // 处理业务...
    return nil  // 返回 error 会触发重试
})

// 启动
engine.Start(ctx)

// 发布
msg, _ := fxmq.NewMessage("order.created", payload)
engine.Publish(ctx, msg)

// 延迟发布
engine.DelayPublish(ctx, msg, 5*time.Second)

// 退出
engine.Shutdown()
```

## API

### NewEngine（必传参数）

```go
engine, err := fxmq.NewEngine(redisAddr, group string, opts ...Option)
```

| 参数 | 说明 |
|------|------|
| `redisAddr` | Redis 地址，如 `"localhost:6379"` |
| `group` | 消费者组名称，如 `"order-service"` |

Consumer 名称默认取 `HOSTNAME` 环境变量，多实例部署时自动唯一（K8s Pod 天然不同）。

### 可选配置（Options）

所有可选项都有合理的默认值，不传也能直接用：

```go
engine, err := fxmq.NewEngine("localhost:6379", "my-service",
    fxmq.WithConcurrency(16),              // 每个 topic 的并发 worker 数（默认 8）
    fxmq.WithMaxRetry(5),                  // 最大重试次数（默认 3）
    fxmq.WithRetryBackoff(2*time.Second),  // 重试基础退避（默认 2s，指数递增）
    fxmq.WithAckTimeout(60*time.Second),   // 消息超时时间（默认 60s）
    fxmq.WithStreamMaxLen(10000),          // Stream 最大长度（默认 0 不限）
    fxmq.WithRedisPassword("secret"),      // Redis 密码
    fxmq.WithRedisDB(1),                   // Redis DB
    fxmq.WithLogger(myLogger),             // 自定义日志
    fxmq.WithRedisClient(existingClient),  // 复用已有 Redis 连接
)
```

### 发布消息

```go
msg, _ := fxmq.NewMessage("topic.name", payload)
msg.SetMeta("key", "value")  // 可选 metadata
engine.Publish(ctx, msg)
```

### 延迟发布

```go
msg, _ := fxmq.NewMessage("topic.name", payload)
engine.DelayPublish(ctx, msg, 30*time.Second)
```

### 订阅消费

```go
engine.Subscribe(ctx, "topic.name", func(ctx context.Context, msg *fxmq.Message) error {
    var data MyStruct
    if err := msg.Decode(&data); err != nil {
        return err  // 返回 error 触发重试
    }
    // 业务逻辑...
    return nil  // 成功自动 ACK
})

engine.Start(ctx)  // 启动延迟轮询 + 超时恢复
```

### 死信队列

超过最大重试次数的消息自动进入死信队列，可查看：

```go
messages, _ := engine.DLQ.List(ctx, "topic.name", 100)
```

## 多实例部署

多个 Pod 部署同一个服务，不需要改代码。Consumer 默认用 HOSTNAME，自动唯一：

```go
// 每个 Pod 代码完全一样
engine, _ := fxmq.NewEngine("redis:6379", "order-service")
```

## 多服务消费同一 Topic

不同服务用不同的 Group，各自独立收到全量消息：

```go
// 短信服务
engine, _ := fxmq.NewEngine("redis:6379", "sms-service")
engine.Subscribe(ctx, "order.created", sendSMS)

// 统计服务
engine, _ := fxmq.NewEngine("redis:6379", "stats-service")
engine.Subscribe(ctx, "order.created", updateStats)
```

## 自定义日志

实现以下接口：

```go
type Logger interface {
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
    Debug(msg string, args ...any)
}
```

## License

MIT
