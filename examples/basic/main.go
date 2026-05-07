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
	// 只需传 Redis 地址和 Group，其他全有默认值
	engine, err := fxmq.NewEngine("localhost:6379", "order-service")
	if err != nil {
		panic(err)
	}

	// 如果要自定义可选参数：
	// engine, err := fxmq.NewEngine("localhost:6379", "order-service",
	//     fxmq.WithConcurrency(8),
	//     fxmq.WithMaxRetry(5),
	//     fxmq.WithRetryBackoff(2*time.Second),
	//     fxmq.WithAckTimeout(60*time.Second),
	//     fxmq.WithRedisPassword("secret"),
	// )

	ctx := context.Background()

	// 订阅
	engine.Subscribe(ctx, "order.created", func(ctx context.Context, msg *fxmq.Message) error {
		var order map[string]interface{}
		if err := msg.Decode(&order); err != nil {
			return err
		}
		fmt.Printf("Processing order: %v, metadata: %v\n", order, msg.Metadata)
		return nil
	})

	// 启动后台任务
	engine.Start(ctx)

	// 发布普通消息
	msg, _ := fxmq.NewMessage("order.created", map[string]interface{}{
		"order_id": "12345",
		"amount":   99.9,
	})
	msg.SetMeta("source", "api-gateway")
	if err := engine.Publish(ctx, msg); err != nil {
		panic(err)
	}
	fmt.Println("Published:", msg.ID)

	// 发布延迟消息（5秒后投递）
	delayMsg, _ := fxmq.NewMessage("order.created", map[string]interface{}{
		"order_id": "67890",
		"amount":   199.0,
	})
	if err := engine.DelayPublish(ctx, delayMsg, 5*time.Second); err != nil {
		panic(err)
	}
	fmt.Println("Delay published, will arrive in 5s")

	// 等待退出信号
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("Shutting down...")
	engine.Shutdown()
	fmt.Println("Done.")
}
