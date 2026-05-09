package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	fxmq "github.com/zengzhifei/forxi-mq"
)

func main() {
	engine, err := fxmq.NewEngine("localhost:6379", "test-group",
		fxmq.WithDashboard(":9091"),
	)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	engine.Subscribe(ctx, "track.access", func(ctx context.Context, msg *fxmq.Message) error {
		fmt.Printf("received: %s\n", msg.ID)
		return nil
	})

	engine.Start(ctx)
	fmt.Println("Dashboard running at http://localhost:9091")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("Shutting down...")
	engine.Shutdown()
}
