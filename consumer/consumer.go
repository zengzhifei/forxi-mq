package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/deadletter"
	"github.com/zengzhifei/forxi-mq/internal"
	"github.com/zengzhifei/forxi-mq/log"
	"github.com/zengzhifei/forxi-mq/mq"
	"github.com/zengzhifei/forxi-mq/retry"
)

// Handler is the function signature for message processing.
type Handler func(ctx context.Context, msg *mq.Message) error

// Consumer reads from Redis Stream using consumer groups.
type Consumer struct {
	rdb         *redis.Client
	group       string
	consumer    string
	concurrency int
	logger      log.Logger
	retry       *retry.Strategy
	dlq         *deadletter.Queue

	mu      sync.Mutex
	cancels []context.CancelFunc
	wg      sync.WaitGroup
}

// New creates a new Consumer.
func New(rdb *redis.Client, cfg mq.Config, logger log.Logger, rs *retry.Strategy, dlq *deadletter.Queue) *Consumer {
	return &Consumer{
		rdb:         rdb,
		group:       cfg.Group,
		consumer:    cfg.Consumer,
		concurrency: cfg.Concurrency,
		logger:      logger,
		retry:       rs,
		dlq:         dlq,
	}
}

// Subscribe starts consuming messages from the given topic.
// It spawns cfg.Concurrency workers, each reading from the same consumer group.
func (c *Consumer) Subscribe(ctx context.Context, topic string, handler Handler) error {
	stream := internal.StreamKey(topic)

	// Ensure the consumer group exists
	err := c.rdb.XGroupCreateMkStream(ctx, stream, c.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("create consumer group: %w", err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancels = append(c.cancels, cancel)
	c.mu.Unlock()

	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		workerName := fmt.Sprintf("%s-%d", c.consumer, i)
		go c.worker(subCtx, stream, topic, workerName, handler)
	}

	c.logger.Info("subscribed",
		"topic", topic, "group", c.group, "concurrency", c.concurrency)
	return nil
}

func (c *Consumer) worker(ctx context.Context, stream, topic, name string, handler Handler) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		results, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: name,
			Streams:  []string{stream, ">"},
			Count:    1,
			Block:    time.Second,
		}).Result()

		if err != nil {
			if err == context.Canceled || ctx.Err() != nil {
				return
			}
			if err != redis.Nil {
				c.logger.Error("xreadgroup error", "stream", stream, "error", err)
				time.Sleep(time.Second)
			}
			continue
		}

		for _, s := range results {
			for _, xmsg := range s.Messages {
				c.process(ctx, topic, stream, xmsg, handler)
			}
		}
	}
}

func (c *Consumer) process(ctx context.Context, topic, stream string, xmsg redis.XMessage, handler Handler) {
	body, ok := xmsg.Values["body"].(string)
	if !ok {
		c.logger.Error("invalid message body", "stream", stream, "id", xmsg.ID)
		c.ack(ctx, stream, xmsg.ID)
		return
	}

	var msg mq.Message
	if err := json.Unmarshal([]byte(body), &msg); err != nil {
		c.logger.Error("unmarshal failed", "stream", stream, "id", xmsg.ID, "error", err)
		c.ack(ctx, stream, xmsg.ID)
		return
	}
	msg.ID = xmsg.ID

	if err := handler(ctx, &msg); err != nil {
		c.handleFailure(ctx, topic, stream, &msg, err)
		return
	}

	// Success: ACK and clear retry counter
	c.ack(ctx, stream, xmsg.ID)
	c.retry.Clear(ctx, topic, xmsg.ID)
}

func (c *Consumer) handleFailure(ctx context.Context, topic, stream string, msg *mq.Message, handlerErr error) {
	count, exhausted, err := c.retry.Attempt(ctx, topic, msg.ID)
	if err != nil {
		c.logger.Error("retry attempt failed", "topic", topic, "msg_id", msg.ID, "error", err)
		return
	}

	if exhausted {
		// Move to dead letter queue, then ACK
		reason := fmt.Sprintf("max retries reached (%d): %v", count, handlerErr)
		_ = c.dlq.Push(ctx, msg, reason)
		c.ack(ctx, stream, msg.ID)
		c.retry.Clear(ctx, topic, msg.ID)
		return
	}

	// Backoff then let it be re-claimed by recovery or re-read
	backoff := c.retry.BackoffDuration(count)
	c.logger.Warn("message processing failed, will retry",
		"topic", topic, "msg_id", msg.ID,
		"attempt", count, "backoff", backoff,
		"error", handlerErr)

	// Sleep for backoff period (respecting context cancellation)
	select {
	case <-time.After(backoff):
	case <-ctx.Done():
	}
	// Don't ACK — leave it pending for XAUTOCLAIM recovery
}

func (c *Consumer) ack(ctx context.Context, stream, id string) {
	if err := c.rdb.XAck(ctx, stream, c.group, id).Err(); err != nil {
		c.logger.Error("ack failed", "stream", stream, "id", id, "error", err)
	}
}

// Shutdown gracefully stops all workers and waits for them to finish.
func (c *Consumer) Shutdown() {
	c.mu.Lock()
	for _, cancel := range c.cancels {
		cancel()
	}
	c.mu.Unlock()
	c.wg.Wait()
}
