package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

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
	ackTimeout  time.Duration
	logger      log.Logger
	retry       *retry.Strategy

	mu      sync.Mutex
	cancels []context.CancelFunc
	wg      sync.WaitGroup
}

// New creates a new Consumer.
func New(rdb *redis.Client, cfg mq.Config, logger log.Logger, rs *retry.Strategy) *Consumer {
	return &Consumer{
		rdb:         rdb,
		group:       cfg.Group,
		consumer:    cfg.Consumer,
		concurrency: cfg.Concurrency,
		ackTimeout:  cfg.AckTimeout,
		logger:      logger,
		retry:       rs,
	}
}

// Subscribe starts consuming messages from the given topic.
// It spawns cfg.Concurrency workers reading new messages (">") and one dedicated
// goroutine reading pending messages ("0") that were claimed by Recovery.
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

	// Workers for new messages
	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		workerName := fmt.Sprintf("%s-%d", c.consumer, i)
		go c.worker(subCtx, stream, topic, workerName, handler)
	}

	// Single goroutine for pending messages (claimed by Recovery via XAUTOCLAIM)
	c.wg.Add(1)
	go c.pendingReader(subCtx, stream, topic, c.consumer, handler)

	c.logger.Info("subscribed",
		"topic", topic, "group", c.group, "concurrency", c.concurrency)
	return nil
}

// worker reads only new messages using ">".
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

// pendingReader reads pending messages (start="0") assigned to this consumer.
// Recovery claims timed-out messages to this consumer via XAUTOCLAIM, so this
// goroutine picks them up for reprocessing.
//
// Key design: each message is only attempted ONCE per Recovery claim cycle.
// After a handler failure, the message ID is recorded locally and skipped until
// Recovery increments the retry counter (indicating a new claim cycle).
func (c *Consumer) pendingReader(ctx context.Context, stream, topic, name string, handler Handler) {
	defer c.wg.Done()

	// Track failed message IDs and the retry count at failure time.
	// Only re-attempt when Recovery increments the count (new claim cycle).
	type failedEntry struct {
		countAtFailure int
	}
	failed := make(map[string]failedEntry)
	const maxFailedEntries = 1000 // safety cap to prevent unbounded growth

	const pollInterval = 2 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		results, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: name,
			Streams:  []string{stream, "0"},
			Count:    10,
		}).Result()

		if err != nil {
			if err == context.Canceled || ctx.Err() != nil {
				return
			}
			if err != redis.Nil {
				c.logger.Error("xreadgroup pending error", "stream", stream, "error", err)
			}
			time.Sleep(pollInterval)
			continue
		}

		hasMessages := false
		for _, s := range results {
			for _, xmsg := range s.Messages {
				hasMessages = true

				// Check if we've already failed on this message in the current cycle
				if entry, skipped := failed[xmsg.ID]; skipped {
					// Check if Recovery has incremented the count (new claim cycle)
					currentCount, _ := c.retry.GetCount(ctx, topic, xmsg.ID)
					if currentCount <= entry.countAtFailure {
						// Same cycle — skip
						continue
					}
					// Recovery has incremented — new cycle, retry is allowed
					delete(failed, xmsg.ID)
				}

				if !c.process(ctx, topic, stream, xmsg, handler) {
					// Handler failed — record current retry count and skip until next cycle
					count, _ := c.retry.GetCount(ctx, topic, xmsg.ID)
					failed[xmsg.ID] = failedEntry{countAtFailure: count}
				} else {
					// Success or ACKed (dead/invalid) — remove from tracking
					delete(failed, xmsg.ID)
				}
			}
		}

		if !hasMessages {
			// No pending messages — clear failed tracking (messages were ACKed externally)
			failed = make(map[string]failedEntry)
		}

		// Safety: if map grows too large (shouldn't happen normally), clear it.
		// Worst case: some messages get retried once extra (at-least-once is fine).
		if len(failed) > maxFailedEntries {
			failed = make(map[string]failedEntry)
		}

		time.Sleep(pollInterval)
	}
}

func (c *Consumer) process(ctx context.Context, topic, stream string, xmsg redis.XMessage, handler Handler) bool {
	body, ok := xmsg.Values["body"].(string)
	if !ok {
		c.logger.Error("invalid message body", "stream", stream, "id", xmsg.ID)
		c.ack(ctx, stream, xmsg.ID)
		return true // not a handler failure, just invalid data
	}

	var msg mq.Message
	if err := json.Unmarshal([]byte(body), &msg); err != nil {
		c.logger.Error("unmarshal failed", "stream", stream, "id", xmsg.ID, "error", err)
		c.ack(ctx, stream, xmsg.ID)
		return true
	}
	msg.ID = xmsg.ID

	// Check if already marked dead (Recovery moved to DLQ but ACK race)
	if c.retry.IsDead(ctx, topic, xmsg.ID) {
		c.ack(ctx, stream, xmsg.ID)
		return true
	}

	// Enforce processing timeout to prevent handler from blocking indefinitely.
	// Timeout is set to ackTimeout so that if handler hangs, the goroutine is freed
	// before Recovery tries to claim the message again.
	handlerCtx, cancel := context.WithTimeout(ctx, c.ackTimeout)
	defer cancel()

	if err := handler(handlerCtx, &msg); err != nil {
		c.handleFailure(ctx, topic, stream, &msg, err)
		return false
	}

	// Success: ACK and clear retry counter
	c.ack(ctx, stream, xmsg.ID)
	c.retry.Clear(ctx, topic, xmsg.ID)
	return true
}

func (c *Consumer) handleFailure(ctx context.Context, topic, stream string, msg *mq.Message, handlerErr error) {
	// Don't ACK — leave in PEL for Recovery to handle via XAUTOCLAIM.
	// Recovery will increment retry counter and eventually move to DLQ.
	c.logger.Warn("message processing failed, leaving for recovery",
		"topic", topic, "msg_id", msg.ID, "error", handlerErr)
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
