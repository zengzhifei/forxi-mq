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

// Lua script for safe lock release: only delete if value matches.
var releaseLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

// Handler is the function signature for message processing.
type Handler func(ctx context.Context, msg *mq.Message) error

// Consumer reads from Redis Stream using consumer groups.
type Consumer struct {
	rdb         *redis.Client
	group       string
	consumer    string
	concurrency int
	ackTimeout  time.Duration
	lockTTL     time.Duration
	logger      log.Logger
	retry       *retry.Strategy

	mu      sync.Mutex
	cancels []context.CancelFunc
	wg      sync.WaitGroup

	// retryCh receives messages claimed by Recovery for reprocessing.
	// This avoids XREADGROUP "0" which resets idle time.
	retryCh   chan retryMessage
	retryOnce sync.Once
}

// retryMessage is a message claimed by Recovery that needs reprocessing.
type retryMessage struct {
	topic  string
	stream string
	xmsg   redis.XMessage
}

// New creates a new Consumer.
func New(rdb *redis.Client, cfg mq.Config, logger log.Logger, rs *retry.Strategy) *Consumer {
	lockTTL := time.Duration(cfg.MaxRetry)*(cfg.AckTimeout+cfg.RecoveryInterval) + cfg.LockBuffer
	return &Consumer{
		rdb:         rdb,
		group:       cfg.Group,
		consumer:    cfg.Consumer,
		concurrency: cfg.Concurrency,
		ackTimeout:  cfg.AckTimeout,
		lockTTL:     lockTTL,
		logger:      logger,
		retry:       rs,
		retryCh:     make(chan retryMessage, 100),
	}
}

// Subscribe starts consuming messages from the given topic.
func (c *Consumer) Subscribe(ctx context.Context, topic string, handler Handler) error {
	stream := internal.StreamKey(topic)

	// Ensure the consumer group exists
	err := c.rdb.XGroupCreateMkStream(ctx, stream, c.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("create consumer group: %w", err)
	}

	// Register topic — XGroupCreateMkStream creates the stream if new
	c.rdb.SAdd(ctx, internal.TopicsSetKey(), topic)

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

	// Single retry worker that processes messages from the retry channel.
	// Started once across all subscriptions.
	c.retryOnce.Do(func() {
		c.wg.Add(1)
		go c.retryWorker(subCtx, handler)
	})

	c.logger.Info("subscribed",
		"topic", topic, "group", c.group, "concurrency", c.concurrency)
	return nil
}

// EnqueueRetry is called by Recovery to deliver a claimed message for reprocessing.
// This avoids using XREADGROUP "0" which resets idle time.
func (c *Consumer) EnqueueRetry(topic, stream string, xmsg redis.XMessage) {
	select {
	case c.retryCh <- retryMessage{topic: topic, stream: stream, xmsg: xmsg}:
	default:
		// Channel full — message stays in PEL, Recovery will pick it up next cycle
		c.logger.Warn("retry channel full, message will be retried next cycle",
			"topic", topic, "msg_id", xmsg.ID)
	}
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

// retryWorker processes messages sent by Recovery via the retry channel.
func (c *Consumer) retryWorker(ctx context.Context, handler Handler) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case rm := <-c.retryCh:
			c.process(ctx, rm.topic, rm.stream, rm.xmsg, handler)
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

	// Check if already marked dead — skip without ACK (leave in PEL for requeue)
	if c.retry.IsDead(ctx, topic, xmsg.ID) {
		return
	}

	// Acquire distributed lock to prevent concurrent processing of the same message.
	lockKey := internal.LockKey(topic, xmsg.ID)
	lockValue := fmt.Sprintf("%s:%d", c.consumer, time.Now().UnixNano())
	acquired, err := c.rdb.SetNX(ctx, lockKey, lockValue, c.lockTTL).Result()
	if err != nil {
		c.logger.Error("acquire lock failed", "topic", topic, "msg_id", xmsg.ID, "error", err)
		return
	}
	if !acquired {
		c.logger.Warn("message already being processed, skipping",
			"topic", topic, "msg_id", xmsg.ID)
		return
	}
	defer c.releaseLock(lockKey, lockValue)

	// Enforce processing timeout to prevent handler from blocking indefinitely.
	handlerCtx, cancel := context.WithTimeout(ctx, c.ackTimeout)
	defer cancel()

	if err := handler(handlerCtx, &msg); err != nil {
		c.handleFailure(ctx, topic, stream, &msg, err)
		return
	}

	// Success: ACK and clear retry counter.
	// If the message was pushed to DLQ while we were processing, clean up the DLQ entry too.
	if dlqEntryID := c.retry.GetDLQEntryID(ctx, topic, xmsg.ID); dlqEntryID != "" {
		dlqKey := internal.DeadLetterKey(topic, c.group)
		if err := c.rdb.XDel(ctx, dlqKey, dlqEntryID).Err(); err != nil {
			c.logger.Error("dlq cleanup failed", "topic", topic, "dlq_entry", dlqEntryID, "error", err)
		}
	}
	c.ack(ctx, stream, xmsg.ID)
	c.retry.Clear(ctx, topic, xmsg.ID)
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

// releaseLock releases a distributed lock using a Lua script that checks the value
// before deleting, ensuring we only release our own lock.
func (c *Consumer) releaseLock(key, value string) {
	// Use background context: parent ctx may already be cancelled during shutdown.
	if err := releaseLockScript.Run(context.Background(), c.rdb, []string{key}, value).Err(); err != nil {
		c.logger.Error("release lock failed", "key", key, "error", err)
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
