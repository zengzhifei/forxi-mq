package recovery

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/deadletter"
	"github.com/zengzhifei/forxi-mq/internal"
	"github.com/zengzhifei/forxi-mq/log"
	"github.com/zengzhifei/forxi-mq/mq"
	"github.com/zengzhifei/forxi-mq/retry"
)

// RetryEnqueuer delivers claimed messages to consumer for reprocessing.
type RetryEnqueuer interface {
	EnqueueRetry(topic, stream string, xmsg redis.XMessage)
}

// Recovery periodically claims pending messages that exceeded ACK timeout.
type Recovery struct {
	rdb        *redis.Client
	group      string
	consumer   string
	topics     []string
	maxRetry   int
	ackTimeout time.Duration
	interval   time.Duration
	logger     log.Logger
	retry      *retry.Strategy
	dlq        *deadletter.Queue
	enqueuer   RetryEnqueuer
}

// New creates a new Recovery process.
func New(rdb *redis.Client, cfg mq.Config, topics []string, logger log.Logger, rs *retry.Strategy, dlq *deadletter.Queue, enqueuer RetryEnqueuer) *Recovery {
	return &Recovery{
		rdb:        rdb,
		group:      cfg.Group,
		consumer:   cfg.Consumer,
		topics:     topics,
		maxRetry:   cfg.MaxRetry,
		ackTimeout: cfg.AckTimeout,
		interval:   cfg.RecoveryInterval,
		logger:     logger,
		retry:      rs,
		dlq:        dlq,
		enqueuer:   enqueuer,
	}
}

// Run starts the recovery loop. Blocks until ctx is cancelled.
func (r *Recovery) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	var tick int

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, topic := range r.topics {
				r.recover(ctx, topic)
			}
			// Clean stale consumers every ~5 minutes (based on tick count)
			tick++
			cleanEvery := int(5*time.Minute/r.interval) + 1
			if tick%cleanEvery == 0 {
				for _, topic := range r.topics {
					r.cleanStaleConsumers(ctx, topic)
				}
			}
		}
	}
}

func (r *Recovery) recover(ctx context.Context, topic string) {
	stream := internal.StreamKey(topic)

	// Claim timed-out messages — message stays in PEL with same ID, just ownership transfers.
	msgs, _, err := r.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    r.group,
		Consumer: r.consumer,
		MinIdle:  r.ackTimeout,
		Start:    "0-0",
		Count:    50,
	}).Result()
	if err != nil {
		if err != redis.Nil {
			r.logger.Error("xautoclaim failed", "stream", stream, "error", err)
		}
		return
	}

	for _, xmsg := range msgs {
		body, ok := xmsg.Values["body"].(string)
		if !ok {
			r.ack(ctx, stream, xmsg.ID)
			continue
		}

		var msg mq.Message
		if err := json.Unmarshal([]byte(body), &msg); err != nil {
			r.ack(ctx, stream, xmsg.ID)
			continue
		}
		msg.ID = xmsg.ID

		// Skip if already marked dead — don't ACK, leave in PEL for potential requeue
		if r.retry.IsDead(ctx, topic, xmsg.ID) {
			continue
		}

		count, err := r.retry.GetCount(ctx, topic, xmsg.ID)
		if err != nil {
			r.logger.Error("get retry count failed", "topic", topic, "msg_id", xmsg.ID, "error", err)
			continue
		}

		// Increment retry count
		r.retry.Increment(ctx, topic, xmsg.ID)
		count++

		if count >= r.maxRetry {
			// Max retries exceeded → move to dead letter queue, don't ACK (leave in PEL for requeue)
			if err := r.dlq.Push(ctx, &msg, "max retries exceeded (timeout)"); err != nil {
				r.logger.Error("dlq push failed", "topic", topic, "msg_id", xmsg.ID, "error", err)
				continue
			}
			r.retry.MarkDead(ctx, topic, xmsg.ID)
			r.logger.Info("message moved to DLQ", "topic", topic, "msg_id", xmsg.ID, "retries", count)
		} else {
			// Deliver to consumer via channel for reprocessing
			r.enqueuer.EnqueueRetry(topic, stream, xmsg)
			r.logger.Info("message claimed for retry", "topic", topic, "msg_id", xmsg.ID, "attempt", count)
		}
	}

	if len(msgs) > 0 {
		r.logger.Info("recovered pending messages", "topic", topic, "count", len(msgs))
	}
}

func (r *Recovery) ack(ctx context.Context, stream, id string) {
	if err := r.rdb.XAck(ctx, stream, r.group, id).Err(); err != nil {
		r.logger.Error("ack failed", "stream", stream, "id", id, "error", err)
	}
}

// cleanStaleConsumers removes consumers that have no pending messages and have been idle too long.
func (r *Recovery) cleanStaleConsumers(ctx context.Context, topic string) {
	stream := internal.StreamKey(topic)
	consumers, err := r.rdb.XInfoConsumers(ctx, stream, r.group).Result()
	if err != nil {
		return
	}

	// Threshold: 2x ackTimeout, minimum 5 minutes
	threshold := r.ackTimeout * 2
	if threshold < 5*time.Minute {
		threshold = 5 * time.Minute
	}

	for _, c := range consumers {
		if c.Name == r.consumer {
			continue
		}
		if c.Pending == 0 && c.Idle >= threshold {
			if err := r.rdb.XGroupDelConsumer(ctx, stream, r.group, c.Name).Err(); err != nil {
				r.logger.Error("del stale consumer failed", "stream", stream, "consumer", c.Name, "error", err)
			} else {
				r.logger.Info("removed stale consumer", "stream", stream, "consumer", c.Name, "idle", c.Idle)
			}
		}
	}
}
