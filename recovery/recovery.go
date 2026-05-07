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
}

// New creates a new Recovery process.
func New(rdb *redis.Client, cfg mq.Config, topics []string, logger log.Logger, rs *retry.Strategy, dlq *deadletter.Queue) *Recovery {
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
	}
}

// Run starts the recovery loop. Blocks until ctx is cancelled.
func (r *Recovery) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, topic := range r.topics {
				r.recover(ctx, topic)
			}
		}
	}
}

func (r *Recovery) recover(ctx context.Context, topic string) {
	stream := internal.StreamKey(topic)

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

		count, err := r.retry.GetCount(ctx, topic, xmsg.ID)
		if err != nil {
			r.logger.Error("get retry count failed", "topic", topic, "msg_id", xmsg.ID, "error", err)
			continue
		}

		if count >= r.maxRetry {
			_ = r.dlq.Push(ctx, &msg, "recovered but max retries exceeded")
			r.ack(ctx, stream, xmsg.ID)
			r.retry.Clear(ctx, topic, xmsg.ID)
		}
		// Otherwise the message is now assigned to this consumer
		// and will be re-delivered via XREADGROUP on next read cycle.
	}

	if len(msgs) > 0 {
		r.logger.Info("recovered pending messages", "topic", topic, "count", len(msgs))
	}
}

func (r *Recovery) ack(ctx context.Context, stream, id string) {
	r.rdb.XAck(ctx, stream, r.group, id)
}
