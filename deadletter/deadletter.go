package deadletter

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
	"github.com/zengzhifei/forxi-mq/log"
	"github.com/zengzhifei/forxi-mq/mq"
)

// Queue manages dead letter operations.
type Queue struct {
	rdb    *redis.Client
	group  string
	logger log.Logger
}

// New creates a new dead letter Queue.
func New(rdb *redis.Client, group string, logger log.Logger) *Queue {
	return &Queue{rdb: rdb, group: group, logger: logger}
}

// Push moves a message to the dead letter stream.
func (q *Queue) Push(ctx context.Context, msg *mq.Message, reason string) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	err = q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: internal.DeadLetterKey(msg.Topic, q.group),
		Values: map[string]interface{}{
			"body":   string(body),
			"reason": reason,
		},
	}).Err()

	if err != nil {
		q.logger.Error("failed to push to dead letter queue",
			"topic", msg.Topic, "msg_id", msg.ID, "error", err)
		return err
	}

	q.logger.Warn("message moved to dead letter queue",
		"topic", msg.Topic, "msg_id", msg.ID, "reason", reason)
	return nil
}

// List reads messages from the dead letter queue for inspection.
func (q *Queue) List(ctx context.Context, topic string, count int64) ([]redis.XMessage, error) {
	return q.rdb.XRangeN(ctx, internal.DeadLetterKey(topic, q.group), "-", "+", count).Result()
}
