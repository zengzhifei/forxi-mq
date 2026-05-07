package producer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
	"github.com/zengzhifei/forxi-mq/mq"
)

// Producer publishes messages to Redis Stream.
type Producer struct {
	rdb          *redis.Client
	streamMaxLen int64
}

// New creates a new Producer.
func New(rdb *redis.Client, streamMaxLen int64) *Producer {
	return &Producer{rdb: rdb, streamMaxLen: streamMaxLen}
}

// Publish sends a message to the specified topic.
func (p *Producer) Publish(ctx context.Context, msg *mq.Message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	args := &redis.XAddArgs{
		Stream: internal.StreamKey(msg.Topic),
		Values: map[string]interface{}{
			"body": string(body),
		},
	}
	if p.streamMaxLen > 0 {
		args.MaxLen = p.streamMaxLen
		args.Approx = true
	}

	id, err := p.rdb.XAdd(ctx, args).Result()
	if err != nil {
		return err
	}
	msg.ID = id
	return nil
}

// DelayPublish sends a message that will be delivered after the specified delay.
func (p *Producer) DelayPublish(ctx context.Context, msg *mq.Message, delay time.Duration) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	score := float64(time.Now().Add(delay).UnixMilli())
	return p.rdb.ZAdd(ctx, internal.DelayKey(msg.Topic), redis.Z{
		Score:  score,
		Member: string(body),
	}).Err()
}
