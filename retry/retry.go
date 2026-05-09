package retry

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
)

// Strategy handles retry counting.
type Strategy struct {
	rdb *redis.Client
	ttl time.Duration
}

// New creates a new retry Strategy.
func New(rdb *redis.Client, ttl time.Duration) *Strategy {
	return &Strategy{rdb: rdb, ttl: ttl}
}

// GetCount returns the current retry count for a message.
// Returns -1 if the message has been marked dead.
func (s *Strategy) GetCount(ctx context.Context, topic, msgID string) (int, error) {
	key := internal.RetryCountKey(topic, msgID)
	val, err := s.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(val)
}

// Increment bumps the retry counter by 1.
func (s *Strategy) Increment(ctx context.Context, topic, msgID string) {
	key := internal.RetryCountKey(topic, msgID)
	s.rdb.Incr(ctx, key)
	s.rdb.Expire(ctx, key, s.ttl)
}

// Clear removes the retry counter for a message (on successful processing).
func (s *Strategy) Clear(ctx context.Context, topic, msgID string) {
	key := internal.RetryCountKey(topic, msgID)
	s.rdb.Del(ctx, key)
}

// MarkDead sets the retry counter to -1, indicating the message is in the dead letter queue.
func (s *Strategy) MarkDead(ctx context.Context, topic, msgID string) {
	key := internal.RetryCountKey(topic, msgID)
	s.rdb.Set(ctx, key, "-1", s.ttl)
}

// IsDead returns true if the message has been marked as dead.
func (s *Strategy) IsDead(ctx context.Context, topic, msgID string) bool {
	key := internal.RetryCountKey(topic, msgID)
	val, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		return false
	}
	return val == "-1"
}
