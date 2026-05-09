package retry

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
)

// Strategy handles retry counting and backoff calculation.
type Strategy struct {
	rdb      *redis.Client
	maxRetry int
	backoff  time.Duration
}

// New creates a new retry Strategy.
func New(rdb *redis.Client, maxRetry int, backoff time.Duration) *Strategy {
	return &Strategy{rdb: rdb, maxRetry: maxRetry, backoff: backoff}
}

// Clear removes the retry counter for a message (on successful processing).
func (s *Strategy) Clear(ctx context.Context, topic, msgID string) {
	key := internal.RetryCountKey(topic, msgID)
	s.rdb.Del(ctx, key)
}

// GetCount returns the current retry count for a message.
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

// MaxRetry returns the configured max retry count.
func (s *Strategy) MaxRetry() int {
	return s.maxRetry
}

// Increment bumps the retry counter by 1 (used by recovery).
func (s *Strategy) Increment(ctx context.Context, topic, msgID string) {
	key := internal.RetryCountKey(topic, msgID)
	s.rdb.Incr(ctx, key)
	s.rdb.Expire(ctx, key, time.Hour)
}
