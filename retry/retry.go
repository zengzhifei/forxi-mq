package retry

import (
	"context"
	"math"
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

// Attempt increments the retry count and returns:
// - count: current retry number (after increment)
// - exhausted: true if max retries reached
func (s *Strategy) Attempt(ctx context.Context, topic, msgID string) (count int, exhausted bool, err error) {
	key := internal.RetryCountKey(topic, msgID)
	val, err := s.rdb.Incr(ctx, key).Result()
	if err != nil {
		return 0, false, err
	}
	// Set TTL so retry keys don't linger forever
	s.rdb.Expire(ctx, key, time.Hour)

	count = int(val)
	return count, count >= s.maxRetry, nil
}

// BackoffDuration returns the delay before the n-th retry (exponential backoff).
func (s *Strategy) BackoffDuration(attempt int) time.Duration {
	multiplier := math.Pow(2, float64(attempt-1))
	return time.Duration(multiplier) * s.backoff
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
