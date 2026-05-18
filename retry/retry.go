package retry

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
)

// Strategy handles retry counting.
type Strategy struct {
	rdb   *redis.Client
	group string
	ttl   time.Duration
}

// New creates a new retry Strategy.
func New(rdb *redis.Client, group string, ttl time.Duration) *Strategy {
	return &Strategy{rdb: rdb, group: group, ttl: ttl}
}

// GetCount returns the current retry count for a message.
func (s *Strategy) GetCount(ctx context.Context, topic, msgID string) (int, error) {
	key := internal.RetryCountKey(topic, s.group, msgID)
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
	key := internal.RetryCountKey(topic, s.group, msgID)
	s.rdb.Incr(ctx, key)
	s.rdb.Expire(ctx, key, s.ttl)
}

// Clear removes the retry counter for a message (on successful processing).
func (s *Strategy) Clear(ctx context.Context, topic, msgID string) {
	key := internal.RetryCountKey(topic, s.group, msgID)
	s.rdb.Del(ctx, key)
}

// MarkDead sets the retry counter to -1:<dlqEntryID>, indicating the message is in the DLQ
// and recording the DLQ entry ID for later cleanup.
func (s *Strategy) MarkDead(ctx context.Context, topic, msgID, dlqEntryID string) {
	key := internal.RetryCountKey(topic, s.group, msgID)
	s.rdb.Set(ctx, key, "-1:"+dlqEntryID, s.ttl)
}

// IsDead returns true if the message has been marked as dead.
func (s *Strategy) IsDead(ctx context.Context, topic, msgID string) bool {
	key := internal.RetryCountKey(topic, s.group, msgID)
	val, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		return false
	}
	return strings.HasPrefix(val, "-1")
}

// GetDLQEntryID returns the DLQ entry ID stored in the retry key, or empty string if not found.
func (s *Strategy) GetDLQEntryID(ctx context.Context, topic, msgID string) string {
	key := internal.RetryCountKey(topic, s.group, msgID)
	val, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		return ""
	}
	if !strings.HasPrefix(val, "-1") {
		return ""
	}
	// Format: "-1:<dlqEntryID>"
	idx := strings.Index(val, ":")
	if idx < 0 {
		return ""
	}
	return val[idx+1:]
}
