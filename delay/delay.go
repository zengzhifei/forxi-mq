package delay

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
	"github.com/zengzhifei/forxi-mq/log"
)

// transferScript atomically pops due messages from ZSET, reads body from Hash,
// pushes to Stream, and cleans up both ZSET and Hash.
// KEYS[1] = delay ZSET key, KEYS[2] = stream key, KEYS[3] = delay data Hash key
var transferScript = redis.NewScript(`
local delay_key = KEYS[1]
local stream_key = KEYS[2]
local data_key = KEYS[3]
local now = ARGV[1]
local limit = tonumber(ARGV[2])
local max_len = tonumber(ARGV[3])

local ids = redis.call('ZRANGEBYSCORE', delay_key, '-inf', now, 'LIMIT', 0, limit)
if #ids == 0 then
    return 0
end

for _, id in ipairs(ids) do
    local body = redis.call('HGET', data_key, id)
    if body then
        if max_len > 0 then
            redis.call('XADD', stream_key, 'MAXLEN', '~', max_len, '*', 'body', body)
        else
            redis.call('XADD', stream_key, '*', 'body', body)
        end
        redis.call('HDEL', data_key, id)
    end
    redis.call('ZREM', delay_key, id)
end

return #ids
`)

// Poller scans delay sorted sets and moves due messages to streams.
type Poller struct {
	rdb          *redis.Client
	topics       []string
	interval     time.Duration
	streamMaxLen int64
	logger       log.Logger
}

// New creates a new delay Poller.
func New(rdb *redis.Client, topics []string, interval time.Duration, streamMaxLen int64, logger log.Logger) *Poller {
	return &Poller{
		rdb:          rdb,
		topics:       topics,
		interval:     interval,
		streamMaxLen: streamMaxLen,
		logger:       logger,
	}
}

// Run starts the polling loop. Blocks until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, topic := range p.topics {
				p.poll(ctx, topic)
			}
		}
	}
}

func (p *Poller) poll(ctx context.Context, topic string) {
	delayKey := internal.DelayKey(topic)
	streamKey := internal.StreamKey(topic)
	dataKey := internal.DelayDataKey(topic)
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)

	result, err := transferScript.Run(ctx, p.rdb,
		[]string{delayKey, streamKey, dataKey},
		now, 100, p.streamMaxLen,
	).Int()

	if err != nil && err != redis.Nil {
		p.logger.Error("delay transfer failed", "topic", topic, "error", err)
		return
	}

	if result > 0 {
		p.logger.Debug("delay messages transferred", "topic", topic, "count", result)
	}
}
