package delay

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
)

// transferScript atomically pops due messages from ZSET, reads body from Hash,
// pushes to Stream, cleans up both ZSET and Hash, and writes delay-map mapping.
// KEYS[1] = delay ZSET key, KEYS[2] = stream key, KEYS[3] = delay data Hash key
// ARGV[1] = now (unix ms), ARGV[2] = limit, ARGV[3] = max_len, ARGV[4] = map_key_prefix, ARGV[5] = map_ttl_seconds
var transferScript = redis.NewScript(`
local delay_key = KEYS[1]
local stream_key = KEYS[2]
local data_key = KEYS[3]
local now = ARGV[1]
local limit = tonumber(ARGV[2])
local max_len = tonumber(ARGV[3])
local map_prefix = ARGV[4]
local map_ttl = tonumber(ARGV[5])

local ids = redis.call('ZRANGEBYSCORE', delay_key, '-inf', now, 'LIMIT', 0, limit)
if #ids == 0 then
    return 0
end

for _, id in ipairs(ids) do
    local body = redis.call('HGET', data_key, id)
    if body then
        local stream_id
        if max_len > 0 then
            stream_id = redis.call('XADD', stream_key, 'MAXLEN', '~', max_len, '*', 'body', body)
        else
            stream_id = redis.call('XADD', stream_key, '*', 'body', body)
        end
        -- Write delay-map: delayID → streamID
        if map_ttl > 0 then
            redis.call('SET', map_prefix .. id, stream_id, 'EX', map_ttl)
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
	mapTTL       time.Duration
	logger       *slog.Logger
}

// New creates a new delay Poller.
func New(rdb *redis.Client, topics []string, interval time.Duration, streamMaxLen int64, mapTTL time.Duration, logger *slog.Logger) *Poller {
	return &Poller{
		rdb:          rdb,
		topics:       topics,
		interval:     interval,
		streamMaxLen: streamMaxLen,
		mapTTL:       mapTTL,
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

	// Map key prefix: "fxmq:delay-map:{topic}:"
	mapPrefix := internal.DelayMapKey(topic, "")
	mapTTLSec := int64(p.mapTTL.Seconds())

	result, err := transferScript.Run(ctx, p.rdb,
		[]string{delayKey, streamKey, dataKey},
		now, 100, p.streamMaxLen, mapPrefix, mapTTLSec,
	).Int()

	if err != nil && err != redis.Nil {
		p.logger.Error("delay transfer failed", "topic", topic, "error", err)
		return
	}

	if result > 0 {
		p.logger.Debug("delay messages transferred", "topic", topic, "count", result)
	}
}
