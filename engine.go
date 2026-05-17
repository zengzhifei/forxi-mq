package fxmq

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/alert"
	"github.com/zengzhifei/forxi-mq/consumer"
	"github.com/zengzhifei/forxi-mq/dashboard"
	"github.com/zengzhifei/forxi-mq/deadletter"
	"github.com/zengzhifei/forxi-mq/delay"
	"github.com/zengzhifei/forxi-mq/internal"
	"github.com/zengzhifei/forxi-mq/log"
	"github.com/zengzhifei/forxi-mq/mq"
	"github.com/zengzhifei/forxi-mq/producer"
	"github.com/zengzhifei/forxi-mq/recovery"
	"github.com/zengzhifei/forxi-mq/retry"
)

// Re-export core types for convenience.
type (
	Message     = mq.Message
	Config      = mq.Config
	Handler     = consumer.Handler
	AlertConfig = alert.Config
)

// Re-export constructor.
var NewMessage = mq.NewMessage

// Option configures the Engine.
type Option func(*Engine)

// WithConcurrency sets the number of workers per subscription (default: 8).
func WithConcurrency(n int) Option {
	return func(e *Engine) { e.cfg.Concurrency = n }
}

// WithMaxRetry sets the max retry count before DLQ (default: 3).
func WithMaxRetry(n int) Option {
	return func(e *Engine) { e.cfg.MaxRetry = n }
}

// WithAckTimeout sets the pending message timeout (default: 60s).
func WithAckTimeout(d time.Duration) Option {
	return func(e *Engine) { e.cfg.AckTimeout = d }
}

// WithStreamMaxLen sets the stream MAXLEN~ trimming (default: 0 = unlimited).
func WithStreamMaxLen(n int64) Option {
	return func(e *Engine) { e.cfg.StreamMaxLen = n }
}

// WithRetention sets time-based retention. Messages older than this are trimmed.
// Uses XTRIM MINID (Redis 6.2+). Default: 0 = keep forever.
func WithRetention(d time.Duration) Option {
	return func(e *Engine) { e.cfg.Retention = d }
}

// WithDLQRetention sets how long dead letter messages are kept (default: 7 days).
func WithDLQRetention(d time.Duration) Option {
	return func(e *Engine) { e.cfg.DLQRetention = d }
}

// WithRedisPassword sets the Redis password.
func WithRedisPassword(password string) Option {
	return func(e *Engine) { e.cfg.RedisPassword = password }
}

// WithRedisDB sets the Redis DB index.
func WithRedisDB(db int) Option {
	return func(e *Engine) { e.cfg.RedisDB = db }
}

// WithLogger sets a custom logger.
func WithLogger(l log.Logger) Option {
	return func(e *Engine) { e.logger = l }
}

// WithDashboard enables the web dashboard on the given address (e.g. ":9090").
func WithDashboard(addr string) Option {
	return func(e *Engine) { e.dashboardAddr = addr }
}

// WithAlert enables webhook alerting when metrics exceed thresholds.
func WithAlert(cfg alert.Config) Option {
	return func(e *Engine) { e.alertCfg = &cfg }
}

// Engine is the top-level entry point that wires all components together.
type Engine struct {
	rdb   *redis.Client
	cfg   Config
	logger log.Logger

	Producer *producer.Producer
	Consumer *consumer.Consumer
	DLQ      *deadletter.Queue
	Retry    *retry.Strategy

	delayPoller   *delay.Poller
	recovery      *recovery.Recovery
	dashboardAddr string
	dashRdb       *redis.Client
	alertCfg      *alert.Config
	cancel        context.CancelFunc
	topics        []string
	pendingSubs   []pendingSub
	bgWg          sync.WaitGroup
}

// NewEngine creates a new Engine.
//
// Required parameters:
//   - redisAddr: Redis server address (e.g. "localhost:6379")
//   - group:     Consumer group name (e.g. "order-service")
//
// Consumer name defaults to HOSTNAME. Use options to customize behavior.
func NewEngine(redisAddr, group string, opts ...Option) (*Engine, error) {
	e := &Engine{
		cfg: Config{
			RedisAddr: redisAddr,
			Group:     group,
		},
	}

	for _, opt := range opts {
		opt(e)
	}

	// Apply defaults for zero-value fields
	e.cfg.ApplyDefaults()

	if err := e.cfg.Validate(); err != nil {
		return nil, err
	}

	// Default logger
	if e.logger == nil {
		e.logger = log.New()
	}

	return e, nil
}

// Publish sends a message to the given topic.
func (e *Engine) Publish(ctx context.Context, msg *Message) error {
	if e.Producer == nil {
		return errors.New("fxmq: engine not started, call Start() before Publish()")
	}
	return e.Producer.Publish(ctx, msg)
}

// DelayPublish sends a message that will be delivered after the specified delay.
func (e *Engine) DelayPublish(ctx context.Context, msg *Message, d time.Duration) error {
	if e.Producer == nil {
		return errors.New("fxmq: engine not started, call Start() before DelayPublish()")
	}
	return e.Producer.DelayPublish(ctx, msg, d)
}

// pendingSub holds a subscription registered before Start.
type pendingSub struct {
	topic   string
	handler Handler
}

// Subscribe registers a handler for a topic and starts consuming.
func (e *Engine) Subscribe(ctx context.Context, topic string, handler Handler) error {
	e.topics = append(e.topics, topic)
	if e.Consumer != nil {
		return e.Consumer.Subscribe(ctx, topic, handler)
	}
	e.pendingSubs = append(e.pendingSubs, pendingSub{topic, handler})
	return nil
}

// initComponents creates the Redis client and all core components.
// Called from Start() when the topic list is known.
func (e *Engine) initComponents() {
	// Create Redis client with pool sized for the known topics
	poolSize := len(e.topics)*e.cfg.Concurrency + 15
	if poolSize < 30 {
		poolSize = 30
	}
	e.rdb = redis.NewClient(&redis.Options{
		Addr:     e.cfg.RedisAddr,
		Password: e.cfg.RedisPassword,
		DB:       e.cfg.RedisDB,
		PoolSize: poolSize,
	})

	retryTTL := e.cfg.Retention
	if retryTTL <= 0 {
		retryTTL = 7 * 24 * time.Hour
	}
	rs := retry.New(e.rdb, e.cfg.Group, retryTTL)
	dlq := deadletter.New(e.rdb, e.cfg.Group, e.logger)
	p := producer.New(e.rdb, e.cfg.StreamMaxLen)
	c := consumer.New(e.rdb, e.cfg, e.logger, rs)

	e.Producer = p
	e.Consumer = c
	e.DLQ = dlq
	e.Retry = rs
}

// Start launches background goroutines (delay poller, pending recovery, retention trimmer).
// Must be called after all Subscribe() calls.
func (e *Engine) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel

	// Create Redis client and all components (pool size calculated from actual topic count)
	e.initComponents()

	// Apply subscriptions registered before Start
	for _, sub := range e.pendingSubs {
		if err := e.Consumer.Subscribe(ctx, sub.topic, sub.handler); err != nil {
			e.logger.Error("failed to apply pending subscription", "topic", sub.topic, "error", err)
		}
	}
	e.pendingSubs = nil

	mapTTL := e.cfg.Retention
	if mapTTL <= 0 {
		mapTTL = 7 * 24 * time.Hour // default 7 days
	}
	e.delayPoller = delay.New(e.rdb, e.topics, e.cfg.DelayPollInterval, e.cfg.StreamMaxLen, mapTTL, e.logger)
	e.bgWg.Add(1)
	go func() {
		defer e.bgWg.Done()
		e.delayPoller.Run(ctx)
	}()

	e.recovery = recovery.New(e.rdb, e.cfg, e.topics, e.logger, e.Retry, e.DLQ, e.Consumer)
	e.bgWg.Add(1)
	go func() {
		defer e.bgWg.Done()
		e.recovery.Run(ctx)
	}()

	// Always run retention trimmer: DLQ uses 7-day default, main stream uses configured retention
	e.bgWg.Add(1)
	go func() {
		defer e.bgWg.Done()
		e.runRetentionTrimmer(ctx)
	}()

	if e.dashboardAddr != "" {
		e.dashRdb = redis.NewClient(&redis.Options{
			Addr:     e.cfg.RedisAddr,
			Password: e.cfg.RedisPassword,
			DB:       e.cfg.RedisDB,
			PoolSize: 8,
		})
		dash := dashboard.New(e.dashRdb, e.dashboardAddr, e.cfg.Group, e.logger)
		dashDone := dash.Start(ctx)
		e.bgWg.Add(1)
		go func() {
			defer e.bgWg.Done()
			<-dashDone
		}()
	}

	if e.alertCfg != nil {
		alerter := alert.New(e.rdb, e.topics, e.cfg.Group, *e.alertCfg, e.logger)
		e.bgWg.Add(1)
		go func() {
			defer e.bgWg.Done()
			alerter.Run(ctx)
		}()
	}

	e.logger.Info("engine started", "topics", e.topics)
}

// runRetentionTrimmer periodically trims streams using MINID based on retention duration.
func (e *Engine) runRetentionTrimmer(ctx context.Context) {
	// Use retention interval or default 1 hour for DLQ-only trimming
	interval := time.Hour
	if e.cfg.Retention > 0 {
		interval = e.cfg.Retention / 10
		if interval < time.Minute {
			interval = time.Minute
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.trimByRetention(ctx)
		}
	}
}

func (e *Engine) trimByRetention(ctx context.Context) {
	// Trim DLQ (always, using configured DLQ retention)
	// Before trimming, ACK the expired dead messages from main stream PEL
	dlqMinID := strconv.FormatInt(time.Now().Add(-e.cfg.DLQRetention).UnixMilli(), 10) + "-0"
	for _, topic := range e.topics {
		dlqKey := internal.DeadLetterKey(topic, e.cfg.Group)
		streamKey := internal.StreamKey(topic)

		// Find DLQ entries that will be trimmed (older than dlqMinID)
		expired, _ := e.rdb.XRangeN(ctx, dlqKey, "-", "("+dlqMinID, 100).Result()
		for _, msg := range expired {
			// Extract original message ID and ACK from main stream PEL
			if body, ok := msg.Values["body"].(string); ok {
				var origMsg mq.Message
				if err := json.Unmarshal([]byte(body), &origMsg); err == nil && origMsg.ID != "" {
					e.rdb.XAck(ctx, streamKey, e.cfg.Group, origMsg.ID)
					// Also clean retry key
					retryKey := internal.RetryCountKey(topic, e.cfg.Group, origMsg.ID)
					e.rdb.Del(ctx, retryKey)
				}
			}
		}

		if err := e.rdb.XTrimMinID(ctx, dlqKey, dlqMinID).Err(); err != nil {
			e.logger.Error("dlq trim failed", "key", dlqKey, "error", err)
		}

		// Clean up empty DLQ key
		if dlqLen, _ := e.rdb.XLen(ctx, dlqKey).Result(); dlqLen == 0 {
			e.rdb.Del(ctx, dlqKey)
		}
	}

	// Trim main stream (only if retention is configured)
	if e.cfg.Retention <= 0 {
		return
	}
	minID := strconv.FormatInt(time.Now().Add(-e.cfg.Retention).UnixMilli(), 10) + "-0"
	for _, topic := range e.topics {
		stream := internal.StreamKey(topic)
		err := e.rdb.XTrimMinID(ctx, stream, minID).Err()
		if err != nil {
			e.logger.Error("retention trim failed",
				"stream", stream, "min_id", minID, "error", err)
		}

	}
}

// Shutdown gracefully stops all consumers and background tasks.
func (e *Engine) Shutdown() {
	if e.cancel != nil {
		e.cancel()
	}
	if e.Consumer != nil {
		e.Consumer.Shutdown()
	}
	e.bgWg.Wait()
	if e.dashRdb != nil {
		e.dashRdb.Close()
	}
	if e.rdb != nil {
		e.rdb.Close()
	}
	e.logger.Info("engine shutdown complete")
}
