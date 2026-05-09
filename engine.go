package fxmq

import (
	"context"
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

// WithRetryBackoff sets the base backoff duration (default: 2s).
func WithRetryBackoff(d time.Duration) Option {
	return func(e *Engine) { e.cfg.RetryBackoff = d }
}

// WithAckTimeout sets the pending message timeout (default: 60s).
func WithAckTimeout(d time.Duration) Option {
	return func(e *Engine) { e.cfg.AckTimeout = d }
}

// WithRecoveryInterval sets how often recovery checks for pending messages (default: 15s).
func WithRecoveryInterval(d time.Duration) Option {
	return func(e *Engine) { e.cfg.RecoveryInterval = d }
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

// WithRedisClient sets a pre-existing Redis client (skips internal creation).
func WithRedisClient(rdb *redis.Client) Option {
	return func(e *Engine) {
		e.rdb = rdb
		e.ownsRdb = false
	}
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
	rdb     *redis.Client
	ownsRdb bool
	cfg     Config
	logger  log.Logger

	Producer *producer.Producer
	Consumer *consumer.Consumer
	DLQ      *deadletter.Queue
	Retry    *retry.Strategy

	delayPoller   *delay.Poller
	recovery      *recovery.Recovery
	dashboardAddr string
	alertCfg      *alert.Config
	cancel        context.CancelFunc
	topics        []string
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
		ownsRdb: true,
	}

	for _, opt := range opts {
		opt(e)
	}

	// Apply defaults for zero-value fields
	e.cfg.ApplyDefaults()

	if err := e.cfg.Validate(); err != nil {
		return nil, err
	}

	// Create Redis client if not provided externally
	if e.rdb == nil {
		e.rdb = redis.NewClient(&redis.Options{
			Addr:     e.cfg.RedisAddr,
			Password: e.cfg.RedisPassword,
			DB:       e.cfg.RedisDB,
		})
	}

	// Default logger
	if e.logger == nil {
		e.logger = log.New()
	}

	rs := retry.New(e.rdb, e.cfg.MaxRetry, e.cfg.RetryBackoff)
	dlq := deadletter.New(e.rdb, e.logger)
	p := producer.New(e.rdb, e.cfg.StreamMaxLen)
	c := consumer.New(e.rdb, e.cfg, e.logger, rs, dlq)

	e.Producer = p
	e.Consumer = c
	e.DLQ = dlq
	e.Retry = rs

	return e, nil
}

// Publish sends a message to the given topic.
func (e *Engine) Publish(ctx context.Context, msg *Message) error {
	return e.Producer.Publish(ctx, msg)
}

// DelayPublish sends a message that will be delivered after the specified delay.
func (e *Engine) DelayPublish(ctx context.Context, msg *Message, d time.Duration) error {
	return e.Producer.DelayPublish(ctx, msg, d)
}

// Subscribe registers a handler for a topic and starts consuming.
func (e *Engine) Subscribe(ctx context.Context, topic string, handler Handler) error {
	e.topics = append(e.topics, topic)
	return e.Consumer.Subscribe(ctx, topic, handler)
}

// Start launches background goroutines (delay poller, pending recovery, retention trimmer).
// Must be called after all Subscribe() calls.
func (e *Engine) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel

	e.delayPoller = delay.New(e.rdb, e.topics, e.cfg.DelayPollInterval, e.cfg.StreamMaxLen, e.logger)
	e.bgWg.Add(1)
	go func() {
		defer e.bgWg.Done()
		e.delayPoller.Run(ctx)
	}()

	e.recovery = recovery.New(e.rdb, e.cfg, e.topics, e.logger, e.Retry, e.DLQ)
	e.bgWg.Add(1)
	go func() {
		defer e.bgWg.Done()
		e.recovery.Run(ctx)
	}()

	if e.cfg.Retention > 0 {
		e.bgWg.Add(1)
		go func() {
			defer e.bgWg.Done()
			e.runRetentionTrimmer(ctx)
		}()
	}

	if e.dashboardAddr != "" {
		dash := dashboard.New(e.rdb, e.dashboardAddr, e.logger)
		dashDone := dash.Start(ctx)
		e.bgWg.Add(1)
		go func() {
			defer e.bgWg.Done()
			<-dashDone
		}()
	}

	if e.alertCfg != nil {
		alerter := alert.New(e.rdb, e.topics, *e.alertCfg, e.logger)
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
	// Trim every 1/10 of retention period, minimum 1 minute
	interval := e.cfg.Retention / 10
	if interval < time.Minute {
		interval = time.Minute
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
	e.Consumer.Shutdown()
	e.bgWg.Wait()
	if e.ownsRdb {
		e.rdb.Close()
	}
	e.logger.Info("engine shutdown complete")
}
