package mq

import (
	"errors"
	"os"
	"time"
)

// Config holds all configuration for the MQ engine.
type Config struct {
	// Required fields (passed via NewEngine arguments)
	RedisAddr string
	Group     string
	Consumer  string

	// Redis options
	RedisPassword string
	RedisDB       int

	// Concurrency: number of workers per subscription (default: 8)
	Concurrency int

	// Retry
	MaxRetry int // max retries before DLQ (default: 3)

	// Timeout
	AckTimeout time.Duration // pending message timeout (default: 60s)

	// Internal intervals (auto-derived, not user-configurable)
	DelayPollInterval time.Duration
	RecoveryInterval  time.Duration

	// Stream trimming (two strategies, can be used together)
	StreamMaxLen int64         // MAXLEN~ trim by count (default: 0 = unlimited)
	Retention    time.Duration // MINID trim by time (default: 0 = unlimited)

	// DLQ retention: how long dead letter messages are kept (default: 7 days)
	DLQRetention time.Duration

	// LockBuffer is added to MaxRetry*(AckTimeout+RecoveryInterval) to form the lock TTL.
	// It guards against handler still running when lock expires. (default: 30s)
	LockBuffer time.Duration
}

// defaultConsumerName returns a unique consumer name from environment.
func defaultConsumerName() string {
	if h := os.Getenv("HOSTNAME"); h != "" {
		return h
	}
	if h, _ := os.Hostname(); h != "" {
		return h
	}
	return "worker-1"
}

// ApplyDefaults fills zero-value fields with sensible defaults.
func (c *Config) ApplyDefaults() {
	if c.Consumer == "" {
		c.Consumer = defaultConsumerName()
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	if c.MaxRetry <= 0 {
		c.MaxRetry = 3
	}
	if c.AckTimeout <= 0 {
		c.AckTimeout = 60 * time.Second
	}
	if c.DLQRetention <= 0 {
		c.DLQRetention = 7 * 24 * time.Hour
	}
	if c.LockBuffer <= 0 {
		c.LockBuffer = 30 * time.Second
	}
	// Internal intervals derived from AckTimeout
	c.DelayPollInterval = 500 * time.Millisecond
	c.RecoveryInterval = max(c.AckTimeout/4, time.Second)
}

// Validate checks required fields.
func (c Config) Validate() error {
	if c.RedisAddr == "" {
		return errors.New("fxmq: RedisAddr is required")
	}
	if c.Group == "" {
		return errors.New("fxmq: Group is required")
	}
	if c.Consumer == "" {
		return errors.New("fxmq: Consumer is required")
	}
	return nil
}
