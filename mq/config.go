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

	// Background task intervals
	DelayPollInterval time.Duration // delay queue poll interval (default: 500ms)
	RecoveryInterval  time.Duration // pending recovery interval (default: 15s)

	// Stream trimming (two strategies, can be used together)
	StreamMaxLen int64         // MAXLEN~ trim by count (default: 0 = unlimited)
	Retention    time.Duration // MINID trim by time (default: 0 = unlimited)
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
	if c.DelayPollInterval <= 0 {
		c.DelayPollInterval = 500 * time.Millisecond
	}
	if c.RecoveryInterval <= 0 {
		c.RecoveryInterval = 15 * time.Second
	}
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
