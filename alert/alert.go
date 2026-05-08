package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
	"github.com/zengzhifei/forxi-mq/log"
)

// Config configures the alert system.
type Config struct {
	// Webhook URL (supports DingTalk, Feishu, WeCom, Slack, or any HTTP endpoint).
	Webhook string

	// Thresholds — alert fires when any metric exceeds its threshold.
	// 0 means disabled (no alert for that metric).
	LagThreshold  int64 // total lag across all groups
	DeadThreshold int64 // total dead letter count
	PendingThreshold int64 // total pending count

	// Cooldown is the minimum interval between repeated alerts for the same topic.
	// Default: 5 minutes.
	Cooldown time.Duration

	// CheckInterval is how often to check metrics. Default: 30 seconds.
	CheckInterval time.Duration
}

func (c *Config) applyDefaults() {
	if c.Cooldown == 0 {
		c.Cooldown = 5 * time.Minute
	}
	if c.CheckInterval == 0 {
		c.CheckInterval = 30 * time.Second
	}
}

// Alerter monitors topics and sends webhook notifications.
type Alerter struct {
	rdb    *redis.Client
	cfg    Config
	topics []string
	logger log.Logger

	mu       sync.Mutex
	lastSent map[string]time.Time // topic -> last alert time
}

// New creates a new Alerter.
func New(rdb *redis.Client, topics []string, cfg Config, logger log.Logger) *Alerter {
	cfg.applyDefaults()
	return &Alerter{
		rdb:      rdb,
		cfg:      cfg,
		topics:   topics,
		logger:   logger,
		lastSent: make(map[string]time.Time),
	}
}

// Run starts the alert check loop. Blocks until ctx is cancelled.
func (a *Alerter) Run(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.check(ctx)
		}
	}
}

func (a *Alerter) check(ctx context.Context) {
	for _, topic := range a.topics {
		streamKey := internal.StreamKey(topic)

		// Check lag & pending
		var totalLag, totalPending int64
		groups, err := a.rdb.XInfoGroups(ctx, streamKey).Result()
		if err == nil {
			for _, g := range groups {
				totalLag += g.Lag
				totalPending += g.Pending
			}
		}

		// Check dead
		totalDead, _ := a.rdb.XLen(ctx, internal.DeadLetterKey(topic)).Result()

		// Evaluate thresholds
		var reasons []string
		if a.cfg.LagThreshold > 0 && totalLag >= a.cfg.LagThreshold {
			reasons = append(reasons, fmt.Sprintf("lag=%d (阈值:%d)", totalLag, a.cfg.LagThreshold))
		}
		if a.cfg.DeadThreshold > 0 && totalDead >= a.cfg.DeadThreshold {
			reasons = append(reasons, fmt.Sprintf("dead=%d (阈值:%d)", totalDead, a.cfg.DeadThreshold))
		}
		if a.cfg.PendingThreshold > 0 && totalPending >= a.cfg.PendingThreshold {
			reasons = append(reasons, fmt.Sprintf("pending=%d (阈值:%d)", totalPending, a.cfg.PendingThreshold))
		}

		if len(reasons) == 0 {
			continue
		}

		// Cooldown check
		a.mu.Lock()
		last, ok := a.lastSent[topic]
		if ok && time.Since(last) < a.cfg.Cooldown {
			a.mu.Unlock()
			continue
		}
		a.lastSent[topic] = time.Now()
		a.mu.Unlock()

		// Send alert
		a.send(topic, reasons)
	}
}

func (a *Alerter) send(topic string, reasons []string) {
	text := fmt.Sprintf("[forxi-mq 告警]\nTopic: %s\n触发条件:\n", topic)
	for _, r := range reasons {
		text += "  - " + r + "\n"
	}

	// Generic webhook payload (compatible with Feishu/DingTalk/WeCom/Slack)
	payload := map[string]any{
		"msg_type": "text",
		"content":  map[string]string{"text": text},
		// DingTalk format
		"msgtype": "text",
		"text":    map[string]string{"content": text},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		a.logger.Error("alert marshal failed", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", a.cfg.Webhook, bytes.NewReader(body))
	if err != nil {
		a.logger.Error("alert request failed", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.logger.Error("alert send failed", "topic", topic, "error", err)
		return
	}
	resp.Body.Close()

	a.logger.Info("alert sent", "topic", topic, "status", resp.StatusCode)
}
