package alert

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
)

// Config configures the alert system.
type Config struct {
	// Webhook URL (supports DingTalk, Feishu, WeCom, or any HTTP endpoint).
	Webhook string

	// Secret for webhook signature verification (required for Feishu/DingTalk signed bots).
	Secret string

	// Type specifies the webhook platform: "feishu", "dingtalk", "wecom", or "" (generic POST).
	// Determines payload format and signing method.
	Type string

	// Thresholds — alert fires when any metric exceeds its threshold.
	// 0 means disabled (no alert for that metric).
	LagThreshold     int64 // total lag across all groups
	DeadThreshold    int64 // total dead letter count
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
	group  string
	topics []string
	logger *slog.Logger

	mu       sync.Mutex
	lastSent map[string]time.Time // topic -> last alert time
}

// New creates a new Alerter.
func New(rdb *redis.Client, topics []string, group string, cfg Config, logger *slog.Logger) *Alerter {
	cfg.applyDefaults()
	return &Alerter{
		rdb:      rdb,
		cfg:      cfg,
		group:    group,
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
		totalDead, _ := a.rdb.XLen(ctx, internal.DeadLetterKey(topic, a.group)).Result()

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
	if a.cfg.Webhook == "" {
		return
	}

	now := time.Now()

	var payload []byte
	var webhookURL string

	switch a.cfg.Type {
	case "feishu":
		payload, webhookURL = a.buildFeishu(a.buildAlertMarkdown(topic, reasons, now))
	case "dingtalk":
		payload, webhookURL = a.buildDingTalk(a.buildAlertText(topic, reasons, now))
	case "wecom":
		payload, webhookURL = a.buildWeCom(a.buildWeComMarkdown(topic, reasons, now))
	default:
		payload, webhookURL = a.buildGeneric(a.buildAlertText(topic, reasons, now))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", webhookURL, bytes.NewReader(payload))
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

// Text builders.

func (a *Alerter) buildAlertText(topic string, reasons []string, now time.Time) string {
	text := fmt.Sprintf("[forxi-mq 告警]\nTopic: %s\n触发条件:\n", topic)
	for _, r := range reasons {
		text += "  - " + r + "\n"
	}
	text += fmt.Sprintf("时间: %s", now.Format("2006-01-02 15:04:05"))
	return text
}

func (a *Alerter) buildAlertMarkdown(topic string, reasons []string, _ time.Time) string {
	md := fmt.Sprintf("**Topic**: %s | **Group**: %s\n\n**触发条件**:\n", topic, a.group)
	for i, r := range reasons {
		if i > 0 {
			md += "\n"
		}
		md += fmt.Sprintf("- %s", r)
	}
	return md
}
