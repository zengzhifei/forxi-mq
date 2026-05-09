package alert

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
	"github.com/zengzhifei/forxi-mq/log"
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
	logger log.Logger

	mu       sync.Mutex
	lastSent map[string]time.Time // topic -> last alert time
}

// New creates a new Alerter.
func New(rdb *redis.Client, topics []string, group string, cfg Config, logger log.Logger) *Alerter {
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
	text := fmt.Sprintf("[forxi-mq 告警]\nTopic: %s\n触发条件:\n", topic)
	for _, r := range reasons {
		text += "  - " + r + "\n"
	}

	var payload []byte
	var webhookURL string

	switch a.cfg.Type {
	case "feishu":
		payload, webhookURL = a.buildFeishu(text)
	case "dingtalk":
		payload, webhookURL = a.buildDingTalk(text)
	case "wecom":
		payload, webhookURL = a.buildWeCom(text)
	default:
		payload, webhookURL = a.buildGeneric(text)
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

// --- Platform-specific builders ---

// buildFeishu constructs a Feishu bot payload with optional signature.
// Sign method: timestamp(sec) + "\n" + secret → HMAC-SHA256 → base64
func (a *Alerter) buildFeishu(text string) ([]byte, string) {
	msg := map[string]any{
		"msg_type": "text",
		"content":  map[string]string{"text": text},
	}

	if a.cfg.Secret != "" {
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		sign := feishuSign(timestamp, a.cfg.Secret)
		msg["timestamp"] = timestamp
		msg["sign"] = sign
	}

	body, _ := json.Marshal(msg)
	return body, a.cfg.Webhook
}

// buildDingTalk constructs a DingTalk bot payload with optional signature.
// Sign method: timestamp(ms) + "\n" + secret → HMAC-SHA256(key=secret) → base64 → URL encode, append to URL
func (a *Alerter) buildDingTalk(text string) ([]byte, string) {
	msg := map[string]any{
		"msgtype": "text",
		"text":    map[string]string{"content": text},
	}

	webhookURL := a.cfg.Webhook
	if a.cfg.Secret != "" {
		timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
		sign := dingtalkSign(timestamp, a.cfg.Secret)
		webhookURL += "&timestamp=" + timestamp + "&sign=" + url.QueryEscape(sign)
	}

	body, _ := json.Marshal(msg)
	return body, webhookURL
}

// buildWeCom constructs a WeCom (企业微信) bot payload. No signing needed (key is in URL).
func (a *Alerter) buildWeCom(text string) ([]byte, string) {
	msg := map[string]any{
		"msgtype": "text",
		"text":    map[string]string{"content": text},
	}
	body, _ := json.Marshal(msg)
	return body, a.cfg.Webhook
}

// buildGeneric constructs a simple JSON POST payload.
func (a *Alerter) buildGeneric(text string) ([]byte, string) {
	msg := map[string]any{
		"text": text,
	}
	body, _ := json.Marshal(msg)
	return body, a.cfg.Webhook
}

// --- Signing helpers ---

// feishuSign: HMAC-SHA256(key="", msg=timestamp+"\n"+secret) → base64
func feishuSign(timestamp, secret string) string {
	stringToSign := timestamp + "\n" + secret
	h := hmac.New(sha256.New, []byte(stringToSign))
	h.Write([]byte{})
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// dingtalkSign: HMAC-SHA256(key=secret, msg=timestamp+"\n"+secret) → base64
func dingtalkSign(timestamp, secret string) string {
	stringToSign := timestamp + "\n" + secret
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
