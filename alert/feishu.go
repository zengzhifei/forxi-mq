package alert

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// feishuSign: HMAC-SHA256(key=stringToSign, msg="") -> base64
func feishuSign(timestamp, secret string) string {
	stringToSign := timestamp + "\n" + secret
	h := hmac.New(sha256.New, []byte(stringToSign))
	h.Write([]byte{})
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// buildFeishu constructs a Feishu interactive card payload.
func (a *Alerter) buildFeishu(markdown string) ([]byte, string) {
	timestamp := time.Now().Unix()

	now := time.Now().Format("2006-01-02 15:04:05")
	markdown += fmt.Sprintf("\n---\n*时间: %s*", now)

	card := map[string]interface{}{
		"schema": "2.0",
		"header": map[string]interface{}{
			"title": map[string]string{
				"tag":     "plain_text",
				"content": "forxi-mq 告警",
			},
			"template": "red",
		},
		"body": map[string]interface{}{
			"elements": []interface{}{
				map[string]interface{}{
					"tag":     "markdown",
					"content": markdown,
				},
			},
		},
	}

	msg := map[string]interface{}{
		"msg_type": "interactive",
		"card":     card,
	}

	if a.cfg.Secret != "" {
		ts := strconv.FormatInt(timestamp, 10)
		sign := feishuSign(ts, a.cfg.Secret)
		msg["timestamp"] = ts
		msg["sign"] = sign
	}

	body, _ := json.Marshal(msg)
	return body, a.cfg.Webhook
}
