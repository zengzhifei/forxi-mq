package alert

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strconv"
	"time"
)

// dingtalkSign: HMAC-SHA256(key=secret, msg=timestamp+"\n"+secret) -> base64
func dingtalkSign(timestamp, secret string) string {
	stringToSign := timestamp + "\n" + secret
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// buildDingTalk constructs a DingTalk bot payload with optional signature.
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
