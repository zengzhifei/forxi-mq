package alert

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// buildWeComMarkdown builds a WeCom markdown message with color formatting.
func (a *Alerter) buildWeComMarkdown(topic string, reasons []string, now time.Time) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("**Topic**: %s | **Group**: %s\n\n", topic, a.group))
	b.WriteString("**触发条件**:\n")
	for _, r := range reasons {
		name, val, th := parseReason(r)
		b.WriteString(fmt.Sprintf("> %s = <font color=\"warning\">%s</font> (阈值 %s)\n", name, val, th))
	}
	b.WriteString(fmt.Sprintf("\n<font color=\"comment\">时间: %s</font>", now.Format("2006-01-02 15:04:05")))
	return b.String()
}

// buildWeCom constructs a WeCom (企业微信) bot markdown payload.
func (a *Alerter) buildWeCom(markdown string) ([]byte, string) {
	msg := map[string]any{
		"msgtype": "markdown",
		"markdown": map[string]string{
			"content": markdown,
		},
	}
	body, _ := json.Marshal(msg)
	return body, a.cfg.Webhook
}

// parseReason parses "lag=150 (阈值:100)" → "lag", "150", "100"
func parseReason(r string) (name, value, threshold string) {
	before, after, ok := strings.Cut(r, " (阈值:")
	if !ok {
		return r, "-", "-"
	}
	threshold = strings.TrimSuffix(after, ")")
	if n, v, found := strings.Cut(before, "="); found {
		return n, v, threshold
	}
	return before, "-", threshold
}
