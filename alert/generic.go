package alert

import "encoding/json"

// buildGeneric constructs a simple JSON POST payload for custom webhooks.
func (a *Alerter) buildGeneric(text string) ([]byte, string) {
	msg := map[string]any{
		"text": text,
	}
	body, _ := json.Marshal(msg)
	return body, a.cfg.Webhook
}
