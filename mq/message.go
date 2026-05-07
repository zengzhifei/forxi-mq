package mq

import (
	"encoding/json"
	"time"
)

// Message represents a message in the queue.
type Message struct {
	ID        string            `json:"id"`
	Topic     string            `json:"topic"`
	Payload   json.RawMessage   `json:"payload"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
}

// NewMessage creates a new message with the given topic and payload.
// payload will be marshaled to JSON.
func NewMessage(topic string, payload any) (*Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Message{
		Topic:     topic,
		Payload:   data,
		Metadata:  make(map[string]string),
		CreatedAt: time.Now(),
	}, nil
}

// Decode unmarshals the payload into dst.
func (m *Message) Decode(dst any) error {
	return json.Unmarshal(m.Payload, dst)
}

// SetMeta sets a metadata key-value pair.
func (m *Message) SetMeta(key, value string) {
	if m.Metadata == nil {
		m.Metadata = make(map[string]string)
	}
	m.Metadata[key] = value
}
