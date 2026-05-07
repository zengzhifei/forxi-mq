package internal

import "fmt"

const prefix = "fxmq"

// StreamKey returns the Redis Stream key for a topic.
func StreamKey(topic string) string {
	return fmt.Sprintf("%s:%s", prefix, topic)
}

// DeadLetterKey returns the dead letter stream key for a topic.
func DeadLetterKey(topic string) string {
	return fmt.Sprintf("%s:dead:%s", prefix, topic)
}

// DelayKey returns the sorted set key for delay messages.
func DelayKey(topic string) string {
	return fmt.Sprintf("%s:delay:%s", prefix, topic)
}

// RetryCountKey returns the retry counter key for a specific message.
func RetryCountKey(topic, msgID string) string {
	return fmt.Sprintf("%s:retry:%s:%s", prefix, topic, msgID)
}
