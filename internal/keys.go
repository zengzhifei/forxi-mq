package internal

import "fmt"

const prefix = "fxmq"

// StreamKey returns the Redis Stream key for a topic.
func StreamKey(topic string) string {
	return fmt.Sprintf("%s:%s", prefix, topic)
}

// DeadLetterKey returns the dead letter stream key for a topic and group.
func DeadLetterKey(topic, group string) string {
	return fmt.Sprintf("%s:dead:%s:%s", prefix, topic, group)
}

// DelayKey returns the sorted set key for delay messages (stores ID → score).
func DelayKey(topic string) string {
	return fmt.Sprintf("%s:delay:%s", prefix, topic)
}

// DelayDataKey returns the hash key that stores delay message bodies (ID → body).
func DelayDataKey(topic string) string {
	return fmt.Sprintf("%s:delay:data:%s", prefix, topic)
}

// DelayMapKey returns the key that maps a delay ID to its stream ID after delivery.
func DelayMapKey(topic, delayID string) string {
	return fmt.Sprintf("%s:delay-map:%s:%s", prefix, topic, delayID)
}

// RetryCountKey returns the retry counter key for a specific message in a group.
func RetryCountKey(topic, group, msgID string) string {
	return fmt.Sprintf("%s:retry:%s:%s:%s", prefix, topic, group, msgID)
}

// LockKey returns the lock key for a message to prevent concurrent processing.
func LockKey(topic, msgID string) string {
	return fmt.Sprintf("%s:lock:%s:%s", prefix, topic, msgID)
}

// TopicsSetKey returns the key for the set that tracks all known topic names.
func TopicsSetKey() string {
	return fmt.Sprintf("%s:topics", prefix)
}
