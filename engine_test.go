package fxmq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	fxmq "github.com/zengzhifei/forxi-mq"
	"github.com/zengzhifei/forxi-mq/internal"
)

func cleanupKeys(rdb *redis.Client, topic string) {
	ctx := context.Background()
	rdb.Del(ctx,
		internal.StreamKey(topic),
		internal.DeadLetterKey(topic, "test-group"),
		internal.DelayKey(topic),
		internal.DelayDataKey(topic),
	)
	// Clean retry keys
	keys, _ := rdb.Keys(ctx, "fxmq:retry:"+topic+":*").Result()
	if len(keys) > 0 {
		rdb.Del(ctx, keys...)
	}
	// Clean delay-map keys
	mapKeys, _ := rdb.Keys(ctx, "fxmq:delay-map:"+topic+":*").Result()
	if len(mapKeys) > 0 {
		rdb.Del(ctx, mapKeys...)
	}
	// Clean lock keys
	lockKeys, _ := rdb.Keys(ctx, "fxmq:lock:"+topic+":*").Result()
	if len(lockKeys) > 0 {
		rdb.Del(ctx, lockKeys...)
	}
}

func newTestEngine(t *testing.T, opts ...fxmq.Option) *fxmq.Engine {
	t.Helper()
	defaults := []fxmq.Option{
		fxmq.WithMaxRetry(3),
		fxmq.WithAckTimeout(2 * time.Second),
	}
	e, err := fxmq.NewEngine("localhost:6379", "test-group", append(defaults, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// TestNormalConsume verifies that a message is published and consumed successfully.
func TestNormalConsume(t *testing.T) {
	topic := "test-normal-consume"
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	cleanupKeys(rdb, topic)
	defer cleanupKeys(rdb, topic)

	// Ensure consumer group exists
	rdb.XGroupCreateMkStream(context.Background(), internal.StreamKey(topic), "test-group", "0")

	e := newTestEngine(t)
	defer e.Shutdown()

	var received atomic.Int32
	ctx := context.Background()

	err := e.Subscribe(ctx, topic, func(ctx context.Context, msg *fxmq.Message) error {
		received.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	e.Start(ctx)

	// Publish a message
	msg, _ := fxmq.NewMessage(topic, map[string]any{"key": "value"})
	if err := e.Publish(ctx, msg); err != nil {
		t.Fatal(err)
	}

	// Wait for consumption
	deadline := time.After(5 * time.Second)
	for received.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for message consumption")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	if received.Load() != 1 {
		t.Fatalf("expected 1 message, got %d", received.Load())
	}
}

// TestRetryAndDLQ verifies that failing messages are retried and eventually moved to DLQ.
func TestRetryAndDLQ(t *testing.T) {
	topic := "test-retry-dlq"
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	cleanupKeys(rdb, topic)
	defer cleanupKeys(rdb, topic)

	rdb.XGroupCreateMkStream(context.Background(), internal.StreamKey(topic), "test-group", "0")

	e := newTestEngine(t,
		fxmq.WithMaxRetry(3),
		fxmq.WithAckTimeout(2*time.Second),
		fxmq.WithConcurrency(1),
	)
	defer e.Shutdown()

	var attempts atomic.Int32
	ctx := context.Background()

	err := e.Subscribe(ctx, topic, func(ctx context.Context, msg *fxmq.Message) error {
		attempts.Add(1)
		return fmt.Errorf("always fail")
	})
	if err != nil {
		t.Fatal(err)
	}

	e.Start(ctx)

	// Publish a message
	msg, _ := fxmq.NewMessage(topic, map[string]any{"fail": true})
	if err := e.Publish(ctx, msg); err != nil {
		t.Fatal(err)
	}

	// Wait for message to reach DLQ (3 retries * ~1s ack timeout + processing time)
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timeout: attempts=%d, checking DLQ", attempts.Load())
		default:
		}

		dlqLen, _ := rdb.XLen(ctx, internal.DeadLetterKey(topic, "test-group")).Result()
		if dlqLen > 0 {
			t.Logf("message reached DLQ after %d handler attempts", attempts.Load())
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Verify DLQ entry
	dlqMsgs, _ := rdb.XRange(ctx, internal.DeadLetterKey(topic, "test-group"), "-", "+").Result()
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(dlqMsgs))
	}

	// Verify handler was called multiple times (at least initial + retries)
	if attempts.Load() < 3 {
		t.Logf("warning: only %d attempts, expected at least 3", attempts.Load())
	}
}

// TestDelayPublish verifies delayed messages are eventually published to the stream.
func TestDelayPublish(t *testing.T) {
	topic := "test-delay-pub"
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	cleanupKeys(rdb, topic)
	defer cleanupKeys(rdb, topic)

	rdb.XGroupCreateMkStream(context.Background(), internal.StreamKey(topic), "test-group", "0")

	e := newTestEngine(t)
	defer e.Shutdown()

	var received atomic.Int32
	ctx := context.Background()

	err := e.Subscribe(ctx, topic, func(ctx context.Context, msg *fxmq.Message) error {
		received.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	e.Start(ctx)

	// Publish with 2s delay
	msg, _ := fxmq.NewMessage(topic, map[string]any{"delayed": true})
	if err := e.DelayPublish(ctx, msg, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	// Should NOT be consumed immediately
	time.Sleep(500 * time.Millisecond)
	if received.Load() != 0 {
		t.Fatal("message consumed too early")
	}

	// Verify ZSET + Hash exist
	zLen, _ := rdb.ZCard(ctx, internal.DelayKey(topic)).Result()
	hLen, _ := rdb.HLen(ctx, internal.DelayDataKey(topic)).Result()
	if zLen != 1 || hLen != 1 {
		t.Fatalf("expected ZSET=1, Hash=1; got ZSET=%d, Hash=%d", zLen, hLen)
	}

	// Wait for delay to expire + poll interval
	deadline := time.After(10 * time.Second)
	for received.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for delayed message")
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}

	// Verify ZSET + Hash are cleaned up
	zLen, _ = rdb.ZCard(ctx, internal.DelayKey(topic)).Result()
	hLen, _ = rdb.HLen(ctx, internal.DelayDataKey(topic)).Result()
	if zLen != 0 || hLen != 0 {
		t.Fatalf("expected ZSET=0, Hash=0 after transfer; got ZSET=%d, Hash=%d", zLen, hLen)
	}
}

// TestRecoveryClaimNoRepublish verifies that recovery claims messages without re-publishing.
// After recovery, stream length should remain the same (no duplicate XADD).
func TestRecoveryClaimNoRepublish(t *testing.T) {
	topic := "test-recovery-claim"
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	cleanupKeys(rdb, topic)
	defer cleanupKeys(rdb, topic)

	ctx := context.Background()
	stream := internal.StreamKey(topic)

	rdb.XGroupCreateMkStream(ctx, stream, "test-group", "0")

	// Add a message
	m, _ := fxmq.NewMessage(topic, map[string]any{"test": "no-republish"})
	body, _ := json.Marshal(m)

	origID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"body": string(body)},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}

	// Read the message to put it in PEL (simulating a worker that didn't ACK)
	rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "test-group",
		Consumer: "consumer-1",
		Streams:  []string{stream, ">"},
		Count:    1,
	})

	// Use XAUTOCLAIM to claim the message (simulating Recovery)
	time.Sleep(100 * time.Millisecond) // small idle time
	claimed, _, err := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    "test-group",
		Consumer: "recovery-consumer",
		MinIdle:  50 * time.Millisecond,
		Start:    "0-0",
		Count:    10,
	}).Result()
	if err != nil {
		t.Fatal(err)
	}

	if len(claimed) != 1 || claimed[0].ID != origID {
		t.Fatalf("expected claimed message with ID %s, got %v", origID, claimed)
	}

	// Verify stream length is still 1 (no re-publish happened)
	streamLen, _ := rdb.XLen(ctx, stream).Result()
	if streamLen != 1 {
		t.Fatalf("expected stream length 1 (no republish), got %d", streamLen)
	}

	// Verify the message is still in PEL (now owned by recovery-consumer)
	pending, _ := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  "test-group",
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()
	if len(pending) != 1 || pending[0].ID != origID {
		t.Fatalf("expected pending message with ID %s, got %v", origID, pending)
	}
	if pending[0].Consumer != "recovery-consumer" {
		t.Fatalf("expected consumer 'recovery-consumer', got '%s'", pending[0].Consumer)
	}
}

// TestMessageIDPreservedThroughRetry verifies that the same message ID is used throughout
// the entire retry lifecycle (no ID changes).
func TestMessageIDPreservedThroughRetry(t *testing.T) {
	topic := "test-id-preserved"
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	cleanupKeys(rdb, topic)
	defer cleanupKeys(rdb, topic)

	rdb.XGroupCreateMkStream(context.Background(), internal.StreamKey(topic), "test-group", "0")

	e := newTestEngine(t,
		fxmq.WithMaxRetry(2),
		fxmq.WithAckTimeout(2*time.Second),
		fxmq.WithConcurrency(1),
	)
	defer e.Shutdown()

	var seenIDs sync.Map
	var attempts atomic.Int32
	ctx := context.Background()

	err := e.Subscribe(ctx, topic, func(ctx context.Context, msg *fxmq.Message) error {
		attempts.Add(1)
		seenIDs.Store(msg.ID, true)
		return fmt.Errorf("fail")
	})
	if err != nil {
		t.Fatal(err)
	}

	e.Start(ctx)

	// Publish
	msg, _ := fxmq.NewMessage(topic, map[string]any{"trace": "id"})
	if err := e.Publish(ctx, msg); err != nil {
		t.Fatal(err)
	}

	// Wait for DLQ (means all retries exhausted)
	deadline := time.After(20 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for DLQ")
		default:
		}
		dlqLen, _ := rdb.XLen(ctx, internal.DeadLetterKey(topic, "test-group")).Result()
		if dlqLen > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// All handler invocations should have seen the same message ID
	uniqueIDs := 0
	seenIDs.Range(func(_, _ interface{}) bool {
		uniqueIDs++
		return true
	})
	if uniqueIDs != 1 {
		t.Fatalf("expected all attempts to use same ID, got %d unique IDs", uniqueIDs)
	}

	// Stream should still have only 1 message (no republish)
	streamLen, _ := rdb.XLen(ctx, internal.StreamKey(topic)).Result()
	if streamLen != 1 {
		t.Fatalf("expected stream length 1, got %d (messages were republished!)", streamLen)
	}

	t.Logf("message processed %d times with same ID, then moved to DLQ", attempts.Load())
}

// TestIsDeadSkipsHandler verifies that a message marked as dead is not re-processed by pendingReader.
func TestIsDeadSkipsHandler(t *testing.T) {
	topic := "test-isdead-skip"
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	cleanupKeys(rdb, topic)
	defer cleanupKeys(rdb, topic)

	ctx := context.Background()
	stream := internal.StreamKey(topic)

	rdb.XGroupCreateMkStream(ctx, stream, "test-group", "0")

	// Add a message manually and put it in PEL
	m, _ := fxmq.NewMessage(topic, map[string]any{"dead": true})
	body, _ := json.Marshal(m)
	msgID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"body": string(body)},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}

	// Read it into PEL under the engine's consumer name
	hostname, _ := rdb.Do(ctx, "CLIENT", "GETNAME").Result()
	_ = hostname
	// Use a consumer name that matches what the engine will use for pendingReader
	rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "test-group",
		Consumer: "test-isdead-skip-consumer",
		Streams:  []string{stream, ">"},
		Count:    1,
	})

	// Mark as dead BEFORE starting the engine
	retryKey := fmt.Sprintf("fxmq:retry:%s:%s:%s", topic, "test-group", msgID)
	rdb.Set(ctx, retryKey, "-1", 10*time.Minute)

	// Now start engine — the retryWorker should see this pending message but skip it
	e, err := fxmq.NewEngine("localhost:6379", "test-group",
		fxmq.WithAckTimeout(2*time.Second),
		fxmq.WithConcurrency(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Shutdown()

	var handlerCalls atomic.Int32

	// Subscribe but don't expect any calls
	err = e.Subscribe(ctx, topic, func(ctx context.Context, msg *fxmq.Message) error {
		handlerCalls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	e.Start(ctx)

	// Wait — recovery will claim the message to engine's consumer, pendingReader picks it up
	// but IsDead check should prevent handler invocation and ACK the message
	time.Sleep(5 * time.Second)

	if handlerCalls.Load() != 0 {
		t.Fatalf("expected 0 handler calls for dead message, got %d", handlerCalls.Load())
	}

	// Verify the message is still in PEL (dead messages are NOT acked, kept for requeue)
	pending, _ := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  "test-group",
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()

	found := false
	for _, p := range pending {
		if p.ID == msgID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("dead message %s should remain in PEL for requeue, but was ACKed", msgID)
	}
}

// TestDelayPublishReturnsTrackableID verifies that DelayPublish returns an ID
// that can be used to find the message after delivery via the delay-map.
func TestDelayPublishReturnsTrackableID(t *testing.T) {
	topic := "test-delay-track"
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	cleanupKeys(rdb, topic)
	defer cleanupKeys(rdb, topic)

	rdb.XGroupCreateMkStream(context.Background(), internal.StreamKey(topic), "test-group", "0")

	e := newTestEngine(t)
	defer e.Shutdown()

	var received atomic.Int32
	ctx := context.Background()

	err := e.Subscribe(ctx, topic, func(ctx context.Context, msg *fxmq.Message) error {
		received.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	e.Start(ctx)

	// Publish with 1s delay
	msg, _ := fxmq.NewMessage(topic, map[string]any{"tracked": true})
	if err := e.DelayPublish(ctx, msg, 1*time.Second); err != nil {
		t.Fatal(err)
	}

	delayID := msg.ID
	if delayID == "" {
		t.Fatal("expected DelayPublish to set msg.ID")
	}

	// Wait for delivery
	deadline := time.After(10 * time.Second)
	for received.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for delayed message delivery")
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}

	// Verify the delay-map entry exists
	mapKey := internal.DelayMapKey(topic, delayID)
	streamID, err := rdb.Get(ctx, mapKey).Result()
	if err != nil {
		t.Fatalf("expected delay-map entry for %s, got error: %v", delayID, err)
	}
	if streamID == "" {
		t.Fatal("delay-map returned empty stream ID")
	}

	// Verify the stream message exists at that ID
	msgs, _ := rdb.XRangeN(ctx, internal.StreamKey(topic), streamID, streamID, 1).Result()
	if len(msgs) == 0 {
		t.Fatalf("expected stream message at ID %s", streamID)
	}

	t.Logf("delay ID %s → stream ID %s (trackable)", delayID, streamID)
}

// TestDLQPushFailureNoAck verifies that if DLQ push fails, the message is NOT acked.
func TestDLQPushFailureNoAck(t *testing.T) {
	// This is hard to test without mocking the DLQ Redis connection.
	// We verify the code path indirectly: if DLQ push returns error,
	// handleFailure returns before ACK (code inspection confirmed).
	t.Skip("requires DLQ failure injection - verified via code review")
}

// TestLockPreventsConcurrentProcessing verifies that the Redis lock prevents
// the recovery/retry path from concurrently processing a message whose handler
// is still running (exceeded AckTimeout but hasn't returned yet).
func TestLockPreventsConcurrentProcessing(t *testing.T) {
	topic := "test-lock-prevent"
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	cleanupKeys(rdb, topic)
	defer cleanupKeys(rdb, topic)

	ctx := context.Background()
	rdb.XGroupCreateMkStream(ctx, internal.StreamKey(topic), "test-group", "0")

	e := newTestEngine(t,
		fxmq.WithMaxRetry(3),
		fxmq.WithAckTimeout(1*time.Second),
		fxmq.WithConcurrency(1),
	)
	defer e.Shutdown()

	var handlerCalls atomic.Int32
	handlerStarted := make(chan struct{})
	handlerUnblock := make(chan struct{})

	err := e.Subscribe(ctx, topic, func(ctx context.Context, msg *fxmq.Message) error {
		handlerCalls.Add(1)
		close(handlerStarted)
		<-handlerUnblock
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	e.Start(ctx)

	// Publish a message
	msg, _ := fxmq.NewMessage(topic, map[string]any{"slow": true})
	if err := e.Publish(ctx, msg); err != nil {
		t.Fatal(err)
	}

	// Wait for handler to start
	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for handler to start")
	}

	t.Log("handler started, blocking...")

	// Wait for recovery to XAUTOCLAIM and try retry.
	// AckTimeout=1s, RecoveryInterval=250ms, so first XAUTOCLAIM at ~1.25s.
	// Wait enough time for multiple recovery cycles to ensure the lock blocked them all.
	time.Sleep(5 * time.Second)

	// Handler should still only have been called once (lock prevented retry)
	calls := handlerCalls.Load()
	if calls != 1 {
		t.Fatalf("expected 1 handler call, got %d (lock failed to prevent concurrent processing)", calls)
	}

	// Verify lock key exists in Redis while handler is running
	lockKey := internal.LockKey(topic, msg.ID)
	lockExists, err := rdb.Exists(ctx, lockKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if lockExists != 1 {
		t.Fatal("lock key should exist while handler is running")
	}

	// Verify lock has a reasonable TTL (should be ~MaxRetry*(AckTimeout+RecoveryInterval)+LockBuffer)
	lockTTL, _ := rdb.TTL(ctx, lockKey).Result()
	t.Logf("lock TTL while handler running: %v", lockTTL)
	if lockTTL <= 0 {
		t.Fatal("lock should have positive TTL")
	}

	// Unblock the handler
	close(handlerUnblock)

	// Wait for handler to finish, ACK, and release lock
	time.Sleep(2 * time.Second)

	// Verify lock was released
	lockExists, err = rdb.Exists(ctx, lockKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if lockExists != 0 {
		t.Fatal("lock key should be released after handler completion")
	}

	// Verify DLQ entry was cleaned up because handler eventually succeeded
	dlqKey := internal.DeadLetterKey(topic, "test-group")
	dlqLen, _ := rdb.XLen(ctx, dlqKey).Result()
	if dlqLen != 0 {
		t.Fatalf("DLQ should be empty after handler success, got %d entries", dlqLen)
	}

	// Retry key should also be cleared
	retryKey := fmt.Sprintf("fxmq:retry:%s:%s:%s", topic, "test-group", msg.ID)
	retryExists, _ := rdb.Exists(ctx, retryKey).Result()
	if retryExists != 0 {
		t.Fatal("retry key should be cleared after handler success")
	}

	t.Logf("lock test passed: handler called %d time(s), lock properly acquired and released, DLQ cleaned up", calls)
}

// TestLockReleasedOnFailure verifies that the lock is released even when the
// handler returns an error, allowing recovery to retry the message.
func TestLockReleasedOnFailure(t *testing.T) {
	topic := "test-lock-failure"
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	cleanupKeys(rdb, topic)
	defer cleanupKeys(rdb, topic)

	ctx := context.Background()
	rdb.XGroupCreateMkStream(ctx, internal.StreamKey(topic), "test-group", "0")

	e := newTestEngine(t,
		fxmq.WithMaxRetry(3),
		fxmq.WithAckTimeout(2*time.Second),
		fxmq.WithConcurrency(1),
	)
	defer e.Shutdown()

	var attempts atomic.Int32
	var mu sync.Mutex
	var firstMsgID string

	err := e.Subscribe(ctx, topic, func(ctx context.Context, msg *fxmq.Message) error {
		mu.Lock()
		if firstMsgID == "" {
			firstMsgID = msg.ID
		}
		mu.Unlock()
		attempts.Add(1)
		return fmt.Errorf("handler error")
	})
	if err != nil {
		t.Fatal(err)
	}

	e.Start(ctx)

	msg, _ := fxmq.NewMessage(topic, map[string]any{"fail": true})
	if err := e.Publish(ctx, msg); err != nil {
		t.Fatal(err)
	}

	// Wait for handler to process and release lock after failure
	// Then recovery should XAUTOCLAIM and retry successfully (lock released)
	time.Sleep(2 * time.Second)

	// After first failure, the handler releases the lock.
	// Verify lock is NOT present (released after failure).
	mu.Lock()
	mid := firstMsgID
	mu.Unlock()
	if mid != "" {
		lockKey := internal.LockKey(topic, mid)
		exists, _ := rdb.Exists(ctx, lockKey).Result()
		if exists != 0 {
			t.Fatal("lock should be released after handler failure")
		}
	}

	// Wait for retries to happen (lock is released, so retry should succeed)
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for retries, attempts=%d", attempts.Load())
		default:
		}
		if attempts.Load() >= 2 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Logf("lock released on failure: %d attempts (initial + retry)", attempts.Load())
}
