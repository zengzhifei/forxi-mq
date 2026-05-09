package dashboard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
	"github.com/zengzhifei/forxi-mq/mq"
)

// --- Response types ---

type overviewResp struct {
	Topics     int   `json:"topics"`
	TotalMsgs  int64 `json:"total_msgs"`
	TotalDead  int64 `json:"total_dead"`
	TotalDelay int64 `json:"total_delay"`
}

type topicInfo struct {
	Name      string      `json:"name"`
	Stored    int64       `json:"stored"`
	Lag       int64       `json:"lag"`
	Pending   int64       `json:"pending"`
	Dead      int64       `json:"dead"`
	Delay     int64       `json:"delay"`
	SelfGroup string      `json:"self_group,omitempty"`
	Groups    []groupInfo `json:"groups,omitempty"`
}

type groupInfo struct {
	Name      string         `json:"name"`
	Pending   int64          `json:"pending"`
	Lag       int64          `json:"lag"`
	Consumers []consumerInfo `json:"consumers,omitempty"`
}

type consumerInfo struct {
	Name    string `json:"name"`
	Pending int64  `json:"pending"`
	Idle    string `json:"idle"`
}

type deadMessage struct {
	ID     string `json:"id"`
	Body   string `json:"body"`
	Reason string `json:"reason"`
}

type delayMessage struct {
	ID    string  `json:"id"`
	Body  string  `json:"body"`
	Score float64 `json:"score"`
	DueAt string  `json:"due_at"`
}

// --- Handlers ---

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"group": s.group})
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	topics := s.discoverTopics(ctx)
	resp := overviewResp{Topics: len(topics)}

	for _, topic := range topics {
		length, _ := s.rdb.XLen(ctx, internal.StreamKey(topic)).Result()
		dead, _ := s.rdb.XLen(ctx, internal.DeadLetterKey(topic, s.group)).Result()
		delay, _ := s.rdb.ZCard(ctx, internal.DelayKey(topic)).Result()
		resp.TotalMsgs += length
		resp.TotalDead += dead
		resp.TotalDelay += delay
	}

	writeJSON(w, resp)
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	discovered := s.discoverTopics(ctx)
	var topics []topicInfo

	for _, topic := range discovered {
		info := s.getTopicInfo(ctx, topic)
		topics = append(topics, info)
	}

	writeJSON(w, topics)
}

func (s *Server) handleTopicDetail(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()
	info := s.getTopicInfo(ctx, topic)
	info.SelfGroup = s.group

	// Get all groups and their consumers
	groups, err := s.rdb.XInfoGroups(ctx, internal.StreamKey(topic)).Result()
	if err == nil {
		for _, g := range groups {
			gi := groupInfo{
				Name:    g.Name,
				Pending: g.Pending,
				Lag:     g.Lag,
			}
			consumers, err := s.rdb.XInfoConsumers(ctx, internal.StreamKey(topic), g.Name).Result()
			if err == nil {
				for _, c := range consumers {
					gi.Consumers = append(gi.Consumers, consumerInfo{
						Name:    c.Name,
						Pending: c.Pending,
						Idle:    c.Idle.String(),
					})
				}
			}
			info.Groups = append(info.Groups, gi)
			// Override top-level stats with self group's values
			if g.Name == s.group {
				info.Lag = g.Lag
				info.Pending = g.Pending
			}
		}
	}

	writeJSON(w, info)
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()
	streamKey := internal.StreamKey(topic)

	count := int64(20)
	if c := r.URL.Query().Get("count"); c != "" {
		if n, err := strconv.ParseInt(c, 10, 64); err == nil {
			count = n
		}
	}

	// Get self group's last-delivered-id to scope messages
	lastDelivered := s.getGroupLastDeliveredID(ctx, streamKey)

	// Cursor-based pagination: cursor is the last ID from previous page
	// For XREVRANGE, cursor means "get messages before this ID"
	end := lastDelivered
	if end == "" {
		end = "+"
	}
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		end = "(" + cursor // exclusive
	}

	msgs, err := s.rdb.XRevRangeN(ctx, streamKey, end, "-", count).Result()
	if err != nil {
		writeJSON(w, map[string]any{"items": []map[string]any{}, "next_cursor": ""})
		return
	}

	var result []map[string]any
	for _, msg := range msgs {
		item := map[string]any{
			"id":     msg.ID,
			"values": filterValues(msg.Values),
		}
		result = append(result, item)
	}

	var nextCursor string
	if int64(len(result)) == count && len(result) > 0 {
		nextCursor = result[len(result)-1]["id"].(string)
	}

	writeJSON(w, map[string]any{"items": result, "next_cursor": nextCursor})
}

func (s *Server) handleLag(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()
	streamKey := internal.StreamKey(topic)

	count := int64(20)
	if c := r.URL.Query().Get("count"); c != "" {
		if n, err := strconv.ParseInt(c, 10, 64); err == nil {
			count = n
		}
	}

	// Use self group's last-delivered-id
	lastID := s.getGroupLastDeliveredID(ctx, streamKey)
	if lastID == "" {
		writeJSON(w, map[string]any{"items": []map[string]any{}, "next_cursor": ""})
		return
	}

	// Cursor-based pagination for lag
	start := lastID
	if start == "0-0" {
		start = "-"
	} else {
		start = "(" + start
	}
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		start = "(" + cursor
	}

	msgs, err := s.rdb.XRangeN(ctx, streamKey, start, "+", count).Result()
	if err != nil {
		writeJSON(w, map[string]any{"items": []map[string]any{}, "next_cursor": ""})
		return
	}

	var result []map[string]any
	for _, msg := range msgs {
		item := map[string]any{
			"id":     msg.ID,
			"values": filterValues(msg.Values),
		}
		result = append(result, item)
	}

	var nextCursor string
	if int64(len(result)) == count && len(result) > 0 {
		nextCursor = result[len(result)-1]["id"].(string)
	}

	writeJSON(w, map[string]any{"items": result, "next_cursor": nextCursor})
}

func (s *Server) handlePending(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()
	streamKey := internal.StreamKey(topic)

	count := int64(20)
	if c := r.URL.Query().Get("count"); c != "" {
		if n, err := strconv.ParseInt(c, 10, 64); err == nil {
			count = n
		}
	}

	// Cursor: start from after this ID
	startID := "-"
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		startID = "(" + cursor
	}

	// Only query self group's pending messages
	var result []map[string]any
	pending, err := s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  s.group,
		Start:  startID,
		End:    "+",
		Count:  count,
	}).Result()
	if err != nil {
		writeJSON(w, map[string]any{"items": []map[string]any{}, "next_cursor": ""})
		return
	}

	// Fetch message bodies
	for _, p := range pending {
		msgs, err := s.rdb.XRangeN(ctx, streamKey, p.ID, p.ID, 1).Result()
		if err != nil || len(msgs) == 0 {
			continue
		}

		// Get real retry count from forxi-mq retry counter
		retryCount, _ := s.rdb.Get(ctx, internal.RetryCountKey(topic, s.group, p.ID)).Int()

		item := map[string]any{
			"id":          p.ID,
			"consumer":    p.Consumer,
			"idle":        p.Idle.String(),
			"retry_count": retryCount,
			"values":      filterValues(msgs[0].Values),
		}
		result = append(result, item)
	}

	var nextCursor string
	if int64(len(result)) >= count && len(result) > 0 {
		nextCursor = result[len(result)-1]["id"].(string)
	}

	writeJSON(w, map[string]any{"items": result, "next_cursor": nextCursor})
}

func (s *Server) handleDeadLetters(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()

	count := int64(20)
	if c := r.URL.Query().Get("count"); c != "" {
		if n, err := strconv.ParseInt(c, 10, 64); err == nil {
			count = n
		}
	}

	start := "-"
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		start = "(" + cursor
	}

	msgs, err := s.rdb.XRangeN(ctx, internal.DeadLetterKey(topic, s.group), start, "+", count).Result()
	if err != nil {
		writeJSON(w, map[string]any{"items": []deadMessage{}, "next_cursor": ""})
		return
	}

	var result []deadMessage
	for _, msg := range msgs {
		dm := deadMessage{ID: msg.ID}
		if body, ok := msg.Values["body"].(string); ok {
			dm.Body = body
		}
		if reason, ok := msg.Values["reason"].(string); ok {
			dm.Reason = reason
		}
		result = append(result, dm)
	}

	var nextCursor string
	if int64(len(result)) == count && len(result) > 0 {
		nextCursor = result[len(result)-1].ID
	}

	writeJSON(w, map[string]any{"items": result, "next_cursor": nextCursor})
}

func (s *Server) handleRequeue(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()

	deadKey := internal.DeadLetterKey(topic, s.group)

	// Batch requeue dead messages (max 200 per request)
	msgs, err := s.rdb.XRangeN(ctx, deadKey, "-", "+", 200).Result()
	if err != nil || len(msgs) == 0 {
		writeJSON(w, map[string]int{"requeued": 0})
		return
	}

	count := 0
	for _, msg := range msgs {
		body, ok := msg.Values["body"].(string)
		if !ok {
			continue
		}

		// Extract original message ID and clear retry/dead state
		var origMsg mq.Message
		if err := json.Unmarshal([]byte(body), &origMsg); err == nil && origMsg.ID != "" {
			// Clear retry counter and dead mark — message is still in PEL,
			// Recovery will pick it up on next cycle and enqueue for retry.
			retryKey := internal.RetryCountKey(topic, s.group, origMsg.ID)
			s.rdb.Del(ctx, retryKey)
		}

		// Remove from DLQ
		s.rdb.XDel(ctx, deadKey, msg.ID)
		count++
	}

	writeJSON(w, map[string]int{"requeued": count})
}

func (s *Server) handleResend(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()

	var req struct {
		Body string `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Body == "" {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: internal.StreamKey(topic),
		Values: map[string]interface{}{"body": req.Body},
	}).Err()
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}

	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleDelayQueue(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()

	count := int64(20)
	if c := r.URL.Query().Get("count"); c != "" {
		if n, err := strconv.ParseInt(c, 10, 64); err == nil {
			count = n
		}
	}

	offset := int64(0)
	if o := r.URL.Query().Get("offset"); o != "" {
		if n, err := strconv.ParseInt(o, 10, 64); err == nil {
			offset = n
		}
	}

	results, err := s.rdb.ZRangeWithScores(ctx, internal.DelayKey(topic), offset, offset+count-1).Result()
	if err != nil {
		writeJSON(w, map[string]any{"items": []delayMessage{}, "next_offset": 0, "has_more": false})
		return
	}

	dataKey := internal.DelayDataKey(topic)
	var msgs []delayMessage
	for _, z := range results {
		id, _ := z.Member.(string)
		ts := int64(z.Score)
		dueAt := time.UnixMilli(ts).Format(time.RFC3339)

		// Get body from Hash
		body, _ := s.rdb.HGet(ctx, dataKey, id).Result()

		msgs = append(msgs, delayMessage{
			ID:    id,
			Body:  body,
			Score: z.Score,
			DueAt: dueAt,
		})
	}

	hasMore := int64(len(msgs)) == count
	nextOffset := offset + int64(len(msgs))

	writeJSON(w, map[string]any{"items": msgs, "next_offset": nextOffset, "has_more": hasMore})
}

func (s *Server) handleSearchMessage(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()
	id := r.URL.Query().Get("id")
	if id == "" {
		writeJSON(w, map[string]any{"found": false})
		return
	}

	streamKey := internal.StreamKey(topic)

	// Search in main stream
	if result, ok := s.findInStream(ctx, streamKey, topic, id); ok {
		writeJSON(w, result)
		return
	}

	// Search in dead letter
	msgs, err := s.rdb.XRangeN(ctx, internal.DeadLetterKey(topic, s.group), id, id, 1).Result()
	if err == nil && len(msgs) > 0 {
		dm := map[string]any{"id": msgs[0].ID}
		if body, ok := msgs[0].Values["body"].(string); ok {
			dm["body"] = body
		}
		if reason, ok := msgs[0].Values["reason"].(string); ok {
			dm["reason"] = reason
		}
		writeJSON(w, map[string]any{
			"found":   true,
			"source":  "dead",
			"status":  "dead",
			"message": dm,
		})
		return
	}

	// Search in delay queue (not yet delivered)
	dataKey := internal.DelayDataKey(topic)
	body, err := s.rdb.HGet(ctx, dataKey, id).Result()
	if err == nil && body != "" {
		score, _ := s.rdb.ZScore(ctx, internal.DelayKey(topic), id).Result()
		dueAt := ""
		if score > 0 {
			dueAt = time.UnixMilli(int64(score)).Format(time.RFC3339)
		}
		writeJSON(w, map[string]any{
			"found":  true,
			"source": "delay",
			"status": "waiting",
			"message": map[string]any{
				"id":     id,
				"body":   body,
				"due_at": dueAt,
			},
		})
		return
	}

	// Search via delay-map (delay message already delivered to stream)
	mapKey := internal.DelayMapKey(topic, id)
	streamID, err := s.rdb.Get(ctx, mapKey).Result()
	if err == nil && streamID != "" {
		if result, ok := s.findInStream(ctx, streamKey, topic, streamID); ok {
			if m, ok2 := result["message"].(map[string]any); ok2 {
				m["delay_id"] = id
			}
			writeJSON(w, result)
			return
		}
		// Stream message already trimmed
		writeJSON(w, map[string]any{
			"found":  true,
			"source": "delay-delivered",
			"status": "trimmed",
			"message": map[string]any{
				"id":        id,
				"stream_id": streamID,
			},
		})
		return
	}

	writeJSON(w, map[string]any{"found": false})
}

// findInStream searches for a message in the stream and determines its status.
func (s *Server) findInStream(ctx context.Context, streamKey, topic, id string) (map[string]any, bool) {
	msgs, err := s.rdb.XRangeN(ctx, streamKey, id, id, 1).Result()
	if err != nil || len(msgs) == 0 {
		return nil, false
	}

	status := s.getMessageStatus(ctx, streamKey, topic, id)

	result := map[string]any{
		"found":  true,
		"source": "stream",
		"status": status,
		"message": map[string]any{
			"id":     msgs[0].ID,
			"values": filterValues(msgs[0].Values),
		},
	}
	return result, true
}

// getMessageStatus determines if a stream message is consumed, pending, retrying, or dead.
func (s *Server) getMessageStatus(ctx context.Context, streamKey, topic, msgID string) string {
	// Check retry counter first
	retryKey := internal.RetryCountKey(topic, s.group, msgID)
	retryVal, err := s.rdb.Get(ctx, retryKey).Result()
	if err == nil {
		if retryVal == "-1" {
			return "dead"
		}
	}

	groups, err := s.rdb.XInfoGroups(ctx, streamKey).Result()
	if err != nil || len(groups) == 0 {
		return "unknown"
	}

	for _, g := range groups {
		// Check if message is after last-delivered-id (lag)
		if compareStreamID(msgID, g.LastDeliveredID) > 0 {
			return "lag"
		}

		// Check if message is in PEL (pending)
		pending, err := s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: streamKey,
			Group:  g.Name,
			Start:  msgID,
			End:    msgID,
			Count:  1,
		}).Result()
		if err == nil && len(pending) > 0 && pending[0].ID == msgID {
			// If retry count > 0, it's actively retrying
			if retryVal != "" {
				count, _ := strconv.Atoi(retryVal)
				if count > 0 {
					return "retrying"
				}
			}
			return "pending"
		}
	}

	return "consumed"
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()

	var req struct {
		Payload  json.RawMessage   `json:"payload"`
		Metadata map[string]string `json:"metadata,omitempty"`
		DueAt    int64             `json:"due_at"` // unix milliseconds, 0 means immediate
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Payload) == 0 {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	msg := &mq.Message{
		Topic:     topic,
		Payload:   req.Payload,
		Metadata:  req.Metadata,
		CreatedAt: time.Now(),
	}

	body, err := json.Marshal(msg)
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}

	// Delay publish if due_at is set and in the future
	if req.DueAt > 0 {
		dueTime := time.UnixMilli(req.DueAt)
		if !dueTime.After(time.Now()) {
			writeJSON(w, map[string]any{"ok": false, "error": "due_at must be in the future"})
			return
		}

		id := generateDelayID()
		score := float64(req.DueAt)
		delayKey := internal.DelayKey(topic)
		dataKey := internal.DelayDataKey(topic)

		pipe := s.rdb.Pipeline()
		pipe.ZAdd(ctx, delayKey, redis.Z{Score: score, Member: id})
		pipe.HSet(ctx, dataKey, id, string(body))
		_, err = pipe.Exec(ctx)
		if err != nil {
			writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"ok": true, "id": id, "type": "delay"})
		return
	}

	// Normal publish
	args := &redis.XAddArgs{
		Stream: internal.StreamKey(topic),
		Values: map[string]interface{}{"body": string(body)},
	}
	id, err := s.rdb.XAdd(ctx, args).Result()
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}

	writeJSON(w, map[string]any{"ok": true, "id": id, "type": "normal"})
}

func (s *Server) handleDeleteDead(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	id := r.PathValue("id")
	ctx := r.Context()

	err := s.rdb.XDel(ctx, internal.DeadLetterKey(topic, s.group), id).Err()
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleDeleteDelay(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()

	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	delayKey := internal.DelayKey(topic)
	dataKey := internal.DelayDataKey(topic)

	pipe := s.rdb.Pipeline()
	pipe.ZRem(ctx, delayKey, req.ID)
	pipe.HDel(ctx, dataKey, req.ID)
	_, err := pipe.Exec(ctx)
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleResetGroup(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	group := r.PathValue("group")
	ctx := r.Context()

	// Only allow resetting self group
	if group != s.group {
		writeJSON(w, map[string]any{"ok": false, "error": "can only reset own group"})
		return
	}

	var req struct {
		ID string `json:"id"` // "0" = reset to beginning, "$" = latest
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		req.ID = "0" // default: re-consume all
	}

	err := s.rdb.XGroupSetID(ctx, internal.StreamKey(topic), group, req.ID).Err()
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()

	streamKey := internal.StreamKey(topic)
	dlqKey := internal.DeadLetterKey(topic, s.group)
	delayKey := internal.DelayKey(topic)
	delayDataKey := internal.DelayDataKey(topic)

	// Check stream is empty
	streamLen, _ := s.rdb.XLen(ctx, streamKey).Result()
	if streamLen > 0 {
		writeJSON(w, map[string]any{"ok": false, "error": "stream is not empty, cannot delete"})
		return
	}

	// Check DLQ is empty
	dlqLen, _ := s.rdb.XLen(ctx, dlqKey).Result()
	if dlqLen > 0 {
		writeJSON(w, map[string]any{"ok": false, "error": "dead letter queue is not empty, cannot delete"})
		return
	}

	// Check delay queue is empty
	delayLen, _ := s.rdb.ZCard(ctx, delayKey).Result()
	if delayLen > 0 {
		writeJSON(w, map[string]any{"ok": false, "error": "delay queue is not empty, cannot delete"})
		return
	}

	// All empty — safe to delete keys
	s.rdb.Del(ctx, streamKey, dlqKey, delayKey, delayDataKey)

	writeJSON(w, map[string]any{"ok": true})
}

// --- Helpers ---

// getGroupLastDeliveredID returns the last-delivered-id for the self group on the given stream.
// Returns "" if the group doesn't exist on this stream.
func (s *Server) getGroupLastDeliveredID(ctx context.Context, streamKey string) string {
	groups, err := s.rdb.XInfoGroups(ctx, streamKey).Result()
	if err != nil {
		return ""
	}
	for _, g := range groups {
		if g.Name == s.group {
			return g.LastDeliveredID
		}
	}
	return ""
}

// discoverTopics scans Redis for all fxmq:* keys and extracts topic names.
func (s *Server) discoverTopics(ctx context.Context) []string {
	seen := make(map[string]bool)

	var cursor uint64
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, "fxmq:*", 100).Result()
		if err != nil {
			break
		}
		for _, key := range keys {
			topic := extractTopic(key)
			if topic != "" && !seen[topic] {
				seen[topic] = true
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	topics := make([]string, 0, len(seen))
	for t := range seen {
		topics = append(topics, t)
	}
	return topics
}

// extractTopic extracts the topic name from a Redis key.
// fxmq:{topic}         → {topic}
// fxmq:dead:{topic}    → {topic}
// fxmq:delay:{topic}   → {topic}
// fxmq:retry:{topic}:* → (ignored, too granular)
func extractTopic(key string) string {
	parts := strings.TrimPrefix(key, "fxmq:")

	if strings.HasPrefix(parts, "dead:") {
		return strings.TrimPrefix(parts, "dead:")
	}
	if strings.HasPrefix(parts, "delay-map:") {
		// delay-map keys are fxmq:delay-map:{topic}:{delayID}, skip them
		return ""
	}
	if strings.HasPrefix(parts, "delay:data:") {
		return strings.TrimPrefix(parts, "delay:data:")
	}
	if strings.HasPrefix(parts, "delay:") {
		return strings.TrimPrefix(parts, "delay:")
	}
	if strings.HasPrefix(parts, "retry:") {
		// retry keys are fxmq:retry:{topic}:{msgID}, skip them
		return ""
	}

	// Direct stream key: fxmq:{topic}
	return parts
}

// compareStreamID compares two Redis stream IDs numerically.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareStreamID(a, b string) int {
	aParts := strings.SplitN(a, "-", 2)
	bParts := strings.SplitN(b, "-", 2)

	aMs, _ := strconv.ParseInt(aParts[0], 10, 64)
	bMs, _ := strconv.ParseInt(bParts[0], 10, 64)

	if aMs != bMs {
		if aMs < bMs {
			return -1
		}
		return 1
	}

	var aSeq, bSeq int64
	if len(aParts) > 1 {
		aSeq, _ = strconv.ParseInt(aParts[1], 10, 64)
	}
	if len(bParts) > 1 {
		bSeq, _ = strconv.ParseInt(bParts[1], 10, 64)
	}

	if aSeq < bSeq {
		return -1
	}
	if aSeq > bSeq {
		return 1
	}
	return 0
}

func (s *Server) getTopicInfo(ctx context.Context, topic string) topicInfo {
	length, _ := s.rdb.XLen(ctx, internal.StreamKey(topic)).Result()
	dead, _ := s.rdb.XLen(ctx, internal.DeadLetterKey(topic, s.group)).Result()
	delayCount, _ := s.rdb.ZCard(ctx, internal.DelayKey(topic)).Result()

	// Sum pending and lag across all groups
	var pending, lag int64
	groups, err := s.rdb.XInfoGroups(ctx, internal.StreamKey(topic)).Result()
	if err == nil {
		for _, g := range groups {
			pending += g.Pending
			lag += g.Lag
		}
	}

	return topicInfo{
		Name:    topic,
		Stored:  length,
		Lag:     lag,
		Pending: pending,
		Dead:    dead,
		Delay:   delayCount,
	}
}

// filterValues removes internal fields (prefixed with _) from stream values before returning to frontend.
func filterValues(values map[string]interface{}) map[string]interface{} {
	filtered := make(map[string]interface{}, len(values))
	for k, v := range values {
		if strings.HasPrefix(k, "_") {
			continue
		}
		filtered[k] = v
	}
	return filtered
}

func generateDelayID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}
