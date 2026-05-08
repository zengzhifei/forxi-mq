package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zengzhifei/forxi-mq/internal"
)

// --- Response types ---

type overviewResp struct {
	Topics     int   `json:"topics"`
	TotalMsgs  int64 `json:"total_msgs"`
	TotalDead  int64 `json:"total_dead"`
	TotalDelay int64 `json:"total_delay"`
}

type topicInfo struct {
	Name    string      `json:"name"`
	Stored  int64       `json:"stored"`
	Lag     int64       `json:"lag"`
	Pending int64       `json:"pending"`
	Dead    int64       `json:"dead"`
	Delay   int64       `json:"delay"`
	Groups  []groupInfo `json:"groups,omitempty"`
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
	Body  string  `json:"body"`
	Score float64 `json:"score"`
	DueAt string  `json:"due_at"`
}

// --- Handlers ---

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	topics := s.discoverTopics(ctx)
	resp := overviewResp{Topics: len(topics)}

	for _, topic := range topics {
		length, _ := s.rdb.XLen(ctx, internal.StreamKey(topic)).Result()
		dead, _ := s.rdb.XLen(ctx, internal.DeadLetterKey(topic)).Result()
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
		}
	}

	writeJSON(w, info)
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()

	count := int64(20)
	if c := r.URL.Query().Get("count"); c != "" {
		if n, err := strconv.ParseInt(c, 10, 64); err == nil {
			count = n
		}
	}

	// Cursor-based pagination: cursor is the last ID from previous page
	// For XREVRANGE, cursor means "get messages before this ID"
	end := "+"
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		end = "(" + cursor // exclusive
	}

	msgs, err := s.rdb.XRevRangeN(ctx, internal.StreamKey(topic), end, "-", count).Result()
	if err != nil {
		writeJSON(w, map[string]any{"items": []map[string]any{}, "next_cursor": ""})
		return
	}

	var result []map[string]any
	for _, msg := range msgs {
		item := map[string]any{
			"id":     msg.ID,
			"values": msg.Values,
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

	// Find the last-delivered-id across all groups (use the smallest one)
	groups, err := s.rdb.XInfoGroups(ctx, streamKey).Result()
	if err != nil || len(groups) == 0 {
		writeJSON(w, map[string]any{"items": []map[string]any{}, "next_cursor": ""})
		return
	}

	// Get the earliest last-delivered-id (messages after this are "lag")
	lastID := groups[0].LastDeliveredID
	for _, g := range groups[1:] {
		if g.LastDeliveredID < lastID {
			lastID = g.LastDeliveredID
		}
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
			"values": msg.Values,
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

	// Get all groups, then get pending messages from each
	groups, err := s.rdb.XInfoGroups(ctx, streamKey).Result()
	if err != nil {
		writeJSON(w, map[string]any{"items": []map[string]any{}, "next_cursor": ""})
		return
	}

	var result []map[string]any
	for _, g := range groups {
		pending, err := s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: streamKey,
			Group:  g.Name,
			Start:  startID,
			End:    "+",
			Count:  count,
		}).Result()
		if err != nil {
			continue
		}

		// Fetch message bodies
		for _, p := range pending {
			msgs, err := s.rdb.XRangeN(ctx, streamKey, p.ID, p.ID, 1).Result()
			if err != nil || len(msgs) == 0 {
				continue
			}
			item := map[string]any{
				"id":          p.ID,
				"group":       g.Name,
				"consumer":    p.Consumer,
				"idle":        p.Idle.String(),
				"retry_count": p.RetryCount,
				"values":      msgs[0].Values,
			}
			result = append(result, item)
		}
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

	msgs, err := s.rdb.XRangeN(ctx, internal.DeadLetterKey(topic), start, "+", count).Result()
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

	deadKey := internal.DeadLetterKey(topic)
	streamKey := internal.StreamKey(topic)

	// Read all dead messages and requeue them
	msgs, err := s.rdb.XRange(ctx, deadKey, "-", "+").Result()
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
		err := s.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]interface{}{"body": body},
		}).Err()
		if err == nil {
			s.rdb.XDel(ctx, deadKey, msg.ID)
			count++
		}
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

	var msgs []delayMessage
	for _, z := range results {
		body, _ := z.Member.(string)
		ts := int64(z.Score)
		dueAt := time.UnixMilli(ts).Format(time.RFC3339)
		msgs = append(msgs, delayMessage{
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

	// Search in main stream
	msgs, err := s.rdb.XRangeN(ctx, internal.StreamKey(topic), id, id, 1).Result()
	if err == nil && len(msgs) > 0 {
		writeJSON(w, map[string]any{
			"found":  true,
			"source": "stream",
			"message": map[string]any{
				"id":     msgs[0].ID,
				"values": msgs[0].Values,
			},
		})
		return
	}

	// Search in dead letter
	msgs, err = s.rdb.XRangeN(ctx, internal.DeadLetterKey(topic), id, id, 1).Result()
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
			"message": dm,
		})
		return
	}

	writeJSON(w, map[string]any{"found": false})
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()

	var req struct {
		Body string `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Body == "" {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	id, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: internal.StreamKey(topic),
		Values: map[string]interface{}{"body": req.Body},
	}).Result()
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}

	writeJSON(w, map[string]any{"ok": true, "id": id})
}

func (s *Server) handleDeleteDead(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	id := r.PathValue("id")
	ctx := r.Context()

	err := s.rdb.XDel(ctx, internal.DeadLetterKey(topic), id).Err()
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
		Body string `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Body == "" {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	err := s.rdb.ZRem(ctx, internal.DelayKey(topic), req.Body).Err()
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

// --- Helpers ---

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

func (s *Server) getTopicInfo(ctx context.Context, topic string) topicInfo {
	length, _ := s.rdb.XLen(ctx, internal.StreamKey(topic)).Result()
	dead, _ := s.rdb.XLen(ctx, internal.DeadLetterKey(topic)).Result()
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

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}
