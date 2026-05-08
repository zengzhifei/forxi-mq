package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
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
	Name      string       `json:"name"`
	Length    int64        `json:"length"`
	Pending   int64        `json:"pending"`
	Dead      int64        `json:"dead"`
	Delay     int64        `json:"delay"`
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
	resp := overviewResp{Topics: len(s.topics)}

	for _, topic := range s.topics {
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
	var topics []topicInfo

	for _, topic := range s.topics {
		info := s.getTopicInfo(ctx, topic)
		topics = append(topics, info)
	}

	writeJSON(w, topics)
}

func (s *Server) handleTopicDetail(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()
	info := s.getTopicInfo(ctx, topic)

	// Get consumer details
	consumers, err := s.rdb.XInfoConsumers(ctx, internal.StreamKey(topic), s.group).Result()
	if err == nil {
		for _, c := range consumers {
			info.Consumers = append(info.Consumers, consumerInfo{
				Name:    c.Name,
				Pending: c.Pending,
				Idle:    c.Idle.String(),
			})
		}
	}

	writeJSON(w, info)
}

func (s *Server) handleDeadLetters(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()

	count := int64(50)
	if c := r.URL.Query().Get("count"); c != "" {
		if n, err := strconv.ParseInt(c, 10, 64); err == nil {
			count = n
		}
	}

	msgs, err := s.rdb.XRangeN(ctx, internal.DeadLetterKey(topic), "-", "+", count).Result()
	if err != nil {
		writeJSON(w, []deadMessage{})
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

	writeJSON(w, result)
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

func (s *Server) handleDelayQueue(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ctx := r.Context()

	results, err := s.rdb.ZRangeWithScores(ctx, internal.DelayKey(topic), 0, 49).Result()
	if err != nil {
		writeJSON(w, []delayMessage{})
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

	writeJSON(w, msgs)
}

// --- Helpers ---

func (s *Server) getTopicInfo(ctx context.Context, topic string) topicInfo {
	length, _ := s.rdb.XLen(ctx, internal.StreamKey(topic)).Result()
	dead, _ := s.rdb.XLen(ctx, internal.DeadLetterKey(topic)).Result()
	delayCount, _ := s.rdb.ZCard(ctx, internal.DelayKey(topic)).Result()

	var pending int64
	pendingInfo, err := s.rdb.XPending(ctx, internal.StreamKey(topic), s.group).Result()
	if err == nil {
		pending = pendingInfo.Count
	}

	return topicInfo{
		Name:    topic,
		Length:  length,
		Pending: pending,
		Dead:    dead,
		Delay:   delayCount,
	}
}

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}
