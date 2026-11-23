package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"flashsale-platform/prototype/internal/inventory"
)

// RedpandaHTTPLog adapts the Redpanda HTTP Proxy v2 API to EventLog. It uses
// manual consumer offsets: a caller commits only after its handler or retry/DLQ
// publication has reached a durable outcome. For full broker-native semantics,
// partition is carried on every Message and offset commit.
type RedpandaHTTPLog struct {
	BaseURL   string
	Client    *http.Client
	mu        sync.Mutex
	sessions  map[string]*rpConsumer
	produced  map[string]map[int]int64
	committed map[string]map[int]int64
	seq       atomic.Uint64
}
type rpConsumer struct {
	baseURI           string
	instance          string
	topic             string
	pending           []Message
	partitionByOffset map[string]int
}
type rpProduceRequest struct {
	Records []rpRecord `json:"records"`
}
type rpRecord struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type rpProduceResponse struct {
	Offsets []struct {
		Partition int   `json:"partition"`
		Offset    int64 `json:"offset"`
	} `json:"offsets"`
}
type rpConsumerCreate struct {
	InstanceID string `json:"instance_id"`
	BaseURI    string `json:"base_uri"`
}
type rpConsumerRecord struct {
	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

func NewRedpandaHTTPLog(baseURL string, client *http.Client) (*RedpandaHTTPLog, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("Redpanda HTTP Proxy base URL is required")
	}
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{Timeout: 8 * time.Second}
	}
	return &RedpandaHTTPLog{BaseURL: baseURL, Client: client, sessions: map[string]*rpConsumer{}, produced: map[string]map[int]int64{}, committed: map[string]map[int]int64{}}, nil
}
func (l *RedpandaHTTPLog) request(ctx context.Context, method, endpoint, contentType, accept string, body any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, rd)
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	resp, err := l.Client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if readErr != nil {
		return resp.StatusCode, nil, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, b, fmt.Errorf("Redpanda HTTP Proxy %s %s: status %d: %s", method, endpoint, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return resp.StatusCode, b, nil
}
func (l *RedpandaHTTPLog) Publish(ctx context.Context, topic string, event inventory.Event, attempt int) (Message, error) {
	payload, err := json.Marshal(Message{Attempt: attempt, Event: event})
	if err != nil {
		return Message{}, err
	}
	key := event.HoldID
	if key == "" {
		key = event.ItemID
	}
	endpoint := l.BaseURL + "/topics/" + url.PathEscape(topic)
	_, b, err := l.request(ctx, http.MethodPost, endpoint, "application/vnd.kafka.json.v2+json", "application/vnd.kafka.v2+json", rpProduceRequest{Records: []rpRecord{{Key: key, Value: string(payload)}}})
	if err != nil {
		return Message{}, err
	}
	var response rpProduceResponse
	if err := json.Unmarshal(b, &response); err != nil {
		return Message{}, err
	}
	if len(response.Offsets) == 0 {
		return Message{}, fmt.Errorf("Redpanda produce response contains no offsets")
	}
	o := response.Offsets[0]
	l.mu.Lock()
	if l.produced[topic] == nil {
		l.produced[topic] = map[int]int64{}
	}
	if o.Offset+1 > l.produced[topic][o.Partition] {
		l.produced[topic][o.Partition] = o.Offset + 1
	}
	l.mu.Unlock()
	return Message{Offset: o.Offset, Partition: o.Partition, Attempt: attempt, Event: event}, nil
}
func consumerKey(topic, group string) string { return topic + "\x00" + group }
func (l *RedpandaHTTPLog) ensureConsumer(ctx context.Context, topic, group string) (*rpConsumer, error) {
	key := consumerKey(topic, group)
	l.mu.Lock()
	existing := l.sessions[key]
	l.mu.Unlock()
	if existing != nil {
		return existing, nil
	}
	name := fmt.Sprintf("flashsale-%d", l.seq.Add(1))
	endpoint := l.BaseURL + "/consumers/" + url.PathEscape(group)
	_, b, err := l.request(ctx, http.MethodPost, endpoint, "application/vnd.kafka.v2+json", "application/vnd.kafka.v2+json", map[string]string{"format": "json", "name": name, "auto.offset.reset": "earliest", "auto.commit.enable": "false", "consumer.request.timeout.ms": "10000"})
	if err != nil {
		return nil, err
	}
	var created rpConsumerCreate
	if err := json.Unmarshal(b, &created); err != nil {
		return nil, err
	}
	if created.InstanceID == "" || created.BaseURI == "" {
		return nil, fmt.Errorf("invalid Redpanda consumer create response")
	}
	base, err := url.Parse(created.BaseURI)
	if err != nil {
		return nil, err
	}
	if !base.IsAbs() {
		root, _ := url.Parse(l.BaseURL + "/")
		base = root.ResolveReference(base)
	}
	// Subscription is explicit; auto-commit is disabled.
	_, _, err = l.request(ctx, http.MethodPost, strings.TrimRight(base.String(), "/")+"/subscription", "application/vnd.kafka.v2+json", "application/vnd.kafka.v2+json", map[string][]string{"topics": {topic}})
	if err != nil {
		return nil, err
	}
	s := &rpConsumer{baseURI: strings.TrimRight(base.String(), "/"), instance: created.InstanceID, topic: topic, partitionByOffset: map[string]int{}}
	l.mu.Lock()
	if prior := l.sessions[key]; prior != nil {
		l.mu.Unlock()
		return prior, nil
	}
	l.sessions[key] = s
	l.mu.Unlock()
	return s, nil
}
func (l *RedpandaHTTPLog) Fetch(ctx context.Context, topic, group string, max int) ([]Message, error) {
	if max <= 0 {
		max = 1
	}
	s, err := l.ensureConsumer(ctx, topic, group)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	if len(s.pending) > 0 {
		n := max
		if n > len(s.pending) {
			n = len(s.pending)
		}
		out := append([]Message(nil), s.pending[:n]...)
		s.pending = s.pending[n:]
		l.mu.Unlock()
		return out, nil
	}
	l.mu.Unlock()
	endpoint := s.baseURI + "/records?timeout=250&max_bytes=1048576"
	_, b, err := l.request(ctx, http.MethodGet, endpoint, "application/vnd.kafka.v2+json", "application/vnd.kafka.json.v2+json", nil)
	if err != nil {
		return nil, err
	}
	var response struct {
		Records []rpConsumerRecord `json:"records"`
	}
	if err := json.Unmarshal(b, &response); err != nil {
		return nil, err
	}
	messages := make([]Message, 0, len(response.Records))
	for _, rec := range response.Records {
		var m Message
		if err := json.Unmarshal([]byte(rec.Value), &m); err != nil {
			return nil, fmt.Errorf("decode Redpanda record %s/%d: %w", topic, rec.Offset, err)
		}
		m.Offset = rec.Offset
		m.Partition = rec.Partition
		if m.Event.Type == "" {
			return nil, fmt.Errorf("Redpanda record has empty event type")
		}
		messages = append(messages, m)
	}
	l.mu.Lock()
	for _, m := range messages {
		s.partitionByOffset[offsetKey(topic, m.Partition, m.Offset)] = m.Partition
	}
	if len(messages) > 0 {
		s.pending = append(s.pending, messages...)
		n := max
		if n > len(s.pending) {
			n = len(s.pending)
		}
		out := append([]Message(nil), s.pending[:n]...)
		s.pending = s.pending[n:]
		l.mu.Unlock()
		return out, nil
	}
	l.mu.Unlock()
	return nil, nil
}
func offsetKey(topic string, partition int, offset int64) string {
	return topic + ":" + strconv.Itoa(partition) + ":" + strconv.FormatInt(offset, 10)
}
func (l *RedpandaHTTPLog) Commit(ctx context.Context, topic, group string, partition int, offset int64) error {
	s, err := l.ensureConsumer(ctx, topic, group)
	if err != nil {
		return err
	}
	// Kafka REST Proxy v2 expects an `offsets` array with the next offset
	// to consume. Using `partitions` is accepted by neither the v2 consumer
	// commit endpoint nor Redpanda's HTTP Proxy compatibility layer.
	body := map[string]any{"offsets": []map[string]any{{"topic": topic, "partition": partition, "offset": offset + 1}}}
	_, _, err = l.request(ctx, http.MethodPost, s.baseURI+"/offsets", "application/vnd.kafka.v2+json", "application/vnd.kafka.v2+json", body)
	if err != nil {
		return err
	}
	l.mu.Lock()
	if l.committed[topic] == nil {
		l.committed[topic] = map[int]int64{}
	}
	if offset+1 > l.committed[topic][partition] {
		l.committed[topic][partition] = offset + 1
	}
	l.mu.Unlock()
	return nil
}
func (l *RedpandaHTTPLog) Lag(ctx context.Context, topic, group string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	// Best-effort local estimate. Production alerting should use Redpanda's
	// broker/admin metrics for durable consumer-group lag across restarts.
	l.mu.Lock()
	defer l.mu.Unlock()
	total := 0
	for part, end := range l.produced[topic] {
		committed := l.committed[topic][part]
		if end > committed {
			total += int(end - committed)
		}
	}
	return total, nil
}
func (l *RedpandaHTTPLog) Close(ctx context.Context) error {
	l.mu.Lock()
	sessions := make(map[string]*rpConsumer, len(l.sessions))
	for k, v := range l.sessions {
		sessions[k] = v
	}
	l.sessions = map[string]*rpConsumer{}
	l.mu.Unlock()
	var first error
	for key, s := range sessions {
		parts := strings.SplitN(key, "\x00", 2)
		if len(parts) != 2 {
			continue
		}
		endpoint := l.BaseURL + "/consumers/" + url.PathEscape(parts[1]) + "/instances/" + url.PathEscape(s.instance)
		_, _, err := l.request(ctx, http.MethodDelete, endpoint, "", "", nil)
		if err != nil && first == nil {
			first = err
		}
	}
	return first
}

var _ EventLog = (*RedpandaHTTPLog)(nil)
