package logstore

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	SchemaV1            = "jb.mesh.log.v1"
	defaultStorageDir   = "~/.jb-mesh/logstore"
	defaultMessageBytes = 4 * 1024
	defaultDataBytes    = 32 * 1024
	defaultRecordBytes  = 64 * 1024
)

var (
	errInvalidRecord = errors.New("invalid log record")
	sensitiveKeys    = []string{"token", "api_key", "apikey", "password", "secret", "auth", "authorization", "cookie", "bearer", "webhooktoken"}
	nodeSanitizer    = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
)

type Config struct {
	Enabled         bool
	Role            string
	StorageDir      string
	Subjects        []string
	RetentionDays   int
	MaxBytes        int64
	Redact          bool
	CaptureEvents   bool
	Strict          bool
	MessageMaxBytes int
	DataMaxBytes    int
	RecordMaxBytes  int
	MaxQueryLimit   int
	MaxQueryWindow  time.Duration
}

func (c Config) withDefaults() Config {
	if c.StorageDir == "" {
		c.StorageDir = defaultStorageDir
	}
	if len(c.Subjects) == 0 {
		c.Subjects = []string{"logs.>"}
	}
	if c.MessageMaxBytes <= 0 {
		c.MessageMaxBytes = defaultMessageBytes
	}
	if c.DataMaxBytes <= 0 {
		c.DataMaxBytes = defaultDataBytes
	}
	if c.RecordMaxBytes <= 0 {
		c.RecordMaxBytes = defaultRecordBytes
	}
	if c.CaptureEvents && !containsSubject(c.Subjects, "events.>") {
		c.Subjects = append(c.Subjects, "events.>")
	}
	return c
}

type Record struct {
	Schema     string         `json:"schema"`
	TS         string         `json:"ts"`
	Level      string         `json:"level"`
	Kind       string         `json:"kind"`
	Node       string         `json:"node"`
	Tool       string         `json:"tool,omitempty"`
	Method     string         `json:"method,omitempty"`
	Subject    string         `json:"subject"`
	Corr       string         `json:"corr,omitempty"`
	DurationMS *float64       `json:"duration_ms,omitempty"`
	OK         *bool          `json:"ok,omitempty"`
	Message    string         `json:"message"`
	Data       map[string]any `json:"data,omitempty"`
	Redacted   bool           `json:"redacted"`
	Truncated  bool           `json:"truncated,omitempty"`
}

type Stats struct {
	OK             bool     `json:"ok"`
	Role           string   `json:"role"`
	StorageDir     string   `json:"storage_dir"`
	Subjects       []string `json:"subjects"`
	RecordsWritten int64    `json:"records_written"`
	BytesWritten   int64    `json:"bytes_written"`
	LastWriteTS    string   `json:"last_write_ts,omitempty"`
	LastError      string   `json:"last_error,omitempty"`
	Backend        Backend  `json:"backend"`
}

type Backend struct {
	Kind string `json:"kind"`
	OK   bool   `json:"ok"`
}

type QueryRequest struct {
	Since  string `json:"since,omitempty"`
	Until  string `json:"until,omitempty"`
	Node   string `json:"node,omitempty"`
	Tool   string `json:"tool,omitempty"`
	Method string `json:"method,omitempty"`
	Kind   string `json:"kind,omitempty"`
	Level  string `json:"level,omitempty"`
	Corr   string `json:"corr,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type QueryResponse struct {
	OK         bool     `json:"ok"`
	Records    []Record `json:"records"`
	Truncated  bool     `json:"truncated"`
	NextCursor string   `json:"next_cursor,omitempty"`
	Limits     Limits   `json:"limits"`
	Error      string   `json:"error,omitempty"`
}

type StatsRequest struct {
	Since   string   `json:"since,omitempty"`
	Until   string   `json:"until,omitempty"`
	GroupBy []string `json:"group_by,omitempty"`
}

type StatsGroup struct {
	Node    string `json:"node,omitempty"`
	Kind    string `json:"kind,omitempty"`
	Tool    string `json:"tool,omitempty"`
	Method  string `json:"method,omitempty"`
	Level   string `json:"level,omitempty"`
	Records int    `json:"records"`
	Errors  int    `json:"errors"`
}

type StatsResponse struct {
	OK     bool         `json:"ok"`
	Since  string       `json:"since,omitempty"`
	Until  string       `json:"until,omitempty"`
	Groups []StatsGroup `json:"groups"`
	Limits Limits       `json:"limits"`
	Error  string       `json:"error,omitempty"`
}

type Limits struct {
	Limit          int    `json:"limit,omitempty"`
	MaxQueryLimit  int    `json:"max_query_limit,omitempty"`
	MaxQueryWindow string `json:"max_query_window,omitempty"`
}

type Store struct {
	cfg   Config
	mu    sync.Mutex
	files map[string]*os.File
	stats Stats
}

func NewStore(cfg Config) (*Store, error) {
	cfg = cfg.withDefaults()
	storageDir := expandHome(cfg.StorageDir)
	if err := os.MkdirAll(filepath.Join(storageDir, "raw"), 0o700); err != nil {
		return nil, err
	}
	return &Store{
		cfg:   cfg,
		files: map[string]*os.File{},
		stats: Stats{OK: true, Role: cfg.Role, StorageDir: cfg.StorageDir, Subjects: append([]string(nil), cfg.Subjects...), Backend: Backend{Kind: "jsonl", OK: true}},
	}, nil
}

func (s *Store) Health() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := s.stats
	stats.Subjects = append([]string(nil), s.stats.Subjects...)
	return stats
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for path, f := range s.files {
		if err := f.Close(); err != nil && first == nil {
			first = err
		}
		delete(s.files, path)
	}
	return first
}

func (s *Store) Append(ctx context.Context, subject string, payload []byte) (Record, error) {
	_ = ctx

	rec, err := Normalize(subject, payload, s.cfg)
	if err != nil {
		s.recordError(err)
		return Record{}, err
	}

	line, err := marshalRecord(rec)
	if err != nil {
		s.recordError(err)
		return Record{}, err
	}
	if len(line) > s.cfg.RecordMaxBytes {
		rec = truncateRecord(rec, line)
		line, err = marshalRecord(rec)
		if err != nil {
			s.recordError(err)
			return Record{}, err
		}
	}

	path, err := s.pathFor(rec)
	if err != nil {
		s.recordError(err)
		return Record{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f, ok := s.files[path]
	if !ok {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			s.setErrorLocked(err)
			return Record{}, err
		}
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			s.setErrorLocked(err)
			return Record{}, err
		}
		s.files[path] = f
	}

	if _, err := f.Write(line); err != nil {
		s.setErrorLocked(err)
		return Record{}, err
	}

	s.stats.RecordsWritten++
	s.stats.BytesWritten += int64(len(line))
	s.stats.LastWriteTS = rec.TS
	s.stats.LastError = ""
	s.stats.Backend.OK = true
	s.stats.OK = true
	return rec, nil
}

func (s *Store) pathFor(rec Record) (string, error) {
	ts, err := time.Parse(time.RFC3339Nano, rec.TS)
	if err != nil {
		return "", fmt.Errorf("parse record timestamp: %w", err)
	}
	date := ts.Format("2006-01-02")
	node := sanitizeNode(rec.Node)
	return filepath.Join(expandHome(s.cfg.StorageDir), "raw", "date="+date, "node="+node+".jsonl"), nil
}

func (s *Store) recordError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setErrorLocked(err)
}

func (s *Store) setErrorLocked(err error) {
	s.stats.LastError = err.Error()
	s.stats.Backend.OK = false
	s.stats.OK = false
}

func Normalize(subject string, payload []byte, cfg Config) (Record, error) {
	cfg = cfg.withDefaults()

	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return Record{}, fmt.Errorf("%w: decode payload: %v", errInvalidRecord, err)
	}

	if strings.HasPrefix(subject, "events.") {
		return normalizeEvent(subject, raw, cfg)
	}

	rec := Record{
		Schema:   asString(raw["schema"]),
		TS:       asString(raw["ts"]),
		Level:    normalizeLevel(asString(raw["level"]), cfg.Strict),
		Kind:     normalizeKind(asString(raw["kind"]), cfg.Strict),
		Node:     asString(raw["node"]),
		Tool:     asString(raw["tool"]),
		Method:   asString(raw["method"]),
		Subject:  subject,
		Corr:     asString(raw["corr"]),
		Message:  asString(raw["message"]),
		Redacted: cfg.Redact,
	}
	if rec.Schema == "" {
		rec.Schema = SchemaV1
	}
	if rec.TS == "" {
		rec.TS = time.Now().Format(time.RFC3339Nano)
	} else if _, err := time.Parse(time.RFC3339Nano, rec.TS); err != nil {
		return Record{}, fmt.Errorf("%w: invalid ts", errInvalidRecord)
	}

	inferFromSubject(subject, &rec)
	if rec.Corr == "" {
		rec.Corr = newCorr()
	}
	if rec.Message == "" {
		rec.Message = subject
	}
	if dm, ok := raw["duration_ms"]; ok {
		if v, ok := asFloat(dm); ok {
			rec.DurationMS = &v
		}
	}
	if okv, ok := raw["ok"]; ok {
		if b, ok := okv.(bool); ok {
			rec.OK = &b
		}
	}
	if data, ok := asMap(raw["data"]); ok {
		rec.Data = copyMap(data)
	}
	if cfg.Redact {
		rec.Data = redactMap(rec.Data)
	}
	rec.Message = truncateString(rec.Message, cfg.MessageMaxBytes, &rec.Truncated)
	rec.Data = truncateData(rec.Data, cfg.DataMaxBytes, &rec.Truncated)
	if err := validate(rec); err != nil {
		return Record{}, err
	}
	return rec, nil
}

func normalizeEvent(subject string, raw map[string]any, cfg Config) (Record, error) {
	data := copyMap(raw)
	flattenEventData(data)
	rec := Record{
		Schema:   SchemaV1,
		TS:       time.Now().Format(time.RFC3339Nano),
		Level:    "info",
		Kind:     "event",
		Node:     asString(raw["node"]),
		Subject:  subject,
		Corr:     firstNonEmpty(asString(raw["corr"]), asString(raw["correlation_id"]), asString(raw["trace_id"])),
		Message:  asString(raw["type"]),
		Data:     data,
		Redacted: cfg.Redact,
	}
	if ts := firstNonEmpty(asString(raw["ts"]), asString(raw["timestamp"])); ts != "" {
		if _, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			rec.TS = ts
		}
	}
	if rec.Corr == "" {
		rec.Corr = newCorr()
	}
	if rec.Message == "" {
		rec.Message = subject
	}
	if cfg.Redact {
		rec.Data = redactMap(rec.Data)
	}
	rec.Message = truncateString(rec.Message, cfg.MessageMaxBytes, &rec.Truncated)
	rec.Data = truncateData(rec.Data, cfg.DataMaxBytes, &rec.Truncated)
	if err := validate(rec); err != nil {
		return Record{}, err
	}
	return rec, nil
}

func validate(rec Record) error {
	if rec.Schema != SchemaV1 || rec.Subject == "" || rec.Message == "" || rec.Node == "" || rec.Level == "" || rec.Kind == "" || rec.TS == "" {
		return fmt.Errorf("%w: missing required fields", errInvalidRecord)
	}
	if _, err := time.Parse(time.RFC3339Nano, rec.TS); err != nil {
		return fmt.Errorf("%w: invalid ts", errInvalidRecord)
	}
	return nil
}

func inferFromSubject(subject string, rec *Record) {
	parts := strings.Split(subject, ".")
	if len(parts) < 2 {
		return
	}
	if rec.Kind == "" {
		rec.Kind = "system"
	}
	switch parts[0] {
	case "logs":
		if len(parts) > 1 && rec.Kind == "system" {
			switch parts[1] {
			case "node":
				rec.Kind = "node"
			case "tool":
				rec.Kind = "tool"
			case "call":
				rec.Kind = "tool_call"
			case "audit":
				rec.Kind = "audit"
			}
		}
		if rec.Node == "" && len(parts) > 2 {
			rec.Node = parts[2]
		}
		if rec.Tool == "" && len(parts) > 3 {
			rec.Tool = parts[3]
		}
		if rec.Method == "" && len(parts) > 4 {
			rec.Method = parts[4]
		}
	case "events":
		if rec.Kind == "system" {
			rec.Kind = "event"
		}
	}
}

func normalizeLevel(level string, strict bool) string {
	switch strings.ToLower(level) {
	case "debug", "info", "warn", "error":
		return strings.ToLower(level)
	case "warning":
		return "warn"
	case "":
		return "info"
	default:
		if strict {
			return ""
		}
		return "info"
	}
}

func normalizeKind(kind string, strict bool) string {
	switch strings.ToLower(kind) {
	case "node", "tool", "tool_call", "event", "audit", "system":
		return strings.ToLower(kind)
	case "":
		return "system"
	default:
		if strict {
			return ""
		}
		return "system"
	}
}

func redactMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := map[string]any{}
	for k, v := range in {
		if isSensitiveKey(k) {
			out[k] = "[REDACTED]"
			continue
		}
		switch tv := v.(type) {
		case map[string]any:
			out[k] = redactMap(tv)
		case []any:
			out[k] = redactSlice(tv)
		default:
			out[k] = v
		}
	}
	return out
}

func redactSlice(in []any) []any {
	out := make([]any, len(in))
	for i, v := range in {
		switch tv := v.(type) {
		case map[string]any:
			out[i] = redactMap(tv)
		case []any:
			out[i] = redactSlice(tv)
		default:
			out[i] = v
		}
	}
	return out
}

func isSensitiveKey(key string) bool {
	k := normalizeSensitiveKey(key)
	for _, needle := range sensitiveKeys {
		if strings.Contains(k, normalizeSensitiveKey(needle)) {
			return true
		}
	}
	return false
}

func normalizeSensitiveKey(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	return s
}

func truncateString(s string, max int, truncated *bool) string {
	if max <= 0 || len([]byte(s)) <= max {
		return s
	}
	*truncated = true
	return string([]byte(s)[:max])
}

func truncateData(data map[string]any, max int, truncated *bool) map[string]any {
	if data == nil {
		return nil
	}
	b, err := json.Marshal(data)
	if err == nil && (max <= 0 || len(b) <= max) {
		return data
	}
	*truncated = true
	sum := sha256.Sum256(b)
	return map[string]any{"truncated": true, "original_bytes": len(b), "sha256": hex.EncodeToString(sum[:])}
}

func truncateRecord(rec Record, originalLine []byte) Record {
	rec.Truncated = true
	sum := sha256.Sum256(originalLine)
	rec.Message = "record truncated"
	rec.Data = map[string]any{"truncated": true, "original_bytes": len(originalLine), "sha256": hex.EncodeToString(sum[:])}
	return rec
}

func sanitizeNode(node string) string {
	node = strings.TrimSpace(node)
	if node == "" {
		return "unknown"
	}
	node = nodeSanitizer.ReplaceAllString(node, "-")
	node = strings.Trim(node, "-.")
	if node == "" {
		return "unknown"
	}
	return node
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, strings.TrimPrefix(path, "~/"))
	}
	return path
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func asMap(v any) (map[string]any, bool) {
	if v == nil {
		return nil, false
	}
	if m, ok := v.(map[string]any); ok {
		return m, true
	}
	if m, ok := v.(map[string]interface{}); ok {
		out := make(map[string]any, len(m))
		for k, val := range m {
			out[k] = val
		}
		return out, true
	}
	return nil, false
}

func copyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func flattenEventData(data map[string]any) {
	nested, ok := asMap(data["data"])
	if !ok {
		return
	}
	for k, v := range nested {
		if _, exists := data[k]; !exists {
			data[k] = v
		}
	}
}

func newCorr() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("corr-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

func containsSubject(subjects []string, want string) bool {
	for _, subject := range subjects {
		if subject == want {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func marshalRecord(rec Record) ([]byte, error) {
	line, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	return append(line, '\n'), nil
}

type Subscriber struct {
	store *Store
	mu    sync.Mutex
	subs  []*nats.Subscription
}

func Subscribe(nc *nats.Conn, store *Store) (*Subscriber, error) {
	s := &Subscriber{store: store}
	for _, subject := range store.cfg.Subjects {
		subject := subject
		sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
			_, _ = store.Append(context.Background(), msg.Subject, msg.Data)
		})
		if err != nil {
			s.Close()
			return nil, err
		}
		s.subs = append(s.subs, sub)
	}
	return s, nil
}

func (s *Subscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for _, sub := range s.subs {
		if err := sub.Unsubscribe(); err != nil && first == nil {
			first = err
		}
	}
	s.subs = nil
	if err := s.store.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

type HealthService struct {
	store *Store
	sub   *nats.Subscription
}

func (s *Store) Subscribe(nc *nats.Conn) (*Subscriber, error) {
	return Subscribe(nc, s)
}

func (s *Store) StartHealthService(nc *nats.Conn, subject string) (*HealthService, error) {
	if subject == "" {
		subject = "logstore.health"
	}
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		resp, err := json.Marshal(s.Health())
		if err != nil {
			fallback, _ := json.Marshal(map[string]any{"ok": false, "error": fmt.Sprintf("marshal health: %v", err)})
			_ = msg.Respond(fallback)
			return
		}
		_ = msg.Respond(resp)
	})
	if err != nil {
		return nil, err
	}
	return &HealthService{store: s, sub: sub}, nil
}

func (h *HealthService) Close() error {
	if h == nil || h.sub == nil {
		return nil
	}
	return h.sub.Unsubscribe()
}

func (s *Store) Tail(req QueryRequest) (QueryResponse, error) {
	if strings.TrimSpace(req.Since) == "" {
		req.Since = "1h"
	}
	return s.Query(req)
}

func (s *Store) Query(req QueryRequest) (QueryResponse, error) {
	recs, truncated, limits, err := s.queryRecords(req)
	if err != nil {
		return QueryResponse{OK: false, Records: []Record{}, Limits: limits, Error: err.Error()}, err
	}
	return QueryResponse{OK: true, Records: recs, Truncated: truncated, Limits: limits}, nil
}

func (s *Store) Stats(req StatsRequest) (StatsResponse, error) {
	qreq := QueryRequest{Since: req.Since, Until: req.Until, Limit: s.effectiveQueryLimit()}
	recs, _, limits, err := s.queryRecords(qreq)
	if err != nil {
		return StatsResponse{OK: false, Groups: []StatsGroup{}, Limits: limits, Error: err.Error()}, err
	}
	groupBy := req.GroupBy
	if len(groupBy) == 0 {
		groupBy = []string{"node", "kind"}
	}
	type bucket struct {
		key string
		val StatsGroup
	}
	groups := map[string]*StatsGroup{}
	order := []string{}
	for _, rec := range recs {
		parts := make([]string, 0, len(groupBy))
		g := StatsGroup{}
		for _, field := range groupBy {
			switch strings.TrimSpace(field) {
			case "node":
				g.Node = rec.Node
				parts = append(parts, "node="+rec.Node)
			case "kind":
				g.Kind = rec.Kind
				parts = append(parts, "kind="+rec.Kind)
			case "tool":
				g.Tool = rec.Tool
				parts = append(parts, "tool="+rec.Tool)
			case "method":
				g.Method = rec.Method
				parts = append(parts, "method="+rec.Method)
			case "level":
				g.Level = rec.Level
				parts = append(parts, "level="+rec.Level)
			}
		}
		key := strings.Join(parts, "|")
		if _, ok := groups[key]; !ok {
			copy := g
			groups[key] = &copy
			order = append(order, key)
		}
		groups[key].Records++
		if rec.Level == "error" || (rec.OK != nil && !*rec.OK) {
			groups[key].Errors++
		}
	}
	out := make([]StatsGroup, 0, len(order))
	for _, key := range order {
		out = append(out, *groups[key])
	}
	return StatsResponse{OK: true, Since: req.Since, Until: req.Until, Groups: out, Limits: limits}, nil
}

func (s *Store) queryRecords(req QueryRequest) ([]Record, bool, Limits, error) {
	limit := req.Limit
	maxLimit := s.effectiveQueryLimit()
	if limit <= 0 || limit > maxLimit {
		limit = maxLimit
	}
	start, end, err := s.resolveTimeRange(req.Since, req.Until)
	limits := Limits{Limit: limit, MaxQueryLimit: maxLimit, MaxQueryWindow: s.cfg.MaxQueryWindow.String()}
	if err != nil {
		return nil, false, limits, err
	}
	paths, err := s.pathsForRange(start, end)
	if err != nil {
		return nil, false, limits, err
	}
	var recs []Record
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		buf := make([]byte, 0, 1024*64)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			var rec Record
			if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
				continue
			}
			if !recordMatches(rec, req, start, end) {
				continue
			}
			recs = append(recs, rec)
		}
		file.Close()
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].TS > recs[j].TS })
	truncated := false
	if len(recs) > limit {
		recs = recs[:limit]
		truncated = true
	}
	return recs, truncated, limits, nil
}

func (s *Store) effectiveQueryLimit() int {
	if s.cfg.MaxQueryLimit > 0 {
		return s.cfg.MaxQueryLimit
	}
	return 1000
}

func (s *Store) resolveTimeRange(since, until string) (time.Time, time.Time, error) {
	end := time.Now()
	if strings.TrimSpace(until) != "" {
		t, err := parseTimeOrDuration(until, time.Time{})
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse until: %w", err)
		}
		end = t
	}
	start := end.Add(-time.Hour)
	if strings.TrimSpace(since) != "" {
		t, err := parseTimeOrDuration(since, end)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse since: %w", err)
		}
		start = t
	}
	if !start.Before(end) {
		return time.Time{}, time.Time{}, fmt.Errorf("since must be before until")
	}
	if s.cfg.MaxQueryWindow > 0 && end.Sub(start) > s.cfg.MaxQueryWindow {
		start = end.Add(-s.cfg.MaxQueryWindow)
	}
	return start, end, nil
}

func parseTimeOrDuration(raw string, anchor time.Time) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	if d, err := time.ParseDuration(raw); err == nil {
		if anchor.IsZero() {
			anchor = time.Now()
		}
		return anchor.Add(-d), nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time %q", raw)
}

func (s *Store) pathsForRange(start, end time.Time) ([]string, error) {
	root := filepath.Join(expandHome(s.cfg.StorageDir), "raw")
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "date=") {
			continue
		}
		day, err := time.Parse("2006-01-02", strings.TrimPrefix(entry.Name(), "date="))
		if err != nil {
			continue
		}
		// Directory names are derived from the record timestamp's own date
		// (for example a PDT record at 2026-05-10T18:54-07:00 is stored under
		// date=2026-05-10). The query window, however, is compared as absolute
		// instants. If we interpret date directories only as UTC days, short
		// evening lookbacks in negative time zones can incorrectly skip the local
		// "today" directory even though matching records are inside it. Widen only
		// the directory prefilter by one day on each side; recordMatches still
		// applies the exact timestamp window before returning records.
		dayEnd := day.Add(24 * time.Hour)
		if dayEnd.Before(start.Add(-24*time.Hour)) || day.After(end.Add(24*time.Hour)) {
			continue
		}
		children, err := os.ReadDir(filepath.Join(root, entry.Name()))
		if err != nil {
			continue
		}
		for _, child := range children {
			if child.IsDir() || !strings.HasPrefix(child.Name(), "node=") || !strings.HasSuffix(child.Name(), ".jsonl") {
				continue
			}
			paths = append(paths, filepath.Join(root, entry.Name(), child.Name()))
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func recordMatches(rec Record, req QueryRequest, start, end time.Time) bool {
	ts, err := time.Parse(time.RFC3339Nano, rec.TS)
	if err != nil {
		return false
	}
	if ts.Before(start) || ts.After(end) {
		return false
	}
	for _, pair := range [][2]string{{strings.TrimSpace(req.Node), rec.Node}, {strings.TrimSpace(req.Tool), rec.Tool}, {strings.TrimSpace(req.Method), rec.Method}, {strings.TrimSpace(req.Kind), rec.Kind}, {strings.TrimSpace(req.Level), rec.Level}, {strings.TrimSpace(req.Corr), rec.Corr}} {
		if pair[0] != "" && pair[0] != pair[1] {
			return false
		}
	}
	return true
}

type QueryService struct {
	subs []*nats.Subscription
}

func (s *Store) StartQueryServices(nc *nats.Conn) (*QueryService, error) {
	bind := []struct {
		subject string
		handler func([]byte) ([]byte, error)
	}{
		{"logstore.query", func(data []byte) ([]byte, error) {
			var req QueryRequest
			_ = json.Unmarshal(data, &req)
			resp, err := s.Query(req)
			if err != nil {
				resp.OK = false
				resp.Error = err.Error()
			}
			return json.Marshal(resp)
		}},
		{"logstore.tail", func(data []byte) ([]byte, error) {
			var req QueryRequest
			_ = json.Unmarshal(data, &req)
			resp, err := s.Tail(req)
			if err != nil {
				resp.OK = false
				resp.Error = err.Error()
			}
			return json.Marshal(resp)
		}},
		{"logstore.stats", func(data []byte) ([]byte, error) {
			var req StatsRequest
			_ = json.Unmarshal(data, &req)
			resp, err := s.Stats(req)
			if err != nil {
				resp.OK = false
				resp.Error = err.Error()
			}
			return json.Marshal(resp)
		}},
	}
	qs := &QueryService{}
	for _, b := range bind {
		b := b
		sub, err := nc.Subscribe(b.subject, func(msg *nats.Msg) {
			payload, err := b.handler(msg.Data)
			if err != nil {
				payload = []byte(`{"ok":false,"error":"` + strings.ReplaceAll(err.Error(), `"`, `'`) + `"}`)
			}
			_ = msg.Respond(payload)
		})
		if err != nil {
			qs.Close()
			return nil, err
		}
		qs.subs = append(qs.subs, sub)
	}
	return qs, nil
}

func (q *QueryService) Close() error {
	if q == nil {
		return nil
	}
	var first error
	for _, sub := range q.subs {
		if err := sub.Unsubscribe(); err != nil && first == nil {
			first = err
		}
	}
	q.subs = nil
	return first
}
