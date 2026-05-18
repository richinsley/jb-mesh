package logstore

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type Producer struct {
	nc   *nats.Conn
	node string
}

type PublishOptions struct {
	Level      string
	Kind       string
	Tool       string
	Method     string
	Corr       string
	Duration   time.Duration
	OK         *bool
	Message    string
	Data       map[string]any
	Subject    string
	OccurredAt time.Time
}

func NewProducer(nc *nats.Conn, node string) *Producer {
	if nc == nil {
		return nil
	}
	return &Producer{nc: nc, node: strings.TrimSpace(node)}
}

func (p *Producer) Publish(opts PublishOptions) error {
	if p == nil || p.nc == nil {
		return nil
	}
	subject := opts.Subject
	if subject == "" {
		subject = SubjectFor(p.node, opts.Kind, opts.Tool, opts.Method)
	}
	if subject == "" {
		return fmt.Errorf("logstore: missing subject")
	}
	rec := map[string]any{
		"level":   opts.Level,
		"kind":    opts.Kind,
		"node":    p.node,
		"tool":    opts.Tool,
		"method":  opts.Method,
		"corr":    opts.Corr,
		"message": opts.Message,
		"data":    opts.Data,
	}
	if !opts.OccurredAt.IsZero() {
		rec["ts"] = opts.OccurredAt.Format(time.RFC3339Nano)
	}
	if opts.Duration > 0 {
		rec["duration_ms"] = float64(opts.Duration) / float64(time.Millisecond)
	}
	if opts.OK != nil {
		rec["ok"] = *opts.OK
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return p.nc.Publish(subject, b)
}

func SubjectFor(node, kind, tool, method string) string {
	node = sanitizeNode(node)
	switch kind {
	case "node":
		return fmt.Sprintf("logs.node.%s", node)
	case "tool":
		if tool == "" {
			return ""
		}
		return fmt.Sprintf("logs.tool.%s.%s", node, sanitizeNode(tool))
	case "tool_call":
		if tool == "" || method == "" {
			return ""
		}
		return fmt.Sprintf("logs.call.%s.%s.%s", node, sanitizeNode(tool), sanitizeNode(method))
	case "audit":
		if tool == "" {
			tool = node
		}
		return fmt.Sprintf("logs.audit.%s", sanitizeNode(tool))
	default:
		return fmt.Sprintf("logs.node.%s", node)
	}
}

func BestEffortPublish(p *Producer, opts PublishOptions) {
	if p == nil {
		return
	}
	_ = p.Publish(opts)
}

func BoolPtr(v bool) *bool { return &v }
