// Package events provides typed event emission and subscription for the jb-mesh
// event bus. System events are emitted automatically by node/tool lifecycle hooks.
// User events are emitted by tools via the SDK.
//
// NATS subject namespace (DESIGN.md §2.3):
//
//	events.tool.<event>   — tool lifecycle
//	events.node.<event>   — node lifecycle
//	events.user.<topic>   — user-defined (free-form)
package events

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// Event is the envelope for all mesh events.
type Event struct {
	Type      string                 `json:"type"` // e.g. "tool.installed", "node.joined"
	Node      string                 `json:"node"` // originating node
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// --- Typed event data constructors ---

// ToolInstalled creates a tool.installed event.
func ToolInstalled(node, tool, version string) Event {
	return Event{
		Type: "tool.installed",
		Node: node,
		Data: map[string]interface{}{
			"tool":    tool,
			"version": version,
		},
	}
}

// ToolConfigured creates a tool.configured event.
func ToolConfigured(node, tool string, config map[string]interface{}) Event {
	return Event{
		Type: "tool.configured",
		Node: node,
		Data: map[string]interface{}{
			"tool":   tool,
			"config": config,
		},
	}
}

// ToolStarted creates a tool.started event.
func ToolStarted(node, tool string) Event {
	return Event{
		Type: "tool.started",
		Node: node,
		Data: map[string]interface{}{
			"tool": tool,
		},
	}
}

// ToolStopped creates a tool.stopped event.
func ToolStopped(node, tool, reason string) Event {
	return Event{
		Type: "tool.stopped",
		Node: node,
		Data: map[string]interface{}{
			"tool":   tool,
			"reason": reason,
		},
	}
}

// ToolCrashed creates a tool.crashed event.
func ToolCrashed(node, tool string, err error, restartCount int) Event {
	return Event{
		Type: "tool.crashed",
		Node: node,
		Data: map[string]interface{}{
			"tool":          tool,
			"error":         err.Error(),
			"restart_count": restartCount,
		},
	}
}

// ToolRemoved creates a tool.removed event.
func ToolRemoved(node, tool string) Event {
	return Event{
		Type: "tool.removed",
		Node: node,
		Data: map[string]interface{}{
			"tool": tool,
		},
	}
}

// NodeJoined creates a node.joined event.
func NodeJoined(node string, capabilities map[string]interface{}) Event {
	return Event{
		Type: "node.joined",
		Node: node,
		Data: map[string]interface{}{
			"capabilities": capabilities,
		},
	}
}

// NodeLeft creates a node.left event.
func NodeLeft(node, reason string) Event {
	return Event{
		Type: "node.left",
		Node: node,
		Data: map[string]interface{}{
			"reason": reason,
		},
	}
}

// NodeHealth creates a node.health event.
func NodeHealth(node, status string, resources map[string]interface{}) Event {
	return Event{
		Type: "node.health",
		Node: node,
		Data: map[string]interface{}{
			"status":    status,
			"resources": resources,
		},
	}
}

// UserEvent creates a user-defined event (events.user.<topic>).
func UserEvent(node, topic string, data map[string]interface{}) Event {
	return Event{
		Type: "user." + topic,
		Node: node,
		Data: data,
	}
}

// --- Bus ---

// Bus handles event emission and subscription over NATS.
type Bus struct {
	nc       *nats.Conn
	nodeName string
}

// NewBus creates an event bus attached to a NATS connection.
func NewBus(nc *nats.Conn, nodeName string) *Bus {
	return &Bus{nc: nc, nodeName: nodeName}
}

// Emit publishes an event to the appropriate NATS subject.
// The subject is derived from the event type: "tool.installed" → "events.tool.installed"
func (b *Bus) Emit(event Event) error {
	event.Timestamp = time.Now()
	if event.Node == "" {
		event.Node = b.nodeName
	}

	subject := "events." + event.Type
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if err := b.nc.Publish(subject, data); err != nil {
		return fmt.Errorf("publish event %s: %w", subject, err)
	}

	log.Printf("[events] emitted %s", event.Type)
	return nil
}

// EventHandler is called when an event is received.
type EventHandler func(Event)

// Subscribe listens for events matching a subject pattern.
// Pattern examples:
//   - "events.tool.*"          — all tool events
//   - "events.node.*"          — all node events
//   - "events.tool.crashed"    — only crash events
//   - "events.>"               — all events
//   - "events.user.>"          — all user events
func (b *Bus) Subscribe(pattern string, handler EventHandler) (*nats.Subscription, error) {
	sub, err := b.nc.Subscribe(pattern, func(msg *nats.Msg) {
		var event Event
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			log.Printf("[events] failed to unmarshal event on %s: %v", msg.Subject, err)
			return
		}
		handler(event)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe %s: %w", pattern, err)
	}
	return sub, nil
}

// SubscribeType is a convenience for subscribing to a specific event type.
// E.g., SubscribeType("tool.crashed", handler)
func (b *Bus) SubscribeType(eventType string, handler EventHandler) (*nats.Subscription, error) {
	return b.Subscribe("events."+eventType, handler)
}

// --- Persistent Events (JetStream) ---

// PersistentBus extends Bus with JetStream-backed event persistence.
// Events are stored in a stream and can be replayed/queried.
type PersistentBus struct {
	*Bus
	js     nats.JetStreamContext
	stream string
}

// PersistConfig controls event persistence behavior.
type PersistConfig struct {
	StreamName string        // default: "MESH_EVENTS"
	MaxAge     time.Duration // how long to keep events, default: 24h
	MaxMsgs    int64         // max events to keep, default: 10000 (0 = unlimited)
}

// DefaultPersistConfig returns sensible defaults.
func DefaultPersistConfig() PersistConfig {
	return PersistConfig{
		StreamName: "MESH_EVENTS",
		MaxAge:     24 * time.Hour,
		MaxMsgs:    10000,
	}
}

// NewPersistentBus creates an event bus with JetStream persistence.
// All events published to events.> are captured in a stream.
func NewPersistentBus(nc *nats.Conn, js nats.JetStreamContext, nodeName string, cfg PersistConfig) (*PersistentBus, error) {
	if cfg.StreamName == "" {
		cfg.StreamName = "MESH_EVENTS"
	}
	if cfg.MaxAge == 0 {
		cfg.MaxAge = 24 * time.Hour
	}

	streamCfg := &nats.StreamConfig{
		Name:     cfg.StreamName,
		Subjects: []string{"events.>"},
		MaxAge:   cfg.MaxAge,
		Storage:  nats.FileStorage,
	}
	if cfg.MaxMsgs > 0 {
		streamCfg.MaxMsgs = cfg.MaxMsgs
	}

	// Create or update the stream
	_, err := js.AddStream(streamCfg)
	if err != nil {
		return nil, fmt.Errorf("create event stream: %w", err)
	}

	return &PersistentBus{
		Bus:    NewBus(nc, nodeName),
		js:     js,
		stream: cfg.StreamName,
	}, nil
}

// History returns recent events matching a subject filter.
// Filter examples: "events.tool.crashed", "events.>", "events.node.*"
func (pb *PersistentBus) History(filter string, limit int) ([]Event, error) {
	// Create an ephemeral ordered consumer with filter
	sub, err := pb.js.SubscribeSync(filter,
		nats.OrderedConsumer(),
		nats.DeliverAll(),
	)
	if err != nil {
		return nil, fmt.Errorf("subscribe history: %w", err)
	}
	defer sub.Unsubscribe()

	var events []Event
	for i := 0; i < limit; i++ {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			break // timeout = no more messages
		}
		var event Event
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			continue
		}
		events = append(events, event)
	}
	return events, nil
}

// HistorySince returns events after a given timestamp.
func (pb *PersistentBus) HistorySince(filter string, since time.Time, limit int) ([]Event, error) {
	sub, err := pb.js.SubscribeSync(filter,
		nats.OrderedConsumer(),
		nats.StartTime(since),
	)
	if err != nil {
		return nil, fmt.Errorf("subscribe history: %w", err)
	}
	defer sub.Unsubscribe()

	var events []Event
	for i := 0; i < limit; i++ {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			break
		}
		var event Event
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			continue
		}
		events = append(events, event)
	}
	return events, nil
}
