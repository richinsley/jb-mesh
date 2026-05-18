// Package state provides tool state reporting and node health tracking
// via NATS JetStream KeyValue stores.
package state

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// ToolState represents the self-reported state of a tool on a node.
type ToolState struct {
	Status      string                 `json:"status"` // idle, busy, warming, draining, error, stopped, crashed
	Node        string                 `json:"node"`
	Tool        string                 `json:"tool"`
	Version     string                 `json:"version,omitempty"`
	ModelLoaded string                 `json:"model_loaded,omitempty"` // e.g., "large-v3"
	VRAMUsedGB  float64                `json:"vram_used_gb,omitempty"`
	QueueDepth  int                    `json:"queue_depth,omitempty"`
	CurrentTask string                 `json:"current_task,omitempty"`
	Agent       string                 `json:"agent,omitempty"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Extra       map[string]interface{} `json:"extra,omitempty"` // tool-specific fields
}

// NodeHealth represents the health/capability state of a mesh node.
type NodeHealth struct {
	Node         string    `json:"node"`
	Status       string    `json:"status"` // online, degraded, offline
	Arch         string    `json:"arch,omitempty"`
	OS           string    `json:"os,omitempty"`
	GPU          bool      `json:"gpu,omitempty"`
	GPUModel     string    `json:"gpu_model,omitempty"`
	VRAMGB       int       `json:"vram_gb,omitempty"`
	CPUCores     int       `json:"cpu_cores,omitempty"`
	MemoryGB     int       `json:"memory_gb,omitempty"`
	DiskGB       int       `json:"disk_gb,omitempty"`
	ToolCount    int       `json:"tool_count,omitempty"`
	RunningTools []string  `json:"running_tools,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Store provides read/write access to tool state and node health
// via JetStream KV buckets.
type Store struct {
	js         nats.JetStreamContext
	toolBucket nats.KeyValue // mesh-state bucket for tool states
	nodeBucket nats.KeyValue // mesh-nodes bucket for node health
}

// NewStore creates a state store, creating the KV buckets if they don't exist.
func NewStore(js nats.JetStreamContext) (*Store, error) {
	toolBucket, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      "mesh-state",
		Description: "Tool state reporting",
		TTL:         5 * time.Minute, // stale entries expire
	})
	if err != nil {
		return nil, fmt.Errorf("create mesh-state bucket: %w", err)
	}

	nodeBucket, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      "mesh-nodes",
		Description: "Node health and capabilities",
		TTL:         5 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("create mesh-nodes bucket: %w", err)
	}

	return &Store{
		js:         js,
		toolBucket: toolBucket,
		nodeBucket: nodeBucket,
	}, nil
}

// toolKey returns the KV key for a tool state: <node>.<tool>
func toolKey(node, tool string) string {
	return node + "." + tool
}

// --- Tool State ---

// SetToolState writes or updates a tool's state.
func (s *Store) SetToolState(state ToolState) error {
	state.UpdatedAt = time.Now()
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	_, err = s.toolBucket.Put(toolKey(state.Node, state.Tool), data)
	if err != nil {
		return fmt.Errorf("put state %s.%s: %w", state.Node, state.Tool, err)
	}
	return nil
}

// GetToolState reads a tool's state from the store.
func (s *Store) GetToolState(node, tool string) (*ToolState, error) {
	entry, err := s.toolBucket.Get(toolKey(node, tool))
	if err != nil {
		return nil, fmt.Errorf("get state %s.%s: %w", node, tool, err)
	}
	var state ToolState
	if err := json.Unmarshal(entry.Value(), &state); err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}
	return &state, nil
}

// ListToolStates returns all tool states in the mesh.
func (s *Store) ListToolStates() ([]ToolState, error) {
	keys, err := s.toolBucket.Keys()
	if err != nil {
		if err == nats.ErrNoKeysFound {
			return nil, nil
		}
		return nil, fmt.Errorf("list tool states: %w", err)
	}

	var states []ToolState
	for _, key := range keys {
		entry, err := s.toolBucket.Get(key)
		if err != nil {
			continue // skip stale/deleted
		}
		var state ToolState
		if err := json.Unmarshal(entry.Value(), &state); err != nil {
			continue
		}
		states = append(states, state)
	}
	return states, nil
}

// DeleteToolState removes a tool's state entry.
func (s *Store) DeleteToolState(node, tool string) error {
	return s.toolBucket.Delete(toolKey(node, tool))
}

// WatchToolStates returns a channel that receives tool state changes.
// Close the returned watcher to stop watching.
func (s *Store) WatchToolStates() (nats.KeyWatcher, error) {
	return s.toolBucket.WatchAll()
}

// --- Node Health ---

// SetNodeHealth writes or updates a node's health state.
func (s *Store) SetNodeHealth(health NodeHealth) error {
	health.UpdatedAt = time.Now()
	data, err := json.Marshal(health)
	if err != nil {
		return fmt.Errorf("marshal health: %w", err)
	}
	_, err = s.nodeBucket.Put(health.Node, data)
	if err != nil {
		return fmt.Errorf("put health %s: %w", health.Node, err)
	}
	return nil
}

// GetNodeHealth reads a node's health state.
func (s *Store) GetNodeHealth(node string) (*NodeHealth, error) {
	entry, err := s.nodeBucket.Get(node)
	if err != nil {
		return nil, fmt.Errorf("get health %s: %w", node, err)
	}
	var health NodeHealth
	if err := json.Unmarshal(entry.Value(), &health); err != nil {
		return nil, fmt.Errorf("unmarshal health: %w", err)
	}
	return &health, nil
}

// ListNodeHealth returns all node health states in the mesh.
func (s *Store) ListNodeHealth() ([]NodeHealth, error) {
	keys, err := s.nodeBucket.Keys()
	if err != nil {
		if err == nats.ErrNoKeysFound {
			return nil, nil
		}
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	var nodes []NodeHealth
	for _, key := range keys {
		entry, err := s.nodeBucket.Get(key)
		if err != nil {
			continue
		}
		var health NodeHealth
		if err := json.Unmarshal(entry.Value(), &health); err != nil {
			continue
		}
		nodes = append(nodes, health)
	}
	return nodes, nil
}

// DeleteNodeHealth removes a node's health entry.
func (s *Store) DeleteNodeHealth(node string) error {
	return s.nodeBucket.Delete(node)
}

// WatchNodes returns a channel that receives node health changes.
func (s *Store) WatchNodes() (nats.KeyWatcher, error) {
	return s.nodeBucket.WatchAll()
}
