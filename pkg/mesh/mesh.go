// Package mesh provides NATS-based service mesh for jb-mesh nodes.
//
// Each node in the mesh registers its tools as NATS micro services.
// Tool calls are routed via NATS subjects, with automatic load balancing
// when multiple nodes offer the same tool.
package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
	"github.com/nats-io/nuid"
	"github.com/richinsley/jb-mesh/pkg/filestore"
)

// ToolEndpoint describes a tool method registered in the mesh
type ToolEndpoint struct {
	Tool    string `json:"tool"`
	Method  string `json:"method"`
	Node    string `json:"node"`
	Subject string `json:"subject"`
}

// MethodSchema holds JSON Schema for a single method's input parameters,
// plus per-method metadata not strictly part of JSON Schema.
type MethodSchema struct {
	Properties  map[string]interface{} `json:"properties,omitempty"`
	Required    []string               `json:"required,omitempty"`
	Description string                 `json:"description,omitempty"`
	Type        string                 `json:"type,omitempty"`

	// Stream is true when the method serves on the `.stream` subject. Callers
	// can use this flag to choose Mesh.Stream vs Mesh.CallWithContext without
	// an out-of-band registry.
	// Omitted from JSON when false to keep wire compatibility with older
	// non-streaming-aware consumers.
	Stream bool `json:"stream,omitempty"`
}

// toolRegistration stores everything needed to re-register a tool after reconnect
type toolRegistration struct {
	name          string
	version       string
	description   string
	methods       []string
	methodSchemas map[string]MethodSchema // method name -> schema (optional)
	handler       ToolHandler
}

// Mesh manages the NATS connection and service registrations for a node
type Mesh struct {
	nc            *nats.Conn
	nodeName      string
	services      []micro.Service
	nodeSubs      []*nats.Subscription // node-targeted direct subscriptions
	fileStore     *filestore.Store     // file store for resolving file params
	registrations []toolRegistration   // saved for re-registration on reconnect
	mu            sync.Mutex
}

// SetFileStore sets the file store used for resolving file params in tool calls.
func (m *Mesh) SetFileStore(store *filestore.Store) {
	m.fileStore = store
}

// Config holds mesh connection settings
type Config struct {
	NATSUrl   string              // NATS server URL (default: nats://localhost:4222)
	NodeName  string              // Human-readable node name
	Token     string              // Optional auth token
	WebSocket NATSWebSocketConfig // Optional WebSocket client settings for ws:// or wss:// URLs
}

// New creates a new mesh instance and connects to NATS
func New(cfg Config) (*Mesh, error) {
	if cfg.NATSUrl == "" {
		cfg.NATSUrl = nats.DefaultURL
	}

	m := &Mesh{
		nodeName: cfg.NodeName,
	}

	opts := []nats.Option{
		nats.ReconnectWait(2 * time.Second),
		nats.MaxReconnects(-1), // reconnect forever
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Printf("[mesh] disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[mesh] reconnected to %s", nc.ConnectedUrl())
			go m.reRegisterTools()
		}),
	}

	nc, err := Connect(cfg, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS at %s: %w", cfg.NATSUrl, err)
	}

	log.Printf("[mesh] connected to %s as %q", nc.ConnectedUrl(), cfg.NodeName)

	m.nc = nc
	return m, nil
}

// CallRequest is the JSON payload for a tool method call
type CallRequest struct {
	Params map[string]interface{} `json:"params"`
	Node   string                 `json:"node,omitempty"` // target specific node (empty = any)
	Corr   string                 `json:"corr,omitempty"`

	// CallID, if set, identifies this call for cancellation routing. When the
	// caller's context fires, it publishes an empty message to
	// `cancel.<CallID>` and the serving node subscribes to that subject for the
	// duration of the call. Empty for non-cancellable calls.
	CallID string `json:"call_id,omitempty"`
}

// CancelSubject returns the NATS subject a caller publishes to in order to
// signal cancellation of a specific in-flight call. The serving node
// subscribes to the same subject for the call's lifetime.
func CancelSubject(callID string) string {
	return "cancel." + callID
}

// StreamFrame is one frame received from a streaming tool call. Partial
// frames carry Chunk; the terminal frame carries Result (or Error) and has
// Done=true. The channel returned by Mesh.Stream is closed after the
// terminal frame is delivered.
//
// the streaming RPC support.
type StreamFrame struct {
	Chunk  interface{} `json:"chunk,omitempty"`
	Result interface{} `json:"result,omitempty"`
	Done   bool        `json:"done"`
	Error  string      `json:"error,omitempty"`
	Node   string      `json:"node,omitempty"`
}

// StreamSubject returns the NATS subject for a streaming tool method.
// Streaming subjects are separate from the single-reply ones so old callers
// using Mesh.Call continue to work even after a method is upgraded to
// streaming.
func StreamSubject(toolName, method string) string {
	return fmt.Sprintf("tools.%s.%s.stream", toolName, method)
}

// NodeStreamSubject returns the node-targeted streaming subject used for
// directing streaming calls to a specific node rather than load-balancing.
func NodeStreamSubject(nodeName, toolName, method string) string {
	return fmt.Sprintf("node.%s.tools.%s.%s.stream", nodeName, toolName, method)
}

// CallResult is the JSON response from a tool method call
type CallResult struct {
	OK     bool        `json:"ok"`
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
	Node   string      `json:"node"`
}

// ToolHandler is a function that handles a tool method call
type ToolHandler func(req CallRequest, method string, params map[string]interface{}) (interface{}, error)

// RegisterTool registers a tool's methods as NATS micro service endpoints.
// Subject pattern: tools.<toolName>.<methodName>
// methodSchemas is optional: map of method name -> input schema (will be included in endpoint metadata).
func (m *Mesh) RegisterTool(toolName, version, description string, methods []string, handler ToolHandler, methodSchemas ...map[string]MethodSchema) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var schemas map[string]MethodSchema
	if len(methodSchemas) > 0 {
		schemas = methodSchemas[0]
	}
	return m.registerToolLocked(toolName, version, description, methods, schemas, handler, true)
}

// registerToolLocked does the actual registration work. Must be called with m.mu held.
// If save is true, the registration is saved for reconnect re-registration.
func (m *Mesh) registerToolLocked(toolName, version, description string, methods []string, methodSchemas map[string]MethodSchema, handler ToolHandler, save bool) error {
	if save {
		m.registrations = append(m.registrations, toolRegistration{
			name:          toolName,
			version:       version,
			description:   description,
			methods:       methods,
			methodSchemas: methodSchemas,
			handler:       handler,
		})
	}

	svc, err := micro.AddService(m.nc, micro.Config{
		Name:        toolName,
		Version:     version,
		Description: description,
		Metadata: map[string]string{
			"node": m.nodeName,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to register service %s: %w", toolName, err)
	}

	// Create a group for the tool under "tools.<toolName>"
	group := svc.AddGroup(fmt.Sprintf("tools.%s", toolName))

	// Register each method as an endpoint
	for _, method := range methods {
		methodName := method // capture for closure

		// Prepare endpoint metadata with schema if available
		var endpointOpts []micro.EndpointOpt
		if methodSchemas != nil {
			if schema, ok := methodSchemas[methodName]; ok {
				schemaJSON, err := json.Marshal(schema)
				if err == nil {
					endpointOpts = append(endpointOpts, micro.WithEndpointMetadata(map[string]string{
						"schema": string(schemaJSON),
					}))
				}
			}
		}

		err := group.AddEndpoint(methodName, micro.HandlerFunc(func(req micro.Request) {
			var callReq CallRequest
			if len(req.Data()) > 0 {
				if err := json.Unmarshal(req.Data(), &callReq); err != nil {
					resp := CallResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err), Node: m.nodeName}
					data, _ := json.Marshal(resp)
					req.Respond(data)
					return
				}
			}
			if callReq.Params == nil {
				callReq.Params = make(map[string]interface{})
			}

			// Resolve file store keys to temp files for known file params
			if m.fileStore != nil {
				cleanup, resolveErr := ResolveFileParams(callReq.Params, m.fileStore, m.nc)
				if resolveErr != nil {
					log.Printf("[mesh] file resolve error: %v", resolveErr)
				}
				if cleanup != nil {
					defer cleanup()
				}
			}

			result, err := handler(callReq, methodName, callReq.Params)
			var resp CallResult
			if err != nil {
				resp = CallResult{OK: false, Error: err.Error(), Node: m.nodeName}
			} else {
				resp = CallResult{OK: true, Result: result, Node: m.nodeName}
			}

			data, _ := json.Marshal(resp)
			req.Respond(data)
		}), endpointOpts...)
		if err != nil {
			svc.Stop()
			return fmt.Errorf("failed to register endpoint %s.%s: %w", toolName, method, err)
		}

		log.Printf("[mesh] registered tools.%s.%s", toolName, methodName)

		// Also register a node-targeted direct subscription:
		// node.<nodeName>.tools.<tool>.<method>
		// This bypasses the queue group for explicit node targeting.
		nodeSubject := fmt.Sprintf("node.%s.tools.%s.%s", m.nodeName, toolName, methodName)
		nodeSub, err := m.nc.Subscribe(nodeSubject, func(msg *nats.Msg) {
			var callReq CallRequest
			if len(msg.Data) > 0 {
				if err := json.Unmarshal(msg.Data, &callReq); err != nil {
					resp := CallResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err), Node: m.nodeName}
					data, _ := json.Marshal(resp)
					msg.Respond(data)
					return
				}
			}
			if callReq.Params == nil {
				callReq.Params = make(map[string]interface{})
			}

			// Resolve file store keys to temp files for known file params
			if m.fileStore != nil {
				cleanup, resolveErr := ResolveFileParams(callReq.Params, m.fileStore, m.nc)
				if resolveErr != nil {
					log.Printf("[mesh] file resolve error: %v", resolveErr)
				}
				if cleanup != nil {
					defer cleanup()
				}
			}

			result, err := handler(callReq, methodName, callReq.Params)
			var resp CallResult
			if err != nil {
				resp = CallResult{OK: false, Error: err.Error(), Node: m.nodeName}
			} else {
				resp = CallResult{OK: true, Result: result, Node: m.nodeName}
			}

			data, _ := json.Marshal(resp)
			msg.Respond(data)
		})
		if err != nil {
			log.Printf("[mesh] warning: failed to register node-targeted %s: %v", nodeSubject, err)
		} else {
			m.nodeSubs = append(m.nodeSubs, nodeSub)
		}
	}

	m.services = append(m.services, svc)
	return nil
}

// Call invokes a tool method anywhere in the mesh via NATS request/reply.
// If targetNode is non-empty, the call is routed directly to that node
// instead of being load-balanced across all nodes offering the tool.
func (m *Mesh) Call(toolName, method string, params map[string]interface{}, timeout time.Duration, targetNode ...string) (*CallResult, error) {
	subject := fmt.Sprintf("tools.%s.%s", toolName, method)
	if len(targetNode) > 0 && targetNode[0] != "" {
		subject = fmt.Sprintf("node.%s.tools.%s.%s", targetNode[0], toolName, method)
	}

	callReq := CallRequest{Params: params}
	if corr, ok := extractCorr(params); ok {
		callReq.Corr = corr
	}
	reqData, err := json.Marshal(callReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	msg, err := m.nc.Request(subject, reqData, timeout)
	if err != nil {
		return nil, fmt.Errorf("call to %s failed: %w", subject, err)
	}

	var result CallResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &result, nil
}

// CallWithContext is like Call but propagates ctx cancellation to the serving
// node via a NATS publish to `cancel.<call_id>`. The serving node subscribes
// to that subject for the call's duration and forwards the signal to its
// jumpboot tool process (where a cooperatively-cancellable method can stop
// early).
//
// A call_id is generated automatically and threaded into the request payload.
// If ctx fires before the response arrives, CallWithContext returns ctx.Err()
// after best-effort publishing the cancel signal — it does not wait for the
// server to acknowledge cancellation. Callers needing a hard deadline should
// derive ctx with context.WithTimeout.
//
// Part of Phase 1 of the streaming+cancellation design — see
// the streaming and cancellation design.
func (m *Mesh) CallWithContext(ctx context.Context, toolName, method string, params map[string]interface{}, targetNode ...string) (*CallResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	subject := fmt.Sprintf("tools.%s.%s", toolName, method)
	if len(targetNode) > 0 && targetNode[0] != "" {
		subject = fmt.Sprintf("node.%s.tools.%s.%s", targetNode[0], toolName, method)
	}

	callID := nuid.Next()
	callReq := CallRequest{Params: params, CallID: callID}
	if corr, ok := extractCorr(params); ok {
		callReq.Corr = corr
	}
	reqData, err := json.Marshal(callReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Use the canonical NATS ctx-aware request. RequestWithContext handles
	// the inbox subscribe/unsubscribe internally and respects ctx.Done().
	msg, err := m.nc.RequestWithContext(ctx, subject, reqData)
	if err != nil {
		// On ctx cancellation, best-effort publish the cancel signal so the
		// serving node can propagate to jumpboot. We don't wait for an ack.
		if ctxErr := ctx.Err(); ctxErr != nil {
			if pubErr := m.nc.Publish(CancelSubject(callID), nil); pubErr != nil {
				log.Printf("[mesh] CallWithContext: cancel publish for %s failed: %v", callID, pubErr)
			}
			return nil, ctxErr
		}
		return nil, fmt.Errorf("call to %s failed: %w", subject, err)
	}

	var result CallResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &result, nil
}

// Stream invokes a streaming tool method and returns a channel that yields
// StreamFrame values as they arrive from the serving node. The channel is
// closed after the terminal frame (Done=true) is delivered, or after ctx
// fires / an error occurs.
//
// The call is routed to the dedicated `tools.<tool>.<method>.stream`
// subject (or its node-targeted variant when targetNode is set), which the
// serving node only registers for methods declared with `stream: true` in
// their manifest. Calls to non-streaming methods on the stream subject get
// no responder.
//
// On ctx.Done, Stream publishes to `cancel.<call_id>` so the serving node
// can propagate the signal to its tool process (the same cancel mechanism as
// CallWithContext). The channel is then closed without waiting for further
// frames.
//
// the streaming RPC support.
func (m *Mesh) Stream(ctx context.Context, toolName, method string, params map[string]interface{}, targetNode ...string) (<-chan StreamFrame, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	subject := StreamSubject(toolName, method)
	if len(targetNode) > 0 && targetNode[0] != "" {
		subject = NodeStreamSubject(targetNode[0], toolName, method)
	}

	callID := nuid.Next()
	callReq := CallRequest{Params: params, CallID: callID}
	if corr, ok := extractCorr(params); ok {
		callReq.Corr = corr
	}
	reqData, err := json.Marshal(callReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Allocate a reply inbox and subscribe via a buffered channel. The buffer
	// gives the serving node a few frames of headroom if our consumer briefly
	// stalls; beyond that we accept NATS' slow-consumer behavior.
	inbox := m.nc.NewRespInbox()
	rawCh := make(chan *nats.Msg, 32)
	sub, err := m.nc.ChanSubscribe(inbox, rawCh)
	if err != nil {
		return nil, fmt.Errorf("subscribe stream inbox: %w", err)
	}

	if err := m.nc.PublishRequest(subject, inbox, reqData); err != nil {
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("publish stream %s: %w", subject, err)
	}

	out := make(chan StreamFrame, 1)
	go func() {
		defer close(out)
		defer sub.Unsubscribe()
		for {
			select {
			case msg, ok := <-rawCh:
				if !ok {
					return
				}
				var frame StreamFrame
				if err := json.Unmarshal(msg.Data, &frame); err != nil {
					// Deliver a synthetic terminal-error frame so the caller
					// gets a clear signal rather than a silently-closed channel.
					select {
					case out <- StreamFrame{Error: fmt.Sprintf("parse frame: %v", err), Done: true}:
					case <-ctx.Done():
					}
					return
				}
				select {
				case out <- frame:
				case <-ctx.Done():
					if pubErr := m.nc.Publish(CancelSubject(callID), nil); pubErr != nil {
						log.Printf("[mesh] Stream: cancel publish for %s failed: %v", callID, pubErr)
					}
					return
				}
				if frame.Done {
					return
				}
			case <-ctx.Done():
				if pubErr := m.nc.Publish(CancelSubject(callID), nil); pubErr != nil {
					log.Printf("[mesh] Stream: cancel publish for %s failed: %v", callID, pubErr)
				}
				return
			}
		}
	}()

	return out, nil
}

func extractCorr(params map[string]interface{}) (string, bool) {
	if len(params) == 0 {
		return "", false
	}
	for _, key := range []string{"corr", "correlation_id", "trace_id"} {
		if v, ok := params[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v), true
		}
	}
	return "", false
}

// ListServices discovers all services in the mesh using NATS micro discovery
func (m *Mesh) ListServices() ([]micro.Info, error) {
	// Use NATS micro's built-in discovery via $SRV.INFO
	infoSubject := "$SRV.INFO"

	var infos []micro.Info
	var mu sync.Mutex

	inbox := m.nc.NewRespInbox()
	sub, err := m.nc.Subscribe(inbox, func(msg *nats.Msg) {
		var info micro.Info
		if err := json.Unmarshal(msg.Data, &info); err == nil {
			mu.Lock()
			infos = append(infos, info)
			mu.Unlock()
		}
	})
	if err != nil {
		return nil, err
	}

	// Publish discovery request and wait for responses
	m.nc.PublishRequest(infoSubject, inbox, nil)
	time.Sleep(500 * time.Millisecond) // collect responses

	// Drain subscription before reading results to avoid race
	sub.Unsubscribe()

	mu.Lock()
	result := make([]micro.Info, len(infos))
	copy(result, infos)
	mu.Unlock()

	return result, nil
}

// JetStream returns a JetStream context for this connection.
// Returns an error if the NATS server does not have JetStream enabled.
func (m *Mesh) JetStream() (jetstream.JetStream, error) {
	js, err := jetstream.New(m.nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream not available: %w", err)
	}
	return js, nil
}

// Conn returns the underlying NATS connection.
// Use sparingly — prefer higher-level Mesh methods.
func (m *Mesh) Conn() *nats.Conn {
	return m.nc
}

// NodeName returns this node's name
func (m *Mesh) NodeName() string {
	return m.nodeName
}

// Connected returns true if connected to NATS
func (m *Mesh) Connected() bool {
	return m.nc != nil && m.nc.IsConnected()
}

// Close disconnects from NATS and stops all services
func (m *Mesh) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, svc := range m.services {
		svc.Stop()
	}
	m.services = nil

	for _, sub := range m.nodeSubs {
		sub.Unsubscribe()
	}
	m.nodeSubs = nil

	if m.nc != nil {
		m.nc.Close()
	}
}

// reRegisterTools re-registers all saved tool registrations after a NATS reconnect.
// Called from the ReconnectHandler goroutine.
func (m *Mesh) reRegisterTools() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.registrations) == 0 {
		return
	}

	log.Printf("[mesh] re-registering %d tool(s) after reconnect...", len(m.registrations))

	// Stop old services and subscriptions — they're zombies after reconnect
	for _, svc := range m.services {
		svc.Stop()
	}
	m.services = nil

	for _, sub := range m.nodeSubs {
		sub.Unsubscribe()
	}
	m.nodeSubs = nil

	// Re-register each saved tool
	for _, reg := range m.registrations {
		if err := m.registerToolLocked(reg.name, reg.version, reg.description, reg.methods, reg.methodSchemas, reg.handler, false); err != nil {
			log.Printf("[mesh] ERROR: failed to re-register tool %s: %v", reg.name, err)
		} else {
			log.Printf("[mesh] re-registered tool %s (%d methods)", reg.name, len(reg.methods))
		}
	}

	// Emit node.joined event so the seed knows we're back
	m.nc.Publish("events.node.joined", []byte(fmt.Sprintf(`{"node":"%s","event":"reconnect"}`, m.nodeName)))

	log.Printf("[mesh] reconnect re-registration complete")
}

// UnregisterTool removes a tool's service registration from the mesh.
// Returns true if the tool was found and removed, false if not found.
func (m *Mesh) UnregisterTool(toolName string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	found := false
	for i, svc := range m.services {
		info := svc.Info()
		if info.Name == toolName {
			svc.Stop()
			m.services = append(m.services[:i], m.services[i+1:]...)
			found = true
			break
		}
	}

	// Also unsubscribe node-targeted subscriptions for this tool
	prefix := fmt.Sprintf("node.%s.tools.%s.", m.nodeName, toolName)
	remaining := m.nodeSubs[:0]
	for _, sub := range m.nodeSubs {
		if sub.Subject == "" || len(sub.Subject) < len(prefix) || sub.Subject[:len(prefix)] != prefix {
			remaining = append(remaining, sub)
		} else {
			sub.Unsubscribe()
		}
	}
	m.nodeSubs = remaining

	return found
}

// UninstallRequest is sent over NATS to request a remote tool uninstall
type UninstallRequest struct {
	ToolName  string `json:"tool_name"`
	RemoveEnv bool   `json:"remove_env"` // Also remove the jumpboot venv
}

// UninstallResult is the response from a remote uninstall request
type UninstallResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Node  string `json:"node"`
}

// SubscribeUninstall listens for uninstall requests on node.<nodeName>.uninstall
func (m *Mesh) SubscribeUninstall(handler func(toolName string, removeEnv bool) error) error {
	subject := fmt.Sprintf("node.%s.uninstall", m.nodeName)
	_, err := m.nc.Subscribe(subject, func(msg *nats.Msg) {
		var req UninstallRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			resp := UninstallResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err), Node: m.nodeName}
			data, _ := json.Marshal(resp)
			msg.Respond(data)
			return
		}

		log.Printf("[mesh] uninstall request: %s", req.ToolName)
		err := handler(req.ToolName, req.RemoveEnv)
		var resp UninstallResult
		if err != nil {
			resp = UninstallResult{OK: false, Error: err.Error(), Node: m.nodeName}
		} else {
			resp = UninstallResult{OK: true, Node: m.nodeName}
		}
		data, _ := json.Marshal(resp)
		msg.Respond(data)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", subject, err)
	}
	log.Printf("[mesh] listening for uninstall requests on %s", subject)
	return nil
}

// RequestUninstall sends an uninstall request to a specific node
func (m *Mesh) RequestUninstall(nodeName, toolName string, removeEnv bool, timeout time.Duration) (*UninstallResult, error) {
	subject := fmt.Sprintf("node.%s.uninstall", nodeName)
	reqData, err := json.Marshal(UninstallRequest{ToolName: toolName, RemoveEnv: removeEnv})
	if err != nil {
		return nil, err
	}

	msg, err := m.nc.Request(subject, reqData, timeout)
	if err != nil {
		return nil, fmt.Errorf("uninstall request to %s failed: %w", nodeName, err)
	}

	var result UninstallResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &result, nil
}

// UpdateRequest is sent over NATS to request a remote tool update
type UpdateRequest struct {
	ToolName string `json:"tool_name"`
	Clean    bool   `json:"clean"` // Force clean rebuild of environment
}

// UpdateResult is the response from a remote update request
type UpdateResult struct {
	OK         bool   `json:"ok"`
	ToolName   string `json:"tool_name,omitempty"`
	OldVersion string `json:"old_version,omitempty"`
	NewVersion string `json:"new_version,omitempty"`
	Error      string `json:"error,omitempty"`
	Node       string `json:"node"`
}

// SubscribeUpdate listens for update requests on node.<nodeName>.update
func (m *Mesh) SubscribeUpdate(handler func(toolName string, clean bool) (oldVer, newVer string, err error)) error {
	subject := fmt.Sprintf("node.%s.update", m.nodeName)
	_, err := m.nc.Subscribe(subject, func(msg *nats.Msg) {
		var req UpdateRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			resp := UpdateResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err), Node: m.nodeName}
			data, _ := json.Marshal(resp)
			msg.Respond(data)
			return
		}

		log.Printf("[mesh] update request: %s", req.ToolName)
		// Run the potentially slow update in a background goroutine so the
		// callback can return and the NATS connection stays responsive.
		go func() {
			oldVer, newVer, err := handler(req.ToolName, req.Clean)
			var resp UpdateResult
			if err != nil {
				resp = UpdateResult{OK: false, Error: err.Error(), Node: m.nodeName}
			} else {
				resp = UpdateResult{OK: true, ToolName: req.ToolName, OldVersion: oldVer, NewVersion: newVer, Node: m.nodeName}
			}
			data, _ := json.Marshal(resp)
			msg.Respond(data)
		}()
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", subject, err)
	}
	log.Printf("[mesh] listening for update requests on %s", subject)
	return nil
}

// ConfigRequest is sent over NATS to get or set tool config on a node
type ConfigRequest struct {
	ToolName string                 `json:"tool_name"`
	Action   string                 `json:"action"` // "get" or "set"
	Values   map[string]interface{} `json:"values,omitempty"`
}

// ConfigResult is the response from a config request
type ConfigResult struct {
	OK     bool                   `json:"ok"`
	Config map[string]interface{} `json:"config,omitempty"`
	Error  string                 `json:"error,omitempty"`
	Node   string                 `json:"node"`
}

type ReleaseInspectRequest struct {
	ToolName string `json:"tool_name"`
}

type ReleaseInspectRepoState struct {
	Root           string `json:"root"`
	ResolvedPath   string `json:"resolved_path"`
	Branch         string `json:"branch"`
	Commit         string `json:"commit"`
	Upstream       string `json:"upstream,omitempty"`
	UpstreamCommit string `json:"upstream_commit,omitempty"`
	Ahead          int    `json:"ahead,omitempty"`
	Behind         int    `json:"behind,omitempty"`
	StatusShort    string `json:"status_short"`
	Dirty          bool   `json:"dirty"`
}

type ReleaseInspectInfo struct {
	ToolName        string                   `json:"tool_name"`
	ToolPath        string                   `json:"tool_path"`
	ManifestVersion string                   `json:"manifest_version"`
	Repo            *ReleaseInspectRepoState `json:"repo,omitempty"`
}

type ReleaseInspectResult struct {
	OK    bool                `json:"ok"`
	Error string              `json:"error,omitempty"`
	Node  string              `json:"node"`
	Info  *ReleaseInspectInfo `json:"info,omitempty"`
}

// SubscribeConfig listens for config requests on node.<nodeName>.config
func (m *Mesh) SubscribeConfig(handler func(toolName, action string, values map[string]interface{}) (map[string]interface{}, error)) error {
	subject := fmt.Sprintf("node.%s.config", m.nodeName)
	_, err := m.nc.Subscribe(subject, func(msg *nats.Msg) {
		var req ConfigRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			resp := ConfigResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err), Node: m.nodeName}
			data, _ := json.Marshal(resp)
			msg.Respond(data)
			return
		}

		log.Printf("[mesh] config %s request: %s", req.Action, req.ToolName)
		cfg, err := handler(req.ToolName, req.Action, req.Values)
		var resp ConfigResult
		if err != nil {
			resp = ConfigResult{OK: false, Error: err.Error(), Node: m.nodeName}
		} else {
			resp = ConfigResult{OK: true, Config: cfg, Node: m.nodeName}
		}
		data, _ := json.Marshal(resp)
		msg.Respond(data)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", subject, err)
	}
	log.Printf("[mesh] listening for config requests on %s", subject)
	return nil
}

// RequestConfig sends a config get/set request to a specific node
func (m *Mesh) RequestConfig(nodeName, toolName, action string, values map[string]interface{}, timeout time.Duration) (*ConfigResult, error) {
	subject := fmt.Sprintf("node.%s.config", nodeName)
	reqData, err := json.Marshal(ConfigRequest{ToolName: toolName, Action: action, Values: values})
	if err != nil {
		return nil, err
	}

	msg, err := m.nc.Request(subject, reqData, timeout)
	if err != nil {
		return nil, fmt.Errorf("config request to %s failed: %w", nodeName, err)
	}

	var result ConfigResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &result, nil
}

// SubscribeReleaseInspect listens for release inspection requests on node.<nodeName>.release.inspect
func (m *Mesh) SubscribeReleaseInspect(handler func(toolName string) (*ReleaseInspectInfo, error)) error {
	subject := fmt.Sprintf("node.%s.release.inspect", m.nodeName)
	_, err := m.nc.Subscribe(subject, func(msg *nats.Msg) {
		var req ReleaseInspectRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			resp := ReleaseInspectResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err), Node: m.nodeName}
			data, _ := json.Marshal(resp)
			msg.Respond(data)
			return
		}
		info, err := handler(req.ToolName)
		var resp ReleaseInspectResult
		if err != nil {
			resp = ReleaseInspectResult{OK: false, Error: err.Error(), Node: m.nodeName}
		} else {
			resp = ReleaseInspectResult{OK: true, Node: m.nodeName, Info: info}
		}
		data, _ := json.Marshal(resp)
		msg.Respond(data)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", subject, err)
	}
	log.Printf("[mesh] listening for release inspection requests on %s", subject)
	return nil
}

// RequestReleaseInspect asks a node to describe the deployed checkout state for a tool.
func (m *Mesh) RequestReleaseInspect(nodeName, toolName string, timeout time.Duration) (*ReleaseInspectResult, error) {
	subject := fmt.Sprintf("node.%s.release.inspect", nodeName)
	reqData, err := json.Marshal(ReleaseInspectRequest{ToolName: toolName})
	if err != nil {
		return nil, err
	}
	msg, err := m.nc.Request(subject, reqData, timeout)
	if err != nil {
		return nil, fmt.Errorf("release inspect request to %s failed: %w", nodeName, err)
	}
	var result ReleaseInspectResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &result, nil
}

// RequestUpdate sends an update request to a specific node
func (m *Mesh) RequestUpdate(nodeName, toolName string, clean bool, timeout time.Duration) (*UpdateResult, error) {
	subject := fmt.Sprintf("node.%s.update", nodeName)
	reqData, err := json.Marshal(UpdateRequest{ToolName: toolName, Clean: clean})
	if err != nil {
		return nil, err
	}

	msg, err := m.nc.Request(subject, reqData, timeout)
	if err != nil {
		return nil, fmt.Errorf("update request to %s failed: %w", nodeName, err)
	}

	var result UpdateResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &result, nil
}

// InstallRequest is sent over NATS to request a remote tool install
type InstallRequest struct {
	Source string `json:"source"` // Git URL or path
}

// InstallResult is the response from a remote install request
type InstallResult struct {
	OK       bool   `json:"ok"`
	ToolName string `json:"tool_name,omitempty"`
	Version  string `json:"version,omitempty"`
	Error    string `json:"error,omitempty"`
	Node     string `json:"node"`
}

// SubscribeInstall listens for install requests on node.<nodeName>.install
// The handler is called with the source URL and should return (toolName, version, error)
func (m *Mesh) SubscribeInstall(handler func(source string) (string, string, error)) error {
	subject := fmt.Sprintf("node.%s.install", m.nodeName)
	_, err := m.nc.Subscribe(subject, func(msg *nats.Msg) {
		var req InstallRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			resp := InstallResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err), Node: m.nodeName}
			data, _ := json.Marshal(resp)
			msg.Respond(data)
			return
		}

		log.Printf("[mesh] install request: %s", req.Source)
		// Run the potentially slow install in a background goroutine so the
		// callback can return and the NATS connection stays responsive.
		go func() {
			toolName, version, err := handler(req.Source)
			var resp InstallResult
			if err != nil {
				resp = InstallResult{OK: false, Error: err.Error(), Node: m.nodeName}
			} else {
				resp = InstallResult{OK: true, ToolName: toolName, Version: version, Node: m.nodeName}
			}
			data, _ := json.Marshal(resp)
			msg.Respond(data)
		}()
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", subject, err)
	}
	log.Printf("[mesh] listening for install requests on %s", subject)
	return nil
}

// RequestInstall sends an install request to a specific node and waits for the result
func (m *Mesh) RequestInstall(nodeName, source string, timeout time.Duration) (*InstallResult, error) {
	subject := fmt.Sprintf("node.%s.install", nodeName)
	reqData, err := json.Marshal(InstallRequest{Source: source})
	if err != nil {
		return nil, err
	}

	msg, err := m.nc.Request(subject, reqData, timeout)
	if err != nil {
		return nil, fmt.Errorf("install request to %s failed: %w", nodeName, err)
	}

	var result InstallResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &result, nil
}

// SchemaRequest is sent over NATS to request a tool's __jb_schema__ from a node
// Subject: node.<nodeName>.tools.<tool>.schema
// Response: SchemaResult

type SchemaRequest struct {
	ToolName string `json:"tool_name"`
}

// SchemaResult is the response containing the __jb_schema__ or error
// The schema is a JSON object with method signatures
type SchemaResult struct {
	OK     bool                   `json:"ok"`
	Schema map[string]interface{} `json:"schema,omitempty"`
	Error  string                 `json:"error,omitempty"`
}

// SchemaInfo holds method signature information for display
// This is extracted from the __jb_schema__ response
type SchemaInfo struct {
	MethodName string                 `json:"method"`
	Params     map[string]interface{} `json:"params,omitempty"` // JSON Schema for params
}

// GetToolSchema requests the schema for a specific tool from a specific node
// Returns the schema as a map (keyed by method name, value is params schema)
// or an error if the node doesn't have the tool or schema fetch fails
func (m *Mesh) GetToolSchema(nodeName, toolName string, timeout time.Duration) (*SchemaResult, error) {
	subject := fmt.Sprintf("node.%s.tools.%s.schema", nodeName, toolName)

	msg, err := m.nc.Request(subject, []byte{}, timeout)
	if err != nil {
		return nil, fmt.Errorf("schema request to %s for %s failed: %w", nodeName, toolName, err)
	}

	var result SchemaResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse schema response: %w", err)
	}
	return &result, nil
}

// SubscribeToolSchema registers a handler for schema requests on node.<nodeName>.tools.*.schema
// The handler receives the tool name and should return the schema or an error
func (m *Mesh) SubscribeToolSchema(handler func(toolName string) (map[string]interface{}, error)) error {
	subject := fmt.Sprintf("node.%s.tools.*.schema", m.nodeName)
	_, err := m.nc.Subscribe(subject, func(msg *nats.Msg) {
		// Parse subject: node.<nodename>.tools.<tool>.schema
		parts := strings.Split(msg.Subject, ".")
		if len(parts) < 5 {
			resp := SchemaResult{OK: false, Error: "invalid subject format"}
			data, _ := json.Marshal(resp)
			msg.Respond(data)
			return
		}
		toolName := parts[3]
		log.Printf("[mesh] schema request for tool: %s", toolName)
		schema, err := handler(toolName)
		var resp SchemaResult
		if err != nil {
			log.Printf("[mesh] schema handler error for %s: %v", toolName, err)
			resp = SchemaResult{OK: false, Error: err.Error()}
		} else {
			resp = SchemaResult{OK: true, Schema: schema}
		}
		data, _ := json.Marshal(resp)
		msg.Respond(data)
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", subject, err)
	}
	log.Printf("[mesh] listening for schema requests on %s", subject)
	return nil
}
