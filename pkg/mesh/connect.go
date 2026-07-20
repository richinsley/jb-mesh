package mesh

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/nats-io/nats.go"
)

// NATSWebSocketConfig holds optional client settings for NATS-over-WebSocket
// connections that are mounted behind an HTTP proxy path.
type NATSWebSocketConfig struct {
	ProxyPath   string
	BearerToken string
	Headers     http.Header
	Query       url.Values
}

// Connect opens a NATS connection using generic mesh config plus any extra
// nats.go options needed by the caller.
func Connect(cfg Config, extraOpts ...nats.Option) (*nats.Conn, error) {
	connectURL := cfg.NATSUrl
	if connectURL == "" {
		connectURL = nats.DefaultURL
	}

	preparedURL, err := prepareConnectURL(connectURL, cfg.WebSocket)
	if err != nil {
		return nil, err
	}

	opts, err := prepareConnectOptions(cfg, extraOpts...)
	if err != nil {
		return nil, err
	}

	return nats.Connect(preparedURL, opts...)
}

func prepareConnectURL(rawURL string, wsCfg NATSWebSocketConfig) (string, error) {
	if rawURL == "" {
		return nats.DefaultURL, nil
	}
	if !hasWebSocketClientConfig(wsCfg) {
		return rawURL, nil
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse NATS URL %q: %w", rawURL, err)
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return "", fmt.Errorf("websocket client options require ws:// or wss:// NATS URL, got %q", rawURL)
	}

	query := parsed.Query()
	for key, values := range wsCfg.Query {
		query.Del(key)
		for _, value := range values {
			query.Add(key, value)
		}
	}
	parsed.RawQuery = query.Encode()

	return parsed.String(), nil
}

func prepareConnectOptions(cfg Config, extraOpts ...nats.Option) ([]nats.Option, error) {
	opts := make([]nats.Option, 0, len(extraOpts)+4)
	if cfg.NodeName != "" {
		opts = append(opts, nats.Name(cfg.NodeName))
	}
	if cfg.Token != "" {
		opts = append(opts, nats.Token(cfg.Token))
	}
	if cfg.Username != "" || cfg.Password != "" {
		if cfg.Username == "" || cfg.Password == "" {
			return nil, fmt.Errorf("NATS username/password auth requires both username and password")
		}
		if cfg.Token != "" {
			return nil, fmt.Errorf("NATS token auth cannot be combined with username/password auth")
		}
		opts = append(opts, nats.UserInfo(cfg.Username, cfg.Password))
	}

	wsHeaders := cloneHeader(cfg.WebSocket.Headers)
	if cfg.WebSocket.BearerToken != "" {
		if wsHeaders == nil {
			wsHeaders = make(http.Header)
		}
		wsHeaders.Set("Authorization", "Bearer "+cfg.WebSocket.BearerToken)
	}
	if len(wsHeaders) > 0 {
		opts = append(opts, nats.WebSocketConnectionHeaders(wsHeaders))
	}
	if cfg.WebSocket.ProxyPath != "" {
		opts = append(opts, nats.ProxyPath(cfg.WebSocket.ProxyPath))
	}

	opts = append(opts, extraOpts...)
	return opts, nil
}

func hasWebSocketClientConfig(cfg NATSWebSocketConfig) bool {
	return cfg.ProxyPath != "" || cfg.BearerToken != "" || len(cfg.Headers) > 0 || len(cfg.Query) > 0
}

func cloneHeader(in http.Header) http.Header {
	if len(in) == 0 {
		return nil
	}
	out := make(http.Header, len(in))
	for key, values := range in {
		copied := make([]string, len(values))
		copy(copied, values)
		out[key] = copied
	}
	return out
}

// ParseKeyValuePairs parses repeated key=value CLI inputs into a multi-value map.
func ParseKeyValuePairs(pairs []string) (map[string][]string, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	result := make(map[string][]string)
	for _, pair := range pairs {
		key, value, ok := strings.Cut(pair, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return nil, fmt.Errorf("expected key=value, got %q", pair)
		}
		result[key] = append(result[key], value)
	}
	return result, nil
}
