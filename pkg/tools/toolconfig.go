package tools

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/richinsley/jb-mesh/pkg/config"
)

// ToolConfigStore manages per-tool configuration (validation, persistence, defaults).
// Config is stored as config.json alongside the tool's jumpboot.yaml.
type ToolConfigStore struct{}

// NewToolConfigStore creates a new config store.
func NewToolConfigStore() *ToolConfigStore {
	return &ToolConfigStore{}
}

// configPath returns the path to a tool's config.json
func (s *ToolConfigStore) configPath(tool *Tool) string {
	return filepath.Join(tool.Path, "config.json")
}

// Load reads a tool's persisted config. Returns empty map if no config file exists.
func (s *ToolConfigStore) Load(tool *Tool) (map[string]interface{}, error) {
	path := s.configPath(tool)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return make(map[string]interface{}), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg map[string]interface{}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	return cfg, nil
}

// Save persists a tool's config to config.json after validation.
func (s *ToolConfigStore) Save(tool *Tool, values map[string]interface{}) error {
	if err := s.Validate(tool.Manifest, values); err != nil {
		return err
	}

	data, err := json.MarshalIndent(values, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	path := s.configPath(tool)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write config %s: %w", path, err)
	}
	return nil
}

// Resolve returns the effective config: defaults merged with persisted values
// merged with any overrides. Later values win.
func (s *ToolConfigStore) Resolve(tool *Tool, overrides map[string]interface{}) (map[string]interface{}, error) {
	schema := tool.Manifest.Config
	result := make(map[string]interface{})

	// Layer 1: defaults from schema
	if schema != nil {
		for key, param := range schema.Schema {
			if param.Default != nil {
				result[key] = param.Default
			}
		}
	}

	// Layer 2: persisted config
	persisted, err := s.Load(tool)
	if err != nil {
		return nil, err
	}
	for k, v := range persisted {
		result[k] = v
	}

	// Layer 3: overrides (e.g., from CLI or API)
	for k, v := range overrides {
		result[k] = v
	}

	// Validate the merged result
	if schema != nil {
		if err := s.Validate(tool.Manifest, result); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// Validate checks values against the manifest's config schema.
func (s *ToolConfigStore) Validate(manifest *config.Manifest, values map[string]interface{}) error {
	schema := manifest.Config
	if schema == nil {
		// No schema defined — accept anything (unconfigurable tool)
		return nil
	}

	// Check required fields
	for _, req := range schema.Required {
		if _, ok := values[req]; !ok {
			return fmt.Errorf("config: required field %q missing", req)
		}
	}

	// Validate each value against its schema
	for key, val := range values {
		param, ok := schema.Schema[key]
		if !ok {
			return fmt.Errorf("config: unknown field %q", key)
		}
		if err := validateParam(key, val, param); err != nil {
			return err
		}
	}

	return nil
}

// validateParam checks a single value against its ConfigParam schema.
func validateParam(key string, val interface{}, param config.ConfigParam) error {
	// Type check
	if err := checkType(key, val, param.Type); err != nil {
		return err
	}

	// Enum check
	if len(param.Enum) > 0 {
		found := false
		for _, allowed := range param.Enum {
			if fmt.Sprintf("%v", val) == fmt.Sprintf("%v", allowed) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("config: field %q value %v not in allowed values %v", key, val, param.Enum)
		}
	}

	return nil
}

// checkType validates that a value matches the declared type.
func checkType(key string, val interface{}, typeName string) error {
	switch typeName {
	case "string":
		if _, ok := val.(string); !ok {
			return fmt.Errorf("config: field %q expected string, got %T", key, val)
		}
	case "int":
		// JSON unmarshals numbers as float64
		switch v := val.(type) {
		case float64:
			if v != float64(int(v)) {
				return fmt.Errorf("config: field %q expected int, got float %v", key, v)
			}
		case int:
			// ok
		default:
			return fmt.Errorf("config: field %q expected int, got %T", key, val)
		}
	case "float":
		switch val.(type) {
		case float64, float32, int:
			// ok
		default:
			return fmt.Errorf("config: field %q expected float, got %T", key, val)
		}
	case "bool":
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("config: field %q expected bool, got %T", key, val)
		}
	case "":
		// No type constraint
	default:
		return fmt.Errorf("config: field %q has unknown type %q", key, typeName)
	}
	return nil
}
