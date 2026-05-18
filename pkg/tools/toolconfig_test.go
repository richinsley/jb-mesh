package tools

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/richinsley/jb-mesh/pkg/config"
)

func makeToolWithConfig(t *testing.T, schema map[string]config.ConfigParam, required []string) *Tool {
	t.Helper()
	dir := t.TempDir()
	return &Tool{
		Name: "test-tool",
		Path: dir,
		Manifest: &config.Manifest{
			Name:    "test-tool",
			Version: "v1.0.0",
			Config: &config.ToolConfig{
				Schema:   schema,
				Required: required,
			},
		},
	}
}

func makeToolNoConfig(t *testing.T) *Tool {
	t.Helper()
	dir := t.TempDir()
	return &Tool{
		Name: "no-config-tool",
		Path: dir,
		Manifest: &config.Manifest{
			Name:    "no-config-tool",
			Version: "v1.0.0",
		},
	}
}

func TestToolConfig_SaveLoad(t *testing.T) {
	tool := makeToolWithConfig(t, map[string]config.ConfigParam{
		"model": {Type: "string", Default: "base"},
	}, nil)

	store := NewToolConfigStore()

	// Save
	values := map[string]interface{}{"model": "large-v3"}
	if err := store.Save(tool, values); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// config.json should exist
	if _, err := os.Stat(filepath.Join(tool.Path, "config.json")); err != nil {
		t.Fatalf("config.json not created: %v", err)
	}

	// Load
	loaded, err := store.Load(tool)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded["model"] != "large-v3" {
		t.Fatalf("expected large-v3, got %v", loaded["model"])
	}
}

func TestToolConfig_LoadMissing(t *testing.T) {
	tool := makeToolWithConfig(t, nil, nil)
	store := NewToolConfigStore()

	loaded, err := store.Load(tool)
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("expected empty map, got %v", loaded)
	}
}

func TestToolConfig_Resolve_DefaultsOnly(t *testing.T) {
	tool := makeToolWithConfig(t, map[string]config.ConfigParam{
		"model":    {Type: "string", Default: "base"},
		"language": {Type: "string", Default: "en"},
	}, nil)

	store := NewToolConfigStore()
	resolved, err := store.Resolve(tool, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved["model"] != "base" {
		t.Fatalf("expected default 'base', got %v", resolved["model"])
	}
	if resolved["language"] != "en" {
		t.Fatalf("expected default 'en', got %v", resolved["language"])
	}
}

func TestToolConfig_Resolve_PersistedOverridesDefaults(t *testing.T) {
	tool := makeToolWithConfig(t, map[string]config.ConfigParam{
		"model": {Type: "string", Default: "base"},
	}, nil)

	store := NewToolConfigStore()

	// Persist a value
	if err := store.Save(tool, map[string]interface{}{"model": "large"}); err != nil {
		t.Fatal(err)
	}

	// Resolve without overrides — persisted wins
	resolved, err := store.Resolve(tool, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved["model"] != "large" {
		t.Fatalf("expected persisted 'large', got %v", resolved["model"])
	}
}

func TestToolConfig_Resolve_OverridesWin(t *testing.T) {
	tool := makeToolWithConfig(t, map[string]config.ConfigParam{
		"model": {Type: "string", Default: "base"},
	}, nil)

	store := NewToolConfigStore()

	// Persist a value
	if err := store.Save(tool, map[string]interface{}{"model": "large"}); err != nil {
		t.Fatal(err)
	}

	// Override at resolve time
	resolved, err := store.Resolve(tool, map[string]interface{}{"model": "tiny"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved["model"] != "tiny" {
		t.Fatalf("expected override 'tiny', got %v", resolved["model"])
	}
}

func TestToolConfig_Validate_Required(t *testing.T) {
	tool := makeToolWithConfig(t, map[string]config.ConfigParam{
		"api_key": {Type: "string"},
	}, []string{"api_key"})

	store := NewToolConfigStore()

	// Missing required field
	err := store.Save(tool, map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing required field")
	}

	// With required field
	err = store.Save(tool, map[string]interface{}{"api_key": "sk-123"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestToolConfig_Validate_TypeCheck(t *testing.T) {
	tool := makeToolWithConfig(t, map[string]config.ConfigParam{
		"count":   {Type: "int"},
		"rate":    {Type: "float"},
		"enabled": {Type: "bool"},
		"name":    {Type: "string"},
	}, nil)

	store := NewToolConfigStore()

	// Valid types
	err := store.Validate(tool.Manifest, map[string]interface{}{
		"count":   float64(5), // JSON numbers are float64
		"rate":    3.14,
		"enabled": true,
		"name":    "test",
	})
	if err != nil {
		t.Fatalf("valid types rejected: %v", err)
	}

	// Wrong type: string for int
	err = store.Validate(tool.Manifest, map[string]interface{}{
		"count": "five",
	})
	if err == nil {
		t.Fatal("expected error for string in int field")
	}

	// Wrong type: int for bool
	err = store.Validate(tool.Manifest, map[string]interface{}{
		"enabled": float64(1),
	})
	if err == nil {
		t.Fatal("expected error for int in bool field")
	}
}

func TestToolConfig_Validate_Enum(t *testing.T) {
	tool := makeToolWithConfig(t, map[string]config.ConfigParam{
		"model": {
			Type:    "string",
			Default: "base",
			Enum:    []interface{}{"tiny", "base", "small", "medium", "large"},
		},
	}, nil)

	store := NewToolConfigStore()

	// Valid enum value
	err := store.Validate(tool.Manifest, map[string]interface{}{"model": "large"})
	if err != nil {
		t.Fatalf("valid enum rejected: %v", err)
	}

	// Invalid enum value
	err = store.Validate(tool.Manifest, map[string]interface{}{"model": "huge"})
	if err == nil {
		t.Fatal("expected error for invalid enum value")
	}
}

func TestToolConfig_Validate_UnknownField(t *testing.T) {
	tool := makeToolWithConfig(t, map[string]config.ConfigParam{
		"model": {Type: "string"},
	}, nil)

	store := NewToolConfigStore()
	err := store.Validate(tool.Manifest, map[string]interface{}{"bogus": "value"})
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestToolConfig_Validate_NoSchema(t *testing.T) {
	tool := makeToolNoConfig(t)
	store := NewToolConfigStore()

	// No schema = accept anything
	err := store.Validate(tool.Manifest, map[string]interface{}{"anything": "goes"})
	if err != nil {
		t.Fatalf("no-schema tool should accept anything: %v", err)
	}
}

func TestToolConfig_Resolve_NoSchema(t *testing.T) {
	tool := makeToolNoConfig(t)
	store := NewToolConfigStore()

	resolved, err := store.Resolve(tool, map[string]interface{}{"key": "val"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved["key"] != "val" {
		t.Fatalf("expected 'val', got %v", resolved["key"])
	}
}

func TestToolConfig_IntRoundtrip(t *testing.T) {
	// JSON round-trip turns ints into float64. Verify we handle this.
	tool := makeToolWithConfig(t, map[string]config.ConfigParam{
		"threads": {Type: "int", Default: float64(4)},
	}, nil)

	store := NewToolConfigStore()

	if err := store.Save(tool, map[string]interface{}{"threads": float64(8)}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := store.Load(tool)
	if err != nil {
		t.Fatal(err)
	}

	// After JSON round-trip, it's float64 — Validate should still accept it as int
	err = store.Validate(tool.Manifest, loaded)
	if err != nil {
		t.Fatalf("int round-trip validation failed: %v", err)
	}
}
