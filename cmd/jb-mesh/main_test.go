package main

import (
	"reflect"
	"testing"
)

func TestParseCLIValue(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want interface{}
	}{
		{name: "string", raw: "whisper", want: "whisper"},
		{name: "number", raw: "42", want: float64(42)},
		{name: "bool", raw: "true", want: true},
		{name: "json object", raw: `{"tool":"whisper","restart_count":2}`, want: map[string]interface{}{"tool": "whisper", "restart_count": float64(2)}},
		{name: "json array", raw: `["a",1,false]`, want: []interface{}{"a", float64(1), false}},
		{name: "empty", raw: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseCLIValue(tt.raw)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parseCLIValue(%q) = %#v, want %#v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestParseEventDataArgs(t *testing.T) {
	got, err := parseEventDataArgs([]string{
		"tool=whisper",
		"restart_count=2",
		"fatal=true",
		`details={"signal":"SIGSEGV"}`,
	})
	if err != nil {
		t.Fatalf("parseEventDataArgs: %v", err)
	}

	want := map[string]interface{}{
		"tool":          "whisper",
		"restart_count": float64(2),
		"fatal":         true,
		"details":       map[string]interface{}{"signal": "SIGSEGV"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseEventDataArgs() = %#v, want %#v", got, want)
	}
}

func TestParseEventDataArgsErrors(t *testing.T) {
	tests := [][]string{
		{"missing-equals"},
		{"=value"},
	}

	for _, args := range tests {
		if _, err := parseEventDataArgs(args); err == nil {
			t.Fatalf("expected error for args %v", args)
		}
	}
}
