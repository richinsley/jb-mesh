package main

import (
	"strings"
	"testing"
)

func TestAvailableStarterTemplates(t *testing.T) {
	names, err := availableStarterTemplates(serviceStarterTemplates)
	if err != nil {
		t.Fatalf("availableStarterTemplates: %v", err)
	}
	if len(names) < 2 {
		t.Fatalf("expected at least 2 templates, got %v", names)
	}
}

func TestStarterTemplateFiles(t *testing.T) {
	files, err := starterTemplateFiles(serviceStarterTemplates, "baseline")
	if err != nil {
		t.Fatalf("starterTemplateFiles: %v", err)
	}
	want := map[string]bool{
		".gitignore":    true,
		"README.md":     true,
		"jumpboot.yaml": true,
		"main.py":       true,
	}
	for _, file := range files {
		delete(want, file)
	}
	if len(want) != 0 {
		t.Fatalf("missing files from baseline template: %v", want)
	}
}

func TestRenderedStarterFiles(t *testing.T) {
	rendered, err := renderedStarterFiles(serviceStarterTemplates, "baseline", starterReplacements("tiny-echo", "Tiny rendered preview", "0.2.0"))
	if err != nil {
		t.Fatalf("renderedStarterFiles: %v", err)
	}

	jumpboot := rendered["jumpboot.yaml"]
	if !strings.Contains(jumpboot, "name: tiny-echo") {
		t.Fatalf("expected rendered jumpboot.yaml to contain service name, got:\n%s", jumpboot)
	}
	if !strings.Contains(jumpboot, "version: 0.2.0") {
		t.Fatalf("expected rendered jumpboot.yaml to contain version override, got:\n%s", jumpboot)
	}
	if !strings.Contains(jumpboot, "description: Tiny rendered preview") {
		t.Fatalf("expected rendered jumpboot.yaml to contain description override, got:\n%s", jumpboot)
	}

	mainPy := rendered["main.py"]
	if !strings.Contains(mainPy, "class TinyEchoService(Service):") {
		t.Fatalf("expected rendered main.py to contain generated class name, got:\n%s", mainPy)
	}
	if !strings.Contains(mainPy, "name = \"tiny-echo\"") {
		t.Fatalf("expected rendered main.py to contain service name, got:\n%s", mainPy)
	}
}
