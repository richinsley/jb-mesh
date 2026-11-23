package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeServiceManifest(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	manifest := "name: " + name + "\nversion: 1.0.0\ndescription: test\nruntime:\n  python: \"3.11\"\n  mode: oneshot\nrpc:\n  methods:\n    health:\n      description: ok\nhealth:\n  method: health\nx-deploy:\n  smoke:\n    method: health\n"
	if err := os.WriteFile(filepath.Join(dir, "jumpboot.yaml"), []byte(manifest), 0644); err != nil {
		t.Fatal(err)
	}
}

// resolved is a path with its symbolic links followed. A temporary folder is often reached
// through one (/var and /private/var on macOS), and the resolver follows links, so the two
// spellings of one folder must compare equal.
func resolved(t *testing.T, path string) string {
	t.Helper()
	out, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("resolve %s: %v", path, err)
	}
	return out
}

func TestResolveReleaseSubjectFromDirectSubdirPath(t *testing.T) {
	home := t.TempDir()
	repo := filepath.Join(t.TempDir(), "repo")
	svcDir := filepath.Join(repo, "services", "demo")
	writeServiceManifest(t, svcDir, "demo")

	subject, err := resolveReleaseSubject(svcDir, home, "node-a", "")
	if err != nil {
		t.Fatalf("resolveReleaseSubject: %v", err)
	}
	if subject.Name != "demo" {
		t.Fatalf("expected demo, got %s", subject.Name)
	}
	if want := resolved(t, svcDir); resolved(t, subject.RepoPath) != want {
		t.Fatalf("expected repo path %s, got %s", want, subject.RepoPath)
	}
}

func TestResolveReleaseSubjectFromTargetSubdir(t *testing.T) {
	home := t.TempDir()
	repo := filepath.Join(t.TempDir(), "repo")
	svcDir := filepath.Join(repo, "services", "demo")
	writeServiceManifest(t, svcDir, "demo")

	targets := []byte("services:\n  demo:\n    node: node-a\n    repo: " + repo + "\n    subdir: services/demo\n")
	cfgDir := home
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfgDir, "release-targets.yaml"), targets, 0644); err != nil {
		t.Fatal(err)
	}

	subject, err := resolveReleaseSubject("demo", home, "", "")
	if err != nil {
		t.Fatalf("resolveReleaseSubject: %v", err)
	}
	if want := resolved(t, svcDir); resolved(t, subject.RepoPath) != want {
		t.Fatalf("expected repo path %s, got %s", want, subject.RepoPath)
	}
}
