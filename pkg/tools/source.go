package tools

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ResolveToolSource resolves a local source path to the service root containing jumpboot.yaml.
// It accepts either the root itself or any descendant path inside that service subtree.
func ResolveToolSource(source string) (string, error) {
	if strings.TrimSpace(source) == "" {
		return "", fmt.Errorf("source path is required")
	}
	if strings.HasPrefix(source, "~") {
		home, _ := os.UserHomeDir()
		source = filepath.Join(home, source[1:])
	}
	absSource, err := filepath.Abs(source)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absSource)
	if err == nil {
		absSource = resolved
	}

	info, err := os.Stat(absSource)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		absSource = filepath.Dir(absSource)
	}

	for {
		manifestPath := filepath.Join(absSource, "jumpboot.yaml")
		if stat, err := os.Stat(manifestPath); err == nil && !stat.IsDir() {
			return absSource, nil
		}
		parent := filepath.Dir(absSource)
		if parent == absSource {
			break
		}
		absSource = parent
	}
	return "", fmt.Errorf("no jumpboot.yaml found at %s or its parents", source)
}
