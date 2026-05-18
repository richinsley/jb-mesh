package config

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ReleaseTargets stores local operator-specific release mappings.
// This file is intentionally local-only (not committed with service repos).
type ReleaseTargets struct {
	Services map[string]ReleaseTarget `yaml:"services"`
}

type ReleaseTarget struct {
	Node   string `yaml:"node,omitempty"`
	Repo   string `yaml:"repo,omitempty"`
	Subdir string `yaml:"subdir,omitempty"`
}

func ReleaseTargetsPath(homeDir string) string {
	return filepath.Join(GetHomeDir(homeDir), "release-targets.yaml")
}

func LoadReleaseTargets(homeDir string) (*ReleaseTargets, error) {
	path := ReleaseTargetsPath(homeDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &ReleaseTargets{Services: map[string]ReleaseTarget{}}, nil
		}
		return nil, err
	}
	var targets ReleaseTargets
	if err := yaml.Unmarshal(data, &targets); err != nil {
		return nil, err
	}
	if targets.Services == nil {
		targets.Services = map[string]ReleaseTarget{}
	}
	return &targets, nil
}
