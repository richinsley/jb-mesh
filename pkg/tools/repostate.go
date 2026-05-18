package tools

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

type RepoState struct {
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

func InspectGitCheckout(path string) (*RepoState, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		resolved = path
	}

	root, err := runGit(path, "rev-parse", "--show-toplevel")
	if err != nil {
		return nil, fmt.Errorf("inspect git root: %w", err)
	}
	branch, err := runGit(path, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return nil, fmt.Errorf("inspect git branch: %w", err)
	}
	commit, err := runGit(path, "rev-parse", "HEAD")
	if err != nil {
		return nil, fmt.Errorf("inspect git commit: %w", err)
	}
	statusShort, err := runGit(path, "status", "--short", "--branch")
	if err != nil {
		return nil, fmt.Errorf("inspect git status: %w", err)
	}
	porcelain, err := runGit(path, "status", "--porcelain")
	if err != nil {
		return nil, fmt.Errorf("inspect git porcelain: %w", err)
	}
	upstream, _ := runGit(path, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
	upstream = strings.TrimSpace(upstream)
	upstreamCommit := ""
	ahead, behind := 0, 0
	if upstream != "" {
		if got, err := runGit(path, "rev-parse", "@{upstream}"); err == nil {
			upstreamCommit = strings.TrimSpace(got)
		}
		if got, err := runGit(path, "rev-list", "--left-right", "--count", "HEAD...@{upstream}"); err == nil {
			fields := strings.Fields(strings.TrimSpace(got))
			if len(fields) == 2 {
				fmt.Sscanf(fields[0], "%d", &ahead)
				fmt.Sscanf(fields[1], "%d", &behind)
			}
		}
	}

	return &RepoState{
		Root:           strings.TrimSpace(root),
		ResolvedPath:   resolved,
		Branch:         strings.TrimSpace(branch),
		Commit:         strings.TrimSpace(commit),
		Upstream:       upstream,
		UpstreamCommit: upstreamCommit,
		Ahead:          ahead,
		Behind:         behind,
		StatusShort:    strings.TrimSpace(statusShort),
		Dirty:          strings.TrimSpace(porcelain) != "",
	}, nil
}

func runGit(path string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = path
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("%s", msg)
	}
	return string(out), nil
}
