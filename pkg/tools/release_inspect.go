package tools

import (
	"fmt"
)

type ReleaseInspection struct {
	ToolName        string     `json:"tool_name"`
	ToolPath        string     `json:"tool_path"`
	ManifestVersion string     `json:"manifest_version"`
	Repo            *RepoState `json:"repo,omitempty"`
}

func (m *Manager) InspectRelease(toolName string) (*ReleaseInspection, error) {
	tool, ok := m.tools[toolName]
	if !ok {
		return nil, fmt.Errorf("tool not found: %s", toolName)
	}
	inspection := &ReleaseInspection{
		ToolName:        tool.Name,
		ToolPath:        tool.Path,
		ManifestVersion: tool.Manifest.Version,
	}
	state, err := InspectGitCheckout(tool.Path)
	if err == nil {
		inspection.Repo = state
	}
	return inspection, nil
}
