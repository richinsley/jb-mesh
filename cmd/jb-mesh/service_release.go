package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/richinsley/jb-mesh/pkg/config"
	"github.com/richinsley/jb-mesh/pkg/mesh"
	toolstate "github.com/richinsley/jb-mesh/pkg/tools"
	"github.com/spf13/cobra"
)

func jsonMarshal(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

//go:embed templates/service_release_proof.md.tmpl
var serviceReleaseProofTemplate string

type releaseSubject struct {
	Name         string
	RepoPath     string
	RepoRootPath string
	Node         string
	Manifest     *config.Manifest
}

type serviceReleaseReport struct {
	GeneratedAt    time.Time                  `json:"generated_at"`
	Service        string                     `json:"service"`
	Node           string                     `json:"node"`
	RepoPath       string                     `json:"repo_path"`
	ProofPath      string                     `json:"proof_path,omitempty"`
	LocalRepo      *toolstate.RepoState       `json:"local_repo"`
	DeployedBefore *mesh.ReleaseInspectResult `json:"deployed_before"`
	Update         *mesh.UpdateResult         `json:"update"`
	DeployedAfter  *mesh.ReleaseInspectResult `json:"deployed_after"`
	HealthMethod   string                     `json:"health_method"`
	Health         *mesh.CallResult           `json:"health"`
	SmokeMethod    string                     `json:"smoke_method"`
	SmokeParams    map[string]interface{}     `json:"smoke_params,omitempty"`
	Smoke          *mesh.CallResult           `json:"smoke"`
}

func serviceReleaseCmd() *cobra.Command {
	var flagNode string
	var flagRepo string
	var flagOut string
	var flagFormat string
	var flagClean bool

	cmd := &cobra.Command{
		Use:   "service-release <service-name|repo-path>",
		Short: "Release a jb-mesh service: inspect → update → verify → proof",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			subject, err := resolveReleaseSubject(args[0], flagHome, flagNode, flagRepo)
			if err != nil {
				return err
			}

			report, err := runServiceRelease(subject, flagClean)
			if err != nil {
				return err
			}

			if flagFormat == "" {
				flagFormat = "md"
			}

			content, err := renderServiceReleaseReport(report, flagFormat)
			if err != nil {
				return err
			}

			proofPath := flagOut
			if proofPath == "" {
				proofPath = defaultReleaseProofPath(subject.Name, flagFormat)
			}
			report.ProofPath = proofPath
			if proofPath != "-" {
				if err := os.MkdirAll(filepath.Dir(proofPath), 0755); err != nil {
					return err
				}
				if err := os.WriteFile(proofPath, content, 0644); err != nil {
					return err
				}
			} else {
				fmt.Print(string(content))
			}

			fmt.Printf("Released %s on %s: %s → %s\n", report.Service, report.Node, report.Update.OldVersion, report.Update.NewVersion)
			if report.ProofPath != "-" {
				fmt.Printf("Proof: %s\n", report.ProofPath)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&flagNode, "node", "", "Override target node (load from release-targets.yaml if omitted)")
	cmd.Flags().StringVar(&flagRepo, "repo", "", "Override local canonical repo path")
	cmd.Flags().StringVar(&flagOut, "out", "", "Write proof to path (- for stdout, default: /tmp/<service>-release-proof-<stamp>.<ext>)")
	cmd.Flags().StringVar(&flagFormat, "format", "md", "Proof format: md or json")
	cmd.Flags().BoolVar(&flagClean, "clean", false, "Force clean package rebuild during managed update")
	return cmd
}

func resolveReleaseSubject(arg, homeDir, nodeOverride, repoOverride string) (*releaseSubject, error) {
	targets, err := config.LoadReleaseTargets(homeDir)
	if err != nil {
		return nil, fmt.Errorf("load release targets: %w", err)
	}

	resolveRepo := func(repo string) (string, error) {
		if repo == "" {
			return "", nil
		}
		abs, err := filepath.Abs(repo)
		if err != nil {
			return "", err
		}
		if _, err := os.Stat(abs); err != nil {
			return "", err
		}
		return abs, nil
	}

	resolveSubjectPath := func(repoPath, subdir string) (string, string, error) {
		root, err := resolveRepo(repoPath)
		if err != nil {
			return "", "", err
		}
		subjectPath := root
		if strings.TrimSpace(subdir) != "" {
			subjectPath = filepath.Join(root, subdir)
		}
		resolved, err := toolstate.ResolveToolSource(subjectPath)
		if err != nil {
			return "", "", err
		}
		return root, resolved, nil
	}

	// Direct path: treat as local canonical repo or service subdir
	if info, err := os.Stat(arg); err == nil && info.IsDir() {
		repoRootPath, repoPath, err := resolveSubjectPath(arg, "")
		if err != nil {
			return nil, fmt.Errorf("canonical repo: %w", err)
		}
		manifest, err := loadManifest(repoPath)
		if err != nil {
			return nil, fmt.Errorf("manifest in %s: %w", repoPath, err)
		}
		nodeName := nodeOverride
		if target, ok := targets.Services[manifest.Name]; ok && nodeName == "" {
			nodeName = target.Node
		}
		if nodeName == "" {
			return nil, fmt.Errorf("no node for %s; set --node or add services.%s.node to %s",
				manifest.Name, manifest.Name, config.ReleaseTargetsPath(homeDir))
		}
		return &releaseSubject{Name: manifest.Name, RepoPath: repoPath, RepoRootPath: repoRootPath, Node: nodeName, Manifest: manifest}, nil
	}

	// Service name lookup
	serviceName := arg
	repoPath := repoOverride
	nodeName := nodeOverride
	var subdir string
	if target, ok := targets.Services[serviceName]; ok {
		if repoPath == "" {
			repoPath = target.Repo
		}
		if nodeName == "" {
			nodeName = target.Node
		}
		if subdir == "" {
			subdir = target.Subdir
		}
	}
	if repoPath == "" {
		return nil, fmt.Errorf("no repo for %s; set --repo or add services.%s.repo to %s",
			serviceName, serviceName, config.ReleaseTargetsPath(homeDir))
	}
	if nodeName == "" {
		return nil, fmt.Errorf("no node for %s; set --node or add services.%s.node to %s",
			serviceName, serviceName, config.ReleaseTargetsPath(homeDir))
	}
	repoRootPath, repoPathAbs, err := resolveSubjectPath(repoPath, subdir)
	if err != nil {
		return nil, fmt.Errorf("canonical repo for %s: %w", serviceName, err)
	}
	manifest, err := loadManifest(repoPathAbs)
	if err != nil {
		return nil, fmt.Errorf("manifest for %s: %w", serviceName, err)
	}
	if manifest.Name != serviceName {
		return nil, fmt.Errorf("manifest name mismatch: expected %q, got %q in %s",
			serviceName, manifest.Name, repoPathAbs)
	}
	return &releaseSubject{Name: serviceName, RepoPath: repoPathAbs, RepoRootPath: repoRootPath, Node: nodeName, Manifest: manifest}, nil
}

func runServiceRelease(subject *releaseSubject, clean bool) (*serviceReleaseReport, error) {
	releaseSpec := subject.Manifest.EffectiveRelease()
	if releaseSpec == nil || releaseSpec.Smoke == nil || strings.TrimSpace(releaseSpec.Smoke.Method) == "" {
		return nil, fmt.Errorf("%s has no x-deploy.smoke.method in jumpboot.yaml", subject.Name)
	}

	// Check local repo state
	inspectPath := subject.RepoRootPath
	if inspectPath == "" {
		inspectPath = subject.RepoPath
	}
	localRepo, err := toolstate.InspectGitCheckout(inspectPath)
	if err != nil {
		return nil, fmt.Errorf("inspect local repo: %w", err)
	}
	if localRepo.Dirty {
		return nil, fmt.Errorf("local repo is dirty; commit or stash changes first:\n%s", localRepo.StatusShort)
	}
	if localRepo.Upstream == "" {
		return nil, fmt.Errorf("local repo has no upstream tracking branch; cannot verify sync with remote")
	}
	if localRepo.Ahead > 0 || localRepo.Behind > 0 {
		return nil, fmt.Errorf("local repo is not in sync with %s (ahead=%d behind=%d); push/pull first",
			localRepo.Upstream, localRepo.Ahead, localRepo.Behind)
	}

	// Connect to mesh
	meshCfg, err := currentMeshConfig()
	if err != nil {
		return nil, err
	}
	m, err := mesh.New(meshCfg)
	if err != nil {
		return nil, fmt.Errorf("mesh connect: %w", err)
	}
	defer m.Close()

	// Inspect deployed state before update
	before, err := m.RequestReleaseInspect(subject.Node, subject.Name, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("pre-update inspection: %w", err)
	}
	if !before.OK {
		return nil, fmt.Errorf("pre-update inspection failed: %s", before.Error)
	}
	if before.Info == nil || before.Info.Repo == nil {
		return nil, fmt.Errorf("pre-update inspection did not include deployed repo state for %s on %s", subject.Name, subject.Node)
	}
	if before.Info.Repo.Dirty {
		return nil, fmt.Errorf("deployed checkout is dirty on %s; aborting before update:\n%s", subject.Node, before.Info.Repo.StatusShort)
	}

	// Run managed update
	updateResult, err := m.RequestUpdate(subject.Node, subject.Name, clean, 10*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("managed update: %w", err)
	}
	if !updateResult.OK {
		return nil, fmt.Errorf("managed update failed: %s", updateResult.Error)
	}

	// Inspect deployed state after update
	after, err := m.RequestReleaseInspect(subject.Node, subject.Name, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("post-update inspection: %w", err)
	}
	if !after.OK {
		return nil, fmt.Errorf("post-update inspection failed: %s", after.Error)
	}

	// Run health check
	healthResult, err := m.Call(subject.Name, "health", nil, 30*time.Second, subject.Node)
	if err != nil {
		return nil, fmt.Errorf("health call: %w", err)
	}
	if !healthResult.OK {
		return nil, fmt.Errorf("health call failed on %s: %s", healthResult.Node, healthResult.Error)
	}

	// Run smoke call if defined
	smokeResult := &mesh.CallResult{OK: true}
	smokeMethod := releaseSpec.Smoke.Method
	smokeParams := releaseSpec.Smoke.Params
	if smokeMethod != "" {
		smokeResult, err = m.Call(subject.Name, smokeMethod, smokeParams, 30*time.Second, subject.Node)
		if err != nil {
			return nil, fmt.Errorf("smoke call %s: %w", smokeMethod, err)
		}
		if !smokeResult.OK {
			return nil, fmt.Errorf("smoke call %s failed on %s: %s", smokeMethod, smokeResult.Node, smokeResult.Error)
		}
	}

	return &serviceReleaseReport{
		GeneratedAt:    time.Now().UTC(),
		Service:        subject.Name,
		Node:           subject.Node,
		RepoPath:       subject.RepoPath,
		LocalRepo:      localRepo,
		DeployedBefore: before,
		Update:         updateResult,
		DeployedAfter:  after,
		HealthMethod:   "health",
		Health:         healthResult,
		SmokeMethod:    smokeMethod,
		SmokeParams:    smokeParams,
		Smoke:          smokeResult,
	}, nil
}

func defaultReleaseProofPath(serviceName, ext string) string {
	stamp := time.Now().UTC().Format("2006-01-02-150405")
	if ext == "" {
		ext = "md"
	}
	return filepath.Join("/tmp", fmt.Sprintf("%s-release-proof-%s.%s", serviceName, stamp, ext))
}

func renderServiceReleaseReport(report *serviceReleaseReport, format string) ([]byte, error) {
	if format == "json" {
		return json.MarshalIndent(report, "", "  ")
	}
	tmpl, err := template.New("proof").Funcs(template.FuncMap{
		"jsonMarshal": jsonMarshal,
	}).Parse(serviceReleaseProofTemplate)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, report); err != nil {
		return nil, fmt.Errorf("render template: %w", err)
	}
	return buf.Bytes(), nil
}
