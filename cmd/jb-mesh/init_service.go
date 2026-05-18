package main

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

//go:embed templates/service-starter/** templates/service-starter/*/.gitignore templates/service-starter/*/state/.gitkeep
var serviceStarterTemplates embed.FS

func initServiceCmd() *cobra.Command {
	var flagTemplate string
	var flagDescription string
	var flagVersion string
	var flagForce bool

	cmd := &cobra.Command{
		Use:   "init-service <name> [path]",
		Short: "Create a new jb-mesh service from a canonical starter template",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := strings.TrimSpace(args[0])
			if err := validateServiceName(name); err != nil {
				return err
			}

			outDir := name
			if len(args) == 2 {
				outDir = args[1]
			}
			absOut, err := filepath.Abs(outDir)
			if err != nil {
				return err
			}

			templateRoot := starterTemplateRoot(flagTemplate)
			if _, err := fs.Stat(serviceStarterTemplates, templateRoot); err != nil {
				return fmt.Errorf("unknown template %q", flagTemplate)
			}

			if err := ensureTargetDir(absOut, flagForce); err != nil {
				return err
			}

			rendered, err := renderedStarterFiles(serviceStarterTemplates, flagTemplate, starterReplacements(name, flagDescription, flagVersion))
			if err != nil {
				return err
			}
			if err := writeRenderedStarterFiles(absOut, rendered); err != nil {
				return err
			}

			fmt.Printf("Initialized %s starter at %s using template %q\n", name, absOut, flagTemplate)
			fmt.Println("Next steps:")
			fmt.Printf("  cd %s\n", absOut)
			fmt.Println("  jb-mesh preflight .")
			fmt.Printf("  git init && git add -A && git commit -m 'init %s service'\n", name)
			return nil
		},
	}

	cmd.Flags().StringVar(&flagTemplate, "template", "baseline", "Starter template: baseline or persistent-msgpack")
	cmd.Flags().StringVar(&flagDescription, "description", "", "Override the starter description")
	cmd.Flags().StringVar(&flagVersion, "version", "", "Override the starter version (default: template version)")
	cmd.Flags().BoolVar(&flagForce, "force", false, "Allow writing into an existing empty directory")
	return cmd
}

func validateServiceName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("service name is required")
	}
	for i, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-'
		if !ok {
			return fmt.Errorf("invalid service name %q: use lowercase letters, digits, and hyphens only", name)
		}
		if i == 0 && r == '-' {
			return fmt.Errorf("invalid service name %q: cannot start with a hyphen", name)
		}
	}
	return nil
}

func ensureTargetDir(path string, force bool) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("target exists and is not a directory: %s", path)
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return fmt.Errorf("target directory is not empty: %s", path)
		}
		if !force {
			return nil
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	return os.MkdirAll(path, 0755)
}

func serviceClassName(name string) string {
	parts := strings.Split(name, "-")
	var b strings.Builder
	for _, part := range parts {
		if part == "" {
			continue
		}
		b.WriteString(strings.ToUpper(part[:1]))
		if len(part) > 1 {
			b.WriteString(part[1:])
		}
	}
	className := b.String()
	if className == "" {
		return "Service"
	}
	if !strings.HasSuffix(className, "Service") {
		className += "Service"
	}
	return className
}

func starterReplacements(name, description, version string) map[string]string {
	replacements := map[string]string{
		"my-service": name,
		"MyService":  serviceClassName(name),
	}
	if description != "" {
		replacements["Short description of what this service does"] = description
		replacements["Long-lived jb-mesh service with MessagePack transport"] = description
	}
	if version != "" {
		replacements["0.1.0"] = version
	}
	return replacements
}

func renderedStarterFiles(fsys fs.FS, templateName string, replacements map[string]string) (map[string]string, error) {
	root := starterTemplateRoot(templateName)
	if _, err := fs.Stat(fsys, root); err != nil {
		return nil, fmt.Errorf("unknown template %q", templateName)
	}

	rendered := make(map[string]string)
	err := fs.WalkDir(fsys, root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root || d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		data, err := fs.ReadFile(fsys, path)
		if err != nil {
			return err
		}
		content := string(data)
		for old, newVal := range replacements {
			content = strings.ReplaceAll(content, old, newVal)
		}
		rendered[filepath.ToSlash(rel)] = content
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rendered, nil
}

func writeRenderedStarterFiles(outDir string, rendered map[string]string) error {
	for _, rel := range sortedRenderedFileNames(rendered) {
		dest := filepath.Join(outDir, rel)
		if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(dest, []byte(rendered[rel]), 0644); err != nil {
			return err
		}
	}
	return nil
}

func sortedRenderedFileNames(rendered map[string]string) []string {
	names := make([]string, 0, len(rendered))
	for name := range rendered {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
