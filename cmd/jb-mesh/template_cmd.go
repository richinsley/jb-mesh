package main

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

func templatesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "templates",
		Short: "Inspect embedded service starter templates",
	}
	cmd.AddCommand(templatesListCmd())
	cmd.AddCommand(templatesShowCmd())
	cmd.AddCommand(templatesRenderCmd())
	return cmd
}

func templatesListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List embedded starter templates",
		RunE: func(cmd *cobra.Command, args []string) error {
			names, err := availableStarterTemplates(serviceStarterTemplates)
			if err != nil {
				return err
			}
			for _, name := range names {
				fmt.Println(name)
			}
			return nil
		},
	}
}

func templatesShowCmd() *cobra.Command {
	var flagFile string

	cmd := &cobra.Command{
		Use:   "show <template>",
		Short: "Show embedded starter template files or file contents",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			templateName := strings.TrimSpace(args[0])
			root := starterTemplateRoot(templateName)
			if _, err := fs.Stat(serviceStarterTemplates, root); err != nil {
				return fmt.Errorf("unknown template %q", templateName)
			}

			if flagFile != "" {
				path := filepath.ToSlash(filepath.Join(root, flagFile))
				data, err := fs.ReadFile(serviceStarterTemplates, path)
				if err != nil {
					return fmt.Errorf("read %s from template %q: %w", flagFile, templateName, err)
				}
				fmt.Print(string(data))
				return nil
			}

			files, err := starterTemplateFiles(serviceStarterTemplates, templateName)
			if err != nil {
				return err
			}
			for i, file := range files {
				data, err := fs.ReadFile(serviceStarterTemplates, filepath.ToSlash(filepath.Join(root, file)))
				if err != nil {
					return err
				}
				if i > 0 {
					fmt.Println()
				}
				fmt.Printf("=== %s ===\n", file)
				fmt.Print(string(data))
				if len(data) == 0 || data[len(data)-1] != '\n' {
					fmt.Println()
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&flagFile, "file", "", "Show only one file from the template (for example: jumpboot.yaml or main.py)")
	return cmd
}

func templatesRenderCmd() *cobra.Command {
	var flagName string
	var flagFile string
	var flagDescription string
	var flagVersion string

	cmd := &cobra.Command{
		Use:   "render <template>",
		Short: "Render a starter template with substitutions, without writing files",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			templateName := strings.TrimSpace(args[0])
			if err := validateServiceName(flagName); err != nil {
				return err
			}

			rendered, err := renderedStarterFiles(serviceStarterTemplates, templateName, starterReplacements(flagName, flagDescription, flagVersion))
			if err != nil {
				return err
			}

			if flagFile != "" {
				content, ok := rendered[filepath.ToSlash(flagFile)]
				if !ok {
					return fmt.Errorf("file %q not found in rendered template %q", flagFile, templateName)
				}
				fmt.Print(content)
				if content == "" || content[len(content)-1] != '\n' {
					fmt.Println()
				}
				return nil
			}

			for i, file := range sortedRenderedFileNames(rendered) {
				if i > 0 {
					fmt.Println()
				}
				fmt.Printf("=== %s ===\n", file)
				fmt.Print(rendered[file])
				if rendered[file] == "" || rendered[file][len(rendered[file])-1] != '\n' {
					fmt.Println()
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&flagName, "name", "", "Service name to render into the template")
	cmd.Flags().StringVar(&flagFile, "file", "", "Render only one file from the template (for example: jumpboot.yaml or main.py)")
	cmd.Flags().StringVar(&flagDescription, "description", "", "Override the starter description")
	cmd.Flags().StringVar(&flagVersion, "version", "", "Override the starter version")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func starterTemplateRoot(name string) string {
	return filepath.ToSlash(filepath.Join("templates", "service-starter", name))
}

func availableStarterTemplates(fsys fs.FS) ([]string, error) {
	root := filepath.ToSlash(filepath.Join("templates", "service-starter"))
	entries, err := fs.ReadDir(fsys, root)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func starterTemplateFiles(fsys fs.FS, name string) ([]string, error) {
	root := starterTemplateRoot(name)
	files := []string{}
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
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}
