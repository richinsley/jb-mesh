package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/richinsley/jb-mesh/pkg/config"
	"github.com/richinsley/jb-mesh/pkg/events"
	"github.com/richinsley/jb-mesh/pkg/filestore"
	"github.com/richinsley/jb-mesh/pkg/mesh"
	"github.com/richinsley/jb-mesh/pkg/node"
	"github.com/richinsley/jb-mesh/pkg/tools"
	"github.com/spf13/cobra"
)

var (
	flagNATS            string
	flagNodeName        string
	flagToken           string
	flagHome            string
	flagLocal           bool
	flagSchema          bool
	flagNoMDNS          bool
	flagSeed            bool
	flagLeaf            string
	flagLeafPort        int
	flagWebsocketHost   string
	flagWebsocketPort   int
	flagNATSWSProxyPath string
	flagNATSWSBearer    string
	flagNATSWSHeaders   []string
	flagNATSWSQuery     []string
)

func main() {
	root := &cobra.Command{
		Use:   "jb-mesh",
		Short: "A NATS-powered mesh of jumpboot nodes running Python tools",
	}

	root.PersistentFlags().StringVar(&flagNATS, "nats", "nats://localhost:4222", "NATS server URL")
	root.PersistentFlags().StringVar(&flagNodeName, "name", "", "Node name (default: hostname)")
	root.PersistentFlags().StringVar(&flagToken, "token", "", "NATS auth token")
	root.PersistentFlags().StringVar(&flagHome, "home", "", "jb-mesh home directory (default: ~/.jb-mesh)")
	root.PersistentFlags().StringVar(&flagNATSWSProxyPath, "nats-ws-proxy-path", "", "WebSocket proxy mount path, e.g. /mesh/nats")
	root.PersistentFlags().StringVar(&flagNATSWSBearer, "nats-ws-bearer-token", "", "Bearer token for NATS WebSocket upgrade auth")
	root.PersistentFlags().StringArrayVar(&flagNATSWSHeaders, "nats-ws-header", nil, "Extra NATS WebSocket header in key=value form (repeatable)")
	root.PersistentFlags().StringArrayVar(&flagNATSWSQuery, "nats-ws-query", nil, "Extra NATS WebSocket query parameter in key=value form (repeatable)")

	root.AddCommand(serveCmd())
	root.AddCommand(listCmd())
	root.AddCommand(callCmd())
	root.AddCommand(statusCmd())
	root.AddCommand(installCmd())
	root.AddCommand(uninstallCmd())
	root.AddCommand(updateCmd())
	root.AddCommand(preflightCmd())
	root.AddCommand(serviceReleaseCmd())
	root.AddCommand(initServiceCmd())
	root.AddCommand(templatesCmd())
	root.AddCommand(filesCmd())
	root.AddCommand(eventsCmd())
	root.AddCommand(logsCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func getNodeName() string {
	if flagNodeName != "" {
		return flagNodeName
	}
	name, _ := os.Hostname()
	return name
}

// extractParams extracts a brief param list from method schema for display
// methodSchema should be a map[string]interface{} with "params" or "properties"
func currentNATSWSConfig() (mesh.NATSWebSocketConfig, error) {
	headersKV, err := mesh.ParseKeyValuePairs(flagNATSWSHeaders)
	if err != nil {
		return mesh.NATSWebSocketConfig{}, fmt.Errorf("parse --nats-ws-header: %w", err)
	}
	queryKV, err := mesh.ParseKeyValuePairs(flagNATSWSQuery)
	if err != nil {
		return mesh.NATSWebSocketConfig{}, fmt.Errorf("parse --nats-ws-query: %w", err)
	}

	var headers http.Header
	if len(headersKV) > 0 {
		headers = make(http.Header, len(headersKV))
		for key, values := range headersKV {
			for _, value := range values {
				headers.Add(key, value)
			}
		}
	}

	var query url.Values
	if len(queryKV) > 0 {
		query = make(url.Values, len(queryKV))
		for key, values := range queryKV {
			for _, value := range values {
				query.Add(key, value)
			}
		}
	}

	return mesh.NATSWebSocketConfig{
		ProxyPath:   flagNATSWSProxyPath,
		BearerToken: flagNATSWSBearer,
		Headers:     headers,
		Query:       query,
	}, nil
}

func currentMeshConfig() (mesh.Config, error) {
	wsCfg, err := currentNATSWSConfig()
	if err != nil {
		return mesh.Config{}, err
	}
	return mesh.Config{
		NATSUrl:   flagNATS,
		NodeName:  getNodeName(),
		Token:     flagToken,
		WebSocket: wsCfg,
	}, nil
}

func extractParams(methodSchema interface{}) string {
	schema, ok := methodSchema.(map[string]interface{})
	if !ok {
		return ""
	}

	// Try "properties" first (standard JSON Schema)
	props, ok := schema["properties"].(map[string]interface{})
	if !ok {
		return ""
	}

	// Try "required" list
	var required []string
	if reqList, ok := schema["required"].([]interface{}); ok {
		for _, r := range reqList {
			if s, ok := r.(string); ok {
				required = append(required, s)
			}
		}
	}
	requiredSet := make(map[string]bool)
	for _, r := range required {
		requiredSet[r] = true
	}

	// Build param list
	var parts []string
	for name, prop := range props {
		propMap, ok := prop.(map[string]interface{})
		if !ok {
			continue
		}
		propType := "any"
		if t, ok := propMap["type"].(string); ok {
			propType = t
		}
		if requiredSet[name] {
			parts = append(parts, fmt.Sprintf("%s: %s", name, propType))
		} else {
			parts = append(parts, fmt.Sprintf("%s?: %s", name, propType))
		}
	}

	if len(parts) == 0 {
		return "(no params)"
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func serveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start a jb-mesh node and join the mesh",
		RunE: func(cmd *cobra.Command, args []string) error {
			wsCfg, err := currentNATSWSConfig()
			if err != nil {
				return err
			}
			cfg := node.Config{
				NodeName:      getNodeName(),
				Token:         flagToken,
				NATSWebSocket: wsCfg,
				HomeDir:       flagHome,
				NoMDNS:        flagNoMDNS,
				WebsocketHost: flagWebsocketHost,
				WebsocketPort: flagWebsocketPort,
				LeafPort:      flagLeafPort,
			}

			// Only pass NATSUrl if user explicitly set --nats
			// (overrides auto-detection and embedded NATS)
			if cmd.Flags().Changed("nats") {
				cfg.NATSUrl = flagNATS
			}

			// Role selection: --seed, --leaf, or auto-detect
			if flagSeed {
				cfg.Role = "seed"
			} else if flagLeaf != "" {
				cfg.Role = "leaf"
				cfg.LeafURL = flagLeaf
			}

			n, err := node.New(cfg)
			if err != nil {
				return err
			}
			defer n.Close()

			localTools := n.ListLocalTools()
			fmt.Printf("Node %q serving %d tools\n", getNodeName(), len(localTools))
			for _, t := range localTools {
				var methods []string
				for m := range t.Manifest.RPC.Methods {
					methods = append(methods, m)
				}
				fmt.Printf("  • %s v%s [%s]\n", t.Name, t.Manifest.Version, strings.Join(methods, ", "))
			}
			fmt.Println("\nListening for calls. Ctrl+C to stop.")

			// Wait for shutdown signal
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
			<-sig

			fmt.Println("\nShutting down...")
			return nil
		},
	}

	cmd.Flags().BoolVar(&flagSeed, "seed", false, "Force seed role (skip mDNS browse)")
	cmd.Flags().StringVar(&flagLeaf, "leaf", "", "Force leaf role, connect to seed at this URL (nats-leaf://host:port)")
	cmd.Flags().IntVar(&flagLeafPort, "leaf-port", 0, "Embedded NATS leaf-node bind port for seed role (0 uses default 7422)")
	cmd.Flags().BoolVar(&flagNoMDNS, "no-mdns", false, "Disable mDNS peer discovery (for Docker/cloud)")
	cmd.Flags().StringVar(&flagWebsocketHost, "websocket-host", "127.0.0.1", "Embedded NATS websocket bind host (private; Portal should terminate public TLS)")
	cmd.Flags().IntVar(&flagWebsocketPort, "websocket-port", 0, "Embedded NATS websocket bind port (0 disables websocket)")
	return cmd
}

func listCmd() *cobra.Command {
	var flagShowParams bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List tools available in the mesh (or --local for this node only)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if flagLocal {
				// Local-only: just read manifests, no NATS needed
				cfg, err := config.LoadWithHome(flagHome)
				if err != nil {
					return err
				}
				mgr := tools.NewManager(cfg)
				if err := mgr.LoadAll(); err != nil {
					return err
				}
				tl := mgr.List()
				if len(tl) == 0 {
					fmt.Println("No tools installed locally.")
					return nil
				}
				for _, t := range tl {
					var methods []string
					for m := range t.Manifest.RPC.Methods {
						methods = append(methods, m)
					}
					fmt.Printf("  %s v%s (%s) [%s]\n", t.Name, t.Manifest.Version, t.Manifest.Runtime.Mode, strings.Join(methods, ", "))
					// If --params, show schema for each method
					if flagShowParams {
						// TODO: local schema fetch via GetSchema
					}
				}
				return nil
			}

			// Mesh-wide: discover via NATS
			meshCfg, err := currentMeshConfig()
			if err != nil {
				return err
			}
			m, err := mesh.New(meshCfg)
			if err != nil {
				return err
			}
			defer m.Close()

			infos, err := m.ListServices()
			if err != nil {
				return err
			}

			if len(infos) == 0 {
				fmt.Println("No tools found in mesh.")
				return nil
			}

			for _, info := range infos {
				nodeMeta := ""
				if n, ok := info.Metadata["node"]; ok {
					nodeMeta = fmt.Sprintf(" (node: %s)", n)
				}
				fmt.Printf("  %s v%s%s\n", info.Name, info.Version, nodeMeta)

				// Pre-fetch schema if --params (with timeout)
				var schemaCache map[string]interface{}
				if flagShowParams {
					if nodeName, ok := info.Metadata["node"]; ok {
						if result, err := m.GetToolSchema(nodeName, info.Name, 2*time.Second); err == nil && result.OK {
							schemaCache = result.Schema
						}
					}
				}

				for _, ep := range info.Endpoints {
					if flagShowParams && schemaCache != nil {
						// Look up method schema
						if methodSchema, ok := schemaCache[ep.Name]; ok {
							params := extractParams(methodSchema)
							fmt.Printf("    • %s [%s] %s\n", ep.Name, ep.Subject, params)
							continue
						}
					}
					fmt.Printf("    • %s [%s]\n", ep.Name, ep.Subject)
				}
			}

			return nil
		},
	}

	cmd.Flags().BoolVar(&flagLocal, "local", false, "Show only locally installed tools")
	cmd.Flags().BoolVar(&flagShowParams, "params", false, "Show method parameters (requires fetch from node)")
	return cmd
}

func callCmd() *cobra.Command {
	var flagCallNode string

	cmd := &cobra.Command{
		Use:   "call <tool>.<method> [key=value ...]",
		Short: "Call a tool method anywhere in the mesh",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse tool.method
			parts := strings.SplitN(args[0], ".", 2)
			if len(parts) != 2 {
				return fmt.Errorf("expected <tool>.<method>, got %q", args[0])
			}
			toolName, method := parts[0], parts[1]

			// Parse key=value params
			params := make(map[string]interface{})
			for _, arg := range args[1:] {
				kv := strings.SplitN(arg, "=", 2)
				if len(kv) != 2 {
					return fmt.Errorf("expected key=value, got %q", arg)
				}
				params[kv[0]] = kv[1]
			}

			meshCfg, err := currentMeshConfig()
			if err != nil {
				return err
			}
			m, err := mesh.New(meshCfg)
			if err != nil {
				return err
			}
			defer m.Close()

			result, err := m.Call(toolName, method, params, 5*time.Minute, flagCallNode)
			if err != nil {
				return err
			}

			data, _ := json.MarshalIndent(result, "", "  ")
			fmt.Println(string(data))
			return nil
		},
	}

	cmd.Flags().StringVar(&flagCallNode, "node", "", "Target a specific node (default: any node)")
	return cmd
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show mesh topology and node status",
		RunE: func(cmd *cobra.Command, args []string) error {
			meshCfg, err := currentMeshConfig()
			if err != nil {
				return err
			}
			m, err := mesh.New(meshCfg)
			if err != nil {
				return err
			}
			defer m.Close()

			fmt.Printf("Connected to NATS: %v\n", m.Connected())
			fmt.Printf("Node: %s\n\n", getNodeName())

			infos, err := m.ListServices()
			if err != nil {
				return err
			}

			fmt.Printf("Services in mesh: %d\n", len(infos))
			for _, info := range infos {
				nodeMeta := info.Metadata["node"]
				fmt.Printf("  %s v%s (node: %s, id: %s)\n", info.Name, info.Version, nodeMeta, info.ID)
			}

			return nil
		},
	}
}

func installCmd() *cobra.Command {
	var flagNode string

	cmd := &cobra.Command{
		Use:   "install <source>",
		Short: "Install a tool from git URL or local path (use --node for remote install)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			source := args[0]

			// Remote install via NATS
			if flagNode != "" {
				meshCfg, err := currentMeshConfig()
				if err != nil {
					return err
				}
				m, err := mesh.New(meshCfg)
				if err != nil {
					return err
				}
				defer m.Close()

				fmt.Printf("Requesting install of %s on node %q...\n", source, flagNode)
				result, err := m.RequestInstall(flagNode, source, 10*time.Minute)
				if err != nil {
					return err
				}

				if !result.OK {
					return fmt.Errorf("remote install failed: %s", result.Error)
				}

				fmt.Printf("Installed %s v%s on node %s\n", result.ToolName, result.Version, result.Node)
				return nil
			}

			// Local install
			cfg, err := config.LoadWithHome(flagHome)
			if err != nil {
				return err
			}
			if err := cfg.EnsureDirs(); err != nil {
				return err
			}

			mgr := tools.NewManager(cfg)
			if err := mgr.LoadAll(); err != nil {
				return err
			}

			tool, err := mgr.Install(source)
			if err != nil {
				return err
			}

			fmt.Printf("Installed %s v%s\n", tool.Name, tool.Manifest.Version)
			return nil
		},
	}

	cmd.Flags().StringVar(&flagNode, "node", "", "Install on a remote node via NATS")
	return cmd
}

func uninstallCmd() *cobra.Command {
	var flagNode string
	var flagRemoveEnv bool

	cmd := &cobra.Command{
		Use:   "uninstall <tool>",
		Short: "Uninstall a tool (use --node for remote uninstall)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			toolName := args[0]

			if flagNode != "" {
				meshCfg, err := currentMeshConfig()
				if err != nil {
					return err
				}
				m, err := mesh.New(meshCfg)
				if err != nil {
					return err
				}
				defer m.Close()

				fmt.Printf("Requesting uninstall of %s on node %q...\n", toolName, flagNode)
				result, err := m.RequestUninstall(flagNode, toolName, flagRemoveEnv, 2*time.Minute)
				if err != nil {
					return err
				}
				if !result.OK {
					return fmt.Errorf("remote uninstall failed: %s", result.Error)
				}
				fmt.Printf("Uninstalled %s from node %s\n", toolName, result.Node)
				return nil
			}

			// Local uninstall
			cfg, err := config.LoadWithHome(flagHome)
			if err != nil {
				return err
			}
			mgr := tools.NewManager(cfg)
			if err := mgr.LoadAll(); err != nil {
				return err
			}
			if err := mgr.Uninstall(toolName, flagRemoveEnv); err != nil {
				return err
			}
			fmt.Printf("Uninstalled %s\n", toolName)
			return nil
		},
	}

	cmd.Flags().StringVar(&flagNode, "node", "", "Uninstall from a remote node")
	cmd.Flags().BoolVar(&flagRemoveEnv, "remove-env", false, "Also remove the Python environment")
	return cmd
}

func updateCmd() *cobra.Command {
	var flagNode string
	var flagClean bool

	cmd := &cobra.Command{
		Use:   "update <tool>",
		Short: "Update a tool (git pull + reinstall if packages changed)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			toolName := args[0]

			if flagNode != "" {
				meshCfg, err := currentMeshConfig()
				if err != nil {
					return err
				}
				m, err := mesh.New(meshCfg)
				if err != nil {
					return err
				}
				defer m.Close()

				fmt.Printf("Requesting update of %s on node %q...\n", toolName, flagNode)
				result, err := m.RequestUpdate(flagNode, toolName, flagClean, 10*time.Minute)
				if err != nil {
					return err
				}
				if !result.OK {
					return fmt.Errorf("remote update failed: %s", result.Error)
				}
				fmt.Printf("Updated %s: %s → %s on node %s\n", toolName, result.OldVersion, result.NewVersion, result.Node)
				return nil
			}

			// Local update
			cfg, err := config.LoadWithHome(flagHome)
			if err != nil {
				return err
			}
			mgr := tools.NewManager(cfg)
			if err := mgr.LoadAll(); err != nil {
				return err
			}
			tool, err := mgr.Update(toolName, flagClean)
			if err != nil {
				return err
			}
			fmt.Printf("Updated %s to v%s\n", tool.Name, tool.Manifest.Version)
			return nil
		},
	}

	cmd.Flags().StringVar(&flagNode, "node", "", "Update on a remote node")
	cmd.Flags().BoolVar(&flagClean, "clean", false, "Force clean rebuild of environment")
	return cmd
}

func connectNATSWithJetStream() (*nats.Conn, nats.JetStreamContext, error) {
	meshCfg, err := currentMeshConfig()
	if err != nil {
		return nil, nil, err
	}
	nc, err := mesh.Connect(meshCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to NATS: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("jetstream: %w", err)
	}
	return nc, js, nil
}

func filesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "files",
		Short: "Manage files in the mesh file store",
	}

	cmd.AddCommand(filesPutCmd())
	cmd.AddCommand(filesGetCmd())
	cmd.AddCommand(filesHeadCmd())
	cmd.AddCommand(filesDeleteCmd())
	cmd.AddCommand(filesListCmd())
	return cmd
}

func filesPutCmd() *cobra.Command {
	var flagContentType string

	cmd := &cobra.Command{
		Use:   "put <key> <file>",
		Short: "Upload a file to the store",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			key, filePath := args[0], args[1]

			data, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("read file: %w", err)
			}

			if flagContentType == "" {
				flagContentType = detectContentType(filePath)
			}

			nc, js, err := connectNATSWithJetStream()
			if err != nil {
				return err
			}
			defer nc.Close()

			store, err := filestore.NewStore(js, filestore.DefaultConfig())
			if err != nil {
				return err
			}

			meta, err := store.Put(key, data, flagContentType)
			if err != nil {
				return err
			}

			fmt.Printf("Stored %s (%d bytes, %s, etag: %s)\n", meta.Key, meta.Size, meta.ContentType, meta.ETag[:12])
			return nil
		},
	}

	cmd.Flags().StringVar(&flagContentType, "type", "", "Content type (auto-detected if omitted)")
	return cmd
}

func filesGetCmd() *cobra.Command {
	var flagOutput string

	cmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Download a file from the store",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]

			nc, js, err := connectNATSWithJetStream()
			if err != nil {
				return err
			}
			defer nc.Close()

			store, err := filestore.NewStore(js, filestore.DefaultConfig())
			if err != nil {
				return err
			}

			data, meta, err := store.Get(key)
			if err != nil {
				return err
			}

			if flagOutput != "" {
				if err := os.WriteFile(flagOutput, data, 0644); err != nil {
					return fmt.Errorf("write file: %w", err)
				}
				fmt.Printf("Downloaded %s → %s (%d bytes)\n", key, flagOutput, meta.Size)
			} else {
				os.Stdout.Write(data)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&flagOutput, "output", "o", "", "Output file path (default: stdout)")
	return cmd
}

func filesHeadCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "head <key>",
		Short: "Show file metadata",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nc, js, err := connectNATSWithJetStream()
			if err != nil {
				return err
			}
			defer nc.Close()

			store, err := filestore.NewStore(js, filestore.DefaultConfig())
			if err != nil {
				return err
			}

			meta, err := store.Head(args[0])
			if err != nil {
				return err
			}

			data, _ := json.MarshalIndent(meta, "", "  ")
			fmt.Println(string(data))
			return nil
		},
	}
}

func filesDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a file from the store",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nc, js, err := connectNATSWithJetStream()
			if err != nil {
				return err
			}
			defer nc.Close()

			store, err := filestore.NewStore(js, filestore.DefaultConfig())
			if err != nil {
				return err
			}

			if err := store.Delete(args[0]); err != nil {
				return err
			}
			fmt.Printf("Deleted %s\n", args[0])
			return nil
		},
	}
}

func filesListCmd() *cobra.Command {
	var flagPrefix string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List files in the store",
		RunE: func(cmd *cobra.Command, args []string) error {
			nc, js, err := connectNATSWithJetStream()
			if err != nil {
				return err
			}
			defer nc.Close()

			store, err := filestore.NewStore(js, filestore.DefaultConfig())
			if err != nil {
				return err
			}

			files, err := store.List(flagPrefix)
			if err != nil {
				return err
			}

			if len(files) == 0 {
				fmt.Println("No files found.")
				return nil
			}

			for _, f := range files {
				fmt.Printf("  %s  %d bytes  %s  %s\n", f.Key, f.Size, f.ContentType, f.Created.Format(time.RFC3339))
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&flagPrefix, "prefix", "", "Filter by key prefix")
	return cmd
}

func eventsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "events",
		Short: "View and watch mesh events",
	}

	cmd.AddCommand(eventsListCmd())
	cmd.AddCommand(eventsWatchCmd())
	cmd.AddCommand(eventsEmitCmd())
	return cmd
}

func eventsListCmd() *cobra.Command {
	var flagLimit int
	var flagFilter string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List recent events from history",
		RunE: func(cmd *cobra.Command, args []string) error {
			nc, js, err := connectNATSWithJetStream()
			if err != nil {
				return err
			}
			defer nc.Close()

			if flagFilter == "" {
				flagFilter = "events.>"
			}

			bus, err := events.NewPersistentBus(nc, js, getNodeName(), events.DefaultPersistConfig())
			if err != nil {
				return err
			}

			evts, err := bus.History(flagFilter, flagLimit)
			if err != nil {
				return err
			}

			if len(evts) == 0 {
				fmt.Println("No events found.")
				return nil
			}

			for _, e := range evts {
				dataJSON, _ := json.Marshal(e.Data)
				fmt.Printf("  %s  %-20s  node=%-12s  %s\n",
					e.Timestamp.Format("15:04:05"),
					e.Type,
					e.Node,
					string(dataJSON),
				)
			}
			return nil
		},
	}

	cmd.Flags().IntVar(&flagLimit, "limit", 50, "Maximum events to return")
	cmd.Flags().StringVar(&flagFilter, "filter", "", "NATS subject filter (default: events.>)")
	return cmd
}

func eventsWatchCmd() *cobra.Command {
	var flagFilter string

	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Watch events in real-time",
		RunE: func(cmd *cobra.Command, args []string) error {
			meshCfg, err := currentMeshConfig()
			if err != nil {
				return err
			}
			nc, err := mesh.Connect(meshCfg)
			if err != nil {
				return fmt.Errorf("connect: %w", err)
			}
			defer nc.Close()

			if flagFilter == "" {
				flagFilter = "events.>"
			}

			bus := events.NewBus(nc, getNodeName())
			_, err = bus.Subscribe(flagFilter, func(e events.Event) {
				dataJSON, _ := json.Marshal(e.Data)
				fmt.Printf("%s  %-20s  node=%-12s  %s\n",
					e.Timestamp.Format("15:04:05"),
					e.Type,
					e.Node,
					string(dataJSON),
				)
			})
			if err != nil {
				return err
			}

			fmt.Printf("Watching %s (Ctrl+C to stop)\n", flagFilter)

			sig := make(chan os.Signal, 1)
			signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
			<-sig
			return nil
		},
	}

	cmd.Flags().StringVar(&flagFilter, "filter", "", "NATS subject filter (default: events.>)")
	return cmd
}

func eventsEmitCmd() *cobra.Command {
	var flagEmitNode string

	cmd := &cobra.Command{
		Use:   "emit <type> [key=value ...]",
		Short: "Emit an event onto the mesh event bus",
		Long:  "Emit a mesh event for testing and debugging. Event types should omit the 'events.' prefix, e.g. 'tool.crashed' or 'user.portal.auth_fail'.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			eventType := strings.TrimSpace(args[0])
			if eventType == "" {
				return fmt.Errorf("event type is required")
			}
			if strings.HasPrefix(eventType, "events.") {
				eventType = strings.TrimPrefix(eventType, "events.")
			}

			data, err := parseEventDataArgs(args[1:])
			if err != nil {
				return err
			}

			meshCfg, err := currentMeshConfig()
			if err != nil {
				return err
			}
			nc, err := mesh.Connect(meshCfg)
			if err != nil {
				return fmt.Errorf("connect: %w", err)
			}
			defer nc.Close()

			bus := events.NewBus(nc, getNodeName())
			event := events.Event{
				Type: eventType,
				Node: flagEmitNode,
				Data: data,
			}
			if event.Node == "" {
				event.Node = getNodeName()
			}

			if err := bus.Emit(event); err != nil {
				return err
			}

			payload, _ := json.Marshal(event.Data)
			fmt.Printf("Emitted events.%s node=%s data=%s\n", event.Type, event.Node, string(payload))
			return nil
		},
	}

	cmd.Flags().StringVar(&flagEmitNode, "node", "", "Override the originating node name in the emitted event")
	return cmd
}

func parseEventDataArgs(args []string) (map[string]interface{}, error) {
	data := make(map[string]interface{}, len(args))
	for _, arg := range args {
		kv := strings.SplitN(arg, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("expected key=value, got %q", arg)
		}
		key := strings.TrimSpace(kv[0])
		if key == "" {
			return nil, fmt.Errorf("empty key in %q", arg)
		}
		data[key] = parseCLIValue(kv[1])
	}
	return data, nil
}

func parseCLIValue(raw string) interface{} {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	var parsed interface{}
	if err := json.Unmarshal([]byte(trimmed), &parsed); err == nil {
		return parsed
	}

	return raw
}

func detectContentType(path string) string {
	ext := ""
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '.' {
			ext = path[i:]
			break
		}
	}
	switch ext {
	case ".txt":
		return "text/plain"
	case ".json":
		return "application/json"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".wav":
		return "audio/wav"
	case ".mp3":
		return "audio/mpeg"
	case ".mp4":
		return "video/mp4"
	case ".pdf":
		return "application/pdf"
	case ".yaml", ".yml":
		return "text/yaml"
	case ".py":
		return "text/x-python"
	case ".go":
		return "text/x-go"
	default:
		return "application/octet-stream"
	}
}
