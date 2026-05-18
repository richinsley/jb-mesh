package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/richinsley/jb-mesh/pkg/logstore"
	"github.com/richinsley/jb-mesh/pkg/mesh"
	"github.com/spf13/cobra"
)

func logsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "logs", Short: "Query logstore health and records"}
	cmd.AddCommand(logsHealthCmd())
	cmd.AddCommand(logsTailCmd())
	cmd.AddCommand(logsQueryCmd())
	cmd.AddCommand(logsStatsCmd())
	return cmd
}

func logsHealthCmd() *cobra.Command {
	var asJSON bool
	return &cobra.Command{
		Use:   "health",
		Short: "Show logstore server health",
		RunE: func(cmd *cobra.Command, args []string) error {
			var resp logstore.Stats
			if err := requestLogstoreViaMesh("logstore.health", nil, &resp); err != nil {
				return err
			}
			if asJSON {
				return printJSON(resp)
			}
			fmt.Printf("ok: %v\nrole: %s\nstorage: %s\nsubjects: %s\nrecords: %d\nbytes: %d\nlast_write: %s\nlast_error: %s\nbackend: %s (ok=%v)\n",
				resp.OK, resp.Role, resp.StorageDir, strings.Join(resp.Subjects, ", "), resp.RecordsWritten, resp.BytesWritten, emptyDash(resp.LastWriteTS), emptyDash(resp.LastError), resp.Backend.Kind, resp.Backend.OK)
			return nil
		},
	}
}

func logsTailCmd() *cobra.Command {
	var req logstore.QueryRequest
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Tail recent log records",
		RunE: func(cmd *cobra.Command, args []string) error {
			var resp logstore.QueryResponse
			if err := requestLogstoreViaMesh("logstore.tail", req, &resp); err != nil {
				return err
			}
			if asJSON {
				return printJSON(resp)
			}
			printQueryResponse(resp)
			return nil
		},
	}
	bindQueryFlags(cmd, &req, &asJSON)
	return cmd
}

func logsQueryCmd() *cobra.Command {
	var req logstore.QueryRequest
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query log records",
		RunE: func(cmd *cobra.Command, args []string) error {
			var resp logstore.QueryResponse
			if err := requestLogstoreViaMesh("logstore.query", req, &resp); err != nil {
				return err
			}
			if asJSON {
				return printJSON(resp)
			}
			printQueryResponse(resp)
			return nil
		},
	}
	bindQueryFlags(cmd, &req, &asJSON)
	return cmd
}

func logsStatsCmd() *cobra.Command {
	var since, until string
	var groupBy string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Aggregate logstore stats",
		RunE: func(cmd *cobra.Command, args []string) error {
			req := logstore.StatsRequest{Since: since, Until: until, GroupBy: splitCSV(groupBy)}
			var resp logstore.StatsResponse
			if err := requestLogstoreViaMesh("logstore.stats", req, &resp); err != nil {
				return err
			}
			if asJSON {
				return printJSON(resp)
			}
			printStatsResponse(resp, req.GroupBy)
			return nil
		},
	}
	cmd.Flags().StringVar(&since, "since", "24h", "Relative duration or RFC3339/day timestamp")
	cmd.Flags().StringVar(&until, "until", "", "Optional end time (duration or RFC3339/day timestamp)")
	cmd.Flags().StringVar(&groupBy, "group-by", "node,kind", "Comma-separated grouping fields")
	cmd.Flags().BoolVar(&asJSON, "json", false, "Print raw JSON response")
	return cmd
}

func bindQueryFlags(cmd *cobra.Command, req *logstore.QueryRequest, asJSON *bool) {
	cmd.Flags().StringVar(&req.Since, "since", "1h", "Relative duration or RFC3339/day timestamp")
	cmd.Flags().StringVar(&req.Until, "until", "", "Optional end time (duration or RFC3339/day timestamp)")
	cmd.Flags().StringVar(&req.Node, "node", "", "Filter by node")
	cmd.Flags().StringVar(&req.Tool, "tool", "", "Filter by tool")
	cmd.Flags().StringVar(&req.Method, "method", "", "Filter by method")
	cmd.Flags().StringVar(&req.Kind, "kind", "", "Filter by kind")
	cmd.Flags().StringVar(&req.Level, "level", "", "Filter by level")
	cmd.Flags().StringVar(&req.Corr, "corr", "", "Filter by correlation id")
	cmd.Flags().IntVar(&req.Limit, "limit", 50, "Maximum records to return")
	cmd.Flags().BoolVar(asJSON, "json", false, "Print raw JSON response")
}

func requestLogstoreViaMesh(subject string, req any, out any) error {
	meshCfg, err := currentMeshConfig()
	if err != nil {
		return err
	}
	nc, err := mesh.Connect(meshCfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer nc.Close()
	var payload []byte
	if req != nil {
		payload, err = json.Marshal(req)
		if err != nil {
			return err
		}
	}
	msg, err := nc.Request(subject, payload, 10*time.Second)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(msg.Data, out); err != nil {
		return err
	}
	return nil
}

func printQueryResponse(resp logstore.QueryResponse) {
	if !resp.OK {
		fmt.Printf("error: %s\n", resp.Error)
		return
	}
	if len(resp.Records) == 0 {
		fmt.Println("No records found.")
		printQueryLimits(resp)
		return
	}
	for _, rec := range resp.Records {
		fmt.Printf("%s  %-5s %-10s node=%-12s", formatTS(rec.TS), rec.Level, rec.Kind, rec.Node)
		if rec.Tool != "" {
			fmt.Printf(" tool=%s", rec.Tool)
		}
		if rec.Method != "" {
			fmt.Printf(" method=%s", rec.Method)
		}
		if rec.Corr != "" {
			fmt.Printf(" corr=%s", rec.Corr)
		}
		fmt.Printf("  %s\n", rec.Message)
	}
	printQueryLimits(resp)
}

func printQueryLimits(resp logstore.QueryResponse) {
	fmt.Printf("limits: limit=%d max=%d window=%s", resp.Limits.Limit, resp.Limits.MaxQueryLimit, emptyDash(resp.Limits.MaxQueryWindow))
	if resp.Truncated {
		fmt.Printf(" truncated=true")
	}
	fmt.Println()
}

func printStatsResponse(resp logstore.StatsResponse, groupBy []string) {
	if !resp.OK {
		fmt.Printf("error: %s\n", resp.Error)
		return
	}
	if len(groupBy) == 0 {
		groupBy = []string{"node", "kind"}
	}
	if len(resp.Groups) == 0 {
		fmt.Println("No records found.")
		fmt.Printf("limits: max=%d window=%s\n", resp.Limits.MaxQueryLimit, emptyDash(resp.Limits.MaxQueryWindow))
		return
	}
	sort.Slice(resp.Groups, func(i, j int) bool { return resp.Groups[i].Records > resp.Groups[j].Records })
	for _, g := range resp.Groups {
		parts := make([]string, 0, len(groupBy))
		for _, field := range groupBy {
			switch strings.TrimSpace(field) {
			case "node":
				parts = append(parts, "node="+emptyDash(g.Node))
			case "kind":
				parts = append(parts, "kind="+emptyDash(g.Kind))
			case "tool":
				parts = append(parts, "tool="+emptyDash(g.Tool))
			case "method":
				parts = append(parts, "method="+emptyDash(g.Method))
			case "level":
				parts = append(parts, "level="+emptyDash(g.Level))
			}
		}
		fmt.Printf("%s  records=%d errors=%d\n", strings.Join(parts, " "), g.Records, g.Errors)
	}
	fmt.Printf("limits: max=%d window=%s\n", resp.Limits.MaxQueryLimit, emptyDash(resp.Limits.MaxQueryWindow))
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func printJSON(v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func formatTS(raw string) string {
	if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return ts.Format("2006-01-02 15:04:05")
	}
	return raw
}

func emptyDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}
