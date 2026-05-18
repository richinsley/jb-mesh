package mesh

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/richinsley/jb-mesh/pkg/filestore"
)

// Known parameter names that indicate file inputs (mirrors OpenClaw plugin FILE_PARAM_NAMES)
var fileParamNames = map[string]bool{
	"audio":      true,
	"image":      true,
	"file":       true,
	"input_file": true,
	"video":      true,
	"document":   true,
	"attachment": true,
	"photo":      true,
	"path":       true,
	"filepath":   true,
	"file_path":  true,
	"input_path": true,
	"input":      true,
	"source":     true,
}

// fileGetResponse mirrors the files.get NATS response.
type fileGetResponse struct {
	OK          bool   `json:"ok"`
	Data        string `json:"data"` // base64
	ContentType string `json:"content_type"`
	Error       string `json:"error,omitempty"`
}

// ResolveFileParams checks params for file store keys and downloads them to temp files.
// A param is resolved if:
// 1. Its name is in the known file param set
// 2. Its value is a string that looks like a file store key (contains "/" but doesn't start with "/" or "~")
// 3. It doesn't look like an absolute path or URL
//
// If the local store doesn't have the file, falls back to a NATS files.get request
// (which reaches the seed node's store). This handles the case where leaf nodes
// don't have JetStream replication from the seed.
//
// Returns a cleanup function that removes temp files.
func ResolveFileParams(params map[string]interface{}, store *filestore.Store, nc ...*nats.Conn) (func(), error) {
	var tempFiles []string

	for key, value := range params {
		strVal, ok := value.(string)
		if !ok || strVal == "" {
			continue
		}

		// Only resolve known file param names
		if !fileParamNames[strings.ToLower(key)] {
			continue
		}

		// Skip if it looks like an absolute path, relative path, or URL
		if strings.HasPrefix(strVal, "/") || strings.HasPrefix(strVal, "~/") ||
			strings.HasPrefix(strVal, "./") || strings.HasPrefix(strVal, "http://") ||
			strings.HasPrefix(strVal, "https://") {
			continue
		}

		// Skip if it doesn't contain "/" (not a key-like pattern)
		if !strings.Contains(strVal, "/") {
			continue
		}

		// Looks like a file store key — try to download it
		var data []byte
		var contentType string

		localData, meta, err := store.Get(strVal)
		if err == nil {
			data = localData
			contentType = meta.ContentType
			log.Printf("[mesh] file resolve: %s=%q found in local store (%d bytes)", key, strVal, len(data))
		} else if len(nc) > 0 && nc[0] != nil {
			// Local store miss — try NATS files.get (reaches seed node)
			log.Printf("[mesh] file resolve: %s=%q not in local store, trying NATS files.get", key, strVal)
			reqData, _ := json.Marshal(map[string]string{"key": strVal})
			msg, natsErr := nc[0].Request("files.get", reqData, 30*time.Second)
			if natsErr == nil {
				var resp fileGetResponse
				if json.Unmarshal(msg.Data, &resp) == nil && resp.OK {
					decoded, decErr := base64.StdEncoding.DecodeString(resp.Data)
					if decErr == nil {
						data = decoded
						contentType = resp.ContentType
						log.Printf("[mesh] file resolve: %s=%q fetched via NATS (%d bytes)", key, strVal, len(data))
					}
				}
			}
			if data == nil {
				log.Printf("[mesh] file resolve: key %q not found locally or via NATS", strVal)
				continue
			}
		} else {
			log.Printf("[mesh] file resolve: key %q not in store: %v", strVal, err)
			continue
		}

		// Determine extension from key or content type
		ext := filepath.Ext(strVal)
		if ext == "" {
			switch {
			case strings.Contains(contentType, "ogg"):
				ext = ".ogg"
			case strings.Contains(contentType, "wav"):
				ext = ".wav"
			case strings.Contains(contentType, "mp3"):
				ext = ".mp3"
			default:
				ext = ".bin"
			}
		}

		// Write to temp file
		tmpFile, err := os.CreateTemp("", "mesh-file-*"+ext)
		if err != nil {
			return nil, fmt.Errorf("create temp file: %w", err)
		}
		if _, err := tmpFile.Write(data); err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			return nil, fmt.Errorf("write temp file: %w", err)
		}
		tmpFile.Close()

		log.Printf("[mesh] file resolve: %s=%q → %s (%d bytes)", key, strVal, tmpFile.Name(), len(data))
		params[key] = tmpFile.Name()
		tempFiles = append(tempFiles, tmpFile.Name())
	}

	cleanup := func() {
		for _, f := range tempFiles {
			os.Remove(f)
		}
	}
	return cleanup, nil
}
