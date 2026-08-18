// File store NATS request handlers.
//
// Exposes pkg/filestore.Store operations over NATS request/reply so that
// TypeScript plugins (and any other NATS client) can access the file store
// without a direct JetStream Object Store connection.
//
// Subjects:
//
//	files.put    — store a file (base64 data)
//	files.get    — retrieve a file (returns base64 data)
//	files.head   — file metadata only
//	files.delete — remove a file
//	files.list   — list files (optional prefix filter)
package mesh

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/richinsley/jb-mesh/pkg/filestore"
)

const fileToolName = "files"

// --- Request/Response types ---

// FilePutRequest is sent to files.put.
type FilePutRequest struct {
	Key         string `json:"key"`
	Data        string `json:"data"` // base64-encoded file content
	ContentType string `json:"content_type"`
}

// FilePutResult is the response from files.put.
type FilePutResult struct {
	OK          bool   `json:"ok"`
	Key         string `json:"key,omitempty"`
	Size        int64  `json:"size,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	ETag        string `json:"etag,omitempty"`
	Error       string `json:"error,omitempty"`
}

// FileGetRequest is sent to files.get.
type FileGetRequest struct {
	Key string `json:"key"`
}

// FileGetResult is the response from files.get.
type FileGetResult struct {
	OK          bool   `json:"ok"`
	Key         string `json:"key,omitempty"`
	Data        string `json:"data,omitempty"` // base64-encoded file content
	ContentType string `json:"content_type,omitempty"`
	Size        int64  `json:"size,omitempty"`
	ETag        string `json:"etag,omitempty"`
	Error       string `json:"error,omitempty"`
}

// FileHeadRequest is sent to files.head.
type FileHeadRequest struct {
	Key string `json:"key"`
}

// FileHeadResult is the response from files.head.
type FileHeadResult struct {
	OK          bool   `json:"ok"`
	Key         string `json:"key,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	Size        int64  `json:"size,omitempty"`
	ETag        string `json:"etag,omitempty"`
	Created     string `json:"created,omitempty"` // RFC3339
	Error       string `json:"error,omitempty"`
}

// FileDeleteRequest is sent to files.delete.
type FileDeleteRequest struct {
	Key string `json:"key"`
}

// FileDeleteResult is the response from files.delete.
type FileDeleteResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// FileListRequest is sent to files.list.
type FileListRequest struct {
	Prefix string `json:"prefix"`
}

// FileListResult is the response from files.list.
type FileListResult struct {
	OK    bool           `json:"ok"`
	Files []FileListItem `json:"files,omitempty"`
	Error string         `json:"error,omitempty"`
}

// FileListItem describes a single file in a list response.
type FileListItem struct {
	Key         string `json:"key"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
	ETag        string `json:"etag"`
	Created     string `json:"created"` // RFC3339
}

// SubscribeFileHandlers registers all file store request handlers on the NATS connection.
// The handlers are not node-scoped — any node with a file store can serve requests.
// Since all nodes share the same NATS Object Store bucket, it doesn't matter which
// node handles the request.
func (m *Mesh) SubscribeFileHandlers(store *filestore.Store) error {
	if err := m.subscribeFilePut(store); err != nil {
		return fmt.Errorf("subscribe files.put: %w", err)
	}
	if err := m.subscribeFileGet(store); err != nil {
		return fmt.Errorf("subscribe files.get: %w", err)
	}
	if err := m.subscribeFileHead(store); err != nil {
		return fmt.Errorf("subscribe files.head: %w", err)
	}
	if err := m.subscribeFileDelete(store); err != nil {
		return fmt.Errorf("subscribe files.delete: %w", err)
	}
	if err := m.subscribeFileList(store); err != nil {
		return fmt.Errorf("subscribe files.list: %w", err)
	}

	m.logf("[mesh] file store handlers registered (5 subjects)")
	return nil
}

// RegisterFileTool exposes the built-in file store through normal mesh tool
// discovery while preserving the legacy raw files.* request/reply subjects.
func (m *Mesh) RegisterFileTool(store *filestore.Store) error {
	return m.RegisterTool(
		fileToolName,
		"1.0.0",
		"Built-in ephemeral mesh file store for exchanging larger blobs by key",
		[]string{"put", "get", "head", "delete", "list"},
		fileToolHandler(store),
		FileToolMethodSchemas(),
	)
}

// FileToolMethodSchemas returns JSON-schema-like metadata for the discoverable
// built-in files tool.
func FileToolMethodSchemas() map[string]MethodSchema {
	keyProp := map[string]interface{}{
		"type":        "string",
		"description": "File store object key.",
	}
	return map[string]MethodSchema{
		"put": {
			Type:        "object",
			Description: "Store a base64-encoded blob in the mesh file store.",
			Required:    []string{"key", "data"},
			Properties: map[string]interface{}{
				"key": keyProp,
				"data": map[string]interface{}{
					"type":        "string",
					"description": "Base64-encoded file content.",
				},
				"content_type": map[string]interface{}{
					"type":        "string",
					"description": "Optional MIME content type. Defaults to application/octet-stream.",
				},
			},
		},
		"get": {
			Type:        "object",
			Description: "Retrieve a blob from the mesh file store by key. Data is returned base64-encoded.",
			Required:    []string{"key"},
			Properties: map[string]interface{}{
				"key": keyProp,
			},
		},
		"head": {
			Type:        "object",
			Description: "Retrieve metadata for a blob without returning its content.",
			Required:    []string{"key"},
			Properties: map[string]interface{}{
				"key": keyProp,
			},
		},
		"delete": {
			Type:        "object",
			Description: "Delete a blob from the mesh file store.",
			Required:    []string{"key"},
			Properties: map[string]interface{}{
				"key": keyProp,
			},
		},
		"list": {
			Type:        "object",
			Description: "List blobs in the mesh file store, optionally filtered by key prefix.",
			Properties: map[string]interface{}{
				"prefix": map[string]interface{}{
					"type":        "string",
					"description": "Optional key prefix filter.",
				},
			},
		},
	}
}

// FileToolSchema returns the method schemas in the same shape as
// node.<node>.tools.<tool>.schema responses.
func FileToolSchema() map[string]interface{} {
	schemas := FileToolMethodSchemas()
	out := make(map[string]interface{}, len(schemas))
	for name, schema := range schemas {
		data, err := json.Marshal(schema)
		if err != nil {
			continue
		}
		var raw map[string]interface{}
		if err := json.Unmarshal(data, &raw); err != nil {
			continue
		}
		out[name] = raw
	}
	return out
}

func fileToolHandler(store *filestore.Store) ToolHandler {
	return func(req CallRequest, method string, params map[string]interface{}) (interface{}, error) {
		switch method {
		case "put":
			return fileToolPut(store, params)
		case "get":
			return fileToolGet(store, params)
		case "head":
			return fileToolHead(store, params)
		case "delete":
			return fileToolDelete(store, params)
		case "list":
			return fileToolList(store, params)
		default:
			return nil, fmt.Errorf("unknown files method %q", method)
		}
	}
}

func fileToolPut(store *filestore.Store, params map[string]interface{}) (FilePutResult, error) {
	key, err := stringParam(params, "key", true)
	if err != nil {
		return FilePutResult{}, err
	}
	encoded, err := stringParam(params, "data", true)
	if err != nil {
		return FilePutResult{}, err
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return FilePutResult{}, fmt.Errorf("invalid base64 data: %w", err)
	}
	ct, err := stringParam(params, "content_type", false)
	if err != nil {
		return FilePutResult{}, err
	}
	if ct == "" {
		ct = "application/octet-stream"
	}
	meta, err := store.Put(key, data, ct)
	if err != nil {
		return FilePutResult{}, err
	}
	return FilePutResult{
		OK:          true,
		Key:         meta.Key,
		Size:        meta.Size,
		ContentType: meta.ContentType,
		ETag:        meta.ETag,
	}, nil
}

func fileToolGet(store *filestore.Store, params map[string]interface{}) (FileGetResult, error) {
	key, err := stringParam(params, "key", true)
	if err != nil {
		return FileGetResult{}, err
	}
	data, meta, err := store.Get(key)
	if err != nil {
		return FileGetResult{}, err
	}
	return FileGetResult{
		OK:          true,
		Key:         meta.Key,
		Data:        base64.StdEncoding.EncodeToString(data),
		ContentType: meta.ContentType,
		Size:        meta.Size,
		ETag:        meta.ETag,
	}, nil
}

func fileToolHead(store *filestore.Store, params map[string]interface{}) (FileHeadResult, error) {
	key, err := stringParam(params, "key", true)
	if err != nil {
		return FileHeadResult{}, err
	}
	meta, err := store.Head(key)
	if err != nil {
		return FileHeadResult{}, err
	}
	return FileHeadResult{
		OK:          true,
		Key:         meta.Key,
		ContentType: meta.ContentType,
		Size:        meta.Size,
		ETag:        meta.ETag,
		Created:     meta.Created.Format("2006-01-02T15:04:05Z07:00"),
	}, nil
}

func fileToolDelete(store *filestore.Store, params map[string]interface{}) (FileDeleteResult, error) {
	key, err := stringParam(params, "key", true)
	if err != nil {
		return FileDeleteResult{}, err
	}
	if err := store.Delete(key); err != nil {
		return FileDeleteResult{}, err
	}
	return FileDeleteResult{OK: true}, nil
}

func fileToolList(store *filestore.Store, params map[string]interface{}) (FileListResult, error) {
	prefix, err := stringParam(params, "prefix", false)
	if err != nil {
		return FileListResult{}, err
	}
	files, err := store.List(prefix)
	if err != nil {
		return FileListResult{}, err
	}
	items := make([]FileListItem, len(files))
	for i, f := range files {
		items[i] = FileListItem{
			Key:         f.Key,
			Size:        f.Size,
			ContentType: f.ContentType,
			ETag:        f.ETag,
			Created:     f.Created.Format("2006-01-02T15:04:05Z07:00"),
		}
	}
	return FileListResult{OK: true, Files: items}, nil
}

func stringParam(params map[string]interface{}, key string, required bool) (string, error) {
	value, ok := params[key]
	if !ok || value == nil {
		if required {
			return "", fmt.Errorf("%s is required", key)
		}
		return "", nil
	}
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	if required && s == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return s, nil
}

func (m *Mesh) subscribeFilePut(store *filestore.Store) error {
	_, err := m.nc.Subscribe("files.put", func(msg *nats.Msg) {
		var req FilePutRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			respondJSON(msg, FilePutResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err)})
			return
		}

		if req.Key == "" {
			respondJSON(msg, FilePutResult{OK: false, Error: "key is required"})
			return
		}

		data, err := base64.StdEncoding.DecodeString(req.Data)
		if err != nil {
			respondJSON(msg, FilePutResult{OK: false, Error: fmt.Sprintf("invalid base64 data: %v", err)})
			return
		}

		ct := req.ContentType
		if ct == "" {
			ct = "application/octet-stream"
		}

		meta, err := store.Put(req.Key, data, ct)
		if err != nil {
			respondJSON(msg, FilePutResult{OK: false, Error: fmt.Sprintf("put failed: %v", err)})
			return
		}

		respondJSON(msg, FilePutResult{
			OK:          true,
			Key:         meta.Key,
			Size:        meta.Size,
			ContentType: meta.ContentType,
			ETag:        meta.ETag,
		})
	})
	return err
}

func (m *Mesh) subscribeFileGet(store *filestore.Store) error {
	_, err := m.nc.Subscribe("files.get", func(msg *nats.Msg) {
		var req FileGetRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			respondJSON(msg, FileGetResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err)})
			return
		}

		if req.Key == "" {
			respondJSON(msg, FileGetResult{OK: false, Error: "key is required"})
			return
		}

		data, meta, err := store.Get(req.Key)
		if err != nil {
			respondJSON(msg, FileGetResult{OK: false, Error: fmt.Sprintf("get failed: %v", err)})
			return
		}

		respondJSON(msg, FileGetResult{
			OK:          true,
			Key:         meta.Key,
			Data:        base64.StdEncoding.EncodeToString(data),
			ContentType: meta.ContentType,
			Size:        meta.Size,
			ETag:        meta.ETag,
		})
	})
	return err
}

func (m *Mesh) subscribeFileHead(store *filestore.Store) error {
	_, err := m.nc.Subscribe("files.head", func(msg *nats.Msg) {
		var req FileHeadRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			respondJSON(msg, FileHeadResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err)})
			return
		}

		if req.Key == "" {
			respondJSON(msg, FileHeadResult{OK: false, Error: "key is required"})
			return
		}

		meta, err := store.Head(req.Key)
		if err != nil {
			respondJSON(msg, FileHeadResult{OK: false, Error: fmt.Sprintf("head failed: %v", err)})
			return
		}

		respondJSON(msg, FileHeadResult{
			OK:          true,
			Key:         meta.Key,
			ContentType: meta.ContentType,
			Size:        meta.Size,
			ETag:        meta.ETag,
			Created:     meta.Created.Format("2006-01-02T15:04:05Z07:00"),
		})
	})
	return err
}

func (m *Mesh) subscribeFileDelete(store *filestore.Store) error {
	_, err := m.nc.Subscribe("files.delete", func(msg *nats.Msg) {
		var req FileDeleteRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			respondJSON(msg, FileDeleteResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err)})
			return
		}

		if req.Key == "" {
			respondJSON(msg, FileDeleteResult{OK: false, Error: "key is required"})
			return
		}

		if err := store.Delete(req.Key); err != nil {
			respondJSON(msg, FileDeleteResult{OK: false, Error: fmt.Sprintf("delete failed: %v", err)})
			return
		}

		respondJSON(msg, FileDeleteResult{OK: true})
	})
	return err
}

func (m *Mesh) subscribeFileList(store *filestore.Store) error {
	_, err := m.nc.Subscribe("files.list", func(msg *nats.Msg) {
		var req FileListRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			respondJSON(msg, FileListResult{OK: false, Error: fmt.Sprintf("invalid request: %v", err)})
			return
		}

		files, err := store.List(req.Prefix)
		if err != nil {
			respondJSON(msg, FileListResult{OK: false, Error: fmt.Sprintf("list failed: %v", err)})
			return
		}

		items := make([]FileListItem, len(files))
		for i, f := range files {
			items[i] = FileListItem{
				Key:         f.Key,
				Size:        f.Size,
				ContentType: f.ContentType,
				ETag:        f.ETag,
				Created:     f.Created.Format("2006-01-02T15:04:05Z07:00"),
			}
		}

		respondJSON(msg, FileListResult{OK: true, Files: items})
	})
	return err
}

// respondJSON marshals v to JSON and sends it as a NATS reply.
func respondJSON(msg *nats.Msg, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("[mesh] failed to marshal response: %v", err)
		return
	}
	if err := msg.Respond(data); err != nil {
		log.Printf("[mesh] failed to respond: %v", err)
	}
}
