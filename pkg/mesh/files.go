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

	log.Printf("[mesh] file store handlers registered (5 subjects)")
	return nil
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
