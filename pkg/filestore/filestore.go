// Package filestore provides S3-compatible file storage over NATS Object Store.
// Files are ephemeral by default (1h TTL) with persistent opt-in.
//
// NATS subject namespace (DESIGN.md §2.3):
//
//	files.put      — store a file
//	files.get      — retrieve a file
//	files.head     — file metadata
//	files.delete   — remove a file
//	files.list     — list files
package filestore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// FileMeta contains metadata about a stored file.
type FileMeta struct {
	Key         string    `json:"key"`
	Size        int64     `json:"size"`
	ContentType string    `json:"content_type"`
	ETag        string    `json:"etag"` // SHA-256 of content
	Created     time.Time `json:"created"`
}

// Store provides file storage using NATS Object Store.
type Store struct {
	js         nats.JetStreamContext
	bucket     nats.ObjectStore
	bucketName string
	defaultTTL time.Duration
}

// Config controls file store behavior.
type Config struct {
	BucketName string        // default: "mesh-files"
	DefaultTTL time.Duration // default TTL for files, default: 1h. 0 = no TTL.
	MaxBytes   int64         // max total bucket size in bytes, default: 0 (unlimited)
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		BucketName: "mesh-files",
		DefaultTTL: 1 * time.Hour,
	}
}

// NewStore creates a file store backed by a NATS Object Store bucket.
func NewStore(js nats.JetStreamContext, cfg Config) (*Store, error) {
	if cfg.BucketName == "" {
		cfg.BucketName = "mesh-files"
	}

	objCfg := &nats.ObjectStoreConfig{
		Bucket:      cfg.BucketName,
		Description: "jb-mesh file store",
		Storage:     nats.FileStorage,
	}
	if cfg.DefaultTTL > 0 {
		objCfg.TTL = cfg.DefaultTTL
	}
	if cfg.MaxBytes > 0 {
		objCfg.MaxBytes = cfg.MaxBytes
	}

	bucket, err := js.CreateObjectStore(objCfg)
	if err != nil {
		return nil, fmt.Errorf("create object store %s: %w", cfg.BucketName, err)
	}

	return &Store{
		js:         js,
		bucket:     bucket,
		bucketName: cfg.BucketName,
		defaultTTL: cfg.DefaultTTL,
	}, nil
}

// Put stores a file and returns its metadata.
func (s *Store) Put(key string, data []byte, contentType string) (*FileMeta, error) {
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	// Compute ETag (SHA-256)
	hash := sha256.Sum256(data)
	etag := hex.EncodeToString(hash[:])

	// Build object meta with headers for content type and etag
	objMeta := &nats.ObjectMeta{
		Name: key,
		Headers: nats.Header{
			"Content-Type": []string{contentType},
			"ETag":         []string{etag},
		},
	}

	// Store the data
	_, err := s.bucket.Put(objMeta, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("put %s: %w", key, err)
	}

	meta := &FileMeta{
		Key:         key,
		Size:        int64(len(data)),
		ContentType: contentType,
		ETag:        etag,
		Created:     time.Now(),
	}

	log.Printf("[filestore] put %s (%d bytes, %s)", key, len(data), contentType)
	return meta, nil
}

// Get retrieves a file's contents and metadata.
func (s *Store) Get(key string) ([]byte, *FileMeta, error) {
	result, err := s.bucket.Get(key)
	if err != nil {
		return nil, nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer result.Close()

	data, err := io.ReadAll(result)
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", key, err)
	}

	info, err := result.Info()
	if err != nil {
		return data, nil, fmt.Errorf("info %s: %w", key, err)
	}

	return data, infoToMeta(info), nil
}

// Head returns metadata for a file without its contents.
func (s *Store) Head(key string) (*FileMeta, error) {
	info, err := s.bucket.GetInfo(key)
	if err != nil {
		return nil, fmt.Errorf("head %s: %w", key, err)
	}
	return infoToMeta(info), nil
}

// Delete removes a file.
func (s *Store) Delete(key string) error {
	if err := s.bucket.Delete(key); err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	log.Printf("[filestore] deleted %s", key)
	return nil
}

// List returns metadata for all files, optionally filtered by prefix.
func (s *Store) List(prefix string) ([]FileMeta, error) {
	infos, err := s.bucket.List()
	if err != nil {
		if err == nats.ErrNoObjectsFound {
			return nil, nil
		}
		return nil, fmt.Errorf("list: %w", err)
	}

	var files []FileMeta
	for _, info := range infos {
		if info == nil || info.Deleted {
			continue
		}
		if prefix != "" && !hasPrefix(info.Name, prefix) {
			continue
		}
		files = append(files, *infoToMeta(info))
	}
	return files, nil
}

// infoToMeta converts NATS ObjectInfo to our FileMeta.
func infoToMeta(info *nats.ObjectInfo) *FileMeta {
	meta := &FileMeta{
		Key:     info.Name,
		Size:    int64(info.Size),
		Created: info.ModTime,
	}

	if info.Headers != nil {
		meta.ContentType = info.Headers.Get("Content-Type")
		meta.ETag = info.Headers.Get("ETag")
	}

	return meta
}

// hasPrefix checks if s starts with prefix.
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
