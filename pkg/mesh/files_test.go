// pkg/mesh/files_test.go
package mesh_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/richinsley/jb-mesh/pkg/filestore"
	"github.com/richinsley/jb-mesh/pkg/mesh"
)

// startEmbeddedNATS starts an in-memory NATS server for testing.
func startEmbeddedNATS(t *testing.T) (*natsserver.Server, *nats.Conn) {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random port
		NoLog:     true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("start nats: %v", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
	})
	return ns, nc
}

func setupFileHandlers(t *testing.T) (*nats.Conn, *filestore.Store) {
	t.Helper()
	ns, nc := startEmbeddedNATS(t)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	store, err := filestore.NewStore(js, filestore.Config{
		BucketName: "test-files",
		DefaultTTL: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("filestore: %v", err)
	}

	m, err := mesh.New(mesh.Config{
		NATSUrl:  ns.ClientURL(),
		NodeName: "test-node",
	})
	if err != nil {
		t.Fatalf("mesh: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	if err := m.SubscribeFileHandlers(store); err != nil {
		t.Fatalf("subscribe file handlers: %v", err)
	}

	// Allow subscriptions to propagate across connections
	nc.Flush()
	time.Sleep(50 * time.Millisecond)

	return nc, store
}

func natsRequest(t *testing.T, nc *nats.Conn, subject string, req interface{}) []byte {
	t.Helper()
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg, err := nc.Request(subject, data, 5*time.Second)
	if err != nil {
		t.Fatalf("request %s: %v", subject, err)
	}
	return msg.Data
}

func TestFilePut(t *testing.T) {
	nc, _ := setupFileHandlers(t)

	content := []byte("hello world")
	req := mesh.FilePutRequest{
		Key:         "test/hello.txt",
		Data:        base64.StdEncoding.EncodeToString(content),
		ContentType: "text/plain",
	}

	resp := natsRequest(t, nc, "files.put", req)

	var result mesh.FilePutResult
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !result.OK {
		t.Fatalf("put failed: %s", result.Error)
	}
	if result.Key != "test/hello.txt" {
		t.Errorf("key = %q, want %q", result.Key, "test/hello.txt")
	}
	if result.Size != int64(len(content)) {
		t.Errorf("size = %d, want %d", result.Size, len(content))
	}
	if result.ContentType != "text/plain" {
		t.Errorf("content_type = %q, want %q", result.ContentType, "text/plain")
	}
	if result.ETag == "" {
		t.Error("etag is empty")
	}
}

func TestFileGet(t *testing.T) {
	nc, _ := setupFileHandlers(t)

	// Put first
	content := []byte("retrieve me")
	putReq := mesh.FilePutRequest{
		Key:         "test/get.txt",
		Data:        base64.StdEncoding.EncodeToString(content),
		ContentType: "text/plain",
	}
	natsRequest(t, nc, "files.put", putReq)

	// Get
	getReq := mesh.FileGetRequest{Key: "test/get.txt"}
	resp := natsRequest(t, nc, "files.get", getReq)

	var result mesh.FileGetResult
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !result.OK {
		t.Fatalf("get failed: %s", result.Error)
	}

	decoded, err := base64.StdEncoding.DecodeString(result.Data)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	if string(decoded) != "retrieve me" {
		t.Errorf("data = %q, want %q", string(decoded), "retrieve me")
	}
	if result.ContentType != "text/plain" {
		t.Errorf("content_type = %q, want %q", result.ContentType, "text/plain")
	}
	if result.Size != int64(len(content)) {
		t.Errorf("size = %d, want %d", result.Size, len(content))
	}
}

func TestFileGetNotFound(t *testing.T) {
	nc, _ := setupFileHandlers(t)

	getReq := mesh.FileGetRequest{Key: "nonexistent/file.txt"}
	resp := natsRequest(t, nc, "files.get", getReq)

	var result mesh.FileGetResult
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if result.OK {
		t.Error("expected failure for nonexistent key")
	}
	if result.Error == "" {
		t.Error("expected error message")
	}
}

func TestFileHead(t *testing.T) {
	nc, _ := setupFileHandlers(t)

	// Put first
	content := []byte("head test data")
	putReq := mesh.FilePutRequest{
		Key:         "test/head.txt",
		Data:        base64.StdEncoding.EncodeToString(content),
		ContentType: "application/octet-stream",
	}
	natsRequest(t, nc, "files.put", putReq)

	// Head
	headReq := mesh.FileHeadRequest{Key: "test/head.txt"}
	resp := natsRequest(t, nc, "files.head", headReq)

	var result mesh.FileHeadResult
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !result.OK {
		t.Fatalf("head failed: %s", result.Error)
	}
	if result.Size != int64(len(content)) {
		t.Errorf("size = %d, want %d", result.Size, len(content))
	}
	if result.ContentType != "application/octet-stream" {
		t.Errorf("content_type = %q, want %q", result.ContentType, "application/octet-stream")
	}
	if result.ETag == "" {
		t.Error("etag is empty")
	}
	if result.Created == "" {
		t.Error("created is empty")
	}
}

func TestFileDelete(t *testing.T) {
	nc, _ := setupFileHandlers(t)

	// Put first
	putReq := mesh.FilePutRequest{
		Key:         "test/delete-me.txt",
		Data:        base64.StdEncoding.EncodeToString([]byte("delete me")),
		ContentType: "text/plain",
	}
	natsRequest(t, nc, "files.put", putReq)

	// Delete
	delReq := mesh.FileDeleteRequest{Key: "test/delete-me.txt"}
	resp := natsRequest(t, nc, "files.delete", delReq)

	var result mesh.FileDeleteResult
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !result.OK {
		t.Fatalf("delete failed: %s", result.Error)
	}

	// Verify deleted — get should fail
	getReq := mesh.FileGetRequest{Key: "test/delete-me.txt"}
	getResp := natsRequest(t, nc, "files.get", getReq)

	var getResult mesh.FileGetResult
	if err := json.Unmarshal(getResp, &getResult); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if getResult.OK {
		t.Error("expected get to fail after delete")
	}
}

func TestFileList(t *testing.T) {
	nc, _ := setupFileHandlers(t)

	// Put several files
	files := []struct{ key, ct string }{
		{"images/a.png", "image/png"},
		{"images/b.jpg", "image/jpeg"},
		{"audio/c.wav", "audio/wav"},
	}
	for _, f := range files {
		putReq := mesh.FilePutRequest{
			Key:         f.key,
			Data:        base64.StdEncoding.EncodeToString([]byte("data-" + f.key)),
			ContentType: f.ct,
		}
		natsRequest(t, nc, "files.put", putReq)
	}

	// List all
	listReq := mesh.FileListRequest{}
	resp := natsRequest(t, nc, "files.list", listReq)

	var result mesh.FileListResult
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !result.OK {
		t.Fatalf("list failed: %s", result.Error)
	}
	if len(result.Files) != 3 {
		t.Errorf("got %d files, want 3", len(result.Files))
	}

	// List with prefix
	listReq = mesh.FileListRequest{Prefix: "images/"}
	resp = natsRequest(t, nc, "files.list", listReq)

	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !result.OK {
		t.Fatalf("list failed: %s", result.Error)
	}
	if len(result.Files) != 2 {
		t.Errorf("got %d files with prefix images/, want 2", len(result.Files))
	}
}

func TestFilePutEmptyKey(t *testing.T) {
	nc, _ := setupFileHandlers(t)

	req := mesh.FilePutRequest{
		Key:         "",
		Data:        base64.StdEncoding.EncodeToString([]byte("data")),
		ContentType: "text/plain",
	}

	resp := natsRequest(t, nc, "files.put", req)

	var result mesh.FilePutResult
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if result.OK {
		t.Error("expected failure for empty key")
	}
}

func TestFilePutInvalidBase64(t *testing.T) {
	nc, _ := setupFileHandlers(t)

	req := mesh.FilePutRequest{
		Key:         "test/bad.txt",
		Data:        "not-valid-base64!!!",
		ContentType: "text/plain",
	}

	resp := natsRequest(t, nc, "files.put", req)

	var result mesh.FilePutResult
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if result.OK {
		t.Error("expected failure for invalid base64")
	}
}

func TestFileListEmpty(t *testing.T) {
	nc, _ := setupFileHandlers(t)

	listReq := mesh.FileListRequest{Prefix: "nothing/"}
	resp := natsRequest(t, nc, "files.list", listReq)

	var result mesh.FileListResult
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !result.OK {
		t.Fatalf("list failed: %s", result.Error)
	}
	if len(result.Files) != 0 {
		t.Errorf("got %d files, want 0", len(result.Files))
	}
}

func TestFilePutGetBinary(t *testing.T) {
	nc, _ := setupFileHandlers(t)

	// Create binary data (not valid UTF-8)
	content := make([]byte, 256)
	for i := range content {
		content[i] = byte(i)
	}

	putReq := mesh.FilePutRequest{
		Key:         "test/binary.bin",
		Data:        base64.StdEncoding.EncodeToString(content),
		ContentType: "application/octet-stream",
	}
	natsRequest(t, nc, "files.put", putReq)

	// Get and verify round-trip
	getReq := mesh.FileGetRequest{Key: "test/binary.bin"}
	resp := natsRequest(t, nc, "files.get", getReq)

	var result mesh.FileGetResult
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !result.OK {
		t.Fatalf("get failed: %s", result.Error)
	}

	decoded, err := base64.StdEncoding.DecodeString(result.Data)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	if len(decoded) != len(content) {
		t.Fatalf("got %d bytes, want %d", len(decoded), len(content))
	}
	for i := range content {
		if decoded[i] != content[i] {
			t.Errorf("byte %d: got %d, want %d", i, decoded[i], content[i])
			break
		}
	}
}
