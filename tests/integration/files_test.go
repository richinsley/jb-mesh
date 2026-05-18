// Package integration provides end-to-end tests for file handling (P4-007f).
//
// These tests require a running jb-mesh node with sysinfo installed.
// They connect to NATS as a client and exercise the full stack:
// Go NATS handlers → jumpboot → Python tool → attach_file → Object Store → NATS get
//
// Skip with: JB_MESH_SKIP_INTEGRATION=1 or when NATS is unreachable.
package integration

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultNATSURL = "nats://localhost:4222"
	timeout        = 15 * time.Second
)

// --- NATS request/response types (mirror pkg/mesh) ---

type CallRequest struct {
	Params map[string]interface{} `json:"params"`
}

type CallResult struct {
	OK     bool                   `json:"ok"`
	Result map[string]interface{} `json:"result,omitempty"`
	Error  string                 `json:"error,omitempty"`
	Node   string                 `json:"node,omitempty"`
}

type FilePutRequest struct {
	Key         string `json:"key"`
	Data        string `json:"data"`
	ContentType string `json:"content_type"`
}

type FilePutResult struct {
	OK          bool   `json:"ok"`
	Key         string `json:"key,omitempty"`
	Size        int64  `json:"size,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	ETag        string `json:"etag,omitempty"`
	Error       string `json:"error,omitempty"`
}

type FileGetRequest struct {
	Key string `json:"key"`
}

type FileGetResult struct {
	OK          bool   `json:"ok"`
	Key         string `json:"key,omitempty"`
	Data        string `json:"data,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	Size        int64  `json:"size,omitempty"`
	ETag        string `json:"etag,omitempty"`
	Error       string `json:"error,omitempty"`
}

type FileListRequest struct {
	Prefix string `json:"prefix,omitempty"`
}

type FileListResult struct {
	OK    bool `json:"ok"`
	Files []struct {
		Key         string `json:"key"`
		Size        int64  `json:"size"`
		ContentType string `json:"content_type"`
		ETag        string `json:"etag"`
		Created     string `json:"created"`
	} `json:"files,omitempty"`
	Error string `json:"error,omitempty"`
}

type FileDeleteRequest struct {
	Key string `json:"key"`
}

type FileDeleteResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// --- Test helpers ---

func getNATSURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}
	return defaultNATSURL
}

func skipIfNoNATS(t *testing.T) {
	t.Helper()
	if os.Getenv("JB_MESH_SKIP_INTEGRATION") == "1" {
		t.Skip("skipping integration test (JB_MESH_SKIP_INTEGRATION=1)")
	}
	nc, err := nats.Connect(getNATSURL(), nats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("skipping: NATS not available at %s: %v", getNATSURL(), err)
	}
	nc.Close()
}

func connect(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(getNATSURL(), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

func natsRequest[Req any, Resp any](t *testing.T, nc *nats.Conn, subject string, req Req) Resp {
	t.Helper()
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	msg, err := nc.Request(subject, data, timeout)
	if err != nil {
		t.Fatalf("NATS request to %s: %v", subject, err)
	}
	var resp Resp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("unmarshal response from %s: %v (body: %s)", subject, err, string(msg.Data))
	}
	return resp
}

// --- Test 1: Direct File Store CRUD ---

func TestFileStoreCRUD(t *testing.T) {
	skipIfNoNATS(t)
	nc := connect(t)

	testKey := fmt.Sprintf("test/integration-%d.txt", time.Now().UnixNano())
	testData := []byte("hello from integration test")
	testB64 := base64.StdEncoding.EncodeToString(testData)

	// PUT
	putResult := natsRequest[FilePutRequest, FilePutResult](t, nc, "files.put", FilePutRequest{
		Key:         testKey,
		Data:        testB64,
		ContentType: "text/plain",
	})
	if !putResult.OK {
		t.Fatalf("put failed: %s", putResult.Error)
	}
	if putResult.Size != int64(len(testData)) {
		t.Errorf("put size: got %d, want %d", putResult.Size, len(testData))
	}

	// LIST — verify it appears
	listResult := natsRequest[FileListRequest, FileListResult](t, nc, "files.list", FileListRequest{
		Prefix: "test/integration-",
	})
	if !listResult.OK {
		t.Fatalf("list failed: %s", listResult.Error)
	}
	found := false
	for _, f := range listResult.Files {
		if f.Key == testKey {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("file %s not found in listing", testKey)
	}

	// GET — verify content
	getResult := natsRequest[FileGetRequest, FileGetResult](t, nc, "files.get", FileGetRequest{
		Key: testKey,
	})
	if !getResult.OK {
		t.Fatalf("get failed: %s", getResult.Error)
	}
	gotData, err := base64.StdEncoding.DecodeString(getResult.Data)
	if err != nil {
		t.Fatalf("decode base64: %v", err)
	}
	if string(gotData) != string(testData) {
		t.Errorf("get data: got %q, want %q", string(gotData), string(testData))
	}

	// DELETE
	deleteResult := natsRequest[FileDeleteRequest, FileDeleteResult](t, nc, "files.delete", FileDeleteRequest{
		Key: testKey,
	})
	if !deleteResult.OK {
		t.Fatalf("delete failed: %s", deleteResult.Error)
	}

	// LIST — verify it's gone (small delay for Object Store propagation)
	time.Sleep(100 * time.Millisecond)
	listResult2 := natsRequest[FileListRequest, FileListResult](t, nc, "files.list", FileListRequest{
		Prefix: "test/integration-",
	})
	if !listResult2.OK {
		t.Fatalf("list after delete failed: %s", listResult2.Error)
	}
	for _, f := range listResult2.Files {
		if f.Key == testKey {
			t.Errorf("file %s still in listing after delete", testKey)
		}
	}
}

// --- Test 2: Tool-Produced File via attach_file (Python → NATS → Go) ---

func TestToolProducesImage(t *testing.T) {
	skipIfNoNATS(t)
	nc := connect(t)

	// Call sysinfo.test_image_output — Python generates PNG, calls attach_file
	result := natsRequest[CallRequest, CallResult](t, nc, "tools.sysinfo.test_image_output", CallRequest{
		Params: map[string]interface{}{},
	})
	if !result.OK {
		t.Fatalf("tool call failed: %s", result.Error)
	}

	// Result should have __mesh_files__
	meshFiles, ok := result.Result["__mesh_files__"]
	if !ok {
		t.Fatal("result missing __mesh_files__")
	}

	files, ok := meshFiles.([]interface{})
	if !ok || len(files) == 0 {
		t.Fatalf("__mesh_files__ not an array or empty: %T", meshFiles)
	}

	fileRef, ok := files[0].(map[string]interface{})
	if !ok {
		t.Fatalf("file ref not an object: %T", files[0])
	}

	fileKey, _ := fileRef["key"].(string)
	contentType, _ := fileRef["content_type"].(string)
	if fileKey == "" {
		t.Fatal("file ref has no key")
	}
	if contentType != "image/png" {
		t.Errorf("content_type: got %q, want image/png", contentType)
	}

	// Verify we can fetch the file from the store
	getResult := natsRequest[FileGetRequest, FileGetResult](t, nc, "files.get", FileGetRequest{
		Key: fileKey,
	})
	if !getResult.OK {
		t.Fatalf("get produced file failed: %s", getResult.Error)
	}

	// Decode and verify it's a valid PNG (starts with PNG magic bytes)
	pngData, err := base64.StdEncoding.DecodeString(getResult.Data)
	if err != nil {
		t.Fatalf("decode base64: %v", err)
	}
	if len(pngData) < 4 || pngData[0] != 0x89 || pngData[1] != 'P' || pngData[2] != 'N' || pngData[3] != 'G' {
		t.Errorf("not a valid PNG: first 4 bytes = %x", pngData[:4])
	}

	// Clean up
	natsRequest[FileDeleteRequest, FileDeleteResult](t, nc, "files.delete", FileDeleteRequest{Key: fileKey})
}

// --- Test 3: Tool-Produced Text File ---

func TestToolProducesTextFile(t *testing.T) {
	skipIfNoNATS(t)
	nc := connect(t)

	result := natsRequest[CallRequest, CallResult](t, nc, "tools.sysinfo.test_file_output", CallRequest{
		Params: map[string]interface{}{"message": "integration test"},
	})
	if !result.OK {
		t.Fatalf("tool call failed: %s", result.Error)
	}

	meshFiles, ok := result.Result["__mesh_files__"]
	if !ok {
		t.Fatal("result missing __mesh_files__")
	}

	files, ok := meshFiles.([]interface{})
	if !ok || len(files) == 0 {
		t.Fatal("__mesh_files__ empty")
	}

	fileRef := files[0].(map[string]interface{})
	fileKey := fileRef["key"].(string)

	// Fetch and verify content
	getResult := natsRequest[FileGetRequest, FileGetResult](t, nc, "files.get", FileGetRequest{Key: fileKey})
	if !getResult.OK {
		t.Fatalf("get failed: %s", getResult.Error)
	}

	data, _ := base64.StdEncoding.DecodeString(getResult.Data)
	if !strings.Contains(string(data), "integration test") {
		t.Errorf("file content doesn't contain message: %s", string(data))
	}

	natsRequest[FileDeleteRequest, FileDeleteResult](t, nc, "files.delete", FileDeleteRequest{Key: fileKey})
}

// --- Test 4: Binary Round Trip (sha256 integrity) ---

func TestBinaryRoundTrip(t *testing.T) {
	skipIfNoNATS(t)
	nc := connect(t)

	// Create 1KB of pseudo-random binary data
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	originalHash := sha256.Sum256(testData)

	testKey := fmt.Sprintf("test/binary-%d.bin", time.Now().UnixNano())

	// PUT
	putResult := natsRequest[FilePutRequest, FilePutResult](t, nc, "files.put", FilePutRequest{
		Key:         testKey,
		Data:        base64.StdEncoding.EncodeToString(testData),
		ContentType: "application/octet-stream",
	})
	if !putResult.OK {
		t.Fatalf("put failed: %s", putResult.Error)
	}

	// GET
	getResult := natsRequest[FileGetRequest, FileGetResult](t, nc, "files.get", FileGetRequest{Key: testKey})
	if !getResult.OK {
		t.Fatalf("get failed: %s", getResult.Error)
	}

	gotData, err := base64.StdEncoding.DecodeString(getResult.Data)
	if err != nil {
		t.Fatalf("decode base64: %v", err)
	}

	gotHash := sha256.Sum256(gotData)
	if originalHash != gotHash {
		t.Errorf("sha256 mismatch: original=%s, got=%s",
			hex.EncodeToString(originalHash[:]),
			hex.EncodeToString(gotHash[:]))
	}

	natsRequest[FileDeleteRequest, FileDeleteResult](t, nc, "files.delete", FileDeleteRequest{Key: testKey})
}

// --- Test 5: Error Cases ---

func TestGetNonexistent(t *testing.T) {
	skipIfNoNATS(t)
	nc := connect(t)

	getResult := natsRequest[FileGetRequest, FileGetResult](t, nc, "files.get", FileGetRequest{
		Key: "nonexistent/file.txt",
	})
	if getResult.OK {
		t.Error("expected error for nonexistent file, got ok")
	}
	if getResult.Error == "" {
		t.Error("expected error message")
	}
}

func TestDeleteNonexistent(t *testing.T) {
	skipIfNoNATS(t)
	nc := connect(t)

	result := natsRequest[FileDeleteRequest, FileDeleteResult](t, nc, "files.delete", FileDeleteRequest{
		Key: "nonexistent/file.txt",
	})
	// Delete of nonexistent should still report ok=false or handle gracefully
	// (behavior depends on NATS Object Store — some treat it as no-op)
	_ = result // Just verify it doesn't panic/timeout
}

func TestPutEmptyKey(t *testing.T) {
	skipIfNoNATS(t)
	nc := connect(t)

	result := natsRequest[FilePutRequest, FilePutResult](t, nc, "files.put", FilePutRequest{
		Key:         "",
		Data:        base64.StdEncoding.EncodeToString([]byte("data")),
		ContentType: "text/plain",
	})
	if result.OK {
		t.Error("expected error for empty key")
	}
}

func TestPutInvalidBase64(t *testing.T) {
	skipIfNoNATS(t)
	nc := connect(t)

	result := natsRequest[FilePutRequest, FilePutResult](t, nc, "files.put", FilePutRequest{
		Key:         "test/invalid.txt",
		Data:        "not-valid-base64!!!",
		ContentType: "text/plain",
	})
	if result.OK {
		t.Error("expected error for invalid base64")
	}
}
