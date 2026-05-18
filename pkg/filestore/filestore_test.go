package filestore

import (
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func startJetStream(t *testing.T) nats.JetStreamContext {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ns := natsserver.RunServer(&opts)
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js
}

func TestNewStore(t *testing.T) {
	js := startJetStream(t)
	store, err := NewStore(js, DefaultConfig())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestPutGet(t *testing.T) {
	js := startJetStream(t)
	store, _ := NewStore(js, DefaultConfig())

	data := []byte("hello world")
	meta, err := store.Put("test.txt", data, "text/plain")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if meta.Key != "test.txt" {
		t.Fatalf("expected key test.txt, got %s", meta.Key)
	}
	if meta.Size != 11 {
		t.Fatalf("expected size 11, got %d", meta.Size)
	}
	if meta.ContentType != "text/plain" {
		t.Fatalf("expected text/plain, got %s", meta.ContentType)
	}
	if meta.ETag == "" {
		t.Fatal("expected non-empty ETag")
	}

	// Get it back
	got, gotMeta, err := store.Get("test.txt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("expected 'hello world', got %q", string(got))
	}
	if gotMeta.Size != 11 {
		t.Fatalf("expected size 11, got %d", gotMeta.Size)
	}
}

func TestPutEmptyKey(t *testing.T) {
	js := startJetStream(t)
	store, _ := NewStore(js, DefaultConfig())

	_, err := store.Put("", []byte("data"), "text/plain")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestHead(t *testing.T) {
	js := startJetStream(t)
	store, _ := NewStore(js, DefaultConfig())

	store.Put("image.png", []byte{0x89, 0x50, 0x4E, 0x47}, "image/png")

	meta, err := store.Head("image.png")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if meta.Key != "image.png" {
		t.Fatalf("expected image.png, got %s", meta.Key)
	}
	if meta.Size != 4 {
		t.Fatalf("expected size 4, got %d", meta.Size)
	}
	if meta.ContentType != "image/png" {
		t.Fatalf("expected image/png, got %s", meta.ContentType)
	}
}

func TestHeadNotFound(t *testing.T) {
	js := startJetStream(t)
	store, _ := NewStore(js, DefaultConfig())

	_, err := store.Head("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestDelete(t *testing.T) {
	js := startJetStream(t)
	store, _ := NewStore(js, DefaultConfig())

	store.Put("temp.txt", []byte("temporary"), "text/plain")

	err := store.Delete("temp.txt")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Should not be gettable anymore
	_, _, err = store.Get("temp.txt")
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestList(t *testing.T) {
	js := startJetStream(t)
	store, _ := NewStore(js, DefaultConfig())

	store.Put("audio/clip1.wav", []byte("wav1"), "audio/wav")
	store.Put("audio/clip2.wav", []byte("wav2"), "audio/wav")
	store.Put("image/photo.png", []byte("png"), "image/png")

	// List all
	all, err := store.List("")
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 files, got %d", len(all))
	}

	// List with prefix
	audio, err := store.List("audio/")
	if err != nil {
		t.Fatalf("List audio: %v", err)
	}
	if len(audio) != 2 {
		t.Fatalf("expected 2 audio files, got %d", len(audio))
	}

	// List with non-matching prefix
	none, err := store.List("video/")
	if err != nil {
		t.Fatalf("List video: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("expected 0 video files, got %d", len(none))
	}
}

func TestListEmpty(t *testing.T) {
	js := startJetStream(t)
	store, _ := NewStore(js, DefaultConfig())

	files, err := store.List("")
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected 0 files, got %d", len(files))
	}
}

func TestETag_ContentAddressed(t *testing.T) {
	js := startJetStream(t)
	store, _ := NewStore(js, DefaultConfig())

	data := []byte("identical content")
	meta1, _ := store.Put("file-a", data, "text/plain")
	meta2, _ := store.Put("file-b", data, "text/plain")

	// Same content = same ETag
	if meta1.ETag != meta2.ETag {
		t.Fatalf("expected same ETag for identical content: %s vs %s", meta1.ETag, meta2.ETag)
	}

	// Different content = different ETag
	meta3, _ := store.Put("file-c", []byte("different"), "text/plain")
	if meta1.ETag == meta3.ETag {
		t.Fatal("expected different ETag for different content")
	}
}

func TestOverwrite(t *testing.T) {
	js := startJetStream(t)
	store, _ := NewStore(js, DefaultConfig())

	store.Put("mutable.txt", []byte("version 1"), "text/plain")
	store.Put("mutable.txt", []byte("version 2"), "text/plain")

	data, _, err := store.Get("mutable.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "version 2" {
		t.Fatalf("expected 'version 2', got %q", string(data))
	}
}

func TestBinaryData(t *testing.T) {
	js := startJetStream(t)
	store, _ := NewStore(js, DefaultConfig())

	// Binary data with all byte values
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}

	_, err := store.Put("binary.bin", data, "application/octet-stream")
	if err != nil {
		t.Fatalf("Put binary: %v", err)
	}

	got, _, err := store.Get("binary.bin")
	if err != nil {
		t.Fatalf("Get binary: %v", err)
	}
	if len(got) != 256 {
		t.Fatalf("expected 256 bytes, got %d", len(got))
	}
	for i, b := range got {
		if b != byte(i) {
			t.Fatalf("byte %d: expected %d, got %d", i, i, b)
		}
	}
}
