// Package integration provides end-to-end tests for leaf node topology (P4-008e).
//
// These tests spin up embedded NATS servers (seed + leaf) programmatically and
// verify cross-node messaging, queue groups, JetStream, events, and resilience.
// No external NATS or mDNS required — fully self-contained.
//
// Skip with: go test -short (mDNS tests only)
package integration

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// --- Helpers for leaf node tests ---

// freePort finds a free TCP port on localhost.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// seedInfo holds seed NATS server and its ports for leaf connection.
type seedInfo struct {
	Server   *natsserver.Server
	LeafPort int
}

// startSeedNATS starts an embedded NATS seed server with JetStream and a leaf port.
func startSeedNATS(t *testing.T) seedInfo {
	t.Helper()
	leafPort := freePort(t)
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random client port
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
		LeafNode: natsserver.LeafNodeOpts{
			Host: "127.0.0.1",
			Port: leafPort,
		},
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create seed NATS: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("seed NATS failed to start")
	}
	t.Cleanup(func() { ns.Shutdown() })
	return seedInfo{Server: ns, LeafPort: leafPort}
}

// startLeafNATS starts an embedded NATS leaf server that connects to the given seed.
func startLeafNATS(t *testing.T, seedLeafPort int) *natsserver.Server {
	t.Helper()
	leafURL, err := url.Parse(fmt.Sprintf("nats-leaf://127.0.0.1:%d", seedLeafPort))
	if err != nil {
		t.Fatalf("parse leaf URL: %v", err)
	}
	opts := &natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1, // random client port
		NoLog:  true,
		NoSigs: true,
		LeafNode: natsserver.LeafNodeOpts{
			Remotes: []*natsserver.RemoteLeafOpts{
				{URLs: []*url.URL{leafURL}},
			},
		},
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create leaf NATS: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("leaf NATS failed to start")
	}
	t.Cleanup(func() { ns.Shutdown() })
	return ns
}

// connectNATS connects a client to a NATS server with auto-cleanup.
func connectNATS(t *testing.T, natsURL string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(natsURL,
		nats.Timeout(5*time.Second),
		nats.ReconnectWait(200*time.Millisecond),
		nats.MaxReconnects(30),
	)
	if err != nil {
		t.Fatalf("connect to %s: %v", natsURL, err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

// waitForLeafConnection waits for the seed to see at least n leaf connections.
func waitForLeafConnection(t *testing.T, seed *natsserver.Server, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if seed.NumLeafNodes() >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d leaf connection(s), got %d", n, seed.NumLeafNodes())
}

// --- Test: Seed starts with leaf port listening ---

func TestSeedStartup_LeafPortListening(t *testing.T) {
	seed := startSeedNATS(t)

	// Verify leaf port is accessible
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", seed.LeafPort), 2*time.Second)
	if err != nil {
		t.Fatalf("leaf port %d not listening: %v", seed.LeafPort, err)
	}
	conn.Close()

	// Verify client connection works
	nc := connectNATS(t, seed.Server.ClientURL())
	if !nc.IsConnected() {
		t.Fatal("client not connected to seed")
	}
}

// --- Test: Leaf connects to seed, bidirectional messaging ---

func TestLeafConnectsToSeed(t *testing.T) {
	seed := startSeedNATS(t)
	leaf := startLeafNATS(t, seed.LeafPort)
	waitForLeafConnection(t, seed.Server, 1, 5*time.Second)

	seedClient := connectNATS(t, seed.Server.ClientURL())
	leafClient := connectNATS(t, leaf.ClientURL())

	// Seed → Leaf
	sub1, err := leafClient.SubscribeSync("test.s2l")
	if err != nil {
		t.Fatal(err)
	}
	seedClient.Flush()
	leafClient.Flush()
	time.Sleep(200 * time.Millisecond) // subscription propagation

	seedClient.Publish("test.s2l", []byte("hello from seed"))
	seedClient.Flush()

	msg, err := sub1.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("leaf didn't receive seed message: %v", err)
	}
	if string(msg.Data) != "hello from seed" {
		t.Fatalf("expected 'hello from seed', got %q", string(msg.Data))
	}

	// Leaf → Seed
	sub2, err := seedClient.SubscribeSync("test.l2s")
	if err != nil {
		t.Fatal(err)
	}
	seedClient.Flush()
	leafClient.Flush()
	time.Sleep(200 * time.Millisecond)

	leafClient.Publish("test.l2s", []byte("hello from leaf"))
	leafClient.Flush()

	msg2, err := sub2.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("seed didn't receive leaf message: %v", err)
	}
	if string(msg2.Data) != "hello from leaf" {
		t.Fatalf("expected 'hello from leaf', got %q", string(msg2.Data))
	}
}

// --- Test: Queue groups load balance across seed and leaf ---

func TestQueueGroupsAcrossLeaf(t *testing.T) {
	seed := startSeedNATS(t)
	leaf := startLeafNATS(t, seed.LeafPort)
	waitForLeafConnection(t, seed.Server, 1, 5*time.Second)

	seedClient := connectNATS(t, seed.Server.ClientURL())
	leafClient := connectNATS(t, leaf.ClientURL())

	var seedCount, leafCount int64

	// Both subscribe to the same queue group (like tool method handlers)
	seedClient.QueueSubscribe("tools.calc.add", "q", func(msg *nats.Msg) {
		atomic.AddInt64(&seedCount, 1)
		msg.Respond([]byte(`{"ok":true,"node":"seed"}`))
	})

	leafClient.QueueSubscribe("tools.calc.add", "q", func(msg *nats.Msg) {
		atomic.AddInt64(&leafCount, 1)
		msg.Respond([]byte(`{"ok":true,"node":"leaf"}`))
	})

	seedClient.Flush()
	leafClient.Flush()
	time.Sleep(2 * time.Second) // queue group propagation across leaf bridge

	// Send requests from both sides to verify load balancing.
	// NATS leaf nodes may prefer local queue members, so we test that
	// calls from the seed reach both, and calls from the leaf reach both.
	seedCaller := connectNATS(t, seed.Server.ClientURL())
	leafCaller := connectNATS(t, leaf.ClientURL())

	// Calls from seed side
	for i := 0; i < 10; i++ {
		_, err := seedCaller.Request("tools.calc.add", []byte(`{"params":{}}`), 3*time.Second)
		if err != nil {
			t.Fatalf("seed request %d: %v", i, err)
		}
	}

	// Calls from leaf side
	for i := 0; i < 10; i++ {
		_, err := leafCaller.Request("tools.calc.add", []byte(`{"params":{}}`), 3*time.Second)
		if err != nil {
			t.Fatalf("leaf request %d: %v", i, err)
		}
	}

	sc := atomic.LoadInt64(&seedCount)
	lc := atomic.LoadInt64(&leafCount)

	// Both should receive at least some requests across 20 total.
	// Note: NATS leaf nodes may prefer local queue members, so distribution
	// won't be perfectly even. The important thing is both participate.
	if sc == 0 || lc == 0 {
		t.Fatalf("queue group not load-balanced: seed=%d leaf=%d (both should be >0)", sc, lc)
	}
	if sc+lc != 20 {
		t.Fatalf("expected 20 total, got seed=%d + leaf=%d = %d", sc, lc, sc+lc)
	}
}

// --- Test: Tool call routed across leaf (request/reply) ---

func TestToolCallAcrossLeaf(t *testing.T) {
	seed := startSeedNATS(t)
	leaf := startLeafNATS(t, seed.LeafPort)
	waitForLeafConnection(t, seed.Server, 1, 5*time.Second)

	// Register a "tool" on the leaf
	leafClient := connectNATS(t, leaf.ClientURL())
	leafClient.Subscribe("tools.calculator.add", func(msg *nats.Msg) {
		var req CallRequest
		json.Unmarshal(msg.Data, &req)
		a, _ := req.Params["a"].(float64)
		b, _ := req.Params["b"].(float64)
		resp, _ := json.Marshal(CallResult{
			OK:     true,
			Result: map[string]interface{}{"sum": a + b},
			Node:   "leaf-node",
		})
		msg.Respond(resp)
	})
	leafClient.Flush()
	time.Sleep(300 * time.Millisecond)

	// Call from seed side
	seedClient := connectNATS(t, seed.Server.ClientURL())
	reqData, _ := json.Marshal(CallRequest{Params: map[string]interface{}{"a": 10.0, "b": 32.0}})
	msg, err := seedClient.Request("tools.calculator.add", reqData, 5*time.Second)
	if err != nil {
		t.Fatalf("cross-leaf tool call failed: %v", err)
	}

	var result CallResult
	json.Unmarshal(msg.Data, &result)
	if !result.OK {
		t.Fatalf("expected OK, got error: %s", result.Error)
	}
	sum, _ := result.Result["sum"].(float64)
	if sum != 42.0 {
		t.Fatalf("expected sum=42, got %v", sum)
	}
	if result.Node != "leaf-node" {
		t.Fatalf("expected node=leaf-node, got %s", result.Node)
	}
}

// --- Test: JetStream Object Store works through leaf ---

func _TODO_TestJetStreamThroughLeaf(t *testing.T) {
	seed := startSeedNATS(t)
	leaf := startLeafNATS(t, seed.LeafPort)
	waitForLeafConnection(t, seed.Server, 1, 5*time.Second)

	// Create Object Store bucket on seed
	seedClient := connectNATS(t, seed.Server.ClientURL())
	js, err := seedClient.JetStream()
	if err != nil {
		t.Fatalf("JetStream on seed: %v", err)
	}

	bucket, err := js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket:   "TEST_FILES",
		MaxBytes: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("create object store: %v", err)
	}

	// Put an object from seed
	testData := []byte("hello from the seed side")
	_, err = bucket.PutBytes("test/file.txt", testData)
	if err != nil {
		t.Fatalf("put object: %v", err)
	}

	// Get the object from leaf.
	// Leaf nodes forward JetStream API requests to the seed transparently.
	leafClient := connectNATS(t, leaf.ClientURL())
	leafJS, err := leafClient.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		t.Fatalf("JetStream on leaf: %v", err)
	}

	// Small delay for JetStream API availability through leaf
	time.Sleep(500 * time.Millisecond)

	leafBucket, err := leafJS.ObjectStore("TEST_FILES")
	if err != nil {
		t.Fatalf("get object store from leaf: %v", err)
	}

	gotData, err := leafBucket.GetBytes("test/file.txt")
	if err != nil {
		t.Fatalf("get object from leaf: %v", err)
	}
	if string(gotData) != string(testData) {
		t.Fatalf("data mismatch: got %q, want %q", string(gotData), string(testData))
	}

	// Put from leaf, get from seed
	leafData := []byte("hello from the leaf side")
	_, err = leafBucket.PutBytes("test/leaf-file.txt", leafData)
	if err != nil {
		t.Fatalf("put from leaf: %v", err)
	}

	gotLeafData, err := bucket.GetBytes("test/leaf-file.txt")
	if err != nil {
		t.Fatalf("get leaf data from seed: %v", err)
	}
	if string(gotLeafData) != string(leafData) {
		t.Fatalf("leaf data mismatch: got %q, want %q", string(gotLeafData), string(leafData))
	}
}

// --- Test: Events propagate through leaf ---

func TestEventsThroughLeaf(t *testing.T) {
	seed := startSeedNATS(t)
	leaf := startLeafNATS(t, seed.LeafPort)
	waitForLeafConnection(t, seed.Server, 1, 5*time.Second)

	seedClient := connectNATS(t, seed.Server.ClientURL())
	leafClient := connectNATS(t, leaf.ClientURL())

	// Subscribe to events on seed
	var seedReceived atomic.Int32
	seedClient.Subscribe("events.>", func(msg *nats.Msg) {
		seedReceived.Add(1)
	})

	// Subscribe to events on leaf
	var leafReceived atomic.Int32
	leafClient.Subscribe("events.>", func(msg *nats.Msg) {
		leafReceived.Add(1)
	})

	seedClient.Flush()
	leafClient.Flush()
	time.Sleep(300 * time.Millisecond) // subscription propagation

	// Leaf emits event → seed should receive
	event := map[string]interface{}{
		"type": "tool.started",
		"node": "leaf-node",
		"data": map[string]interface{}{"tool": "whisper"},
	}
	eventData, _ := json.Marshal(event)
	leafClient.Publish("events.tool.started", eventData)
	leafClient.Flush()

	// Seed emits event → leaf should receive
	event2 := map[string]interface{}{
		"type": "node.joined",
		"node": "seed-node",
		"data": map[string]interface{}{"tools": 3},
	}
	eventData2, _ := json.Marshal(event2)
	seedClient.Publish("events.node.joined", eventData2)
	seedClient.Flush()

	time.Sleep(500 * time.Millisecond)

	if seedReceived.Load() < 1 {
		t.Error("seed didn't receive events from leaf")
	}
	if leafReceived.Load() < 1 {
		t.Error("leaf didn't receive events from seed")
	}
}

// --- Test: Seed disconnect — local tools survive, reconnect recovers ---

func TestSeedDisconnect_LocalToolsSurvive(t *testing.T) {
	// Use fixed ports for seed so we can restart on same ports
	clientPort := freePort(t)
	leafPort := freePort(t)

	seedOpts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      clientPort,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
		LeafNode: natsserver.LeafNodeOpts{
			Host: "127.0.0.1",
			Port: leafPort,
		},
	}
	seed, err := natsserver.NewServer(seedOpts)
	if err != nil {
		t.Fatalf("create seed: %v", err)
	}
	go seed.Start()
	if !seed.ReadyForConnections(5 * time.Second) {
		t.Fatal("seed failed to start")
	}

	leaf := startLeafNATS(t, leafPort)
	waitForLeafConnection(t, seed, 1, 5*time.Second)

	// Register a "local tool" on the leaf (simulates a jumpboot tool)
	leafClient := connectNATS(t, leaf.ClientURL())
	leafClient.Subscribe("local.tool.ping", func(msg *nats.Msg) {
		msg.Respond([]byte("pong"))
	})
	leafClient.Flush()

	// Verify local tool works
	resp, err := leafClient.Request("local.tool.ping", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("local tool before disconnect: %v", err)
	}
	if string(resp.Data) != "pong" {
		t.Fatalf("expected pong, got %q", string(resp.Data))
	}

	// Register a cross-node tool on the seed
	seedClient := connectNATS(t, seed.ClientURL())
	seedClient.Subscribe("tools.remote.echo", func(msg *nats.Msg) {
		msg.Respond([]byte("echo"))
	})
	seedClient.Flush()
	time.Sleep(300 * time.Millisecond)

	// Verify cross-node works before disconnect
	resp, err = leafClient.Request("tools.remote.echo", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("cross-node before disconnect: %v", err)
	}
	if string(resp.Data) != "echo" {
		t.Fatalf("expected echo, got %q", string(resp.Data))
	}

	// Kill the seed
	seed.Shutdown()
	time.Sleep(500 * time.Millisecond)

	// Local tool on leaf should still work (local NATS still running)
	resp, err = leafClient.Request("local.tool.ping", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("local tool after seed disconnect should still work: %v", err)
	}
	if string(resp.Data) != "pong" {
		t.Fatalf("expected pong after disconnect, got %q", string(resp.Data))
	}

	// Cross-node call should fail (seed is down)
	_, err = leafClient.Request("tools.remote.echo", nil, 1*time.Second)
	if err == nil {
		t.Fatal("cross-node call should fail with seed down")
	}

	// Restart the seed on the same ports
	newSeedOpts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      clientPort,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
		LeafNode: natsserver.LeafNodeOpts{
			Host: "127.0.0.1",
			Port: leafPort,
		},
	}
	newSeed, err := natsserver.NewServer(newSeedOpts)
	if err != nil {
		t.Fatalf("restart seed: %v", err)
	}
	go newSeed.Start()
	if !newSeed.ReadyForConnections(5 * time.Second) {
		t.Fatal("restarted seed failed to start")
	}
	t.Cleanup(func() { newSeed.Shutdown() })

	// Wait for leaf to reconnect
	waitForLeafConnection(t, newSeed, 1, 10*time.Second)

	// Re-register the tool on the new seed connection with a delayed name
	// Use a different subject to avoid stale subscription races from the old session
	newSeedClient := connectNATS(t, newSeed.ClientURL())
	newSeedClient.Subscribe("tools.remote.echo", func(msg *nats.Msg) {
		msg.Respond([]byte("echo-recovered"))
	})
	newSeedClient.Flush()
	time.Sleep(500 * time.Millisecond)

	// Cross-node should recover (retry a few times for leaf subscription propagation)
	var respRecovery *nats.Msg
	for i := 0; i < 5; i++ {
		respRecovery, err = leafClient.Request("tools.remote.echo", nil, 2*time.Second)
		if err == nil && string(respRecovery.Data) == "echo-recovered" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("cross-node after reconnect: %v", err)
	}
	if string(respRecovery.Data) != "echo-recovered" {
		t.Fatalf("expected echo-recovered, got %q", string(respRecovery.Data))
	}
}

// --- Test: Node-targeted calls work across leaf bridge ---

func TestNodeTargetedCallAcrossLeaf(t *testing.T) {
	seed := startSeedNATS(t)
	leaf := startLeafNATS(t, seed.LeafPort)
	waitForLeafConnection(t, seed.Server, 1, 5*time.Second)

	// Register same tool on both nodes with node-targeted subjects
	seedClient := connectNATS(t, seed.Server.ClientURL())
	seedClient.Subscribe("node.seed-box.tools.calc.add", func(msg *nats.Msg) {
		resp, _ := json.Marshal(CallResult{
			OK:     true,
			Result: map[string]interface{}{"from": "seed"},
			Node:   "seed-box",
		})
		msg.Respond(resp)
	})

	leafClient := connectNATS(t, leaf.ClientURL())
	leafClient.Subscribe("node.leaf-box.tools.calc.add", func(msg *nats.Msg) {
		resp, _ := json.Marshal(CallResult{
			OK:     true,
			Result: map[string]interface{}{"from": "leaf"},
			Node:   "leaf-box",
		})
		msg.Respond(resp)
	})

	seedClient.Flush()
	leafClient.Flush()
	time.Sleep(300 * time.Millisecond)

	// Call from seed, targeting leaf explicitly
	caller := connectNATS(t, seed.Server.ClientURL())
	msg, err := caller.Request("node.leaf-box.tools.calc.add", []byte(`{"params":{}}`), 3*time.Second)
	if err != nil {
		t.Fatalf("node-targeted call to leaf: %v", err)
	}
	var result CallResult
	json.Unmarshal(msg.Data, &result)
	if !result.OK {
		t.Fatalf("expected OK: %s", result.Error)
	}
	if result.Result["from"] != "leaf" {
		t.Fatalf("expected from=leaf, got %v", result.Result["from"])
	}

	// Call from leaf, targeting seed explicitly
	msg2, err := leafClient.Request("node.seed-box.tools.calc.add", []byte(`{"params":{}}`), 3*time.Second)
	if err != nil {
		t.Fatalf("node-targeted call to seed: %v", err)
	}
	var result2 CallResult
	json.Unmarshal(msg2.Data, &result2)
	if result2.Result["from"] != "seed" {
		t.Fatalf("expected from=seed, got %v", result2.Result["from"])
	}
}

// --- Test: File store NATS handlers work through leaf ---

func TestFileStoreHandlersThroughLeaf(t *testing.T) {
	seed := startSeedNATS(t)
	leaf := startLeafNATS(t, seed.LeafPort)
	waitForLeafConnection(t, seed.Server, 1, 5*time.Second)

	// Register file store handlers on seed (simulates what node.go does)
	seedClient := connectNATS(t, seed.Server.ClientURL())
	js, err := seedClient.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Create the object store bucket
	_, err = js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket: "MESH_FILES",
	})
	if err != nil {
		t.Fatalf("create object store: %v", err)
	}

	// Simple files.put handler on seed
	seedClient.Subscribe("files.put", func(msg *nats.Msg) {
		var req FilePutRequest
		json.Unmarshal(msg.Data, &req)

		data, err := base64.StdEncoding.DecodeString(req.Data)
		if err != nil {
			resp, _ := json.Marshal(FilePutResult{OK: false, Error: err.Error()})
			msg.Respond(resp)
			return
		}

		bucket, _ := js.ObjectStore("MESH_FILES")
		bucket.PutBytes(req.Key, data)

		resp, _ := json.Marshal(FilePutResult{
			OK:   true,
			Key:  req.Key,
			Size: int64(len(data)),
		})
		msg.Respond(resp)
	})

	seedClient.Subscribe("files.get", func(msg *nats.Msg) {
		var req FileGetRequest
		json.Unmarshal(msg.Data, &req)

		bucket, _ := js.ObjectStore("MESH_FILES")
		data, err := bucket.GetBytes(req.Key)
		if err != nil {
			resp, _ := json.Marshal(FileGetResult{OK: false, Error: err.Error()})
			msg.Respond(resp)
			return
		}

		resp, _ := json.Marshal(FileGetResult{
			OK:   true,
			Key:  req.Key,
			Data: base64.StdEncoding.EncodeToString(data),
			Size: int64(len(data)),
		})
		msg.Respond(resp)
	})

	seedClient.Flush()
	time.Sleep(300 * time.Millisecond)

	// Use file store from leaf client
	leafClient := connectNATS(t, leaf.ClientURL())

	testData := []byte("file stored through leaf bridge")
	testKey := fmt.Sprintf("test/leaf-%d.txt", time.Now().UnixNano())

	// PUT from leaf
	putResult := natsRequest[FilePutRequest, FilePutResult](t, leafClient, "files.put", FilePutRequest{
		Key:         testKey,
		Data:        base64.StdEncoding.EncodeToString(testData),
		ContentType: "text/plain",
	})
	if !putResult.OK {
		t.Fatalf("put from leaf: %s", putResult.Error)
	}
	if putResult.Size != int64(len(testData)) {
		t.Errorf("size: got %d, want %d", putResult.Size, len(testData))
	}

	// GET from leaf
	getResult := natsRequest[FileGetRequest, FileGetResult](t, leafClient, "files.get", FileGetRequest{
		Key: testKey,
	})
	if !getResult.OK {
		t.Fatalf("get from leaf: %s", getResult.Error)
	}
	gotData, _ := base64.StdEncoding.DecodeString(getResult.Data)
	if string(gotData) != string(testData) {
		t.Fatalf("data mismatch: got %q, want %q", string(gotData), string(testData))
	}
}

// --- Test: Multiple leaves connect to one seed ---

func TestMultipleLeavesOneSeed(t *testing.T) {
	seed := startSeedNATS(t)
	leaf1 := startLeafNATS(t, seed.LeafPort)
	leaf2 := startLeafNATS(t, seed.LeafPort)
	waitForLeafConnection(t, seed.Server, 2, 5*time.Second)

	// Register tools on each leaf with node-targeted subjects
	leaf1Client := connectNATS(t, leaf1.ClientURL())
	leaf1Client.Subscribe("node.gpu-1.tools.whisper.transcribe", func(msg *nats.Msg) {
		resp, _ := json.Marshal(CallResult{
			OK:     true,
			Result: map[string]interface{}{"text": "from gpu-1"},
			Node:   "gpu-1",
		})
		msg.Respond(resp)
	})

	leaf2Client := connectNATS(t, leaf2.ClientURL())
	leaf2Client.Subscribe("node.gpu-2.tools.whisper.transcribe", func(msg *nats.Msg) {
		resp, _ := json.Marshal(CallResult{
			OK:     true,
			Result: map[string]interface{}{"text": "from gpu-2"},
			Node:   "gpu-2",
		})
		msg.Respond(resp)
	})

	leaf1Client.Flush()
	leaf2Client.Flush()
	time.Sleep(300 * time.Millisecond)

	// Call specific nodes from seed
	caller := connectNATS(t, seed.Server.ClientURL())

	msg1, err := caller.Request("node.gpu-1.tools.whisper.transcribe", []byte(`{"params":{}}`), 3*time.Second)
	if err != nil {
		t.Fatalf("call gpu-1: %v", err)
	}
	var r1 CallResult
	json.Unmarshal(msg1.Data, &r1)
	if r1.Node != "gpu-1" {
		t.Fatalf("expected gpu-1, got %s", r1.Node)
	}

	msg2, err := caller.Request("node.gpu-2.tools.whisper.transcribe", []byte(`{"params":{}}`), 3*time.Second)
	if err != nil {
		t.Fatalf("call gpu-2: %v", err)
	}
	var r2 CallResult
	json.Unmarshal(msg2.Data, &r2)
	if r2.Node != "gpu-2" {
		t.Fatalf("expected gpu-2, got %s", r2.Node)
	}
}

// --- Test: Leaf-to-leaf communication (through seed) ---

func TestLeafToLeafCommunication(t *testing.T) {
	seed := startSeedNATS(t)
	leaf1 := startLeafNATS(t, seed.LeafPort)
	leaf2 := startLeafNATS(t, seed.LeafPort)
	waitForLeafConnection(t, seed.Server, 2, 5*time.Second)

	// Tool on leaf1
	leaf1Client := connectNATS(t, leaf1.ClientURL())
	leaf1Client.Subscribe("tools.embed.embed", func(msg *nats.Msg) {
		resp, _ := json.Marshal(CallResult{
			OK:     true,
			Result: map[string]interface{}{"vector": []float64{0.1, 0.2}},
			Node:   "leaf-1",
		})
		msg.Respond(resp)
	})
	leaf1Client.Flush()
	time.Sleep(300 * time.Millisecond)

	// Call from leaf2 → should route through seed → leaf1
	leaf2Client := connectNATS(t, leaf2.ClientURL())
	msg, err := leaf2Client.Request("tools.embed.embed", []byte(`{"params":{"text":"hello"}}`), 5*time.Second)
	if err != nil {
		t.Fatalf("leaf-to-leaf call: %v", err)
	}

	var result CallResult
	json.Unmarshal(msg.Data, &result)
	if !result.OK {
		t.Fatalf("expected OK: %s", result.Error)
	}
	if result.Node != "leaf-1" {
		t.Fatalf("expected leaf-1, got %s", result.Node)
	}
}

// --- Test: JetStream event stream through leaf ---

func TestJetStreamEventStreamThroughLeaf(t *testing.T) {
	seed := startSeedNATS(t)
	leaf := startLeafNATS(t, seed.LeafPort)
	waitForLeafConnection(t, seed.Server, 1, 5*time.Second)

	// Create JetStream event stream on seed
	seedClient := connectNATS(t, seed.Server.ClientURL())
	js, err := seedClient.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST_EVENTS",
		Subjects: []string{"events.>"},
		MaxAge:   time.Hour,
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Publish event from leaf
	leafClient := connectNATS(t, leaf.ClientURL())
	event := map[string]interface{}{
		"type": "tool.installed",
		"node": "node-a",
		"data": map[string]interface{}{"tool": "whisper", "version": "1.0.0"},
	}
	eventData, _ := json.Marshal(event)
	leafClient.Publish("events.tool.installed", eventData)
	leafClient.Flush()
	time.Sleep(500 * time.Millisecond) // propagation + stream capture

	// Read from JetStream on seed — event should be persisted
	sub, err := js.SubscribeSync("events.>", nats.OrderedConsumer(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe stream: %v", err)
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no event in stream: %v", err)
	}

	var received map[string]interface{}
	json.Unmarshal(msg.Data, &received)
	if received["type"] != "tool.installed" {
		t.Fatalf("expected tool.installed, got %v", received["type"])
	}
}

// --- Test: Concurrent calls across leaf bridge ---

func TestConcurrentCallsAcrossLeaf(t *testing.T) {
	seed := startSeedNATS(t)
	leaf := startLeafNATS(t, seed.LeafPort)
	waitForLeafConnection(t, seed.Server, 1, 5*time.Second)

	// Register tool on leaf
	leafClient := connectNATS(t, leaf.ClientURL())
	leafClient.Subscribe("tools.slow.work", func(msg *nats.Msg) {
		time.Sleep(50 * time.Millisecond) // simulate work
		resp, _ := json.Marshal(CallResult{OK: true, Result: map[string]interface{}{"done": true}, Node: "leaf"})
		msg.Respond(resp)
	})
	leafClient.Flush()
	time.Sleep(300 * time.Millisecond)

	// Fire 10 concurrent calls from seed
	caller := connectNATS(t, seed.Server.ClientURL())
	var wg sync.WaitGroup
	var errors atomic.Int32

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := caller.Request("tools.slow.work", []byte(`{"params":{}}`), 10*time.Second)
			if err != nil {
				errors.Add(1)
			}
		}()
	}

	wg.Wait()
	if errors.Load() > 0 {
		t.Fatalf("%d out of 10 concurrent calls failed", errors.Load())
	}
}
