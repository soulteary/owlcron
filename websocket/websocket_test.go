package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	gorillaWS "github.com/gorilla/websocket"
	"go.uber.org/zap"
)

func TestClientIDsAreUniqueUnderConcurrency(t *testing.T) {
	ids := make(chan string, 10000)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ids <- generateClientID()
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate websocket identity %q", id)
		}
		seen[id] = true
	}
}

func TestClosingOneClientPreservesConcurrentConnections(t *testing.T) {
	server := NewServer(nil, zap.NewNop())
	httpServer := httptest.NewServer(http.HandlerFunc(server.HandleWebSocket))
	defer httpServer.Close()
	connections := make([]*gorillaWS.Conn, 0, 16)
	for i := 0; i < 16; i++ {
		conn, _, err := gorillaWS.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		connections = append(connections, conn)
	}
	// A successful client handshake can precede registration on the server.
	// Wait for registration, while still failing if IDs overwrite one another.
	registrationDeadline := time.Now().Add(2 * time.Second)
	for len(server.GetClients()) != len(connections) && time.Now().Before(registrationDeadline) {
		time.Sleep(time.Millisecond)
	}
	before := server.GetClients()
	if len(before) != len(connections) {
		t.Fatalf("concurrent clients replaced each other: %d clients for %d connections", len(before), len(connections))
	}
	if err := connections[0].Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(server.GetClients()) == len(before) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	after := server.GetClients()
	if len(after) != len(before)-1 {
		t.Fatalf("closing one socket removed other clients: %d -> %d", len(before), len(after))
	}
	for id, client := range after {
		if before[id] != client {
			t.Fatal("connection identity changed after another socket closed")
		}
	}
}
