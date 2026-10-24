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

type shutdownHandler struct {
	entered, release, disconnected chan struct{}
	once                           sync.Once
}

func (h *shutdownHandler) OnConnect(*Client)           {}
func (h *shutdownHandler) OnDisconnect(*Client)        { h.once.Do(func() { close(h.disconnected) }) }
func (h *shutdownHandler) OnError(*Client, error)      {}
func (h *shutdownHandler) OnMessage(*Client, *Message) { close(h.entered); <-h.release }
func TestShutdownWaitsForActiveHandlerAndRejectsNewConnections(t *testing.T) {
	h := &shutdownHandler{entered: make(chan struct{}), release: make(chan struct{}), disconnected: make(chan struct{})}
	server := NewServer(h, zap.NewNop())
	httpServer := httptest.NewServer(http.HandlerFunc(server.HandleWebSocket))
	defer httpServer.Close()
	conn, _, err := gorillaWS.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.WriteJSON(map[string]interface{}{"type": "test"})
	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not enter")
	}
	finished := make(chan struct{})
	go func() { server.Shutdown(); close(finished) }()
	deadline := time.Now().Add(time.Second)
	for {
		server.mu.RLock()
		stopping := server.stopping
		server.mu.RUnlock()
		if stopping {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("shutdown did not close admission")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-finished:
		t.Fatal("shutdown skipped active handler")
	default:
	}
	recorder := httptest.NewRecorder()
	server.HandleWebSocket(recorder, httptest.NewRequest("GET", "http://localhost/ws", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-shutdown status %d", recorder.Code)
	}
	close(h.release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish")
	}
	select {
	case <-h.disconnected:
	default:
		t.Fatal("disconnect callback not drained")
	}
}
func TestNodeEndpointRejectsBrowserOrigin(t *testing.T) {
	server := NewServer(nil, zap.NewNop())
	r := httptest.NewRequest("GET", "https://localhost/cluster/ws", nil)
	r.Header.Set("Origin", "https://localhost")
	w := httptest.NewRecorder()
	server.HandleNodeWebSocket(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("browser entered cluster surface: %d", w.Code)
	}
}
