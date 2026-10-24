package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func TestHTTPServerTimeoutDefaultsAndValidation(t *testing.T) {
	server, err := newHTTPServer(&types.Config{}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	if server.ReadHeaderTimeout != 10*time.Second || server.ReadTimeout != 30*time.Second || server.WriteTimeout != 35*time.Second || server.IdleTimeout != time.Minute {
		t.Fatalf("unexpected defaults: %+v", server)
	}
	for _, c := range []*types.Config{{HTTPReadHeaderTimeoutSeconds: -1}, {HTTPReadTimeoutSeconds: -1}, {HTTPWriteTimeoutSeconds: -1}, {HTTPIdleTimeoutSeconds: -1}, {HTTPRequestTimeoutSeconds: -1}, {HTTPWriteTimeoutSeconds: 3601}} {
		if _, err := newHTTPServer(c, http.NotFoundHandler()); err == nil {
			t.Fatalf("invalid config accepted: %+v", c)
		}
	}
}

func TestAPIRequestDeadlineCancelsConcurrentWork(t *testing.T) {
	const count = 16
	canceled := make(chan struct{}, count)
	late := make(chan struct{})
	handler := boundedAPIRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		<-late
		defer func() { canceled <- struct{}{} }()
		if _, err := w.Write([]byte("late success")); err != http.ErrHandlerTimeout {
			t.Errorf("late write: %v", err)
		}
	}), 30*time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/app/status", nil))
			if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "late success") {
				t.Errorf("timeout response: %d %q", response.Code, response.Body.String())
			}
		}()
	}
	wg.Wait()
	close(late)
	for i := 0; i < count; i++ {
		select {
		case <-canceled:
		case <-time.After(time.Second):
			t.Fatal("handler context not canceled")
		}
	}
}

func TestHTTPStreamingAndWebSocketWriterRemainUnwrapped(t *testing.T) {
	for _, url := range []string{"/ws", "/api/app/get_job_log?stream=1&download=1", "/api/app/get_live_job_log/v1?stream=1&download=1"} {
		original := httptest.NewRecorder()
		handler := boundedAPIRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if w != original {
				t.Errorf("wrapped streaming writer for %s", url)
			}
			if _, ok := r.Context().Deadline(); ok != strings.HasPrefix(url, "/api/") {
				t.Errorf("unexpected total deadline for %s", url)
			}
			w.WriteHeader(204)
		}), time.Millisecond)
		handler.ServeHTTP(original, httptest.NewRequest("GET", url, nil))
	}
	// A streaming query cannot disable the timeout on unrelated API routes.
	for _, url := range []string{"/api/app/get_schedule?stream=1&download=1", "/api/app/fetch_delete_job_log?stream=1&download=1"} {
		response := httptest.NewRecorder()
		boundedAPIRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, ok := r.Context().Deadline(); !ok {
				t.Errorf("missing deadline: %s", url)
			}
		}), time.Second).ServeHTTP(response, httptest.NewRequest("GET", url, nil))
	}
}

func serveHTTPBoundaryTest(t *testing.T, server *http.Server) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close(); <-done })
	return listener
}

func TestHTTPTransportRejectsSlowHeadersAndBody(t *testing.T) {
	for _, stage := range []string{"headers", "body"} {
		t.Run(stage, func(t *testing.T) {
			entered := make(chan error, 1)
			server := &http.Server{ReadHeaderTimeout: 40 * time.Millisecond, ReadTimeout: 90 * time.Millisecond, WriteTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, err := io.ReadAll(r.Body)
				entered <- err
				w.WriteHeader(204)
			})}
			listener := serveHTTPBoundaryTest(t, server)
			conn, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			if stage == "headers" {
				_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: slow")
				if err != nil {
					t.Fatal(err)
				}
				_, err = bufio.NewReader(conn).ReadByte()
				if err == nil {
					t.Fatal("incomplete header remained usable")
				}
				select {
				case <-entered:
					t.Fatal("incomplete headers reached handler")
				default:
				}
			} else {
				_, err = io.WriteString(conn, "POST / HTTP/1.1\r\nHost: slow\r\nContent-Length: 10\r\n\r\nx")
				if err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-entered:
					if err == nil {
						t.Fatal("slow body unexpectedly succeeded")
					}
				case <-time.After(time.Second):
					t.Fatal("slow body read did not terminate")
				}
			}
		})
	}
}

func TestHTTPTransportBoundsSlowResponseAndKeepalive(t *testing.T) {
	for _, stage := range []string{"write", "idle"} {
		t.Run(stage, func(t *testing.T) {
			server := &http.Server{ReadHeaderTimeout: time.Second, WriteTimeout: 40 * time.Millisecond, IdleTimeout: 40 * time.Millisecond, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if stage == "write" {
					time.Sleep(100 * time.Millisecond)
				}
				fmt.Fprint(w, "ok")
			})}
			listener := serveHTTPBoundaryTest(t, server)
			conn, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: slow\r\n\r\n")
			reader := bufio.NewReader(conn)
			response, err := http.ReadResponse(reader, nil)
			if stage == "write" {
				if err == nil {
					response.Body.Close()
					t.Fatal("late response succeeded past write deadline")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			_, err = reader.ReadByte()
			if err == nil {
				t.Fatal("idle connection remained open")
			}
		})
	}
}

func TestHTTPWebSocketHijackingSurvivesAPIWrapper(t *testing.T) {
	hijacked := make(chan error, 1)
	handler := boundedAPIRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, ok := w.(http.Hijacker)
		if !ok {
			hijacked <- fmt.Errorf("missing Hijacker")
			return
		}
		conn, _, err := h.Hijack()
		if err == nil {
			conn.Close()
		}
		hijacked <- err
	}), time.Second)
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/ws", nil)
	response, _ := server.Client().Do(req)
	if response != nil {
		response.Body.Close()
	}
	if err := <-hijacked; err != nil {
		t.Fatal(err)
	}
}

func TestHTTPTransportStopsClientThatDoesNotRead(t *testing.T) {
	stopped := make(chan error, 1)
	server := &http.Server{ReadHeaderTimeout: time.Second, WriteTimeout: 50 * time.Millisecond, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chunk := make([]byte, 64<<10)
		for i := 0; i < 1024; i++ {
			if _, err := w.Write(chunk); err != nil {
				stopped <- err
				return
			}
		}
		stopped <- nil
	})}
	listener := serveHTTPBoundaryTest(t, server)
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(1024)
	}
	_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: slow\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if err == nil {
			t.Fatal("unread response did not encounter write deadline")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unread response kept handler blocked")
	}
}

func TestAPIRequestDeadlineInterruptsSlowBody(t *testing.T) {
	stopped := make(chan error, 1)
	handler := boundedAPIRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, err := io.ReadAll(r.Body); stopped <- err }), 50*time.Millisecond)
	server := &http.Server{ReadHeaderTimeout: time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: time.Second, Handler: handler}
	listener := serveHTTPBoundaryTest(t, server)
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	start := time.Now()
	_, err = io.WriteString(conn, "POST /api/user/login HTTP/1.1\r\nHost: slow\r\nContent-Length: 10\r\n\r\nx")
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d", response.StatusCode)
	}
	if time.Since(start) > 750*time.Millisecond {
		t.Fatal("timeout waited for transport ReadTimeout")
	}
	select {
	case err := <-stopped:
		if err == nil {
			t.Fatal("body read completed without timeout")
		}
	case <-time.After(time.Second):
		t.Fatal("body reader remained blocked")
	}
}

func TestHTTPStreamContextDeadlineAndTransportInterruption(t *testing.T) {
	stopped := make(chan error, 1)
	handler := boundedAPIRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		stopped <- r.Context().Err()
	}), time.Second, 30*time.Millisecond)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/api/app/get_job_log?stream=1&download=1", nil))
	if err := <-stopped; err != context.DeadlineExceeded {
		t.Fatalf("stream deadline %v", err)
	}
}

func TestNormalAPICompletionDoesNotPoisonKeepalive(t *testing.T) {
	handler := boundedAPIRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") }), 50*time.Millisecond)
	server := httptest.NewServer(handler)
	defer server.Close()
	client := server.Client()
	for i := 0; i < 3; i++ {
		response, err := client.Get(server.URL + "/api/app/status")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || string(body) != "ok" {
			t.Fatalf("keepalive response %q %v", body, err)
		}
	}
}

func TestHTTPStreamDeadlineInterruptsBlockedWrite(t *testing.T) {
	stopped := make(chan error, 1)
	handler := boundedAPIRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chunk := make([]byte, 64<<10)
		for i := 0; i < 1024; i++ {
			if _, err := w.Write(chunk); err != nil {
				stopped <- err
				return
			}
		}
		stopped <- nil
	}), time.Second, 50*time.Millisecond)
	server := &http.Server{ReadHeaderTimeout: time.Second, WriteTimeout: 3 * time.Second, Handler: handler}
	listener := serveHTTPBoundaryTest(t, server)
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(1024)
	}
	_, err = io.WriteString(conn, "GET /api/app/get_job_log?stream=1&download=1 HTTP/1.1\r\nHost: slow\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if err == nil {
			t.Fatal("stream write escaped timeout")
		}
	case <-time.After(time.Second):
		t.Fatal("stream waited for transport write timeout")
	}
}
