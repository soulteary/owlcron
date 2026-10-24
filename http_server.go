package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/soulteary/owlcron/types"
)

// newHTTPServer applies deadlines to the transport as well as bounded API work.
// WebSockets retain the original ResponseWriter and its Hijacker capability.
func newHTTPServer(config *types.Config, handler http.Handler) (*http.Server, error) {
	if err := types.ValidateHTTPConfig(config); err != nil {
		return nil, err
	}
	seconds := func(value, fallback int) time.Duration {
		if value == 0 {
			value = fallback
		}
		return time.Duration(value) * time.Second
	}
	header := seconds(config.HTTPReadHeaderTimeoutSeconds, 10)
	read := seconds(config.HTTPReadTimeoutSeconds, 30)
	write := seconds(config.HTTPWriteTimeoutSeconds, 35)
	idle := seconds(config.HTTPIdleTimeoutSeconds, 60)
	request := seconds(config.HTTPRequestTimeoutSeconds, 30)
	stream := seconds(config.HTTPStreamTimeoutSeconds, 300)
	return &http.Server{
		Addr:              fmt.Sprintf(":%d", config.HTTPPort),
		ReadHeaderTimeout: header, ReadTimeout: read, WriteTimeout: write, IdleTimeout: idle,
		MaxHeaderBytes: 1 << 20,
		Handler:        serveBasePath(config.BasePath, boundedAPIRequests(handler, request, stream)),
	}, nil
}

func boundedAPIRequests(handler http.Handler, timeout time.Duration, streamTimeouts ...time.Duration) http.Handler {
	streamTimeout := 300 * time.Second
	if len(streamTimeouts) > 0 {
		streamTimeout = streamTimeouts[0]
	}
	bounded := http.TimeoutHandler(handler, timeout, `{"code":1,"description":"Request deadline exceeded"}`)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Streaming logs have a bounded initial snapshot and independent per-chunk
		// write deadlines. Buffering them here would defeat bounded-memory downloads.
		path := strings.TrimSuffix(r.URL.Path, "/v1")
		streaming := r.Method == http.MethodGet && r.URL.Query().Get("stream") == "1" && r.URL.Query().Get("download") == "1" &&
			(path == "/api/app/get_job_log" || path == "/api/app/get_live_job_log")
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			handler.ServeHTTP(w, r)
			return
		}
		duration := timeout
		if streaming {
			duration = streamTimeout
		}
		ctx, cancel := context.WithTimeout(r.Context(), duration)
		// A context alone cannot interrupt a blocked Body.Read. Set the transport
		// deadline on expiry so the timeout response cannot wait behind that read.
		callbackDone := make(chan struct{})
		stop := context.AfterFunc(ctx, func() {
			defer close(callbackDone)
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				controller := http.NewResponseController(w)
				_ = controller.SetReadDeadline(time.Now())
				if streaming {
					_ = controller.SetWriteDeadline(time.Now())
				}
			}
		})
		defer func() {
			// Do not let a callback alter deadlines after this connection is reused.
			if !stop() {
				<-callbackDone
			}
			cancel()
		}()
		if streaming {
			handler.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		bounded.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Strip the public prefix before auth and deadline routing. Cluster transport
// stays at its canonical root path and is authenticated by the node endpoint.
func serveBasePath(prefix string, next http.Handler) http.Handler {
	if prefix == "" {
		return next
	}
	stripped := http.StripPrefix(prefix, next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cluster/ws" {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == prefix || r.URL.Path == "/" {
			http.Redirect(w, r, prefix+"/", http.StatusTemporaryRedirect)
			return
		}
		escaped := strings.ToLower(r.URL.EscapedPath())
		if !strings.HasPrefix(r.URL.Path, prefix+"/") || strings.Contains(escaped, "%2f") || strings.Contains(escaped, "%5c") || strings.HasPrefix(r.URL.Path, prefix+"/cluster/") {
			http.NotFound(w, r)
			return
		}
		stripped.ServeHTTP(w, r)
	})
}
