package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newRemoteContainerControl only operates on an explicitly named disposable service.
func newRemoteContainerControl(t *testing.T, container string) func(*testing.T, string) {
	t.Helper()
	if container == "" {
		t.Fatal("isolated-service control requires an explicit container")
	}
	if controlDir := os.Getenv("OWLCRON_TEST_CONTROL_DIR"); controlDir != "" {
		exchange := func(t *testing.T, action string) map[string]string {
			t.Helper()
			var nonce [16]byte
			if _, err := rand.Read(nonce[:]); err != nil {
				t.Fatal(err)
			}
			base := filepath.Join(controlDir, hex.EncodeToString(nonce[:]))
			requestPath, responsePath := base+".request", base+".response"
			tempPath := base + ".tmp"
			defer os.Remove(tempPath)
			defer os.Remove(requestPath)
			defer os.Remove(responsePath)
			payload, err := json.Marshal(struct {
				Container string `json:"container"`
				Action    string `json:"action"`
			}{container, action})
			if err != nil {
				t.Fatal(err)
			}
			// Only container/action metadata crosses this private IPC directory.
			// Rootful Docker and the host controller may have different UIDs.
			if err := os.WriteFile(tempPath, payload, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(tempPath, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(tempPath, requestPath); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(70 * time.Second)
			for time.Now().Before(deadline) {
				data, err := os.ReadFile(responsePath)
				if os.IsNotExist(err) {
					time.Sleep(20 * time.Millisecond)
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				var response struct {
					OK     bool              `json:"ok"`
					Error  string            `json:"error"`
					Labels map[string]string `json:"labels"`
				}
				if err := json.Unmarshal(data, &response); err != nil {
					t.Fatalf("decode service controller response: %v", err)
				}
				if !response.OK {
					t.Fatalf("isolated service %s failed: %s", action, response.Error)
				}
				return response.Labels
			}
			t.Fatalf("isolated service controller timed out on %s", action)
			return nil
		}
		if exchange(t, "inspect")["io.owlcron.acceptance"] != "true" {
			t.Fatal("refusing to fault service container without io.owlcron.acceptance=true")
		}
		return func(t *testing.T, action string) {
			t.Helper()
			switch action {
			case "pause", "unpause", "stop", "start", "restart", "kill":
			default:
				t.Fatalf("unsupported isolated-service control action %q", action)
			}
			exchange(t, action)
		}
	}
	// Use the engine API so the Linux runner does not need a Docker CLI.
	docker := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", "/var/run/docker.sock")
	}}, Timeout: 60 * time.Second}
	t.Cleanup(func() { docker.CloseIdleConnections() })
	request := func(t *testing.T, method, path string) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), method, "http://docker"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := docker.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			t.Fatalf("isolated service control %s: HTTP %d: %s", path, resp.StatusCode, body)
		}
		return body
	}
	containerPath := "/containers/" + url.PathEscape(container)
	var inspection struct {
		Config struct{ Labels map[string]string }
	}
	if err := json.Unmarshal(request(t, http.MethodGet, containerPath+"/json"), &inspection); err != nil {
		t.Fatal(err)
	}
	if inspection.Config.Labels["io.owlcron.acceptance"] != "true" {
		t.Fatal("refusing to fault service container without io.owlcron.acceptance=true")
	}
	return func(t *testing.T, action string) {
		t.Helper()
		switch action {
		case "pause", "unpause", "stop", "start", "restart", "kill":
		default:
			t.Fatalf("unsupported isolated-service control action %q", action)
		}
		suffix := ""
		if action == "stop" || action == "restart" {
			suffix = "?t=1"
		}
		if action == "kill" {
			allowed := false
			for _, variable := range []string{"OWLCRON_TEST_REDIS_CONTAINER", "OWLCRON_TEST_S3_CONTAINER", "OWLCRON_TEST_COUCHBASE_CONTAINER"} {
				allowed = allowed || os.Getenv(variable) == container
			}
			if !allowed {
				t.Fatal("SIGKILL requires a container explicitly registered by this acceptance run")
			}
			// Revalidate the ownership label immediately before the destructive action.
			if err := json.Unmarshal(request(t, http.MethodGet, containerPath+"/json"), &inspection); err != nil || inspection.Config.Labels["io.owlcron.acceptance"] != "true" {
				t.Fatal("SIGKILL target no longer belongs to this acceptance run")
			}
			suffix = "?signal=KILL"
		}
		request(t, http.MethodPost, containerPath+"/"+action+suffix)
	}

}

// This validates the controller IPC only; remote acceptance still uses real services.
func TestRemoteContainerControlFileProtocol(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("OWLCRON_TEST_CONTROL_DIR", dir)
	stop := make(chan struct{})
	done := make(chan struct{})
	actions := make(chan string, 4)
	errs := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				files, err := filepath.Glob(filepath.Join(dir, "*.request"))
				if err != nil {
					errs <- err
					return
				}
				for _, path := range files {
					if info, err := os.Stat(path); err != nil || info.Mode().Perm()&0044 != 0044 {
						errs <- fmt.Errorf("control metadata is not readable across container/host UIDs: %v", err)
						return
					}
					data, err := os.ReadFile(path)
					if err != nil {
						errs <- err
						return
					}
					var req struct{ Container, Action string }
					if err := json.Unmarshal(data, &req); err != nil {
						errs <- err
						return
					}
					if req.Container != "isolated-test-service" {
						errs <- fmt.Errorf("unexpected service %q", req.Container)
						return
					}
					actions <- req.Action
					body := []byte(`{"ok":true,"labels":{"io.owlcron.acceptance":"true"}}`)
					temp := strings.TrimSuffix(path, ".request") + ".controller-tmp"
					if err := os.WriteFile(temp, body, 0600); err != nil {
						errs <- err
						return
					}
					if err := os.Remove(path); err != nil {
						errs <- err
						return
					}
					if err := os.Rename(temp, strings.TrimSuffix(path, ".request")+".response"); err != nil {
						errs <- err
						return
					}
				}
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
		select {
		case err := <-errs:
			t.Error(err)
		default:
		}
	})
	control := newRemoteContainerControl(t, "isolated-test-service")
	control(t, "pause")
	control(t, "unpause")
	control(t, "kill")
	for _, want := range []string{"inspect", "pause", "unpause", "kill"} {
		if got := <-actions; got != want {
			t.Fatalf("action %q, want %q", got, want)
		}
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("IPC files leaked after completion: %v %v", files, err)
	}
}
