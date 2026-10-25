package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func processRemoteStorageOutageCompletionSpool(t *testing.T, configure func(...*processFixture)) {
	f := newProcessFixture(t)
	configure(f)
	var variable string
	switch f.Config.StorageEngine {
	case "S3":
		variable = "OWLCRON_TEST_S3_CONTAINER"
	case "Couchbase":
		variable = "OWLCRON_TEST_COUCHBASE_CONTAINER"
	case "Redis":
		variable = "OWLCRON_TEST_REDIS_CONTAINER"
	default:
		t.Fatalf("unsupported remote outage backend %q", f.Config.StorageEngine)
	}
	container := os.Getenv(variable)
	if container == "" {
		t.Fatalf("explicit process outage acceptance requires %s", variable)
	}
	control := processRemoteContainerControl(t, container)
	var completions atomic.Int32
	var starts atomic.Int32
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			w.WriteHeader(400)
			return
		}
		if body["action"] == "job_complete" {
			completions.Add(1)
		}
		if body["action"] == "job_start" {
			starts.Add(1)
		}
		w.WriteHeader(204)
	}))
	defer hook.Close()
	f.Config.WebHookAllowedHosts = []string{"127.0.0.1"}
	release := filepath.Join(f.Dir, "release")
	marker := filepath.Join(f.Dir, "executions")
	script := filepath.Join(f.Dir, "outage.sh")
	// Report the successful protocol outcome before the outage, but keep the
	// actual process running until the release barrier. Thus finalization has no
	// protocol update blocked on storage before it writes the local spool.
	body := "IFS= read -r input\nprintf 'execution\\n' >> '" + marker + "'\nprintf 'before-outage\\n'\nprintf '%s\\n' '{\"complete\":true,\"code\":0}'\nwhile test ! -f '" + release + "'; do sleep 0.05; done\nprintf 'after-outage\\n'\nexit 0\n"
	if err := os.WriteFile(script, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	p := f.Start(t)
	id := processRecoveryEvent(t, f, script, "storage-outage", map[string]any{"web_hook": hook.URL})
	awaitProcess(t, 10*time.Second, "plugin executing before storage outage", func() bool { data, _ := os.ReadFile(marker); return string(data) == "execution\n" })
	store := f.Store(t)
	awaitProcess(t, 10*time.Second, "task identity and protocol success durably persisted before storage outage", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rows, err := store.ListGet(ctx, "active_jobs", 0, 0)
		if err != nil || starts.Load() != 1 {
			return false
		}
		for _, row := range rows {
			job, ok := row.(map[string]any)
			if ok && job["id"] == id && job["pid"] != nil && job["pid"] != float64(0) && job["complete"] == true && job["code"] == float64(0) {
				return true
			}
		}
		return false
	})
	control("pause")
	// Cleanup restores only this explicitly labelled, disposable service.
	t.Cleanup(func() { control("unpause"); control("start") })
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	_, err := store.Get(ctx, "events:storage-outage")
	cancel()
	if err == nil {
		t.Fatal("paused real storage accepted a read instead of timing out")
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	spool := filepath.Join(f.Config.LogDir, "jobs", id+".completed.json")
	awaitProcess(t, 45*time.Second, "completion durably spooled while storage paused", func() bool {
		data, err := os.ReadFile(spool)
		if err != nil {
			return false
		}
		var job map[string]any
		return json.Unmarshal(data, &job) == nil && job["complete"] == true && job["code"] == float64(0)
	})
	p.Kill(t)
	control("unpause")
	if f.Config.StorageEngine == "Redis" {
		control("kill") // Real SIGKILL, after daemon death, with AOF-backed storage.
	} else {
		control("stop")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 700*time.Millisecond)
	_, err = store.Get(ctx, "events:storage-outage")
	cancel()
	if err == nil {
		t.Fatal("stopped real storage accepted a read")
	}
	if _, err := os.Stat(spool); err != nil {
		t.Fatal("daemon death discarded completion spool")
	}
	control("start")
	ready := func() {
		awaitProcess(t, 60*time.Second, "committed event survives storage service start/restart", func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := store.Get(ctx, "events:storage-outage")
			return err == nil
		})
	}
	ready()
	control("restart")
	ready()
	f.Start(t)
	awaitProcess(t, 25*time.Second, "spool, remote log, history and notification recovered", func() bool {
		if _, err := os.Stat(spool); !os.IsNotExist(err) {
			return false
		}
		r := f.Request(t, "/api/app/get_job_status", map[string]any{"id": id})
		job, _ := r["job"].(map[string]any)
		return r["complete"] == true && job["code"] == float64(0) && completions.Load() == 1
	})
	log := processRecoveredLog(t, f, id)
	if !strings.Contains(log, "before-outage\n") || !strings.Contains(log, "after-outage\n") {
		t.Fatal("recovered log lost data across storage outage")
	}
	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			if got := processRecoveredLog(t, f, id); got != log {
				t.Error("concurrent readers observed different recovered log contents")
			}
			processAssertSingleHistory(t, f, "storage-outage", id)
		}()
	}
	readers.Wait()
	data, _ := os.ReadFile(marker)
	if string(data) != "execution\n" {
		t.Fatal("completed task replayed after remote storage recovery")
	}
}

func processRemoteContainerControl(t *testing.T, container string) func(string) {
	t.Helper()
	if container == "" {
		t.Fatal("isolated service control requires an explicit container")
	}
	validateAction := func(action string) {
		t.Helper()
		switch action {
		case "inspect", "pause", "unpause", "stop", "start", "restart", "kill":
		default:
			t.Fatalf("unsupported isolated service action %q", action)
		}
	}
	if dir := os.Getenv("OWLCRON_TEST_CONTROL_DIR"); dir != "" {
		request := func(action string) {
			t.Helper()
			validateAction(action)
			var nonce [16]byte
			if _, err := rand.Read(nonce[:]); err != nil {
				t.Fatal(err)
			}
			base := filepath.Join(dir, hex.EncodeToString(nonce[:]))
			body, err := json.Marshal(map[string]string{"container": container, "action": action})
			if err != nil {
				t.Fatal(err)
			}
			// Metadata only: allow a non-root host controller to read requests
			// created by a rootful Docker runner inside this private directory.
			if err := os.WriteFile(base+".tmp", body, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(base+".tmp", 0644); err != nil {
				t.Fatal(err)
			}
			defer os.Remove(base + ".tmp")
			if err := os.Rename(base+".tmp", base+".request"); err != nil {
				t.Fatal(err)
			}
			defer os.Remove(base + ".request")
			defer os.Remove(base + ".response")
			deadline := time.Now().Add(70 * time.Second)
			for time.Now().Before(deadline) {
				raw, err := os.ReadFile(base + ".response")
				if os.IsNotExist(err) {
					time.Sleep(30 * time.Millisecond)
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
				if err := json.Unmarshal(raw, &response); err != nil {
					t.Fatal("invalid isolated storage control response")
				}
				if !response.OK {
					t.Fatalf("isolated storage controller rejected %s", action)
				}
				if action == "inspect" && response.Labels["io.owlcron.acceptance"] != "true" {
					t.Fatal("refusing to fault storage container without io.owlcron.acceptance=true")
				}
				return
			}
			t.Fatal("isolated storage controller response timeout")
		}
		request("inspect")
		return request
	}
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", "/var/run/docker.sock")
	}}, Timeout: 60 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	path := "/containers/" + url.PathEscape(container)
	request := func(method, suffix string) []byte {
		t.Helper()
		r, err := http.NewRequest(method, "http://docker"+path+suffix, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal("isolated remote storage service control failed")
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode >= 300 {
			t.Fatalf("isolated storage service control returned HTTP %d", resp.StatusCode)
		}
		return body
	}
	var inspection struct {
		Config struct{ Labels map[string]string }
		State  struct{ Paused, Running bool }
	}
	if err := json.Unmarshal(request("GET", "/json"), &inspection); err != nil {
		t.Fatal(err)
	}
	if inspection.Config.Labels["io.owlcron.acceptance"] != "true" {
		t.Fatal("refusing to fault storage container without io.owlcron.acceptance=true")
	}
	return func(action string) {
		validateAction(action)
		if action == "unpause" || action == "start" {
			if err := json.Unmarshal(request("GET", "/json"), &inspection); err != nil {
				t.Fatal(err)
			}
			if action == "unpause" && !inspection.State.Paused {
				return
			}
			if action == "start" && inspection.State.Running {
				return
			}
		}
		suffix := "/" + action
		if action == "stop" || action == "restart" {
			suffix += fmt.Sprint("?t=1")
		}
		if action == "kill" {
			allowed := false
			for _, variable := range []string{"OWLCRON_TEST_REDIS_CONTAINER", "OWLCRON_TEST_S3_CONTAINER", "OWLCRON_TEST_COUCHBASE_CONTAINER"} {
				allowed = allowed || os.Getenv(variable) == container
			}
			if !allowed {
				t.Fatal("SIGKILL requires a container explicitly registered by this acceptance run")
			}
			if err := json.Unmarshal(request("GET", "/json"), &inspection); err != nil || inspection.Config.Labels["io.owlcron.acceptance"] != "true" {
				t.Fatal("SIGKILL target no longer belongs to this acceptance run")
			}
			suffix += "?signal=KILL"
		}
		request("POST", suffix)
	}
}
