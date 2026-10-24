package api

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// Exercise the same authenticated API, scheduler and plugin lifecycle against
// each real backend. Remote tests opt in to disposable integration services.
func TestSingleNodeSystemLifecycle(t *testing.T) {
	for _, backend := range []string{"Filesystem", "S3", "Couchbase"} {
		t.Run(backend, func(t *testing.T) {
			dir := t.TempDir()
			cfg := &types.Config{Hostname: "system-test", HTTPPort: 3012, StorageEngine: backend, StorageBaseDir: filepath.Join(dir, "data"), LogDir: filepath.Join(dir, "logs"), QueueDir: filepath.Join(dir, "queue"), SecretKey: "integration-secret-0123456789abcdef", MasterPingTimeout: 60, SchedulerStartupGrace: 3600}
			prefix := "system-" + time.Now().Format("20060102-150405.000000000")
			switch backend {
			case "S3":
				endpoint := os.Getenv("OWLCRON_TEST_S3_ENDPOINT")
				if endpoint == "" {
					t.Skip("set OWLCRON_TEST_S3_ENDPOINT and create owlcron-integration bucket")
				}
				cfg.StorageOptions = map[string]interface{}{"endpoint": endpoint, "bucket": "owlcron-integration", "access_key": "owlcron-test", "secret_key": "owlcron-test-secret", "prefix": prefix}
			case "Couchbase":
				connection := os.Getenv("OWLCRON_TEST_COUCHBASE_CONNECTION")
				if connection == "" {
					t.Skip("set OWLCRON_TEST_COUCHBASE_CONNECTION")
				}
				cfg.StorageOptions = map[string]interface{}{"connection_string": connection, "bucket": "owlcron-integration", "username": "Administrator", "password": "owlcron-test-secret", "prefix": prefix + ":"}
			}
			makeEngine := func() (*engine.Engine, http.Handler) {
				e, err := engine.NewEngine(cfg, zap.NewNop())
				if err != nil {
					t.Fatal(err)
				}
				if err = e.SaveUserToStorage(&types.User{ID: "admin", Username: "admin", Enabled: true, Privileges: types.Privileges{"admin": true}}); err != nil {
					t.Fatal(err)
				}
				if err = e.Startup(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = e.Shutdown() })
				if !e.EnsureLeadership() {
					t.Fatal("not leader")
				}
				mux := http.NewServeMux()
				NewServer(e, zap.NewNop()).RegisterRoutes(mux)
				return e, mux
			}
			e, handler := makeEngine()
			session := addSecurityUser(t, e, "operator", types.Privileges{"admin": true})
			script := filepath.Join(dir, "plugin.sh")
			if err := os.WriteFile(script, []byte("read input\nprintf 'INPUT:%s\\n' \"$input\"\nprintf '%s\\n' '{\"progress\":0.5}'\nsleep 0.5\nprintf '%s\\n' '{\"complete\":1,\"code\":0,\"description\":\"system complete\"}'\nprintf 'SYSTEM LOG\\n'\n"), 0700); err != nil {
				t.Fatal(err)
			}
			command, _ := json.Marshal("/bin/sh " + script)
			requests := []struct{ path, body string }{
				{"create_category", `{"id":"general","title":"General","enabled":1}`},
				{"create_plugin", `{"id":"protocol","title":"Protocol","enabled":1,"command":` + string(command) + `,"params":[{"id":"answer","type":"text","value":"default"}]}`},
				{"create_event", `{"id":"scheduled","title":"System Event","enabled":1,"category":"general","target":"local","plugin":"protocol","params":{"answer":"integrated"},"timing":{},"timezone":"UTC","max_children":1}`},
			}
			for _, request := range requests {
				response := securityRequest(t, handler, "/api/app/"+request.path, request.body, session)
				if response["code"] != float64(0) {
					t.Fatalf("%s: %#v", request.path, response)
				}
			}
			minute := time.Now().Truncate(time.Minute).Add(time.Minute)
			e.GetScheduler().MinuteTickAt(minute)
			deadline := time.Now().Add(10 * time.Second)
			id := ""
			progress := false
			for time.Now().Before(deadline) {
				for candidate, job := range e.GetActiveJobs() {
					if job.Event == "scheduled" {
						id = candidate
						if job.Progress == 0.5 {
							progress = true
						}
					}
				}
				if id != "" {
					job, ok := e.LookupJob(id)
					if ok && job.Complete && len(e.GetActiveJobs()) == 0 {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
			if id == "" || !progress {
				t.Fatal("scheduled dispatch or plugin progress not observed")
			}
			details := securityRequest(t, handler, "/api/app/get_job_details", `{"id":"`+id+`"}`, session)
			if details["code"] != float64(0) {
				t.Fatalf("details: %#v", details)
			}
			job := details["job"].(map[string]interface{})
			if job["complete"] != true || job["code"] != float64(0) || job["description"] != "system complete" {
				t.Fatalf("completion: %#v", job)
			}
			log := securityRequest(t, handler, "/api/app/get_job_log", `{"id":"`+id+`"}`, session)
			if !strings.Contains(log["log"].(string), "SYSTEM LOG") || !strings.Contains(log["log"].(string), `"answer":"integrated"`) {
				t.Fatalf("log or params lost: %#v", log)
			}
			if err := e.Shutdown(); err != nil {
				t.Fatal(err)
			}
			restored, handler := makeEngine()
			restored.GetScheduler().MinuteTickAt(minute)
			history := securityRequest(t, handler, "/api/app/get_event_history", `{"id":"scheduled"}`, session)
			if history["code"] != float64(0) || len(history["rows"].([]interface{})) != 1 {
				t.Fatalf("restart duplicated/dropped history: %#v", history)
			}
			details = securityRequest(t, handler, "/api/app/get_job_details", `{"id":"`+id+`"}`, session)
			if details["code"] != float64(0) || details["job"].(map[string]interface{})["description"] != "system complete" {
				t.Fatal("restart detail lost")
			}
			log = securityRequest(t, handler, "/api/app/get_job_log", `{"id":"`+id+`"}`, session)
			if log["code"] != float64(0) || !strings.Contains(log["log"].(string), "SYSTEM LOG") {
				t.Fatal("restart log lost")
			}
		})
	}
}
