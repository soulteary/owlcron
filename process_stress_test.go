package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This bounded default regression keeps the original 15-second convergence
// deadline, and adds parallel API traffic and deliberately slow receivers.
func TestProcessClusterLoadedConvergence(t *testing.T) {
	processClusterWithLoad(t, nil, processClusterLoad{Profile: "cluster_load", Readers: 8, Concurrent: 8, Batches: 2, HookDelay: 100 * time.Millisecond})
}

// Each iteration creates and destroys three real daemons. Duration is a minimum
// observation window; an iteration is always allowed to finish its fault matrix.
func TestProcessClusterSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("independent-process sustained acceptance")
	}
	raw := os.Getenv("OWLCRON_PROCESS_SOAK_DURATION")
	if raw == "" {
		t.Skip("set OWLCRON_PROCESS_SOAK_DURATION to enable sustained process acceptance")
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration < 30*time.Second || duration > 24*time.Hour {
		t.Fatal("OWLCRON_PROCESS_SOAK_DURATION must be between 30s and 24h")
	}
	parallel := 8
	if raw := os.Getenv("OWLCRON_PROCESS_SOAK_PARALLEL"); raw != "" {
		parallel, err = strconv.Atoi(raw)
		if err != nil || parallel < 1 || parallel > 12 {
			t.Fatal("OWLCRON_PROCESS_SOAK_PARALLEL must be between 1 and 12")
		}
	}
	started := time.Now()
	cycles, jobs := 0, 0
	var maxRTO time.Duration
	for time.Since(started) < duration {
		cycles++
		t.Run(strconv.Itoa(cycles), func(t *testing.T) {
			processClusterWithLoad(t, nil, processClusterLoad{Profile: "cluster_fault_soak", Readers: 16, Concurrent: parallel, Batches: 4, HookDelay: 100 * time.Millisecond, Observe: func(m processClusterMeasurement) {
				jobs += m.Jobs
				if m.FailoverRTO > maxRTO {
					maxRTO = m.FailoverRTO
				}
			}})
		})
		if t.Failed() {
			break
		}
	}
	t.Logf("sustained acceptance cycles=%d jobs=%d elapsed=%s max_failover_to_new_completion=%s", cycles, jobs, time.Since(started), maxRTO)
}

type processClusterReaders struct {
	Stop  func()
	Stats func() (int64, int64, int64, int64)
	Fault func(*processFixture, bool)
}

func startProcessClusterReaders(t *testing.T, count int, fixtures ...*processFixture) processClusterReaders {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var requests, healthyErrors, faultErrors, canceledErrors atomic.Int64
	var faultMu sync.Mutex
	faults := map[*processFixture]bool{}
	isFault := func(f *processFixture) bool { faultMu.Lock(); defer faultMu.Unlock(); return faults[f] }
	for i := 0; i < count; i++ {
		fixture := fixtures[i%len(fixtures)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(25 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				faultBefore := isFault(fixture)
				req, err := http.NewRequestWithContext(ctx, "POST", fixture.URL+"/api/app/list_servers", bytes.NewBufferString("{}"))
				if err == nil {
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("X-API-Key", fixture.APIKey)
					resp, requestErr := fixture.Client.Do(req)
					requests.Add(1)
					err = requestErr
					if resp != nil {
						readErr := validateProcessClusterReadResponse(resp)
						resp.Body.Close()
						if err == nil {
							err = readErr
						}
					}
				}
				if err != nil {
					switch {
					case ctx.Err() != nil:
						canceledErrors.Add(1)
					case faultBefore || isFault(fixture):
						faultErrors.Add(1)
					default:
						healthyErrors.Add(1)
					}
				}
			}
		}()
	}
	var once sync.Once
	return processClusterReaders{
		Stats: func() (int64, int64, int64, int64) {
			return requests.Load(), healthyErrors.Load(), faultErrors.Load(), canceledErrors.Load()
		},
		Fault: func(f *processFixture, fault bool) { faultMu.Lock(); faults[f] = fault; faultMu.Unlock() },
		Stop: func() {
			once.Do(func() {
				cancel()
				wg.Wait()
				t.Logf("background cluster reads=%d healthy_errors=%d fault_window_errors=%d canceled_errors=%d", requests.Load(), healthyErrors.Load(), faultErrors.Load(), canceledErrors.Load())
				if n := healthyErrors.Load(); n > 0 {
					t.Errorf("healthy cluster API read failures=%d, budget 0", n)
				}
			})
		},
	}
}

// Result files contain only measurements, never fixture configuration or payloads.
func writeProcessClusterEvidence(t *testing.T, load processClusterLoad, m processClusterMeasurement) {
	t.Helper()
	dir := os.Getenv("OWLCRON_ACCEPTANCE_RESULTS")
	if t.Skipped() || dir == "" || load.Profile == "" {
		return
	}
	result := struct {
		Profile            string  `json:"profile"`
		Platform           string  `json:"platform"`
		Passed             bool    `json:"passed"`
		Jobs               int     `json:"completed"`
		Parallel           int     `json:"parallel_jobs"`
		Readers            int     `json:"readers"`
		HookDelayMS        int64   `json:"hook_delay_ms"`
		ElapsedSeconds     float64 `json:"elapsed_seconds"`
		MaxDispatchMS      float64 `json:"max_dispatch_ms"`
		FailoverRTOMS      float64 `json:"failover_to_new_completion_ms"`
		RTOLimitMS         int     `json:"rto_limit_ms"`
		ConvergenceLimitMS int     `json:"convergence_limit_ms"`
		CompletedPerSecond float64 `json:"completed_per_second"`
	}{load.Profile, runtime.GOOS + "/" + runtime.GOARCH, m.Passed, m.Completed, load.Concurrent, load.Readers, load.HookDelay.Milliseconds(), m.Elapsed.Seconds(), float64(m.MaxDispatch) / float64(time.Millisecond), float64(m.FailoverRTO) / float64(time.Millisecond), 30000, 15000, 0}
	if m.Elapsed > 0 {
		result.CompletedPerSecond = float64(m.Completed) / m.Elapsed.Seconds()
	}
	dataMap := map[string]any{}
	initial, _ := json.Marshal(result)
	_ = json.Unmarshal(initial, &dataMap)
	dataMap["accepted"] = m.Accepted
	dataMap["background_reads"] = m.Reads
	dataMap["healthy_errors"] = m.HealthyErrors
	dataMap["fault_window_errors"] = m.FaultWindowErrors
	dataMap["canceled_errors"] = m.CanceledErrors
	data, err := json.MarshalIndent(dataMap, "", "  ")
	if err == nil {
		err = os.MkdirAll(dir, 0700)
	}
	name := load.EvidencePath
	if name == "" {
		name = processClusterEvidencePath(load.Profile)
	}
	temporary := name + ".tmp"
	if err == nil {
		err = os.WriteFile(temporary, append(data, '\n'), 0600)
		if err == nil {
			err = os.Rename(temporary, name)
		}
	}
	if err != nil {
		t.Errorf("write cluster acceptance measurements: %v", err)
	}
}

func processClusterEvidencePath(profile string) string {
	dir := os.Getenv("OWLCRON_ACCEPTANCE_RESULTS")
	if dir == "" || profile == "" {
		return ""
	}
	return filepath.Join(dir, fmt.Sprintf("%s-%d.json", profile, time.Now().UnixNano()))
}

// Register before creating fixtures: testing runs cleanups in reverse order,
// so teardown errors are included in the final artifact without extending the
// measured workload duration by fixture shutdown time.
func registerProcessClusterFinalEvidence(t *testing.T, load processClusterLoad, metrics *processClusterMeasurement, started time.Time) func() {
	t.Helper()
	if !testing.Short() {
		writeProcessClusterEvidence(t, load, *metrics)
	}
	t.Cleanup(func() {
		metrics.Passed = !t.Failed()
		writeProcessClusterEvidence(t, load, *metrics)
	})
	return func() { metrics.Elapsed = time.Since(started) }
}

// A real failing child test proves that teardown failure can never leave a
// success artifact, even after all workload assertions have passed.
func TestProcessClusterEvidenceIncludesCleanupFailure(t *testing.T) {
	if os.Getenv("OWLCRON_EVIDENCE_CLEANUP_CHILD") == "1" {
		metrics := processClusterMeasurement{Accepted: 1, Completed: 1, FailoverRTO: time.Second}
		load := processClusterLoad{Profile: "cluster_load"}
		load.EvidencePath = processClusterEvidencePath(load.Profile)
		end := registerProcessClusterFinalEvidence(t, load, &metrics, time.Now().Add(-time.Second))
		defer end()
		t.Cleanup(func() { t.Error("injected daemon cleanup failure") })
		return
	}
	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestProcessClusterEvidenceIncludesCleanupFailure$", "-test.count=1")
	cmd.Env = append(os.Environ(), "OWLCRON_EVIDENCE_CLEANUP_CHILD=1", "OWLCRON_ACCEPTANCE_RESULTS="+dir)
	output, err := cmd.CombinedOutput()
	if err == nil || ctx.Err() != nil {
		t.Fatalf("child must finish with cleanup failure: err=%v context=%v output=%s", err, ctx.Err(), output)
	}
	files, err := filepath.Glob(filepath.Join(dir, "cluster_load-*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("expected one final measurement artifact: files=%v err=%v output=%s", files, err, output)
	}
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err = json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	if record["passed"] != false || record["accepted"] != float64(1) || record["completed"] != float64(1) || record["elapsed_seconds"].(float64) < 1 {
		t.Fatalf("cleanup failure must retain measurements and reject success: %#v", record)
	}
}

// HTTP success alone cannot establish that the authenticated application read
// succeeded. Bound the complete body and reject missing or malformed fields.
func validateProcessClusterReadResponse(resp *http.Response) error {
	const maxBody = 1 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return err
	}
	if len(raw) > maxBody {
		return fmt.Errorf("cluster read response exceeds %d bytes", maxBody)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cluster read status %d", resp.StatusCode)
	}
	var result struct {
		Code    *int                       `json:"code"`
		Servers map[string]json.RawMessage `json:"servers"`
	}
	if err = json.Unmarshal(raw, &result); err != nil {
		return fmt.Errorf("invalid cluster read JSON: %w", err)
	}
	if result.Code == nil || *result.Code != 0 {
		return fmt.Errorf("cluster read application error or missing code")
	}
	if result.Servers == nil {
		return fmt.Errorf("cluster read missing servers object")
	}
	for _, server := range result.Servers {
		var object map[string]json.RawMessage
		if err = json.Unmarshal(server, &object); err != nil || object == nil {
			return fmt.Errorf("cluster read invalid server object")
		}
	}
	return nil
}

func TestProcessClusterReadRejectsApplicationAndMalformedResponses(t *testing.T) {
	cases := []struct {
		name, body string
		valid      bool
	}{
		{"success", `{"code":0,"servers":{"worker":{"hostname":"worker","disabled":false}}}`, true},
		{"empty_cluster", `{"code":0,"servers":{}}`, true},
		{"application_error", `{"code":1,"servers":{}}`, false},
		{"bad_json", `{"code":0,`, false},
		{"missing_code", `{"servers":{}}`, false},
		{"null_servers", `{"code":0,"servers":null}`, false},
		{"array_servers", `{"code":0,"servers":[]}`, false},
		{"null_server", `{"code":0,"servers":{"worker":null}}`, false},
		{"trailing_json", `{"code":0,"servers":{}} {}`, false},
		{"oversized", `{"code":0,"servers":{},"padding":"` + strings.Repeat("x", 1<<20) + `"}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			response, err := server.Client().Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			err = validateProcessClusterReadResponse(response)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v validation_error=%v", tc.valid, err)
			}
		})
	}
}

// One fixed deadline is shared by all observers in a released batch. Check the
// clock after the callback too: a slow callback must never turn late work green.
func awaitProcessClusterUntil(deadline time.Time, predicate func(context.Context) bool) (time.Time, bool) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	for time.Now().Before(deadline) {
		if predicate(ctx) {
			observedAt := time.Now()
			if !observedAt.After(deadline) && ctx.Err() == nil {
				return observedAt, true
			}
			return time.Time{}, false
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return time.Time{}, false
		case <-timer.C:
		}
	}
	return time.Time{}, false
}

func processClusterRequestContext(ctx context.Context, fixture *processFixture, path string, body any) (map[string]any, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", fixture.URL+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", fixture.APIKey)
	response, err := fixture.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	raw, err = io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 4<<20 {
		return nil, fmt.Errorf("cluster observer response too large")
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cluster observer status %d", response.StatusCode)
	}
	var result map[string]any
	err = json.Unmarshal(raw, &result)
	return result, err
}

func TestProcessClusterDeadlineRejectsLateSuccess(t *testing.T) {
	deadline := time.Now().Add(20 * time.Millisecond)
	_, ok := awaitProcessClusterUntil(deadline, func(ctx context.Context) bool { time.Sleep(60 * time.Millisecond); return true })
	if ok {
		t.Fatal("callback success after the deadline was accepted")
	}
	// A second job cannot borrow time spent observing the first one.
	_, ok = awaitProcessClusterUntil(deadline, func(ctx context.Context) bool { return true })
	if ok {
		t.Fatal("expired batch deadline was restarted for the next job")
	}
}
