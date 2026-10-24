package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Samples are bounded histograms, so a 24-hour run does not retain every request.
type processLoadMetrics struct {
	RaceEnabled                                  bool `json:"race_enabled"`
	measurementEnd                               time.Time
	Backend                                      string  `json:"backend"`
	SetupSeconds                                 float64 `json:"setup_seconds"`
	MeasurementSeconds                           float64 `json:"measurement_seconds"`
	measurementStart                             time.Time
	EventCount                                   int   `json:"event_count"`
	Concurrency                                  int   `json:"concurrency"`
	LogBytes                                     int64 `json:"verified_log_bytes"`
	APISamples                                   int64 `json:"api_samples"`
	mu                                           sync.Mutex
	Profile                                      string             `json:"profile"`
	Platform                                     string             `json:"platform"`
	Started                                      time.Time          `json:"started"`
	ElapsedSeconds                               float64            `json:"elapsed_seconds"`
	Accepted                                     int                `json:"accepted"`
	Completed                                    int                `json:"completed"`
	Errors                                       int                `json:"errors"`
	Cycles                                       int                `json:"cycles"`
	RSSPeakBytes                                 int64              `json:"rss_sampled_peak_bytes,omitempty"`
	API                                          map[string]float64 `json:"api_latency_ms"`
	Job                                          map[string]float64 `json:"job_latency_ms"`
	Restart                                      map[string]float64 `json:"restart_ready_ms,omitempty"`
	Throughput                                   float64            `json:"completed_per_second"`
	Passed                                       bool               `json:"passed"`
	apiHistogram, jobHistogram, restartHistogram [22]int64
}

func processLoadObserve(hist *[22]int64, d time.Duration) {
	ms := (d + time.Millisecond - 1) / time.Millisecond
	i := 0
	for i < len(hist)-1 && ms > 1<<i {
		i++
	}
	hist[i]++
}
func processLoadPercentiles(hist [22]int64) map[string]float64 {
	var count int64
	for _, n := range hist {
		count += n
	}
	out := map[string]float64{}
	for _, q := range []struct {
		name  string
		value float64
	}{{"p50_upper_bound", .50}, {"p95_upper_bound", .95}, {"p99_upper_bound", .99}} {
		var seen int64
		for i, n := range hist {
			seen += n
			if count > 0 && float64(seen) >= float64(count)*q.value {
				out[q.name] = float64(int64(1) << i)
				break
			}
		}
	}
	return out
}
func processLoadEnvInt(t *testing.T, key string, fallback, max int) int {
	t.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > max {
		t.Fatalf("%s must be between 1 and %d", key, max)
	}
	return n
}
func newProcessLoadMetrics(t *testing.T, profile string) *processLoadMetrics {
	t.Helper()
	m := &processLoadMetrics{RaceEnabled: processLoadRaceEnabled, Profile: profile, Platform: runtime.GOOS + "/" + runtime.GOARCH, Started: time.Now()}
	t.Cleanup(func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.ElapsedSeconds = time.Since(m.Started).Seconds()
		if !m.measurementStart.IsZero() {
			end := m.measurementEnd
			if end.IsZero() {
				end = time.Now()
			}
			m.MeasurementSeconds = end.Sub(m.measurementStart).Seconds()
			m.Throughput = float64(m.Completed) / m.MeasurementSeconds
		}
		m.API = processLoadPercentiles(m.apiHistogram)
		m.Job = processLoadPercentiles(m.jobHistogram)
		m.Restart = processLoadPercentiles(m.restartHistogram)
		m.Passed = !t.Failed()
		raw, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("PROCESS_LOAD_METRICS %s", raw)
		if dir := os.Getenv("OWLCRON_ACCEPTANCE_RESULTS"); dir != "" {
			if err = os.MkdirAll(dir, 0700); err == nil {
				err = os.WriteFile(filepath.Join(dir, profile+"-"+m.Started.UTC().Format("20060102T150405.000000000")+".json"), append(raw, '\n'), 0600)
			}
			if err != nil {
				t.Error(err)
			}
		}
	})
	return m
}
func processLoadRequest(f *processFixture, m *processLoadMetrics, path string, body any) (map[string]any, error) {
	started := time.Now()
	defer func() {
		m.mu.Lock()
		processLoadObserve(&m.apiHistogram, time.Since(started))
		m.APISamples++
		m.mu.Unlock()
	}()
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequest("POST", f.URL+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-API-Key", f.APIKey)
	resp, err := f.Client.Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result map[string]any
	err = json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&result)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 || result["code"] != float64(0) {
		return nil, fmt.Errorf("%s status %d: %v", path, resp.StatusCode, result)
	}
	return result, nil
}
func (m *processLoadMetrics) sampleRSS(p *daemonProcess) {
	if runtime.GOOS != "linux" {
		return
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", p.cmd.Process.Pid))
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				return
			}
			n, _ := strconv.ParseInt(fields[1], 10, 64)
			m.mu.Lock()
			if n*1024 > m.RSSPeakBytes {
				m.RSSPeakBytes = n * 1024
			}
			m.mu.Unlock()
			return
		}
	}
}
func processLoadWitness(path string) (map[string]int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, line := range strings.Fields(string(raw)) {
		counts[line]++
	}
	return counts, nil
}
func processLoadSortedIDs(ids map[string]time.Time) []string {
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Budgets are acceptance profile bounds, not deployment service-level claims.
func (m *processLoadMetrics) finish(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	m.measurementEnd = time.Now()
	api := processLoadPercentiles(m.apiHistogram)["p99_upper_bound"]
	job := processLoadPercentiles(m.jobHistogram)["p99_upper_bound"]
	restart := processLoadPercentiles(m.restartHistogram)["p99_upper_bound"]
	rss := m.RSSPeakBytes
	m.mu.Unlock()
	for _, budget := range []struct {
		env      string
		value    float64
		fallback int
	}{{"OWLCRON_LOAD_API_P99_MS", api, 4096}, {"OWLCRON_LOAD_JOB_P99_MS", job, 16384}, {"OWLCRON_LOAD_RESTART_P99_MS", restart, 16384}, {"OWLCRON_LOAD_RSS_MIB", float64(rss) / (1 << 20), 512}} {
		limit := processLoadEnvInt(t, budget.env, budget.fallback, 1<<30)
		if budget.value > float64(limit) {
			t.Errorf("profile acceptance budget %s exceeded: %.2f > %d", budget.env, budget.value, limit)
		}
	}
}
