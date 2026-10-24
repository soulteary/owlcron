package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func TestDailyStatsResetKeepsDaemonUptime(t *testing.T) {
	e := newSchedulingTestEngine(t, "daily-stats")
	now := time.Date(2026, 10, 7, 0, 30, 0, 0, time.Local)
	e.startedAt = now.Add(-time.Hour).Unix()
	e.state.Stats["started"] = float64(e.startedAt)
	e.state.Stats["jobs_completed"] = 2
	e.state.Stats["jobs_elapsed"] = 0.75
	e.resetDailyStats(now)
	if e.GetState().Stats["jobs_completed"] != 2 {
		t.Fatal("first day initialization discarded current totals")
	}
	e.resetDailyStats(now.AddDate(0, 0, 1))
	state := e.GetState()
	if state.Stats["jobs_completed"] != 0 || state.Stats["jobs_elapsed"] != 0 || int64(state.Stats["started"]) != e.startedAt {
		t.Fatal("daily reset changed uptime or retained yesterday's totals")
	}
	prior := &types.State{Enabled: true, Stats: map[string]float64{"started": 1, "jobs_completed": 3, "jobs_elapsed": 0.625}}
	raw, _ := json.Marshal(prior)
	if err := e.storage.Put(context.Background(), "global/state", raw); err != nil {
		t.Fatal(err)
	}
	if err := e.loadStateFromStorage(); err != nil {
		t.Fatal(err)
	}
	if int64(e.GetState().Stats["started"]) != e.startedAt || e.GetState().Stats["jobs_elapsed"] != 0.625 {
		t.Fatal("state reload inherited old daemon uptime or rounded elapsed totals")
	}
}
