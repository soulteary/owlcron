package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

// A short default regression runs two real restart/maintenance cycles. Operators
// can request 24h explicitly; test invocation timeout must exceed that duration.
func TestProcessSoakProfile(t *testing.T) {
	duration := time.Duration(0)
	if raw := os.Getenv("OWLCRON_SOAK_DURATION"); raw != "" {
		var err error
		duration, err = time.ParseDuration(raw)
		if err != nil || duration < time.Minute || duration > 48*time.Hour {
			t.Fatal("OWLCRON_SOAK_DURATION must be between 1m and 48h")
		}
	}
	clients := processLoadEnvInt(t, "OWLCRON_SOAK_CONCURRENCY", 2, 256)
	batch := processLoadEnvInt(t, "OWLCRON_SOAK_BATCH_JOBS", 4, 10000)
	l := newProcessLoadProfile(t, "soak", clients, 4)
	defer l.m.finish(t)
	until := time.Now().Add(duration)
	for cycle := 0; cycle < 2 || time.Now().Before(until); cycle++ {
		l.batch(t, batch, clients)
		l.verify(t)
		// Seed an explicitly expired historical record, never age a real active job.
		id := fmt.Sprintf("soak-expired-%d", cycle)
		expired := &types.Job{ID: id, Event: "load", Complete: true, TimeEnd: float64(time.Now().Add(-48 * time.Hour).Unix())}
		l.f.Put(t, "jobs:"+id, expired)
		logPath := filepath.Join(l.f.Config.LogDir, "jobs", id+".log")
		if err := os.WriteFile(logPath, []byte("expired-log\n"), 0600); err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-48 * time.Hour)
		if err := os.Chtimes(logPath, old, old); err != nil {
			t.Fatal(err)
		}
		if err := storage.HistoryUpsert(context.Background(), l.f.Store(t), expired); err != nil {
			t.Fatal(err)
		}
		awaitProcess(t, 20*time.Second, "soak retention maintenance", func() bool {
			rows, err := storage.HistoryJobs(context.Background(), l.f.Store(t))
			if err != nil {
				return false
			}
			for _, row := range rows {
				if row.ID == id {
					return false
				}
			}
			_, statErr := os.Stat(logPath)
			return os.IsNotExist(statErr)
		})
		l.p.KillDaemonOnly(t)
		restart := time.Now()
		l.p = l.f.Start(t)
		awaitProcess(t, 15*time.Second, "soak restart leadership", func() bool { return l.f.Request(t, "/api/app/status", nil)["master"] == true })
		l.m.mu.Lock()
		processLoadObserve(&l.m.restartHistogram, time.Since(restart))
		l.m.Cycles++
		l.m.mu.Unlock()
		l.verify(t)
	}
}
