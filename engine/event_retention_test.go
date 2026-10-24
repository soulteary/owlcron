package engine

import (
	"context"
	"encoding/json"
	"github.com/soulteary/owlcron/types"
	"os"
	"testing"
	"time"
)

func TestEventLogRetentionSnapshotAndLocalCleanup(t *testing.T) {
	e := jobTestEngine(t)
	e.multi.Master = false
	e.config.JobLogExpireDays = 30
	days := 1
	event := &types.Event{ID: "short", LogExpireDays: &days}
	job := e.jobManager.createJob(event, "local", 0, 0)
	days = 0
	if *job.LogExpireDays != 1 {
		t.Fatal("job snapshot aliases event")
	}
	clone := types.CloneJob(job)
	*clone.LogExpireDays = 7
	if *job.LogExpireDays != 1 {
		t.Fatal("job clone aliases snapshot")
	}
	inherited := e.jobManager.createJob(&types.Event{}, "local", 0, 0)
	e.config.JobLogExpireDays = 0
	if *inherited.LogExpireDays != 30 {
		t.Fatal("global policy not snapshotted")
	}
	now := time.Now()
	for _, keep := range []bool{false, true} {
		id := "short"
		policy := 1
		if keep {
			id = "forever"
			policy = 0
		}
		path := retentionLog(t, e, id, now)
		raw, _ := json.Marshal(&types.Job{ID: id, Complete: true, TimeEnd: float64(now.Add(-48 * time.Hour).Unix()), LogExpireDays: &policy})
		if err := e.storage.Put(context.Background(), "jobs:"+id, raw); err != nil {
			t.Fatal(err)
		}
		if err := e.RunHistoryMaintenance(context.Background(), now); err != nil {
			t.Fatal(err)
		}
		_, err := os.Stat(path)
		if keep && err != nil {
			t.Fatal("permanent log removed", err)
		}
		if !keep && !os.IsNotExist(err) {
			t.Fatal("event expiry ignored with global disabled", err)
		}
	}
}
