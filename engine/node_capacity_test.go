package engine

import (
	"github.com/soulteary/owlcron/types"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestNodeCapacityIncludesReservationsAndOverlappingGroups(t *testing.T) {
	e := newSchedulingTestEngine(t, "capacity-node")
	t.Cleanup(func() { e.mu.Lock(); e.activeJobs = map[string]*types.Job{}; e.mu.Unlock() })
	for _, group := range []*types.ServerGroup{{ID: "wide", Regexp: ".*", MaxJobs: 5}, {ID: "strict", Regexp: "^capacity-node$", MaxJobs: 2}} {
		if err := e.SaveServerGroupToStorage(group); err != nil {
			t.Fatal(err)
		}
	}
	e.activeJobs["reserved"] = &types.Job{ID: "reserved", Hostname: e.Hostname(), Pending: true}
	if got := e.jobManager.availableServers([]string{"local", e.Hostname()}); len(got) != 2 {
		t.Fatalf("available: %v", got)
	}
	if err := e.jobManager.checkNodeCapacity([]*types.Job{{Hostname: "local"}, {Hostname: e.Hostname()}}); err == nil {
		t.Fatal("batch exceeded strictest limit")
	}
	e.activeJobs["running"] = &types.Job{ID: "running", Hostname: e.Hostname()}
	if got := e.jobManager.chooseServers(&types.Event{Target: "local"}); len(got) != 0 {
		t.Fatal("local target bypassed capacity")
	}
	if got := e.jobManager.chooseServers(&types.Event{Target: e.Hostname()}); len(got) != 0 {
		t.Fatal("direct host bypassed capacity")
	}
	delete(e.activeJobs, "running")
	if err := e.jobManager.checkNodeCapacity([]*types.Job{{Hostname: "local"}}); err != nil {
		t.Fatal(err)
	}
}

func TestNodeCapacityConcurrentAdmission(t *testing.T) {
	e := newSchedulingTestEngine(t, "capacity-concurrent")
	setMasterForTest(e)
	event := saveSchedulingFixture(t, e, "capacity-event")
	script := filepath.Join(e.config.LogDir, "wait.sh")
	if err := os.WriteFile(script, []byte("sleep 5\nprintf '%s\\n' '{\"complete\":true,\"code\":0}'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.SavePluginToStorage(&types.Plugin{ID: "complete", Enabled: true, Command: "/bin/sh " + script}); err != nil {
		t.Fatal(err)
	}
	if err := e.SaveServerGroupToStorage(&types.ServerGroup{ID: "capacity", Regexp: ".*", MaxJobs: 2}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := []string{}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids, err := e.jobManager.LaunchJobWithIDs(event, 0)
			if err == nil {
				mu.Lock()
				accepted = append(accepted, ids...)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(accepted) != 2 {
		t.Fatalf("accepted %d jobs, want 2", len(accepted))
	}
	for _, id := range accepted {
		e.jobManager.AbortJob(id, "test cleanup")
	}
	e.jobManager.Wait()
}
