package monitor

import (
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestMemoryAndCPUFlagsAreIndependent(t *testing.T) {
	m := NewResourceMonitor(zap.NewNop())
	job := &types.Job{ID: "limits", Mem: &types.ResourceUsage{Current: 200}, CPU: &types.ResourceUsage{Current: 1}}
	m.flags[job.ID] = resourceFlags{memory: time.Now().Unix() - 10}
	if exceeded, _ := m.CheckResourceLimits(job, 100, 5, 50, 5); !exceeded {
		t.Fatal("CPU below threshold cleared sustained memory violation")
	}
	m.Forget(job.ID)
	if len(m.flags) != 0 {
		t.Fatal("completed job flags leaked")
	}
}
func TestZeroSustainAbortsImmediately(t *testing.T) {
	m := NewResourceMonitor(zap.NewNop())
	job := &types.Job{ID: "instant", CPU: &types.ResourceUsage{Current: 101}}
	if exceeded, _ := m.CheckResourceLimits(job, 0, 0, 100, 0); !exceeded {
		t.Fatal("zero CPU sustain disabled enforcement")
	}
}
