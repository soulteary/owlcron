package engine

import "time"

// MetricsSnapshot contains only bounded-label operational state already held in
// memory. It does not walk storage, spool directories, histories or job logs.
type MetricsSnapshot struct {
	Master             bool
	Worker             bool
	SchedulerEnabled   bool
	ActiveJobs         int
	PendingJobs        int
	InternalQueueTasks int
	ShutdownRequested  bool
}

func (e *Engine) MetricsSnapshot() MetricsSnapshot {
	e.mu.RLock()
	snapshot := MetricsSnapshot{
		Master:            e.multi.Master && (e.leaseUntil == 0 || e.leaseUntil > time.Now().UnixNano()),
		Worker:            e.multi.Slave,
		SchedulerEnabled:  e.state.Enabled,
		ShutdownRequested: e.ctx.Err() != nil,
	}
	for _, job := range e.activeJobs {
		if job.Pending {
			snapshot.PendingJobs++
		} else {
			snapshot.ActiveJobs++
		}
	}
	e.mu.RUnlock()
	// Sample queue state separately, without nesting locks used by admission.
	e.queue.mu.RLock()
	snapshot.InternalQueueTasks = len(e.queue.internalQueue)
	e.queue.mu.RUnlock()
	return snapshot
}
