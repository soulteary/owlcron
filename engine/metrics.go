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
	SplitStorage       bool
	SpoolBytes         int64
	SpoolLimitBytes    int64
	SpoolBlocked       bool
	BlobPutTotal       int64
	BlobGetTotal       int64
	BlobErrorTotal     int64
	BlobPutNanos       int64
	BlobGCScanned      int64
	BlobGCDeleted      int64
	BlobGCErrors       int64
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
	if e.jobLogs != nil {
		snapshot.SplitStorage = true
		if e.spoolBudget != nil {
			snapshot.SpoolBytes, snapshot.SpoolLimitBytes, snapshot.SpoolBlocked = e.spoolBudget.snapshot()
		}
		snapshot.BlobPutTotal = e.splitStats.puts.Load()
		snapshot.BlobGetTotal = e.splitStats.gets.Load()
		snapshot.BlobErrorTotal = e.splitStats.errors.Load()
		snapshot.BlobPutNanos = e.splitStats.putNanos.Load()
		snapshot.BlobGCScanned = e.splitStats.gcScanned.Load()
		snapshot.BlobGCDeleted = e.splitStats.gcDeleted.Load()
		snapshot.BlobGCErrors = e.splitStats.gcErrors.Load()
	}
	return snapshot
}
