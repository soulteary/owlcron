package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// runHistoryMaintenanceIfDue is called by the engine loop. Explicit maintenance
// bypasses the interval, so operators and tests can drive recovery immediately.
func (e *Engine) runHistoryMaintenanceIfDue(now time.Time) {
	interval := e.config.HistoryCleanupInterval
	if interval <= 0 {
		interval = 60
	}
	e.maintenanceLaunchMu.Lock()
	defer e.maintenanceLaunchMu.Unlock()
	if e.maintenanceStopping || e.ctx.Err() != nil || e.maintenanceRunning {
		return
	}
	if !e.lastMaintenance.IsZero() && now.Sub(e.lastMaintenance) < time.Duration(interval)*time.Second {
		return
	}
	e.lastMaintenance = now
	e.maintenanceRunning = true
	// Shutdown closes this launch gate before waiting. Add can never race a
	// zero-counter Wait, including maintenance requested before Startup.
	e.loopWG.Add(1)
	go func() {
		defer func() {
			e.maintenanceLaunchMu.Lock()
			e.maintenanceRunning = false
			e.maintenanceLaunchMu.Unlock()
			e.loopWG.Done()
		}()
		if err := e.RunHistoryMaintenance(e.ctx, now); err != nil && !errors.Is(err, context.Canceled) {
			e.logger.Warn("History maintenance failed; pending work will be retried", zap.Error(err))
		}
	}()
}

// RunHistoryMaintenance performs a bounded retention pass. Zero retention days
// disables that policy. Tombstones are deliberately never expired: a delayed
// worker completion must not resurrect a job removed by retention.
func (e *Engine) RunHistoryMaintenance(ctx context.Context, now time.Time) error {
	e.maintenanceMu.Lock()
	defer e.maintenanceMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	batch := e.config.HistoryCleanupBatchSize
	if batch <= 0 {
		batch = 100
	}
	if e.spoolBudget != nil {
		if err := e.spoolBudget.refresh(ctx); err != nil {
			return err
		}
	}
	if e.IsMaster() && e.EnsureLeadership() {
		if err := e.RetryHistoryDeletions(ctx); err != nil {
			return err
		}
		if err := e.collectLogObjects(ctx); err != nil {
			e.logger.Warn("Log object collection will retry", zap.String("error", e.RedactSensitiveText(err.Error())))
		}
		// Event-level overrides may expire logs even when the global policy is off.
		if err := e.cleanSharedHistory(ctx, now, batch); err != nil {
			return err
		}
	}
	if err := e.evictDeletedHistory(ctx, batch); err != nil {
		return err
	}
	if err := e.cleanLocalLogs(ctx, now, batch); err != nil {
		return err
	}
	if e.spoolBudget != nil {
		return e.spoolBudget.refresh(ctx)
	}
	return nil
}

func expiredJob(job *types.Job, now time.Time, days int) bool {
	return days > 0 && job.Complete && job.TimeEnd > 0 && job.TimeEnd <= float64(now.Unix())-float64(days)*86400
}

func (e *Engine) pendingHistoryJobs(ctx context.Context) (map[string]bool, error) {
	pending := make(map[string]bool)
	items, err := e.storage.ListGet(ctx, "remote_completions", 0, 0)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		raw, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		var entry struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, err
		}
		if entry.ID == "" {
			return nil, fmt.Errorf("remote completion missing id")
		}
		pending[entry.ID] = true
	}
	return pending, nil
}

// cleanLocalLogs never trusts the LogFile supplied by a remote job. Only regular
// files directly inside this node's jobs directory can be removed. In-flight
// completion spools and remote delivery outboxes protect their associated logs.
func (e *Engine) cleanLocalLogs(ctx context.Context, now time.Time, batch int) error {
	e.jobManager.finishMu.Lock()
	defer e.jobManager.finishMu.Unlock()
	pending, err := e.pendingHistoryJobs(ctx)
	if err != nil {
		return err
	}
	dir := filepath.Join(e.config.LogDir, "jobs")
	info, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("unsafe local job log directory")
	}
	logRoot, err := os.OpenRoot(e.config.LogDir)
	if err != nil {
		return err
	}
	defer logRoot.Close()
	root, err := logRoot.OpenRoot("jobs")
	if err != nil {
		return err
	}
	defer root.Close()
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	active := e.GetActiveJobs()
	scanned := 0
	last := ""
	for _, entry := range entries {
		name := entry.Name()
		if name <= e.localLogCursor || !strings.HasSuffix(name, ".log") {
			continue
		}
		if scanned >= batch {
			break
		}
		scanned++
		last = name
		if err := ctx.Err(); err != nil {
			return err
		}
		id := strings.TrimSuffix(name, ".log")
		if active[id] != nil || pending[id] {
			continue
		}
		if _, err := os.Lstat(e.jobManager.completionSpoolPath(id)); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		deleted, err := e.IsJobHistoryDeleted(id)
		if err != nil {
			return err
		}
		logDeleted, err := e.IsJobLogDeleted(id)
		if err != nil {
			return err
		}
		if !deleted && !logDeleted {
			// An incomplete or unknown canonical record is protected after a
			// restart. TTL never guesses whether an orphan has finished.
			raw, err := e.storage.Get(ctx, "jobs:"+id)
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			var job types.Job
			if err := json.Unmarshal(raw, &job); err != nil {
				return err
			}
			if !job.Complete || job.StorageError != "" {
				continue
			}
			if !expiredJob(&job, now, e.jobLogRetentionDays(&job)) {
				// Once a v2 root is sealed, this exact local file is only a
				// cache. Keeping it until remote retention expires would fill
				// the recovery budget even while uploads are healthy.
				if e.jobLogs == nil {
					continue
				}
				manifest, err := e.jobLogInfo(ctx, id)
				if errors.Is(err, storage.ErrNotFound) {
					continue
				}
				if err != nil {
					return err
				}
				if manifest.Format != 2 || !manifest.Final || manifest.Deleted || manifest.Offset != info.Size() {
					continue
				}
			}
		}
		if err := root.Remove(name); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if scanned < batch {
		e.localLogCursor = ""
	} else {
		e.localLogCursor = last
	}
	return nil
}

func (e *Engine) cleanSharedHistory(ctx context.Context, now time.Time, batch int) error {
	if !e.EnsureLeadership() {
		return nil
	}
	if err := storage.HistoryMigrate(ctx, e.storage); err != nil {
		return err
	}
	raw, err := e.storage.Get(ctx, "maintenance:history_cursor")
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	cursor := ""
	if err == nil {
		if err := json.Unmarshal(raw, &cursor); err != nil {
			return fmt.Errorf("invalid retention cursor: %w", err)
		}
	}
	jobs, next, err := storage.HistoryPage(ctx, e.storage, cursor, batch)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !e.IsMaster() || !e.EnsureLeadership() {
			return nil
		}
		if expiredJob(job, now, e.config.JobDataExpireDays) {
			if err := e.DeleteJobHistory(job.ID); err != nil && !errors.Is(err, ErrHistoryCleanupPending) {
				return err
			}
		} else if expiredJob(job, now, e.jobLogRetentionDays(job)) {
			if err := e.DeleteJobLog(job.ID); err != nil && !errors.Is(err, ErrHistoryCleanupPending) {
				return err
			}
		}
	}
	raw, err = json.Marshal(next)
	if err != nil {
		return err
	}
	return e.storage.Put(ctx, "maintenance:history_cursor", raw)
}

// A worker may cache a completed job after another node has removed its shared
// history index. Reconcile a bounded number of cached jobs against permanent
// tombstones, without requiring those jobs to remain in the history index.
func (e *Engine) evictDeletedHistory(ctx context.Context, batch int) error {
	e.mu.RLock()
	ids := make([]string, 0, len(e.deadJobs))
	for id := range e.deadJobs {
		if id > e.deadJobRetentionCursor {
			ids = append(ids, id)
		}
	}
	e.mu.RUnlock()
	sort.Strings(ids)
	if len(ids) > batch {
		ids = ids[:batch]
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.mu.RLock()
		job := types.CloneJob(e.deadJobs[id])
		active := e.activeJobs[id] != nil
		e.mu.RUnlock()
		if job == nil || active || !job.Complete || job.StorageError != "" {
			continue
		}
		if _, err := os.Lstat(e.jobManager.completionSpoolPath(id)); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		deleted, err := e.IsJobHistoryDeleted(id)
		if err != nil {
			return err
		}
		if !deleted {
			continue
		}
		// Tombstones make any late outbox replay harmless, but retain pending cache
		// entries until their completion delivery has been acknowledged.
		pending, err := e.pendingHistoryJobs(ctx)
		if err != nil {
			return err
		}
		if pending[id] {
			continue
		}
		e.mu.Lock()
		current := e.deadJobs[id]
		if e.activeJobs[id] == nil && current != nil && current.Complete && current.StorageError == "" {
			delete(e.deadJobs, id)
		}
		e.mu.Unlock()
	}
	if len(ids) < batch {
		e.deadJobRetentionCursor = ""
	} else {
		e.deadJobRetentionCursor = ids[len(ids)-1]
	}
	return nil
}

// Completion recovery also holds finishMu across backend operations. Keeping it
// off the main loop lets heartbeats progress while retention owns that mutex.
func (e *Engine) runCompletionRecovery() {
	e.maintenanceLaunchMu.Lock()
	defer e.maintenanceLaunchMu.Unlock()
	if e.maintenanceStopping || e.ctx.Err() != nil || e.completionRecoveryRunning {
		return
	}
	e.completionRecoveryRunning = true
	e.loopWG.Add(1)
	go func() {
		defer func() {
			e.maintenanceLaunchMu.Lock()
			e.completionRecoveryRunning = false
			e.maintenanceLaunchMu.Unlock()
			e.loopWG.Done()
		}()
		if err := e.jobManager.RetryPendingCompletions(); err != nil {
			e.logger.Warn("Completion recovery failed; pending work will be retried", zap.Error(err))
		}
		if e.ctx.Err() == nil && !e.IsMaster() {
			e.jobManager.RetryRemoteCompletions()
		}
	}()
}

// Legacy jobs without a snapshot retain the global fallback behavior.
func (e *Engine) jobLogRetentionDays(job *types.Job) int {
	if job.LogExpireDays != nil {
		return *job.LogExpireDays
	}
	return e.config.JobLogExpireDays
}
