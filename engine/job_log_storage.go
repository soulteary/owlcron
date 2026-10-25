package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

const defaultJobLogHardLimit int64 = 64 * 1024 * 1024

func (e *Engine) logHardLimit() int64 {
	if e.config.JobLogHardMaxSize > 0 {
		return e.config.JobLogHardMaxSize
	}
	return defaultJobLogHardLimit
}

func (jm *JobManager) jobLogLimit(job *types.Job) func() int64 {
	jobID, initialLimit := job.ID, job.LogMaxSize
	inherited := jm.engine.config.JobLogMaxSize
	if job.Category != "" {
		if c, ok := jm.engine.GetCategory(job.Category); ok && c.LogMaxSize > 0 {
			inherited = c.LogMaxSize
		}
	}
	return func() int64 {
		limit := initialLimit
		jm.engine.mu.RLock()
		if live := jm.engine.activeJobs[jobID]; live != nil {
			limit = live.LogMaxSize
		}
		jm.engine.mu.RUnlock()
		if limit <= 0 {
			limit = inherited
		}
		hard := jm.engine.logHardLimit()
		if limit <= 0 || limit > hard {
			limit = hard
		}
		return limit
	}
}

func (jm *JobManager) logLock(id string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return &jm.logLocks[h.Sum32()%uint32(len(jm.logLocks))]
}

// Live synchronization never runs network I/O on the heartbeat/sampling loop.
// There is no unbounded queue: busy jobs are picked up by a later sample.
func (jm *JobManager) scheduleLiveLogSync(job *types.Job) {
	e := jm.engine
	e.maintenanceLaunchMu.Lock()
	defer e.maintenanceLaunchMu.Unlock()
	if e.maintenanceStopping || e.ctx.Err() != nil {
		return
	}
	jm.liveLogMu.Lock()
	defer jm.liveLogMu.Unlock()
	if jm.liveLogJobs[job.ID] || len(jm.liveLogJobs) >= cap(jm.logUploadSlots) {
		return
	}
	jm.liveLogJobs[job.ID] = true
	snapshot := types.CloneJob(job)
	// A plugin may report complete before exiting and continue writing stdout.
	// Only the actual process completion path may seal a log manifest.
	snapshot.Complete = false
	e.loopWG.Add(1)
	go func() {
		defer func() {
			jm.liveLogMu.Lock()
			delete(jm.liveLogJobs, snapshot.ID)
			jm.liveLogMu.Unlock()
			e.loopWG.Done()
		}()
		if err := jm.uploadJobLogToStorage(snapshot); err != nil && e.ctx.Err() == nil {
			jm.logger.Warn("Live job log synchronization failed", zap.String("job_id", snapshot.ID), zap.Error(err))
		}
	}()
}

// uploadJobLogToStorage reads only bytes beyond the durable manifest offset.
// A failed append's exact reservation is retried before newer bytes are sent.
func (jm *JobManager) uploadJobLogToStorage(job *types.Job) error {
	ctx, cancel := context.WithTimeout(jm.engine.ctx, 30*time.Second)
	defer cancel()
	lock := jm.logLock(job.ID)
	lock.Lock()
	defer lock.Unlock()
	select {
	case jm.logUploadSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-jm.logUploadSlots }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if deleted, err := jm.engine.isJobLogDeleted(ctx, job.ID); err != nil || deleted {
		return err
	}
	if job.LogFile == "" {
		if job.LogFileSize > 0 {
			return jm.engine.requireDurableJobLog(ctx, job.ID, job.LogFileSize, job.Complete)
		}
		return nil
	}
	file, err := os.Open(job.LogFile)
	if os.IsNotExist(err) {
		if job.LogFileSize > 0 {
			return jm.engine.requireDurableJobLog(ctx, job.ID, job.LogFileSize, job.Complete)
		}
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if job.Complete && size < job.LogFileSize {
		return fmt.Errorf("completed local job log was truncated: %s", job.ID)
	}
	if size > jm.engine.logHardLimit() {
		return storage.ErrJobLogLimit
	}
	manifest, err := jm.engine.jobLogInfo(ctx, job.ID)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	if manifest.Deleted {
		return nil
	}
	if manifest.Offset > size || manifest.PendingEnd > size {
		return fmt.Errorf("local job log is shorter than durable upload: %s", job.ID)
	}
	buf := make([]byte, storage.JobLogChunkSize)
	for manifest.Offset < size {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := manifest.Offset + storage.JobLogChunkSize
		if end > size {
			end = size
		}
		if manifest.PendingEnd > 0 {
			end = manifest.PendingEnd
		}
		data := buf[:end-manifest.Offset]
		if _, err := file.ReadAt(data, manifest.Offset); err != nil {
			return err
		}
		manifest, err = jm.engine.jobLogAppend(ctx, job.ID, manifest.Offset, data, job.Complete && end == size, jm.engine.logHardLimit())
		if errors.Is(err, storage.ErrJobLogDeleted) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	if job.Complete && !manifest.Final || size == 0 && errors.Is(err, storage.ErrNotFound) {
		_, err = jm.engine.jobLogAppend(ctx, job.ID, manifest.Offset, nil, job.Complete, jm.engine.logHardLimit())
		if errors.Is(err, storage.ErrJobLogDeleted) {
			return nil
		}
		return err
	}
	return nil
}

func (e *Engine) requireDurableJobLog(ctx context.Context, id string, size int64, final bool) error {
	if deleted, err := e.isJobLogDeleted(ctx, id); err != nil || deleted {
		return err
	}
	m, err := e.jobLogInfo(ctx, id)
	if err == nil {
		if m.Deleted || m.Offset >= size && (!final || m.Final) {
			return nil
		}
		return fmt.Errorf("job log upload is not durable: %s", id)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	_, legacy, err := e.jobLogRead(ctx, id, 0, 1)
	if err == nil && legacy.Offset >= size {
		return nil
	}
	if err != nil {
		return fmt.Errorf("job log missing before completion persistence: %s: %w", id, err)
	}
	return fmt.Errorf("job log missing before completion persistence: %s", id)
}

// writeJobLog accepts bounded legacy inline completion messages. New workers
// synchronize local files incrementally and never send whole logs over sockets.
func (e *Engine) writeJobLog(ctx context.Context, id string, data []byte) error {
	if int64(len(data)) > e.logHardLimit() || len(data) > 1024*1024 {
		return storage.ErrJobLogLimit
	}
	if deleted, err := e.isJobLogDeleted(ctx, id); err != nil || deleted {
		return err
	}
	m, err := e.jobLogInfo(ctx, id)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	if m.Deleted {
		return nil
	}
	// Legacy inline messages contain a full snapshot, so validate the committed
	// prefix before accepting a suffix. Normal file sync never resends prefixes.
	compareEnd := m.Offset
	if compareEnd > int64(len(data)) {
		compareEnd = int64(len(data))
	}
	for at := int64(0); at < compareEnd; {
		n := int64(storage.JobLogChunkSize)
		if n > compareEnd-at {
			n = compareEnd - at
		}
		previous, _, readErr := e.jobLogRead(ctx, id, at, int(n))
		if errors.Is(readErr, storage.ErrJobLogDeleted) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
		if !bytes.Equal(previous, data[at:at+n]) {
			return storage.ErrConflict
		}
		at += n
	}
	if m.Offset > int64(len(data)) {
		return nil
	}
	for m.Offset < int64(len(data)) {
		end := m.Offset + storage.JobLogChunkSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		if m.PendingEnd > 0 {
			end = m.PendingEnd
		}
		if end > int64(len(data)) {
			return storage.ErrConflict
		}
		m, err = e.jobLogAppend(ctx, id, m.Offset, data[m.Offset:end], end == int64(len(data)), e.logHardLimit())
		if errors.Is(err, storage.ErrJobLogDeleted) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	if !m.Final {
		_, err = e.jobLogAppend(ctx, id, m.Offset, nil, true, e.logHardLimit())
		if errors.Is(err, storage.ErrJobLogDeleted) {
			return nil
		}
	}
	return err
}

// ReadStoredJobLog is a compatibility helper with a bounded one-page result.
func (e *Engine) ReadStoredJobLog(ctx context.Context, id string) ([]byte, error) {
	data, _, err := e.ReadJobLogRange(ctx, id, 0, storage.JobLogChunkSize)
	return data, err
}

func (e *Engine) ReadJobLogRange(ctx context.Context, id string, offset, limit int64) ([]byte, int64, error) {
	if id == "" || offset < 0 || limit <= 0 || limit > 1024*1024 {
		return nil, 0, fmt.Errorf("invalid job log range")
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if deleted, err := e.storage.Exists(ctx, "deleted_jobs:"+id); err != nil {
		return nil, 0, err
	} else if deleted {
		return nil, 0, storage.ErrNotFound
	}
	if deleted, err := e.isJobLogDeleted(ctx, id); err != nil {
		return nil, 0, err
	} else if deleted {
		return nil, 0, storage.ErrNotFound
	}
	// Serving a local active log does not wait for the next synchronization tick.
	job, exists := e.LookupJob(id)
	if exists && !job.Complete && (job.Hostname == e.Hostname() || job.Hostname == "local") {
		if data, total, err := e.readLocalJobLogRange(ctx, id, offset, limit); err == nil {
			return data, total, nil
		} else if !os.IsNotExist(err) {
			return nil, 0, err
		}
	}
	data, manifest, err := e.jobLogRead(ctx, id, offset, int(limit))
	if errors.Is(err, storage.ErrJobLogDeleted) {
		return nil, 0, storage.ErrNotFound
	}
	if errors.Is(err, storage.ErrNotFound) && exists && (job.Hostname == e.Hostname() || job.Hostname == "local") {
		return e.readLocalJobLogRange(ctx, id, offset, limit)
	}
	if err != nil {
		return nil, 0, err
	}
	if deleted, err := e.isJobLogDeleted(ctx, id); err != nil {
		return nil, 0, err
	} else if deleted {
		return nil, 0, storage.ErrNotFound
	}
	return data, manifest.Offset, nil
}

func (e *Engine) readLocalJobLogRange(ctx context.Context, id string, offset, limit int64) ([]byte, int64, error) {
	if filepath.Base(id) != id || id == "." || id == ".." {
		return nil, 0, fmt.Errorf("invalid local log id")
	}
	root, err := os.OpenRoot(e.config.LogDir)
	if err != nil {
		return nil, 0, err
	}
	defer root.Close()
	file, err := root.Open(filepath.Join("jobs", id+".log"))
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	if !info.Mode().IsRegular() {
		return nil, 0, fmt.Errorf("not a regular job log")
	}
	total := info.Size()
	if offset >= total {
		return []byte{}, total, nil
	}
	if limit > total-offset {
		limit = total - offset
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	data := make([]byte, int(limit))
	n, err := file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, 0, err
	}
	if deleted, err := e.isJobLogDeleted(ctx, id); err != nil {
		return nil, 0, err
	} else if deleted {
		return nil, 0, storage.ErrNotFound
	}
	return data[:n], total, nil
}
