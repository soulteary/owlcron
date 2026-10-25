package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/soulteary/owlcron/blob"
	"github.com/soulteary/owlcron/storage"
	"go.uber.org/zap"
)

const defaultSpoolMaxBytes int64 = 1 << 30

// A root is opened before Startup can mutate accounts, history or leases. New
// namespaces are created only by the offline administration command.
func (e *Engine) initializeLogStorage(ctx context.Context) error {
	format := 1
	if c := e.config.JobLogStorage; c != nil && c.WriteFormat != 0 {
		format = c.WriteFormat
	}
	if format != 1 && format != 2 {
		return fmt.Errorf("unsupported job log write format %d", format)
	}
	layout, err := storage.LoadStorageLayout(ctx, e.storage)
	if e.config.BlobStorage == nil {
		if err == nil {
			return fmt.Errorf("split storage layout requires blob_storage configuration")
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("read storage layout: %w", err)
		}
		if format == 2 {
			return fmt.Errorf("job log format 2 requires blob_storage configuration")
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("split storage layout must be initialized offline: %w", err)
	}
	registry, err := storage.NewBlobRegistry(e.config.BlobStorage)
	if err != nil {
		return err
	}
	e.blobStores = registry
	e.storageLayout = layout
	if err = e.validateLogStorage(ctx); err != nil {
		return err
	}
	grace := 10 * time.Minute
	maxBytes := defaultSpoolMaxBytes
	resume := 80
	if c := e.config.JobLogStorage; c != nil {
		if c.SpoolMaxBytes < 0 || c.SpoolResumePercent < 0 || c.SpoolResumePercent >= 90 || c.GCGraceSeconds < 0 || c.GCBatchSize < 0 || c.GCBatchSize > 1000 {
			return fmt.Errorf("invalid split log spool or garbage collection settings")
		}
		if c.SpoolMaxBytes > 0 {
			maxBytes = c.SpoolMaxBytes
		}
		if c.SpoolResumePercent > 0 {
			resume = c.SpoolResumePercent
		}
		if c.GCGraceSeconds > 0 {
			grace = time.Duration(c.GCGraceSeconds) * time.Second
		}
	}
	stores := registry.Stores()
	for id, store := range stores {
		stores[id] = &observedBlobStore{Store: store, stats: &e.splitStats}
	}
	e.jobLogs, err = storage.NewJobLogRepository(e.storage, stores, storage.JobLogRepositoryOptions{
		Namespace: layout.NamespaceID, WriteStore: registry.WriteStoreID(), WriteFormat: format,
		PendingTimeout: 2 * time.Minute, GCGracePeriod: grace,
	})
	if err != nil {
		return err
	}
	e.blobGCCursors = make(map[string]string)
	e.spoolBudget = newNodeSpoolBudget(maxBytes, resume, []string{filepath.Join(e.config.LogDir, "jobs"), e.config.QueueDir})
	return nil
}

func (e *Engine) validateLogStorage(ctx context.Context) error {
	if e.blobStores == nil {
		_, err := storage.LoadStorageLayout(ctx, e.storage)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("split storage layout requires blob_storage configuration")
	}
	probeCtx, cancel := context.WithTimeout(ctx, ReadinessTimeout)
	defer cancel()
	layout, err := storage.ValidateStorageLayout(probeCtx, e.storage, e.blobStores)
	if err != nil {
		return fmt.Errorf("validate split storage layout: %w", err)
	}
	if layout.NamespaceID != e.storageLayout.NamespaceID {
		return fmt.Errorf("storage namespace changed; restart with the verified configuration")
	}
	if err = probeCtx.Err(); err != nil {
		return err
	}
	return nil
}

func (e *Engine) jobLogInfo(ctx context.Context, id string) (storage.JobLogManifest, error) {
	if e.jobLogs != nil {
		return e.jobLogs.Info(ctx, id)
	}
	return storage.JobLogInfo(ctx, e.storage, id)
}
func (e *Engine) jobLogAppend(ctx context.Context, id string, offset int64, data []byte, final bool, maxBytes int64) (storage.JobLogManifest, error) {
	if e.jobLogs != nil {
		return e.jobLogs.Append(ctx, id, offset, data, final, maxBytes)
	}
	return storage.JobLogAppend(ctx, e.storage, id, offset, data, final, maxBytes)
}
func (e *Engine) jobLogRead(ctx context.Context, id string, offset int64, limit int) ([]byte, storage.JobLogManifest, error) {
	if e.jobLogs != nil {
		return e.jobLogs.Read(ctx, id, offset, limit)
	}
	return storage.JobLogRead(ctx, e.storage, id, offset, limit)
}
func (e *Engine) jobLogDelete(ctx context.Context, id string) error {
	if e.jobLogs != nil {
		return e.jobLogs.Delete(ctx, id)
	}
	return storage.JobLogDelete(ctx, e.storage, id)
}

// Admission never changes leadership or stops recovery. Existing processes can
// continue writing to their bounded local spool while a remote service is down.
func (e *Engine) checkLogAdmission(ctx context.Context) error {
	if e.jobLogs == nil {
		return nil
	}
	if e.spoolBudget != nil {
		if err := e.spoolBudget.admit(); err != nil {
			return err
		}
	}
	if err := e.storageReadiness(ctx); err != nil {
		return fmt.Errorf("durable log storage is unavailable: %w", err)
	}
	return nil
}

// The maintenance mutex serializes cursor changes. Inventory is paged; each
// pass visits one store, and the next full scan catches objects from late PUTs.
func (e *Engine) collectLogObjects(ctx context.Context) error {
	if e.jobLogs == nil || e.blobStores == nil {
		return nil
	}
	if !e.blobGCMu.TryLock() {
		return nil
	}
	defer e.blobGCMu.Unlock()
	ids := make([]string, 0, len(e.blobStores.Stores()))
	for id := range e.blobStores.Stores() {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return nil
	}
	id := ids[e.blobGCStore%len(ids)]
	e.blobGCStore = (e.blobGCStore + 1) % len(ids)
	limit := 128
	if c := e.config.JobLogStorage; c != nil && c.GCBatchSize > 0 {
		limit = c.GCBatchSize
	}
	passCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := e.jobLogs.CollectStore(passCtx, id, e.blobGCCursors[id], limit)
	if errors.Is(err, blob.ErrInvalidCursor) {
		result.NextCursor = ""
	}
	e.blobGCCursors[id] = result.NextCursor
	e.splitStats.gcScanned.Add(int64(result.Scanned))
	e.splitStats.gcDeleted.Add(int64(result.Deleted))
	if err != nil {
		e.splitStats.gcErrors.Add(1)
	}
	return err
}

type splitStorageStats struct {
	puts      atomic.Int64
	gets      atomic.Int64
	errors    atomic.Int64
	putNanos  atomic.Int64
	gcScanned atomic.Int64
	gcDeleted atomic.Int64
	gcErrors  atomic.Int64
}

type observedBlobStore struct {
	blob.Store
	stats *splitStorageStats
}

func (s *observedBlobStore) Put(ctx context.Context, spec blob.Write, body io.Reader) error {
	start := time.Now()
	s.stats.puts.Add(1)
	err := s.Store.Put(ctx, spec, body)
	s.stats.putNanos.Add(time.Since(start).Nanoseconds())
	if err != nil {
		s.stats.errors.Add(1)
	}
	return err
}
func (s *observedBlobStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	s.stats.gets.Add(1)
	body, err := s.Store.Open(ctx, key)
	if err != nil {
		s.stats.errors.Add(1)
		return nil, err
	}
	return &observedBlobReader{ReadCloser: body, stats: s.stats}, nil
}
func (s *observedBlobStore) Delete(ctx context.Context, key string) error {
	err := s.Store.Delete(ctx, key)
	if err != nil {
		s.stats.errors.Add(1)
	}
	return err
}
func (s *observedBlobStore) Scan(ctx context.Context, prefix, cursor string, limit int) (blob.Page, error) {
	inventory, ok := s.Store.(blob.Inventory)
	if !ok {
		return blob.Page{}, fmt.Errorf("blob store does not support bounded inventory")
	}
	return inventory.Scan(ctx, prefix, cursor, limit)
}

type observedBlobReader struct {
	io.ReadCloser
	stats  *splitStorageStats
	failed bool
}

func (r *observedBlobReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil && err != io.EOF && !r.failed {
		r.stats.errors.Add(1)
		r.failed = true
	}
	return n, err
}

// Object collection and spool reconciliation cannot depend on a possibly daily
// history retention interval. The launch gate bounds this to one background pass
// per minute, and Shutdown waits for the active pass.
func (e *Engine) runSplitMaintenanceIfDue(now time.Time) {
	if e.jobLogs == nil {
		return
	}
	e.maintenanceLaunchMu.Lock()
	defer e.maintenanceLaunchMu.Unlock()
	if e.maintenanceStopping || e.ctx.Err() != nil || e.blobMaintenanceRunning || (!e.lastBlobMaintenance.IsZero() && now.Sub(e.lastBlobMaintenance) < time.Minute) {
		return
	}
	e.lastBlobMaintenance = now
	e.blobMaintenanceRunning = true
	e.loopWG.Add(1)
	go func() {
		defer func() {
			e.maintenanceLaunchMu.Lock()
			e.blobMaintenanceRunning = false
			e.maintenanceLaunchMu.Unlock()
			e.loopWG.Done()
		}()
		ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
		defer cancel()
		// Local cleanup runs on workers too and shares its cursor with
		// retention. A busy retention pass will do the same cleanup itself.
		if e.maintenanceMu.TryLock() {
			batch := e.config.HistoryCleanupBatchSize
			if batch <= 0 {
				batch = 100
			}
			err := e.cleanLocalLogs(ctx, now, batch)
			e.maintenanceMu.Unlock()
			if err != nil && ctx.Err() == nil {
				e.logger.Warn("Local log cache cleanup will retry", zap.String("error", e.RedactSensitiveText(err.Error())))
			}
		}
		if e.spoolBudget != nil {
			if err := e.spoolBudget.refresh(ctx); err != nil {
				if ctx.Err() == nil {
					e.logger.Warn("Recovery spool inspection will retry", zap.String("error", e.RedactSensitiveText(err.Error())))
				}
				return
			}
		}
		if e.IsMaster() && e.EnsureLeadership() {
			if err := e.collectLogObjects(ctx); err != nil && ctx.Err() == nil {
				e.logger.Warn("Log object collection will retry", zap.String("error", e.RedactSensitiveText(err.Error())))
			}
		}
	}()
}
