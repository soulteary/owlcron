package api

import (
	"errors"
	"fmt"
	"net/http"
)

// Metrics uses the existing authenticators, with HTTP statuses suitable for a
// scraper. A dedicated privilege avoids granting collectors administration.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}
	auth, err := authenticateLimited(s.engine, r, s.logger, s.loginLimiter, s.clientIP(r))
	if err != nil {
		var limited *loginLimitError
		if errors.As(err, &limited) {
			sendLoginLimited(w, limited.retry)
			return
		}
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}
	if !auth.IsAdmin && !auth.Privileges["view_metrics"] {
		http.Error(w, "Metrics privilege required", http.StatusForbidden)
		return
	}
	snapshot := s.engine.MetricsSnapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	boolGauge := func(value bool) int {
		if value {
			return 1
		}
		return 0
	}
	gauges := []struct {
		name, help string
		value      int
	}{
		{"owlcron_role_master", "Whether this node reports an unexpired master role in its local state.", boolGauge(snapshot.Master)},
		{"owlcron_role_worker", "Whether this node currently reports a worker role.", boolGauge(snapshot.Worker)},
		{"owlcron_scheduler_enabled", "Whether scheduling is enabled in this node's state snapshot.", boolGauge(snapshot.SchedulerEnabled)},
		{"owlcron_jobs_active", "Non-pending jobs in this node's active-job registry; may include remote jobs on the master.", snapshot.ActiveJobs},
		{"owlcron_jobs_pending", "Pending jobs in this node's active-job registry.", snapshot.PendingJobs},
		{"owlcron_internal_queue_tasks", "Delayed tasks in this node's in-memory internal queue; not durable event queue depth.", snapshot.InternalQueueTasks},
		{"owlcron_shutdown_requested", "Whether shutdown or restart has been requested on this node.", boolGauge(snapshot.ShutdownRequested)},
	}
	for _, gauge := range gauges {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", gauge.name, gauge.help, gauge.name, gauge.name, gauge.value)
	}
	if snapshot.SplitStorage {
		for _, sample := range []struct {
			name, help, kind string
			value            int64
		}{
			{"owlcron_spool_bytes", "Last reconciled local job log and recovery spool bytes.", "gauge", snapshot.SpoolBytes},
			{"owlcron_spool_limit_bytes", "Configured node recovery spool byte budget.", "gauge", snapshot.SpoolLimitBytes},
			{"owlcron_spool_admission_blocked", "Whether local spool pressure pauses new admissions.", "gauge", int64(boolGauge(snapshot.SpoolBlocked))},
			{"owlcron_blob_put_total", "Immutable object upload attempts.", "counter", snapshot.BlobPutTotal},
			{"owlcron_blob_get_total", "Immutable object read attempts.", "counter", snapshot.BlobGetTotal},
			{"owlcron_blob_errors_total", "Observed immutable object I/O errors.", "counter", snapshot.BlobErrorTotal},
			{"owlcron_blob_gc_scanned_total", "Objects inspected by background log collection.", "counter", snapshot.BlobGCScanned},
			{"owlcron_blob_gc_deleted_total", "Unreferenced objects deleted by background log collection.", "counter", snapshot.BlobGCDeleted},
			{"owlcron_blob_gc_errors_total", "Collection passes reporting an error.", "counter", snapshot.BlobGCErrors},
		} {
			_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", sample.name, sample.help, sample.name, sample.kind, sample.name, sample.value)
		}
		_, _ = fmt.Fprintf(w, "# HELP owlcron_blob_put_seconds_total Total time spent in object upload attempts.\n# TYPE owlcron_blob_put_seconds_total counter\nowlcron_blob_put_seconds_total %.9f\n", float64(snapshot.BlobPutNanos)/1e9)
	}

}
