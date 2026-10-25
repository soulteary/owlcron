package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

type notificationSpool struct {
	ID      string          `json:"id"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// enqueueNotification falls back to a synced local outbox when shared storage is
// unavailable. The file remains until the shared queue confirms durable admission.
func (e *Engine) enqueueNotification(ctx context.Context, id, kind string, payload any) error {
	if e.notifications == nil {
		return fmt.Errorf("notification queue unavailable")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(raw) > 256<<10 {
		return fmt.Errorf("notification payload exceeds 256 KiB")
	}
	e.notificationSpoolMu.Lock()
	defer e.notificationSpoolMu.Unlock()
	if err = e.notifications.Enqueue(ctx, id, kind, payload); err == nil {
		return nil
	}
	dir := filepath.Join(e.config.QueueDir, "notifications")
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(id))
	path := filepath.Join(dir, hex.EncodeToString(sum[:])+".json")
	if _, err = os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	encoded, err := json.Marshal(notificationSpool{id, kind, raw})
	if err != nil {
		return err
	}
	return e.writeSpoolFile(path, encoded, ".notification-*")
}

func (e *Engine) replayNotificationSpool(ctx context.Context) error {
	e.notificationSpoolMu.Lock()
	defer e.notificationSpoolMu.Unlock()
	dir := filepath.Join(e.config.QueueDir, "notifications")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	processed := 0
	var firstErr error
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		if processed >= 100 {
			break
		}
		processed++
		path := filepath.Join(dir, entry.Name())
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		var n notificationSpool
		// A corrupt or unexpectedly large spool record must not allocate without bound
		// or prevent every subsequent notification from recovering.
		dec := json.NewDecoder(io.LimitReader(f, (256<<10)+4097))
		err = dec.Decode(&n)
		if err == nil {
			var extra any
			if tailErr := dec.Decode(&extra); tailErr != io.EOF {
				err = errors.New("notification outbox has trailing data")
			}
		}
		_ = f.Close()
		if err == nil && (len(n.Payload) > 256<<10 || n.ID == "" || len(n.ID) > 1024 || n.Kind == "" || len(n.Kind) > 64) {
			err = errors.New("invalid notification outbox record")
		}
		if err != nil {
			if renameErr := os.Rename(path, path+".invalid"); renameErr != nil {
				return renameErr
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("invalid notification outbox record quarantined: %s", entry.Name())
			}
			continue
		}
		if err = e.notifications.Enqueue(ctx, n.ID, n.Kind, n.Payload); err != nil {
			return err
		}
		if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return firstErr
}

func (e *Engine) deliverNotification(ctx context.Context, n *Notification) error {
	switch n.Kind {
	case "account_email":
		return e.deliverAccountEmail(ctx, n)
	case "email":
		var p EmailPayload
		if err := json.Unmarshal(n.Payload, &p); err != nil {
			return &NotificationDeliveryError{Err: err, Permanent: true}
		}
		return e.jobManager.mailManager.deliverEmail(ctx, p)
	case "webhook":
		var p WebHookPayload
		if err := json.Unmarshal(n.Payload, &p); err != nil {
			return &NotificationDeliveryError{Err: err, Permanent: true}
		}
		return e.jobManager.webHookManager.DeliverWebHook(ctx, p, n.ID)
	default:
		return &NotificationDeliveryError{Err: fmt.Errorf("unknown notification kind"), Permanent: true}
	}
}
func (e *Engine) runNotifications() {
	e.maintenanceLaunchMu.Lock()
	defer e.maintenanceLaunchMu.Unlock()
	if e.maintenanceStopping || e.ctx.Err() != nil || e.notificationRunning || e.notifications == nil {
		return
	}
	e.notificationRunning = true
	e.loopWG.Add(1)
	go func() {
		defer func() {
			e.maintenanceLaunchMu.Lock()
			e.notificationRunning = false
			e.maintenanceLaunchMu.Unlock()
			e.loopWG.Done()
		}()
		if err := e.replayNotificationSpool(e.ctx); err != nil && e.ctx.Err() == nil {
			e.logger.Warn("Notification outbox replay failed", zap.String("error", e.RedactSensitiveText(err.Error())))
		}
		// Claims are shared and fenced, so every node can drain its durable queue.
		if err := e.notifications.RunOnce(e.ctx); err != nil && e.ctx.Err() == nil {
			e.logger.Warn("Notification delivery pass failed", zap.String("error", e.RedactSensitiveText(err.Error())))
		}
	}()
}

// Completion admission is part of the existing completion-spool transaction:
// recovery repeats these stable IDs before removing the completion spool.
func (jm *JobManager) enqueueCompletionNotifications(job *types.Job) error {
	type targets struct {
		Success   string `json:"success"`
		Failure   string `json:"failure"`
		JobURL    string `json:"job_url"`
		GlobalURL string `json:"global_url"`
	}
	plan := targets{Success: job.NotifySuccess, Failure: job.NotifyFail, JobURL: job.WebHook, GlobalURL: jm.engine.config.UniversalWebHook}
	if event, ok := jm.engine.scheduler.GetEvent(job.Event); ok {
		if plan.Success == "" {
			plan.Success = event.NotifySuccess
		}
		if plan.Failure == "" {
			plan.Failure = event.NotifyFail
		}
	}
	initial, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	atomicStore, ok := jm.engine.storage.(storage.AtomicStorage)
	if !ok {
		return storage.ErrUnsupported
	}
	sum := sha256.Sum256([]byte(job.ID))
	if err = atomicStore.Update(context.Background(), "notification_plans/"+hex.EncodeToString(sum[:]), func(old []byte) ([]byte, error) {
		if len(old) > 0 {
			return old, json.Unmarshal(old, &plan)
		}
		return initial, nil
	}); err != nil {
		return err
	}
	if err := jm.webHookManager.enqueueJobCompleteHookTargets(job, plan.JobURL, plan.GlobalURL); err != nil {
		return err
	}
	success, failure := plan.Success, plan.Failure

	if job.Code == 0 {
		return jm.mailManager.SendJobSuccessEmail(job, success)
	}
	return jm.mailManager.SendJobFailureEmail(job, failure)
}

// Expose queue operations for authenticated administrator handlers.
func (e *Engine) ListNotifications(ctx context.Context, state string, limit int) ([]Notification, error) {
	rows, err := e.notifications.List(ctx, state, limit)
	for i := range rows {
		rows[i].LastError = e.RedactSensitiveText(rows[i].LastError)
	}
	return rows, err
}
func (e *Engine) RetryNotification(ctx context.Context, id string) error {
	return e.notifications.RetryDead(ctx, id)
}
