package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

type notificationOutage struct{ storage.Storage }

func (s notificationOutage) Update(context.Context, string, func([]byte) ([]byte, error)) error {
	return errors.New("backend unavailable")
}
func (s notificationOutage) ListKeysPrefix(ctx context.Context, p string) ([]string, error) {
	return s.Storage.(storage.PrefixKeyLister).ListKeysPrefix(ctx, p)
}
func TestNotificationLocalOutboxConcurrentRecovery(t *testing.T) {
	e := jobTestEngine(t)
	ctx := context.Background()
	good := e.notifications
	broken, err := NewNotificationQueue(notificationOutage{e.storage}, NotificationQueueConfig{}, func(context.Context, *Notification) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	e.notifications = broken
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.enqueueNotification(ctx, "same-id", "email", map[string]string{"body": "durable"}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	paths, _ := filepath.Glob(filepath.Join(e.config.QueueDir, "notifications", "*.json"))
	if len(paths) != 1 {
		t.Fatalf("outbox count %d", len(paths))
	}
	info, err := os.Stat(paths[0])
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("outbox permissions %v %v", info, err)
	}
	e.notifications = good
	if err = e.replayNotificationSpool(ctx); err != nil {
		t.Fatal(err)
	}
	n, err := good.Get(ctx, "same-id")
	if err != nil || n.State != "pending" {
		t.Fatalf("not recovered: %v %v", n, err)
	}
	if err = e.replayNotificationSpool(ctx); err != nil {
		t.Fatal(err)
	}
	paths, _ = filepath.Glob(filepath.Join(e.config.QueueDir, "notifications", "*.json"))
	if len(paths) != 0 {
		t.Fatal("acknowledged spool retained")
	}
}
func TestCompletionRecoveryAdmitsNotificationsOnce(t *testing.T) {
	e := jobTestEngine(t)
	ctx := context.Background()
	var deliveries atomic.Int32
	e.notifications.deliver = func(context.Context, *Notification) error { deliveries.Add(1); return nil }
	job := &types.Job{ID: "notification-recovery", Event: "missing-event", Complete: true, TimeEnd: 1, NotifySuccess: "one@example.test,two@example.test", WebHook: "https://example.test/hook"}
	if err := e.jobManager.writeCompletionSpool(job); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.persistCompletion(job); err != nil {
		t.Fatal(err)
	}
	if err := e.notifications.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if deliveries.Load() != 3 {
		t.Fatalf("deliveries %d want 3", deliveries.Load())
	}
	if err := e.jobManager.persistCompletion(job); err != nil {
		t.Fatal(err)
	}
	if err := e.notifications.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if deliveries.Load() != 3 {
		t.Fatal("completion replay duplicated notifications")
	}
}

func TestCompletionNotificationTargetsRemainFrozen(t *testing.T) {
	e := jobTestEngine(t)
	ctx := context.Background()
	e.config.UniversalWebHook = "https://example.test/original"
	job := &types.Job{ID: "frozen-notification", Event: "none", Complete: true, TimeEnd: 1, NotifySuccess: "original@example.test"}
	for i := 0; i < 3; i++ {
		if err := e.jobManager.persistCompletion(job); err != nil {
			t.Fatal(err)
		}
		e.config.UniversalWebHook = "https://example.test/changed"
		job.NotifySuccess = "changed@example.test"
	}
	rows, err := e.notifications.List(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("changed targets created additional deliveries: %d", len(rows))
	}
	for _, row := range rows {
		if strings.Contains(string(row.Payload), "changed") {
			t.Fatalf("target changed %s", row.Payload)
		}
	}
}

func TestNotificationConfigurationValidation(t *testing.T) {
	for _, cfg := range []types.Config{
		{SMTPPort: -1}, {SMTPPort: 65536}, {SMTPTimeoutSeconds: 301}, {SMTPTimeoutSeconds: -1},
		{SMTPTLSMode: "invalid"}, {SMTPUsername: "only-user"}, {SMTPPassword: "only-password"},
		{SMTPTLSMode: "none", SMTPUsername: "user", SMTPPassword: "secret"}, {MaxEmailsPerDay: -1},
		{NotificationWorkers: 65}, {NotificationMaxAttempts: 101},
	} {
		dir := t.TempDir()
		cfg.StorageBaseDir = filepath.Join(dir, "data")
		cfg.LogDir = filepath.Join(dir, "logs")
		cfg.QueueDir = filepath.Join(dir, "queue")
		if e, err := NewEngine(&cfg, nil); err == nil {
			e.cancel()
			t.Fatalf("invalid config accepted: %+v", cfg)
		}
	}
}
