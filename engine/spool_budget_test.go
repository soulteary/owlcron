package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSpoolBudgetFilesQuotaReserveAndResume(t *testing.T) {
	dir := t.TempDir()
	b := newNodeSpoolBudget(10000, 80, []string{dir})
	e := &Engine{spoolBudget: b}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		f, err := os.CreateTemp(dir, "log-*")
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer f.Close()
			w := &spoolLogWriter{budget: b, dest: f}
			_, err := w.Write(bytes.Repeat([]byte("x"), 1000))
			if err != nil && !errors.Is(err, ErrSpoolCapacityExceeded) {
				t.Errorf("write: %v", err)
			}
		}()
	}
	wg.Wait()
	actual, err := spoolTreeBytes(context.Background(), dir)
	if err != nil || actual != 9500 {
		t.Fatalf("quota: bytes=%d err=%v", actual, err)
	}
	if !errors.Is(b.admit(), ErrSpoolCapacityExceeded) {
		t.Fatal("full spool admitted new task")
	}
	recovery := filepath.Join(dir, "finished.json")
	if err := e.writeSpoolFile(recovery, bytes.Repeat([]byte("r"), 400), ".complete-*"); err != nil {
		t.Fatalf("recovery reserve lost: %v", err)
	}
	// Replacing a 400-byte record requires room for its full temporary file.
	if err := e.writeSpoolFile(recovery, bytes.Repeat([]byte("n"), 400), ".complete-*"); !errors.Is(err, ErrSpoolCapacityExceeded) {
		t.Fatalf("unaccounted temporary space: %v", err)
	}
	data, _ := os.ReadFile(recovery)
	if !bytes.Equal(data, bytes.Repeat([]byte("r"), 400)) {
		t.Fatal("quota failure destroyed previous recovery record")
	}
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if entry.Name() != "finished.json" {
			if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := b.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.admit(); err != nil {
		t.Fatalf("low watermark did not resume: %v", err)
	}
	if err := e.writeSpoolFile(recovery, []byte("short"), ".complete-*"); err != nil {
		t.Fatal(err)
	}
	n, _, _ := b.snapshot()
	if n != 5 {
		t.Fatalf("replacement accounting: %d", n)
	}
}

func TestSpoolBudgetRestartAndSymlinkBoundary(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "previous.log"), bytes.Repeat([]byte("x"), 900), 0600); err != nil {
		t.Fatal(err)
	}
	b := newNodeSpoolBudget(1000, 80, []string{dir, dir})
	if err := b.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	n, _, blocked := b.snapshot()
	if n != 900 || !blocked {
		t.Fatalf("restart/duplicate roots: %d %v", n, blocked)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(dir, "outside")); err != nil {
		t.Fatal(err)
	}
	if err := b.refresh(context.Background()); err == nil {
		t.Fatal("spool scanner followed symlink")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.refresh(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
