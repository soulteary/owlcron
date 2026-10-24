package engine

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestBoundedLogConcurrentQuotaAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.log")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	var callbacks atomic.Int32
	writer, err := newBoundedLogWriter(file, func() int64 { return 1000 }, func(error) { callbacks.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = writer.Write(bytes.Repeat([]byte("x"), 37)) }()
	}
	wg.Wait()
	if !errors.Is(writer.Err(), ErrLogCapacityExceeded) {
		t.Fatal(writer.Err())
	}
	if callbacks.Load() != 1 {
		t.Fatalf("callbacks %d", callbacks.Load())
	}
	file.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 1000 {
		t.Fatalf("size %d", len(data))
	}
	file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	retry, err := newBoundedLogWriter(file, func() int64 { return 1000 }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := retry.Write([]byte("retry")); n != 0 || !errors.Is(err, ErrLogCapacityExceeded) {
		t.Fatalf("retry %d %v", n, err)
	}
}

func TestBoundedLogDynamicLimitAndCallbackUnlocked(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "log")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var limit atomic.Int64
	limit.Store(100)
	var writer *boundedLogWriter
	writer, err = newBoundedLogWriter(file, limit.Load, func(err error) {
		if writer.Err() != err {
			t.Error("first failure lost")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("12345")); err != nil {
		t.Fatal(err)
	}
	limit.Store(7)
	if n, err := writer.Write([]byte("67890")); n != 2 || !errors.Is(err, ErrLogCapacityExceeded) {
		t.Fatalf("write %d %v", n, err)
	}
	info, _ := file.Stat()
	if info.Size() != 7 {
		t.Fatal(info.Size())
	}
}

type failingLogDestination struct {
	err   error
	short bool
}

func (w failingLogDestination) Write(p []byte) (int, error) {
	if w.short {
		return len(p) - 1, nil
	}
	return 0, w.err
}

func TestBoundedLogDiskFailureSticky(t *testing.T) {
	diskErr := errors.New("disk unavailable")
	for _, test := range []struct {
		name string
		dest io.Writer
		want error
	}{{"disk", failingLogDestination{err: diskErr}, diskErr}, {"short", failingLogDestination{short: true}, io.ErrShortWrite}} {
		t.Run(test.name, func(t *testing.T) {
			w := &boundedLogWriter{file: test.dest}
			_, err := w.Write([]byte("abc"))
			if !errors.Is(err, test.want) {
				t.Fatal(err)
			}
			if n, err := w.Write([]byte("x")); n != 0 || !errors.Is(err, test.want) {
				t.Fatalf("retry %d %v", n, err)
			}
		})
	}
}
