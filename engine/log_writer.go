package engine

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var ErrLogCapacityExceeded = errors.New("job log capacity exceeded")

// boundedLogWriter applies one quota to all writers of a task's log. Opening a
// retry uses the existing file size, so retries cannot reset the quota. limit is
// evaluated for each write to honor runtime changes; nonpositive means unlimited.
type boundedLogWriter struct {
	mu      sync.Mutex
	file    io.Writer
	size    int64
	limit   func() int64
	onError func(error)
	err     error
}

func newBoundedLogWriter(file *os.File, limit func() int64, onError func(error)) (*boundedLogWriter, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &boundedLogWriter{file: file, size: info.Size(), limit: limit, onError: onError}, nil
}

func (w *boundedLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return 0, err
	}
	data := p
	var quotaErr error
	if w.limit != nil {
		limit := w.limit()
		if limit > 0 && int64(len(p)) > limit-w.size {
			remaining := limit - w.size
			if remaining < 0 {
				remaining = 0
			}
			data = p[:int(remaining)]
			quotaErr = fmt.Errorf("%w (%d bytes)", ErrLogCapacityExceeded, limit)
		}
	}
	n := 0
	var err error
	if len(data) > 0 {
		n, err = w.file.Write(data)
		w.size += int64(n)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = quotaErr
	}
	if err != nil {
		w.err = err
	}
	callback := w.onError
	w.mu.Unlock()
	if err != nil && callback != nil {
		callback(err)
	}
	return n, err
}

func (w *boundedLogWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}
