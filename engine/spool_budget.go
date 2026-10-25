package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

var ErrSpoolCapacityExceeded = errors.New("node recovery spool capacity exceeded")

// Engine-managed log writes leave 5% for recovery records. Admission pauses at
// 90% and resumes below the configured low watermark. Removed files are credited
// by reconciliation, never speculatively before a durable completion succeeds.
type nodeSpoolBudget struct {
	mu            sync.Mutex
	roots         []string
	maximum       int64
	resumePercent int
	used          int64
	blocked       bool
	initialized   bool
	inspectionErr error
}

func newNodeSpoolBudget(maximum int64, resume int, roots []string) *nodeSpoolBudget {
	return &nodeSpoolBudget{maximum: maximum, resumePercent: resume, roots: roots}
}
func fraction(value int64, percent int) int64 {
	return value/100*int64(percent) + value%100*int64(percent)/100
}
func (b *nodeSpoolBudget) updateBlocked() {
	if b.used >= fraction(b.maximum, 90) {
		b.blocked = true
	}
	if b.blocked && b.used <= fraction(b.maximum, b.resumePercent) {
		b.blocked = false
	}
}
func (b *nodeSpoolBudget) refresh(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	err := b.refreshLocked(ctx)
	b.inspectionErr = err
	if err != nil {
		b.blocked = true
	}
	return err
}
func (b *nodeSpoolBudget) refreshLocked(ctx context.Context) error {
	roots := make([]string, 0, len(b.roots))
	for _, root := range b.roots {
		absolute, err := filepath.Abs(root)
		if err != nil {
			return err
		}
		roots = append(roots, filepath.Clean(absolute))
	}
	sort.Strings(roots)
	var total int64
	var previous string
	for _, root := range roots {
		if root == previous || (previous != "" && strings.HasPrefix(root, previous+string(filepath.Separator))) {
			continue
		}
		n, err := spoolTreeBytes(ctx, root)
		if err != nil {
			return err
		}
		if n > math.MaxInt64-total {
			return fmt.Errorf("spool byte count overflow")
		}
		total += n
		previous = root
	}
	b.used = total
	b.initialized = true
	b.updateBlocked()
	return nil
}

// ReadDir is paged so a large recovery directory does not allocate its entire
// entry list. Symlinks are never followed into another volume or directory.
func spoolTreeBytes(ctx context.Context, path string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return 0, fmt.Errorf("symlink in recovery spool")
	}
	if info.Mode().IsRegular() {
		return info.Size(), nil
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("nonregular recovery spool entry")
	}
	dir, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer dir.Close()
	var total int64
	for {
		entries, readErr := dir.ReadDir(128)
		for _, entry := range entries {
			n, err := spoolTreeBytes(ctx, filepath.Join(path, entry.Name()))
			if err != nil {
				return 0, err
			}
			if n > math.MaxInt64-total {
				return 0, fmt.Errorf("spool byte count overflow")
			}
			total += n
		}
		if readErr == io.EOF {
			return total, nil
		}
		if readErr != nil {
			return 0, readErr
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}
}
func (b *nodeSpoolBudget) admit() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.initialized {
		if err := b.refreshLocked(context.Background()); err != nil {
			return err
		}
	}
	if b.inspectionErr != nil {
		return b.inspectionErr
	}
	b.updateBlocked()
	if b.blocked {
		return ErrSpoolCapacityExceeded
	}
	return nil
}
func (b *nodeSpoolBudget) snapshot() (int64, int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.maximum, b.blocked
}

type spoolLogWriter struct {
	budget *nodeSpoolBudget
	dest   io.Writer
}

func (w *spoolLogWriter) Write(data []byte) (int, error) {
	b := w.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.initialized {
		if err := b.refreshLocked(context.Background()); err != nil {
			return 0, err
		}
	}
	ceiling := fraction(b.maximum, 95)
	if int64(len(data)) > ceiling-b.used {
		if err := b.refreshLocked(context.Background()); err != nil {
			return 0, err
		}
	}
	allowed := ceiling - b.used
	if allowed < 0 {
		allowed = 0
	}
	var quota error
	if int64(len(data)) > allowed {
		data = data[:int(allowed)]
		quota = ErrSpoolCapacityExceeded
	}
	n := 0
	var err error
	if len(data) != 0 {
		n, err = w.dest.Write(data)
		b.used += int64(n)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
	}
	b.updateBlocked()
	if err == nil {
		err = quota
	}
	return n, err
}
func (jm *JobManager) newJobLogWriter(file *os.File, limit func() int64, onError func(error)) (*boundedLogWriter, error) {
	w, err := newBoundedLogWriter(file, limit, onError)
	if err == nil && jm.engine.spoolBudget != nil {
		w.file = &spoolLogWriter{budget: jm.engine.spoolBudget, dest: w.file}
	}
	return w, err
}

// Reject oversized allocations before building their in-memory file contents.
// The eventual writer still checks capacity while holding the budget lock.
func (e *Engine) checkSpoolWriteSize(size int64) error {
	b := e.spoolBudget
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.initialized || size > b.maximum-b.used {
		if err := b.refreshLocked(context.Background()); err != nil {
			return err
		}
	}
	if size > b.maximum-b.used {
		b.blocked = true
		return ErrSpoolCapacityExceeded
	}
	return nil
}

// writeSpoolFile accounts for the temporary replacement as well as the old
// record until rename. A sync/rename error is reconciled from the final file;
// an error never causes the previous recovery record to be deleted.
func (e *Engine) writeSpoolFile(path string, data []byte, pattern string) error {
	b := e.spoolBudget
	oldSize := int64(0)
	if b != nil {
		b.mu.Lock()
		defer b.mu.Unlock()
		if !b.initialized || int64(len(data)) > b.maximum-b.used {
			if err := b.refreshLocked(context.Background()); err != nil {
				return err
			}
		}
		if int64(len(data)) > b.maximum-b.used {
			b.blocked = true
			return ErrSpoolCapacityExceeded
		}
		if info, err := os.Lstat(path); err == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("nonregular recovery record")
			}
			oldSize = info.Size()
		} else if !os.IsNotExist(err) {
			return err
		}
		defer func() {
			if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() {
				b.used += info.Size() - oldSize
			}
			b.updateBlocked()
		}()
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
