package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

// Sixteen configured stores may be visited once each in a minute-based
// maintenance rotation. Preserve cursors beyond one complete rotation.
const scanLifetime = 30 * time.Minute
const maxFilesystemScans = 8
const maxKeyDepth = 16

type Filesystem struct {
	root  *os.Root
	mu    sync.Mutex
	scans map[string]*filesystemScan
}

func NewFilesystem(baseDir string) (*Filesystem, error) {
	if baseDir == "" {
		return nil, fmt.Errorf("%w: filesystem base_dir is required", ErrInvalidConfig)
	}
	if err := os.MkdirAll(baseDir, 0700); err != nil {
		return nil, mapFileError(err)
	}
	root, err := os.OpenRoot(baseDir)
	if err != nil {
		return nil, mapFileError(err)
	}
	return &Filesystem{root: root, scans: make(map[string]*filesystemScan)}, nil
}

func (s *Filesystem) checkPath(key string, allowMissing bool) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	parts := strings.Split(key, "/")
	if len(parts) > maxKeyDepth {
		return fmt.Errorf("%w: object path is too deep", ErrInvalidConfig)
	}
	for i := range parts {
		info, err := s.root.Lstat(strings.Join(parts[:i+1], "/"))
		if errors.Is(err, os.ErrNotExist) && allowMissing {
			return nil
		}
		if err != nil {
			return mapFileError(err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlinks are not object paths", ErrIntegrity)
		}
		if i < len(parts)-1 && !info.IsDir() {
			return fmt.Errorf("%w: object parent is not a directory", ErrIntegrity)
		}
	}
	return nil
}

func (s *Filesystem) syncDirectory(dir string) error {
	f, err := s.root.Open(dir)
	if err != nil {
		return mapFileError(err)
	}
	defer f.Close()
	return f.Sync()
}

func (s *Filesystem) Put(ctx context.Context, spec Write, body io.Reader) error {
	data, err := prepare(ctx, spec, body)
	if err != nil {
		return err
	}
	if err := s.checkPath(spec.Key, true); err != nil {
		return err
	}
	dir := path.Dir(spec.Key)
	if err := s.root.MkdirAll(dir, 0700); err != nil {
		return mapFileError(err)
	}
	if err := s.checkPath(spec.Key, true); err != nil {
		return err
	}
	// Persist every new directory entry, not just the eventual object's parent.
	for parent := dir; ; parent = path.Dir(parent) {
		if err := s.syncDirectory(parent); err != nil {
			return err
		}
		if parent == "." {
			break
		}
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	tmp := path.Join(dir, ".owlcron-upload-"+hex.EncodeToString(nonce[:]))
	f, err := s.root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return mapFileError(err)
	}
	defer s.root.Remove(tmp)
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Linking a complete file provides no-replace publication. Plain rename
	// would silently replace a different object at the same immutable key.
	err = s.root.Link(tmp, spec.Key)
	if errors.Is(err, os.ErrExist) {
		old, readErr := s.Open(ctx, spec.Key)
		if readErr != nil {
			return readErr
		}
		existing, readErr := io.ReadAll(io.LimitReader(old, spec.Size+1))
		closeErr := old.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if !bytes.Equal(existing, data) {
			return fmt.Errorf("%w: immutable key contains different bytes", ErrIntegrity)
		}
		return s.syncDirectory(dir)
	}
	if err != nil {
		return mapFileError(err)
	}
	return s.syncDirectory(dir)
}

func (s *Filesystem) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.checkPath(key, false); err != nil {
		return nil, err
	}
	f, err := s.root.Open(key)
	if err != nil {
		return nil, mapFileError(err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, mapFileError(err)
	}
	if !info.Mode().IsRegular() || info.Size() > MaxObjectSize {
		f.Close()
		return nil, fmt.Errorf("%w: invalid object type or length", ErrIntegrity)
	}
	return &contextReadCloser{contextReader{ctx, f}, f}, nil
}

type contextReadCloser struct {
	contextReader
	closer io.Closer
}

func (r *contextReadCloser) Close() error { return r.closer.Close() }

func (s *Filesystem) Stat(ctx context.Context, key string) (Info, error) {
	if err := ctx.Err(); err != nil {
		return Info{}, err
	}
	if err := s.checkPath(key, false); err != nil {
		return Info{}, err
	}
	info, err := s.root.Stat(key)
	if err != nil {
		return Info{}, mapFileError(err)
	}
	if !info.Mode().IsRegular() || info.Size() > MaxObjectSize {
		return Info{}, fmt.Errorf("%w: invalid object type or length", ErrIntegrity)
	}
	return Info{Key: key, Size: info.Size(), LastModified: info.ModTime()}, nil
}

func (s *Filesystem) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkPath(key, false); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	info, err := s.root.Lstat(key)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return mapFileError(err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: object is not a regular file", ErrIntegrity)
	}
	if err = s.root.Remove(key); err != nil && !errors.Is(err, os.ErrNotExist) {
		return mapFileError(err)
	}
	return s.syncDirectory(path.Dir(key))
}

type scanDirectory struct {
	file *os.File
	key  string
}
type filesystemScan struct {
	prefix  string
	touched time.Time
	stack   []scanDirectory
}

func (scan *filesystemScan) close() {
	for _, dir := range scan.stack {
		dir.file.Close()
	}
	scan.stack = nil
}

// Scan holds at most eight short-lived directory walkers. Each call examines at
// most max(256, 2*limit) directory entries and never loads/sorts a whole directory.
// Cursors are single-use and process-local; a lost response simply starts a new
// eventually repeated inventory. Empty pages are possible when filtering keys.
func (s *Filesystem) Scan(ctx context.Context, prefix, cursor string, limit int) (Page, error) {
	if err := validateScan(prefix, limit); err != nil {
		return Page{}, err
	}
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for token, scan := range s.scans {
		if now.Sub(scan.touched) > scanLifetime {
			scan.close()
			delete(s.scans, token)
		}
	}
	var scan *filesystemScan
	if cursor != "" {
		scan = s.scans[cursor]
		if scan == nil || scan.prefix != prefix {
			return Page{}, ErrInvalidCursor
		}
		delete(s.scans, cursor)
	} else {
		if len(s.scans) >= maxFilesystemScans {
			var oldest string
			for token, other := range s.scans {
				if oldest == "" || other.touched.Before(s.scans[oldest].touched) {
					oldest = token
				}
			}
			s.scans[oldest].close()
			delete(s.scans, oldest)
		}
		dir := path.Dir(prefix)
		if strings.HasSuffix(prefix, "/") {
			dir = strings.TrimSuffix(prefix, "/")
		}
		if dir == "" {
			dir = "."
		}
		if dir != "." {
			if err := s.checkPath(dir, false); err != nil {
				if errors.Is(err, ErrNotFound) {
					return Page{}, nil
				}
				return Page{}, err
			}
		}
		f, err := s.root.Open(dir)
		if errors.Is(err, os.ErrNotExist) {
			return Page{}, nil
		}
		if err != nil {
			return Page{}, mapFileError(err)
		}
		scan = &filesystemScan{prefix: prefix, stack: []scanDirectory{{f, dir}}}
	}
	keep := false
	defer func() {
		if !keep {
			scan.close()
		}
	}()
	page := Page{Objects: make([]Info, 0, limit)}
	budget := max(256, 2*limit)
	for examined := 0; len(scan.stack) > 0 && len(page.Objects) < limit && examined < budget; examined++ {
		if err := ctx.Err(); err != nil {
			return Page{}, err
		}
		dir := scan.stack[len(scan.stack)-1]
		entries, err := dir.file.ReadDir(1)
		if err != nil && !errors.Is(err, io.EOF) {
			return Page{}, mapFileError(err)
		}
		if len(entries) == 0 {
			dir.file.Close()
			scan.stack = scan.stack[:len(scan.stack)-1]
			continue
		}
		entry := entries[0]
		key := path.Join(dir.key, entry.Name())
		if entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		if strings.HasPrefix(entry.Name(), ".owlcron-") {
			// A killed uploader can leave an unpublished temporary file. Removing
			// an old temp can only make its link fail; it cannot unpublish data.
			if strings.HasPrefix(entry.Name(), ".owlcron-upload-") {
				if info, e := entry.Info(); e == nil && info.Mode().IsRegular() && now.Sub(info.ModTime()) > time.Hour {
					if e = s.root.Remove(key); e == nil {
						_ = s.syncDirectory(dir.key)
					}
				}
			}
			continue
		}
		if entry.IsDir() {
			if len(scan.stack) >= maxKeyDepth {
				return Page{}, fmt.Errorf("%w: inventory directory depth exceeded", ErrIntegrity)
			}
			f, e := s.root.Open(key)
			if errors.Is(e, os.ErrNotExist) {
				continue
			}
			if e != nil {
				return Page{}, mapFileError(e)
			}
			scan.stack = append(scan.stack, scanDirectory{f, key})
			continue
		}
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		info, e := entry.Info()
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return Page{}, mapFileError(e)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		page.Objects = append(page.Objects, Info{Key: key, Size: info.Size(), LastModified: info.ModTime()})
	}
	if len(scan.stack) > 0 {
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return Page{}, err
		}
		page.NextCursor = hex.EncodeToString(token[:])
		scan.touched = now
		s.scans[page.NextCursor] = scan
		keep = true
	}
	return page, nil
}

func (s *Filesystem) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for token, scan := range s.scans {
		scan.close()
		delete(s.scans, token)
	}
	return s.root.Close()
}

func mapFileError(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	if errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("%w: %w", ErrPermission, err)
	}
	return err
}
