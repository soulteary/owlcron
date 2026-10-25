// Package blob stores small, immutable objects independently of scheduler state.
// Callers allocate unique keys before writing and may retry a key only with the
// same bytes. A successful Put is not a publication transaction: the caller must
// publish its reference through an atomic metadata update.
package blob

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const MaxObjectSize int64 = 64 * 1024
const MaxScanLimit = 1000

var (
	ErrNotFound      = errors.New("blob not found")
	ErrPermission    = errors.New("blob access denied")
	ErrIntegrity     = errors.New("blob integrity failure")
	ErrInvalidConfig = errors.New("invalid blob configuration")
	ErrInvalidCursor = errors.New("invalid or expired blob inventory cursor")
)

type Write struct {
	Key    string
	Size   int64
	SHA256 [32]byte
}

type Info struct {
	Key          string
	Size         int64
	LastModified time.Time
}

type Page struct {
	Objects    []Info
	NextCursor string
}

type Store interface {
	Put(context.Context, Write, io.Reader) error
	Open(context.Context, string) (io.ReadCloser, error)
	Stat(context.Context, string) (Info, error)
	Delete(context.Context, string) error
}

// Inventory is deliberately separate from the normal object access contract.
// Scans are not snapshots. A page may be empty while NextCursor is nonempty.
// Cursors may expire or become invalid after a process restart; restart at ""
// on ErrInvalidCursor. Periodic scans must restart to find late writes.
type Inventory interface {
	Scan(ctx context.Context, prefix, cursor string, limit int) (Page, error)
}

func ValidateKey(key string) error {
	if key == "" || len(key) > 1024 || strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") || strings.ContainsAny(key, "\\\x00\r\n") {
		return fmt.Errorf("%w: invalid object key", ErrInvalidConfig)
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." || part == ".." || len(part) > 255 || strings.HasPrefix(part, ".owlcron-") {
			return fmt.Errorf("%w: invalid object key component", ErrInvalidConfig)
		}
	}
	return nil
}

func validatePrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	return ValidateKey(strings.TrimSuffix(prefix, "/"))
}

func validateScan(prefix string, limit int) error {
	if err := validatePrefix(prefix); err != nil {
		return err
	}
	if limit < 1 || limit > MaxScanLimit {
		return fmt.Errorf("%w: inventory limit must be 1..%d", ErrInvalidConfig, MaxScanLimit)
	}
	return nil
}

func prepare(ctx context.Context, spec Write, body io.Reader) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateKey(spec.Key); err != nil {
		return nil, err
	}
	if spec.Size < 0 || spec.Size > MaxObjectSize || body == nil {
		return nil, fmt.Errorf("%w: object size must be 0..%d", ErrInvalidConfig, MaxObjectSize)
	}
	data, err := io.ReadAll(io.LimitReader(contextReader{ctx, body}, spec.Size+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != spec.Size || sha256.Sum256(data) != spec.SHA256 {
		return nil, fmt.Errorf("%w: body length or SHA-256 differs from upload plan", ErrIntegrity)
	}
	return data, ctx.Err()
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
