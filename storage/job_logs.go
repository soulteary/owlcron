package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const JobLogChunkSize = 64 * 1024

// JobLogAbsoluteMax bounds manifest corruption and cleanup work independently of configuration.
const JobLogAbsoluteMax int64 = 1 << 30

var ErrJobLogDeleted = errors.New("job log deleted")
var ErrJobLogLimit = errors.New("job log capacity exceeded")

type JobLogManifest struct {
	Format      int    `json:"format,omitempty"`
	Offset      int64  `json:"offset"`
	Final       bool   `json:"final"`
	Deleted     bool   `json:"deleted"`
	PendingEnd  int64  `json:"pending_end,omitempty"`
	PendingHash string `json:"pending_hash,omitempty"`
}

// RangeStorage reads at most limit bytes from a legacy object without buffering
// its entire body. Unsupported backends must not fall back to Get.
type RangeStorage interface {
	GetRange(context.Context, string, int64, int) ([]byte, int64, error)
}

func jobLogPrefix(id string) string {
	return "job_log_chunks/" + base64.RawURLEncoding.EncodeToString([]byte(id)) + "/"
}
func jobLogManifestKey(id string) string {
	return "job_log_manifest/" + base64.RawURLEncoding.EncodeToString([]byte(id))
}
func JobLogInfo(ctx context.Context, s Storage, id string) (JobLogManifest, error) {
	var m JobLogManifest
	b, e := s.Get(ctx, jobLogManifestKey(id))
	if e != nil {
		return m, e
	}
	e = decodeJobLogManifest(b, &m)
	return m, e
}

// JobLogAppend reserves a bounded append, extends at most two fixed-size chunks,
// then publishes its end offset. Chunk prefixes cannot change or shrink. A
// pending reservation is resumed with the same offset and bytes after a crash.
func JobLogAppend(ctx context.Context, s Storage, id string, offset int64, data []byte, final bool, maxBytes int64) (JobLogManifest, error) {
	var result JobLogManifest
	if id == "" || offset < 0 || len(data) > JobLogChunkSize {
		return result, fmt.Errorf("invalid job log append")
	}
	if maxBytes <= 0 || maxBytes > JobLogAbsoluteMax || offset > maxBytes-int64(len(data)) {
		return result, ErrJobLogLimit
	}
	a, ok := s.(AtomicStorage)
	if !ok {
		return result, ErrUnsupported
	}
	// Stage outside the manifest callback: filesystem transactions hold a store-wide
	// lock, and remote CAS callbacks may be evaluated repeatedly.
	keys := []string{}
	if len(data) > 0 {
		digest := sha256.Sum256(data)
		hash := hex.EncodeToString(digest[:])
		if e := a.Update(ctx, jobLogManifestKey(id), func(raw []byte) ([]byte, error) {
			var m JobLogManifest
			if len(raw) > 0 {
				if e := decodeJobLogManifest(raw, &m); e != nil {
					return nil, e
				}
			}
			if m.Deleted {
				return nil, ErrJobLogDeleted
			}
			end := offset + int64(len(data))
			if offset > m.Offset || (offset < m.Offset && end > m.Offset) || (m.Final && end > m.Offset) {
				return nil, ErrConflict
			}
			if offset < m.Offset {
				return raw, nil
			}
			if m.PendingEnd != 0 && (m.PendingEnd != end || m.PendingHash != hash) {
				return nil, ErrConflict
			}
			m.PendingEnd = end
			m.PendingHash = hash
			return json.Marshal(m)
		}); e != nil {
			return result, e
		}

		for pos := 0; pos < len(data); {
			at := offset + int64(pos)
			within := int(at % JobLogChunkSize)
			n := JobLogChunkSize - within
			if n > len(data)-pos {
				n = len(data) - pos
			}
			key := jobLogPrefix(id) + fmt.Sprintf("%020d", at/JobLogChunkSize)
			keys = append(keys, key)
			part := data[pos : pos+n]
			if e := a.Update(ctx, key, func(old []byte) ([]byte, error) {
				var c jobLogChunk
				if len(old) > 0 {
					if e := json.Unmarshal(old, &c); e != nil {
						return nil, e
					}
				}
				if c.Deleted {
					return nil, ErrJobLogDeleted
				}
				if within > len(c.Data) {
					return nil, ErrConflict
				}
				overlap := len(c.Data) - within
				if overlap > len(part) {
					overlap = len(part)
				}
				if !bytes.Equal(c.Data[within:within+overlap], part[:overlap]) {
					return nil, ErrConflict
				}
				if overlap < len(part) {
					c.Data = append(c.Data, part[overlap:]...)
				}
				return json.Marshal(c)
			}); e != nil {
				return result, e
			}
			pos += n
		}
	}

	err := a.Update(ctx, jobLogManifestKey(id), func(raw []byte) ([]byte, error) {
		var m JobLogManifest
		if len(raw) > 0 {
			if e := decodeJobLogManifest(raw, &m); e != nil {
				return nil, e
			}
		}
		if m.Deleted {
			return nil, ErrJobLogDeleted
		}
		end := offset + int64(len(data))
		if offset > m.Offset || (offset < m.Offset && end > m.Offset) {
			return nil, ErrConflict
		}
		if end <= m.Offset && offset < m.Offset {
			result = m
			return raw, nil
		}
		if m.Final && end > m.Offset {
			return nil, ErrConflict
		}
		if len(data) == 0 && m.PendingEnd != 0 {
			return nil, ErrConflict
		}
		m.Offset = end
		m.PendingEnd = 0
		m.PendingHash = ""
		m.Final = m.Final || final
		result = m
		return json.Marshal(m)
	})
	if errors.Is(err, ErrJobLogDeleted) {
		for _, key := range keys {
			if e := jobLogDeleteChunk(ctx, a, key); e != nil {
				return result, e
			}
		}
	}

	return result, err
}

// JobLogRead returns a bounded range by fetching only overlapping fixed chunks.
func JobLogRead(ctx context.Context, s Storage, id string, offset int64, limit int) ([]byte, JobLogManifest, error) {
	m, e := JobLogInfo(ctx, s, id)
	if errors.Is(e, ErrNotFound) {
		if offset < 0 || limit <= 0 || limit > 1024*1024 {
			return nil, m, fmt.Errorf("invalid job log range")
		}
		r, ok := s.(RangeStorage)
		if !ok {
			return nil, m, ErrUnsupported
		}
		b, total, e := r.GetRange(ctx, "job_logs:"+id, offset, limit)
		m.Offset = total
		m.Final = true
		return b, m, e
	}
	if e != nil {
		return nil, m, e
	}
	if m.Deleted {
		return nil, m, ErrJobLogDeleted
	}
	if offset < 0 || limit <= 0 || limit > 1024*1024 {
		return nil, m, fmt.Errorf("invalid job log range")
	}
	if offset >= m.Offset {
		return []byte{}, m, nil
	}
	end := offset + int64(limit)
	if end > m.Offset {
		end = m.Offset
	}
	out := make([]byte, 0, end-offset)
	for at := offset; at < end; {
		key := jobLogPrefix(id) + fmt.Sprintf("%020d", at/JobLogChunkSize)
		raw, e := s.Get(ctx, key)
		if e != nil {
			return nil, m, e
		}
		var c jobLogChunk
		if e := json.Unmarshal(raw, &c); e != nil {
			return nil, m, e
		}
		if c.Deleted {
			return nil, m, ErrJobLogDeleted
		}
		if len(c.Data) > JobLogChunkSize {
			return nil, m, fmt.Errorf("oversized log chunk")
		}
		lo := int(at % JobLogChunkSize)
		n := JobLogChunkSize - lo
		if int64(n) > end-at {
			n = int(end - at)
		}
		if lo+n > len(c.Data) {
			return nil, m, fmt.Errorf("incomplete committed job log")
		}
		out = append(out, c.Data[lo:lo+n]...)
		at += int64(n)
	}

	return out, m, nil
}

// JobLogDelete installs a permanent fence, then replaces all committed and
// reserved chunks with small tombstones, including chunks not yet staged.
// Repeating it after any failure resumes reclamation safely.
func JobLogDelete(ctx context.Context, s Storage, id string) error {
	a, ok := s.(AtomicStorage)
	if !ok {
		return ErrUnsupported
	}
	var m JobLogManifest
	if e := a.Update(ctx, jobLogManifestKey(id), func(raw []byte) ([]byte, error) {
		m = JobLogManifest{}
		if len(raw) > 0 {
			if e := decodeJobLogManifest(raw, &m); e != nil {
				return nil, e
			}
		}
		m.Deleted = true
		m.Final = true
		return json.Marshal(m)
	}); e != nil {
		return e
	}
	end := m.Offset
	if m.PendingEnd > end {
		end = m.PendingEnd
	}
	for at := int64(0); at < end; at += JobLogChunkSize {
		key := jobLogPrefix(id) + fmt.Sprintf("%020d", at/JobLogChunkSize)
		if e := jobLogDeleteChunk(ctx, a, key); e != nil {
			return e
		}
	}

	// Generic remote Delete may fetch the entire old object to install its
	// backend tombstone. Require an explicit bounded removal capability.
	remover, ok := s.(LegacyLogRemover)
	if !ok {
		return ErrUnsupported
	}
	e := remover.RemoveLegacyJobLog(ctx, id)
	if errors.Is(e, ErrNotFound) {
		return nil
	}
	return e
}

type jobLogChunk struct {
	Data    []byte `json:"data,omitempty"`
	Deleted bool   `json:"deleted,omitempty"`
}

func jobLogDeleteChunk(ctx context.Context, a AtomicStorage, key string) error {
	return a.Update(ctx, key, func([]byte) ([]byte, error) { return json.Marshal(jobLogChunk{Deleted: true}) })
}

// decodeJobLogManifest rejects corrupt bounds before they can hide data or drive
// unbounded chunk cleanup. A reservation always covers one nonempty append.
func decodeJobLogManifest(raw []byte, m *JobLogManifest) error {
	if err := json.Unmarshal(raw, m); err != nil {
		return fmt.Errorf("invalid job log manifest: %w", err)
	}
	if m.Format != 0 && m.Format != 1 {
		return ErrJobLogFormat
	}
	if m.Offset < 0 || m.Offset > JobLogAbsoluteMax {
		return fmt.Errorf("invalid job log manifest offset")
	}
	if m.PendingEnd == 0 {
		if m.PendingHash != "" {
			return fmt.Errorf("invalid job log manifest pending hash")
		}
		return nil
	}
	if m.PendingEnd <= m.Offset || m.PendingEnd > JobLogAbsoluteMax || m.PendingEnd-m.Offset > JobLogChunkSize {
		return fmt.Errorf("invalid job log manifest pending range")
	}
	hash, err := hex.DecodeString(m.PendingHash)
	if err != nil || len(hash) != sha256.Size || hex.EncodeToString(hash) != m.PendingHash {
		return fmt.Errorf("invalid job log manifest pending hash")
	}
	return nil
}
