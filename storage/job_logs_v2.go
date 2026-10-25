package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/soulteary/owlcron/blob"
)

const (
	JobLogV2Format         = 2
	JobLogIDMaxBytes       = 180
	JobLogIndexPageEntries = 128
	JobLogV2RootMaxBytes   = 32 * 1024
	JobLogV2PageMaxBytes   = 64 * 1024
	jobLogV2MaxPages       = int(JobLogAbsoluteMax / JobLogChunkSize / JobLogIndexPageEntries)
)

var (
	ErrJobLogIntegrity = errors.New("job log integrity failure")
	ErrJobLogFormat    = errors.New("unsupported job log format")
	ErrJobLogPending   = errors.New("job log has an outstanding upload")
)

// JobLogObjectRef is relative to a root's namespace and owner. IDs carry their
// reservation sequence, kind and unique nonce. Published IDs are never reused.
type JobLogObjectRef struct {
	ID     string `json:"id"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type JobLogV2ChunkUpdate struct {
	Index  int64           `json:"index"`
	Object JobLogObjectRef `json:"object"`
}

type JobLogV2PageUpdate struct {
	Index  int             `json:"index"`
	Object JobLogObjectRef `json:"object"`
}

// JobLogV2Pending is the entire bounded allocation plan. Every object must be
// registered here before any PUT. Its bytes can be reconstructed from the
// committed tail, the caller's exact input and these allocated references.
type JobLogV2Pending struct {
	AttemptID     string                `json:"attempt_id"`
	UploadSeq     uint64                `json:"upload_seq"`
	BaseCommitSeq uint64                `json:"base_commit_seq"`
	Start         int64                 `json:"start"`
	End           int64                 `json:"end"`
	InputSHA256   string                `json:"input_sha256"`
	Final         bool                  `json:"final"`
	Deadline      int64                 `json:"deadline"`
	Chunks        []JobLogV2ChunkUpdate `json:"chunks"`
	Pages         []JobLogV2PageUpdate  `json:"pages"`
}

type JobLogV2Root struct {
	Format      int               `json:"format"`
	Namespace   string            `json:"namespace_id"`
	Incarnation string            `json:"incarnation"`
	StoreID     string            `json:"store_id"`
	CommitSeq   uint64            `json:"commit_seq"`
	IssuedSeq   uint64            `json:"issued_seq"`
	Offset      int64             `json:"offset"`
	Final       bool              `json:"final"`
	Deleted     bool              `json:"deleted"`
	Pages       []JobLogObjectRef `json:"pages"`
	Pending     *JobLogV2Pending  `json:"pending,omitempty"`
}

type jobLogV2Page struct {
	Format      int               `json:"format"`
	Incarnation string            `json:"incarnation"`
	Index       int               `json:"index"`
	Chunks      []JobLogObjectRef `json:"chunks"`
}

type JobLogRepositoryOptions struct {
	Namespace      string
	WriteStore     string
	WriteFormat    int
	PendingTimeout time.Duration
	GCGracePeriod  time.Duration
}

// JobLogRepository explicitly separates authoritative state from immutable
// content. It never routes arbitrary Storage operations to the blob backend.
type JobLogRepository struct {
	meta   Storage
	atomic AtomicStorage
	stores map[string]blob.Store
	opts   JobLogRepositoryOptions
	now    func() time.Time
}

func NewJobLogRepository(meta Storage, stores map[string]blob.Store, opts JobLogRepositoryOptions) (*JobLogRepository, error) {
	if meta == nil {
		return nil, fmt.Errorf("job log metadata store is required")
	}
	if opts.WriteFormat == 0 {
		opts.WriteFormat = 1
	}
	if opts.WriteFormat != 1 && opts.WriteFormat != 2 {
		return nil, ErrJobLogFormat
	}
	if opts.PendingTimeout == 0 {
		opts.PendingTimeout = 5 * time.Minute
	}
	if opts.PendingTimeout < 0 || opts.GCGracePeriod < 0 {
		return nil, fmt.Errorf("invalid job log timeout")
	}
	a, _ := meta.(AtomicStorage)
	if opts.WriteFormat == 2 && (a == nil || !validLogComponent(opts.Namespace) || !validLogComponent(opts.WriteStore) || stores[opts.WriteStore] == nil) {
		return nil, fmt.Errorf("v2 logs require atomic metadata, a namespace and a configured write store")
	}
	copyStores := make(map[string]blob.Store, len(stores))
	for id, store := range stores {
		copyStores[id] = store
	}
	return &JobLogRepository{meta: meta, atomic: a, stores: copyStores, opts: opts, now: time.Now}, nil
}

func JobLogManifestKey(id string) string { return jobLogManifestKey(id) }

func JobLogIDFromManifestKey(key string) (string, bool, error) {
	const prefix = "job_log_manifest/"
	if !strings.HasPrefix(key, prefix) {
		return "", false, nil
	}
	encoded := strings.TrimPrefix(key, prefix)
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(raw) == 0 || len(raw) > JobLogIDMaxBytes || base64.RawURLEncoding.EncodeToString(raw) != encoded {
		return "", true, fmt.Errorf("invalid job log manifest key")
	}
	return string(raw), true, nil
}

func validLogComponent(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func validLogHex(s string, size int) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == size && hex.EncodeToString(b) == s
}

func newLogNonce() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func newLogObjectID(seq uint64, kind byte) (string, error) {
	nonce, err := newLogNonce()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%020d-%c-%s", seq, kind, nonce), nil
}

func parseLogObjectID(id string) (uint64, byte, error) {
	if len(id) != 55 || id[20] != '-' || id[22] != '-' || (id[21] != 'c' && id[21] != 'p') || !validLogHex(id[23:], 16) {
		return 0, 0, fmt.Errorf("%w: invalid object id", ErrJobLogIntegrity)
	}
	n, err := strconv.ParseUint(id[:20], 10, 64)
	if err != nil || n == 0 || fmt.Sprintf("%020d", n) != id[:20] {
		return 0, 0, fmt.Errorf("%w: invalid object sequence", ErrJobLogIntegrity)
	}
	return n, id[21], nil
}

func logObjectKey(namespace, id, incarnation, objectID string) string {
	return namespace + "/logs/" + base64.RawURLEncoding.EncodeToString([]byte(id)) + "/" + incarnation + "/" + objectID
}

func parseLogObjectKey(key string) (namespace, id, incarnation, objectID string, err error) {
	parts := strings.Split(key, "/")
	if len(parts) != 5 || parts[1] != "logs" || !validLogComponent(parts[0]) || !validLogHex(parts[3], 16) {
		err = fmt.Errorf("%w: invalid log object key", ErrJobLogIntegrity)
		return
	}
	raw, e := base64.RawURLEncoding.DecodeString(parts[2])
	if e != nil || len(raw) == 0 || len(raw) > JobLogIDMaxBytes || base64.RawURLEncoding.EncodeToString(raw) != parts[2] {
		err = fmt.Errorf("%w: invalid log object owner", ErrJobLogIntegrity)
		return
	}
	if _, _, e = parseLogObjectID(parts[4]); e != nil {
		err = e
		return
	}
	return parts[0], string(raw), parts[3], parts[4], nil
}

func RebindJobLogObjectKey(key, newNamespace string) (string, error) {
	if !validLogComponent(newNamespace) {
		return "", fmt.Errorf("invalid log namespace")
	}
	_, id, incarnation, objectID, err := parseLogObjectKey(key)
	if err != nil {
		return "", err
	}
	return logObjectKey(newNamespace, id, incarnation, objectID), nil
}

func strictLogJSON(raw []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return fmt.Errorf("%w: %v", ErrJobLogIntegrity, err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("%w: trailing JSON data", ErrJobLogIntegrity)
	}
	return nil
}

func logManifestFormat(raw []byte) (int, error) {
	var header struct {
		Format int `json:"format"`
	}
	if len(raw) == 0 {
		return 0, ErrNotFound
	}
	if len(raw) > JobLogV2RootMaxBytes {
		return 0, fmt.Errorf("%w: oversized manifest", ErrJobLogIntegrity)
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return 0, fmt.Errorf("%w: invalid manifest", ErrJobLogIntegrity)
	}
	if header.Format == 0 || header.Format == 1 {
		return 1, nil
	}
	if header.Format != 2 {
		return 0, ErrJobLogFormat
	}
	return 2, nil
}

func validateLogRef(ref JobLogObjectRef, kind byte, issued uint64) error {
	seq, k, err := parseLogObjectID(ref.ID)
	if err != nil {
		return err
	}
	if seq > issued || k != kind || ref.Size <= 0 || ref.Size > JobLogChunkSize || !validLogHex(ref.SHA256, sha256.Size) {
		return fmt.Errorf("%w: invalid object reference", ErrJobLogIntegrity)
	}
	return nil
}

func DecodeJobLogV2Root(raw []byte) (*JobLogV2Root, error) {
	if len(raw) == 0 || len(raw) > JobLogV2RootMaxBytes {
		return nil, fmt.Errorf("%w: invalid root size", ErrJobLogIntegrity)
	}
	var r JobLogV2Root
	if err := strictLogJSON(raw, &r); err != nil {
		return nil, err
	}
	if err := validateLogRoot(&r); err != nil {
		return nil, err
	}
	return &r, nil
}

func EncodeJobLogV2Root(r *JobLogV2Root) ([]byte, error) {
	if err := validateLogRoot(r); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(raw) > JobLogV2RootMaxBytes {
		return nil, fmt.Errorf("%w: oversized root", ErrJobLogIntegrity)
	}
	return raw, nil
}

func validateLogRoot(r *JobLogV2Root) error {
	if r == nil || r.Format != 2 {
		return ErrJobLogFormat
	}
	if !validLogComponent(r.Namespace) || !validLogComponent(r.StoreID) || !validLogHex(r.Incarnation, 16) || r.Offset < 0 || r.Offset > JobLogAbsoluteMax || len(r.Pages) > jobLogV2MaxPages {
		return fmt.Errorf("%w: invalid root bounds or identity", ErrJobLogIntegrity)
	}
	count := int((r.Offset + JobLogChunkSize*JobLogIndexPageEntries - 1) / (JobLogChunkSize * JobLogIndexPageEntries))
	if r.Deleted {
		if !r.Final || r.Pending != nil || len(r.Pages) != 0 {
			return fmt.Errorf("%w: invalid deleted root", ErrJobLogIntegrity)
		}
		return nil
	}
	if len(r.Pages) != count {
		return fmt.Errorf("%w: incomplete root index", ErrJobLogIntegrity)
	}
	seen := make(map[string]bool, len(r.Pages)+4)
	for _, ref := range r.Pages {
		if err := validateLogRef(ref, 'p', r.IssuedSeq); err != nil {
			return err
		}
		if seen[ref.ID] {
			return fmt.Errorf("%w: duplicate page", ErrJobLogIntegrity)
		}
		seen[ref.ID] = true
	}
	p := r.Pending
	if p == nil {
		return nil
	}
	if r.Final || !validLogHex(p.AttemptID, 16) || p.UploadSeq == 0 || p.UploadSeq != r.IssuedSeq || p.BaseCommitSeq != r.CommitSeq || p.Start != r.Offset || p.End <= p.Start || p.End > JobLogAbsoluteMax || p.End-p.Start > JobLogChunkSize || !validLogHex(p.InputSHA256, sha256.Size) || p.Deadline <= 0 {
		return fmt.Errorf("%w: invalid pending upload", ErrJobLogIntegrity)
	}
	first, last := p.Start/JobLogChunkSize, (p.End-1)/JobLogChunkSize
	if len(p.Chunks) != int(last-first+1) || len(p.Chunks) > 2 {
		return fmt.Errorf("%w: invalid pending chunks", ErrJobLogIntegrity)
	}
	for i, c := range p.Chunks {
		if c.Index != first+int64(i) {
			return fmt.Errorf("%w: invalid chunk position", ErrJobLogIntegrity)
		}
		if err := validateLogRef(c.Object, 'c', p.UploadSeq); err != nil {
			return err
		}
		seq, _, _ := parseLogObjectID(c.Object.ID)
		want := min(int64(JobLogChunkSize), p.End-c.Index*JobLogChunkSize)
		if seq != p.UploadSeq || c.Object.Size != want || seen[c.Object.ID] {
			return fmt.Errorf("%w: invalid pending chunk allocation", ErrJobLogIntegrity)
		}
		seen[c.Object.ID] = true
	}
	firstPage, lastPage := int(first/JobLogIndexPageEntries), int(last/JobLogIndexPageEntries)
	if len(p.Pages) != lastPage-firstPage+1 || len(p.Pages) > 2 {
		return fmt.Errorf("%w: invalid pending pages", ErrJobLogIntegrity)
	}
	for i, page := range p.Pages {
		if page.Index != firstPage+i {
			return fmt.Errorf("%w: invalid page position", ErrJobLogIntegrity)
		}
		if err := validateLogRef(page.Object, 'p', p.UploadSeq); err != nil {
			return err
		}
		seq, _, _ := parseLogObjectID(page.Object.ID)
		if seq != p.UploadSeq || seen[page.Object.ID] {
			return fmt.Errorf("%w: invalid pending page allocation", ErrJobLogIntegrity)
		}
		seen[page.Object.ID] = true
	}
	return nil
}

func RebindJobLogV2Root(raw []byte, namespace, storeID string) ([]byte, error) {
	r, err := DecodeJobLogV2Root(raw)
	if err != nil {
		return nil, err
	}
	if r.Pending != nil {
		return nil, ErrJobLogPending
	}
	r.Namespace = namespace
	r.StoreID = storeID
	return EncodeJobLogV2Root(r)
}

func (r *JobLogV2Root) view() JobLogManifest {
	m := JobLogManifest{Format: 2, Offset: r.Offset, Final: r.Final, Deleted: r.Deleted}
	if r.Pending != nil {
		m.PendingEnd = r.Pending.End
		m.PendingHash = r.Pending.InputSHA256
	}
	return m
}

func (s *JobLogRepository) root(ctx context.Context, id string) (*JobLogV2Root, int, error) {
	if len(id) == 0 || len(id) > JobLogIDMaxBytes {
		return nil, 0, fmt.Errorf("invalid job log id")
	}
	raw, err := s.meta.Get(ctx, jobLogManifestKey(id))
	if err != nil {
		return nil, 0, err
	}
	format, err := logManifestFormat(raw)
	if err != nil || format != 2 {
		return nil, format, err
	}
	r, err := DecodeJobLogV2Root(raw)
	if err != nil {
		return nil, 2, err
	}
	if s.opts.Namespace != "" && r.Namespace != s.opts.Namespace {
		return nil, 2, fmt.Errorf("%w: log namespace differs from configured layout", ErrJobLogIntegrity)
	}
	if s.stores[r.StoreID] == nil {
		return nil, 2, fmt.Errorf("%w: log store %q is not configured", ErrJobLogIntegrity, r.StoreID)
	}
	return r, 2, nil
}

func (s *JobLogRepository) Info(ctx context.Context, id string) (JobLogManifest, error) {
	r, format, err := s.root(ctx, id)
	if err != nil {
		return JobLogManifest{}, err
	}
	if format != 2 {
		return JobLogInfo(ctx, s.meta, id)
	}
	return r.view(), nil
}

func (s *JobLogRepository) readV2Root(ctx context.Context, id string) (*JobLogV2Root, error) {
	r, format, err := s.root(ctx, id)
	if err != nil {
		return nil, err
	}
	if format != 2 || r == nil {
		return nil, ErrJobLogFormat
	}
	return r, nil
}

func (s *JobLogRepository) newRoot() (*JobLogV2Root, error) {
	incarnation, err := newLogNonce()
	if err != nil {
		return nil, err
	}
	return &JobLogV2Root{Format: 2, Namespace: s.opts.Namespace, StoreID: s.opts.WriteStore, Incarnation: incarnation, Pages: []JobLogObjectRef{}}, nil
}

func logDigest(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }

func readLogObject(ctx context.Context, store blob.Store, key string, ref JobLogObjectRef) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := store.Open(ctx, key)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return nil, fmt.Errorf("%w: missing referenced object", ErrJobLogIntegrity)
		}
		return nil, err
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, ref.Size+1))
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if int64(len(data)) != ref.Size || logDigest(data) != ref.SHA256 {
		return nil, fmt.Errorf("%w: object size or hash mismatch", ErrJobLogIntegrity)
	}
	return data, nil
}

func (s *JobLogRepository) readPage(ctx context.Context, id string, r *JobLogV2Root, index int) (*jobLogV2Page, []byte, error) {
	if index < 0 || index >= len(r.Pages) {
		return nil, nil, fmt.Errorf("%w: page outside committed range", ErrJobLogIntegrity)
	}
	ref := r.Pages[index]
	raw, err := readLogObject(ctx, s.stores[r.StoreID], logObjectKey(r.Namespace, id, r.Incarnation, ref.ID), ref)
	if err != nil {
		return nil, nil, err
	}
	var page jobLogV2Page
	if err = strictLogJSON(raw, &page); err != nil {
		return nil, nil, err
	}
	if page.Format != 2 || page.Incarnation != r.Incarnation || page.Index != index {
		return nil, nil, fmt.Errorf("%w: wrong index page owner", ErrJobLogIntegrity)
	}
	nChunks := (r.Offset + JobLogChunkSize - 1) / JobLogChunkSize
	want := min(int64(JobLogIndexPageEntries), nChunks-int64(index*JobLogIndexPageEntries))
	if want <= 0 || int64(len(page.Chunks)) != want {
		return nil, nil, fmt.Errorf("%w: incomplete index page", ErrJobLogIntegrity)
	}
	seen := map[string]bool{}
	for i, c := range page.Chunks {
		if err = validateLogRef(c, 'c', r.IssuedSeq); err != nil {
			return nil, nil, err
		}
		logical := int64(index*JobLogIndexPageEntries + i)
		if c.Size != min(int64(JobLogChunkSize), r.Offset-logical*JobLogChunkSize) || seen[c.ID] {
			return nil, nil, fmt.Errorf("%w: invalid indexed chunk", ErrJobLogIntegrity)
		}
		seen[c.ID] = true
	}
	return &page, raw, nil
}

func (s *JobLogRepository) readRootRange(ctx context.Context, id string, r *JobLogV2Root, offset int64, limit int) ([]byte, error) {
	if r.Deleted {
		return nil, ErrJobLogDeleted
	}
	if offset >= r.Offset {
		return []byte{}, nil
	}
	end := min(r.Offset, offset+int64(limit))
	out := make([]byte, 0, end-offset)
	pageIndex := -1
	var page *jobLogV2Page
	for at := offset; at < end; {
		chunkIndex := at / JobLogChunkSize
		index := int(chunkIndex / JobLogIndexPageEntries)
		if index != pageIndex {
			var err error
			page, _, err = s.readPage(ctx, id, r, index)
			if err != nil {
				return nil, err
			}
			pageIndex = index
		}
		ref := page.Chunks[chunkIndex%JobLogIndexPageEntries]
		chunk, err := readLogObject(ctx, s.stores[r.StoreID], logObjectKey(r.Namespace, id, r.Incarnation, ref.ID), ref)
		if err != nil {
			return nil, err
		}
		lo := at % JobLogChunkSize
		n := min(int64(len(chunk))-lo, end-at)
		if n <= 0 {
			return nil, fmt.Errorf("%w: invalid chunk range", ErrJobLogIntegrity)
		}
		out = append(out, chunk[lo:lo+n]...)
		at += n
	}
	return out, nil
}

func (s *JobLogRepository) Read(ctx context.Context, id string, offset int64, limit int) ([]byte, JobLogManifest, error) {
	if offset < 0 || limit <= 0 || limit > 1024*1024 {
		return nil, JobLogManifest{}, fmt.Errorf("invalid job log range")
	}
	for retry := 0; retry < 3; retry++ {
		r, format, err := s.root(ctx, id)
		if errors.Is(err, ErrNotFound) || err == nil && format != 2 {
			return JobLogRead(ctx, s.meta, id, offset, limit)
		}
		if err != nil {
			return nil, JobLogManifest{}, err
		}
		out, err := s.readRootRange(ctx, id, r, offset, limit)
		if err == nil {
			current, currentFormat, readErr := s.root(ctx, id)
			if readErr != nil {
				if errors.Is(readErr, ErrNotFound) {
					readErr = fmt.Errorf("%w: committed root disappeared", ErrJobLogIntegrity)
				}
				return nil, r.view(), readErr
			}
			if currentFormat != 2 || current.Incarnation != r.Incarnation {
				return nil, r.view(), ErrJobLogIntegrity
			}
			if current.Deleted {
				return nil, current.view(), ErrJobLogDeleted
			}
			return out, r.view(), nil
		}
		if !errors.Is(err, ErrJobLogIntegrity) {
			return nil, r.view(), err
		}
		current, currentFormat, readErr := s.root(ctx, id)
		if readErr != nil {
			if errors.Is(readErr, ErrNotFound) {
				readErr = fmt.Errorf("%w: committed root disappeared", ErrJobLogIntegrity)
			}
			return nil, r.view(), readErr
		}
		if currentFormat != 2 {
			return nil, r.view(), ErrJobLogIntegrity
		}
		if current.Incarnation != r.Incarnation || current.CommitSeq == r.CommitSeq {
			return nil, r.view(), err
		}
	}
	return nil, JobLogManifest{}, fmt.Errorf("%w: log changed while reading", ErrConflict)
}

func nextLogSequence(seq uint64) (uint64, error) {
	if seq == math.MaxUint64 {
		return 0, fmt.Errorf("%w: exhausted log sequence", ErrJobLogIntegrity)
	}
	return seq + 1, nil
}
