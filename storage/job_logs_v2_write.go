package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/soulteary/owlcron/blob"
)

type frozenLogObject struct {
	ref  JobLogObjectRef
	data []byte
}

type frozenLogPlan struct {
	pending *JobLogV2Pending
	chunks  []frozenLogObject
	pages   []frozenLogObject
}

func sameLogInput(p *JobLogV2Pending, offset int64, data []byte) bool {
	return p != nil && p.Start == offset && p.End == offset+int64(len(data)) && p.InputSHA256 == logDigest(data)
}

func (s *JobLogRepository) prepareAppend(ctx context.Context, id string, r *JobLogV2Root, data []byte, final bool) (*frozenLogPlan, error) {
	p := r.Pending
	resuming := p != nil
	if resuming {
		// A live upload can be retried after the process completes. Keep the
		// original reservation's final intent; the completion path can seal
		// the committed length with an empty append afterward.
		if !sameLogInput(p, r.Offset, data) {
			return nil, ErrConflict
		}
	} else {
		seq, err := nextLogSequence(r.IssuedSeq)
		if err != nil {
			return nil, err
		}
		attempt, err := newLogNonce()
		if err != nil {
			return nil, err
		}
		p = &JobLogV2Pending{AttemptID: attempt, UploadSeq: seq, BaseCommitSeq: r.CommitSeq, Start: r.Offset, End: r.Offset + int64(len(data)), InputSHA256: logDigest(data), Final: final, Deadline: s.now().Add(s.opts.PendingTimeout).UnixNano()}
	}
	plan := &frozenLogPlan{pending: p}
	oldPages := map[int]*jobLogV2Page{}
	loadPage := func(index int) (*jobLogV2Page, error) {
		if page := oldPages[index]; page != nil {
			return page, nil
		}
		page := &jobLogV2Page{Format: 2, Incarnation: r.Incarnation, Index: index, Chunks: []JobLogObjectRef{}}
		if index < len(r.Pages) {
			var err error
			page, _, err = s.readPage(ctx, id, r, index)
			if err != nil {
				return nil, err
			}
		}
		oldPages[index] = page
		return page, nil
	}
	for pos := 0; pos < len(data); {
		at := r.Offset + int64(pos)
		index := at / JobLogChunkSize
		within := int(at % JobLogChunkSize)
		page, err := loadPage(int(index / JobLogIndexPageEntries))
		if err != nil {
			return nil, err
		}
		body := []byte{}
		if within > 0 {
			slot := int(index % JobLogIndexPageEntries)
			if slot >= len(page.Chunks) {
				return nil, fmt.Errorf("%w: missing committed tail", ErrJobLogIntegrity)
			}
			oldRef := page.Chunks[slot]
			body, err = readLogObject(ctx, s.stores[r.StoreID], logObjectKey(r.Namespace, id, r.Incarnation, oldRef.ID), oldRef)
			if err != nil {
				return nil, err
			}
			if len(body) != within {
				return nil, fmt.Errorf("%w: unexpected committed tail length", ErrJobLogIntegrity)
			}
		}
		n := min(JobLogChunkSize-within, len(data)-pos)
		body = append(body, data[pos:pos+n]...)
		ref := JobLogObjectRef{Size: int64(len(body)), SHA256: logDigest(body)}
		if resuming {
			ref.ID = p.Chunks[len(plan.chunks)].Object.ID
			if p.Chunks[len(plan.chunks)].Object != ref {
				return nil, fmt.Errorf("%w: reconstructed chunk differs from reservation", ErrJobLogIntegrity)
			}
		} else {
			ref.ID, err = newLogObjectID(p.UploadSeq, 'c')
			if err != nil {
				return nil, err
			}
			p.Chunks = append(p.Chunks, JobLogV2ChunkUpdate{Index: index, Object: ref})
		}
		plan.chunks = append(plan.chunks, frozenLogObject{ref: ref, data: body})
		slot := int(index % JobLogIndexPageEntries)
		if slot == len(page.Chunks) {
			page.Chunks = append(page.Chunks, ref)
		} else if slot < len(page.Chunks) {
			page.Chunks[slot] = ref
		} else {
			return nil, fmt.Errorf("%w: chunk allocation leaves a hole", ErrJobLogIntegrity)
		}
		pos += n
	}
	firstPage := int((p.Start / JobLogChunkSize) / JobLogIndexPageEntries)
	lastPage := int(((p.End - 1) / JobLogChunkSize) / JobLogIndexPageEntries)
	for index := firstPage; index <= lastPage; index++ {
		page := oldPages[index]
		body, err := encodeLogPage(page)
		if err != nil {
			return nil, err
		}
		ref := JobLogObjectRef{Size: int64(len(body)), SHA256: logDigest(body)}
		if resuming {
			ref.ID = p.Pages[len(plan.pages)].Object.ID
			if p.Pages[len(plan.pages)].Object != ref {
				return nil, fmt.Errorf("%w: reconstructed page differs from reservation", ErrJobLogIntegrity)
			}
		} else {
			ref.ID, err = newLogObjectID(p.UploadSeq, 'p')
			if err != nil {
				return nil, err
			}
			p.Pages = append(p.Pages, JobLogV2PageUpdate{Index: index, Object: ref})
		}
		plan.pages = append(plan.pages, frozenLogObject{ref: ref, data: body})
	}
	return plan, nil
}

func encodeLogPage(page *jobLogV2Page) ([]byte, error) {
	raw, err := json.Marshal(page)
	if err != nil {
		return nil, err
	}
	if len(raw) > JobLogV2PageMaxBytes {
		return nil, fmt.Errorf("%w: oversized index page", ErrJobLogIntegrity)
	}
	return raw, nil
}

// putLogObject checks replays against the actual body. It never interprets
// HEAD-then-PUT as a lock: reservation ownership and unique IDs provide that
// isolation, and concurrent replays are constrained to identical bytes.
func putLogObject(ctx context.Context, store blob.Store, key string, object frozenLogObject) error {
	verify := func() (bool, error) {
		body, err := store.Open(ctx, key)
		if errors.Is(err, blob.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		defer body.Close()
		data, err := io.ReadAll(io.LimitReader(body, object.ref.Size+1))
		if err != nil {
			return false, err
		}
		if int64(len(data)) != object.ref.Size || logDigest(data) != object.ref.SHA256 {
			return false, fmt.Errorf("%w: existing upload differs from frozen bytes", ErrJobLogIntegrity)
		}
		return true, ctx.Err()
	}
	exists, err := verify()
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	digest := sha256.Sum256(object.data)
	err = store.Put(ctx, blob.Write{Key: key, Size: object.ref.Size, SHA256: digest}, bytes.NewReader(object.data))
	if err == nil {
		return ctx.Err()
	}
	confirmed, readErr := verify()
	if readErr == nil && confirmed {
		return nil
	}
	if errors.Is(readErr, ErrJobLogIntegrity) {
		return readErr
	}
	return err
}

func (s *JobLogRepository) committedReplay(ctx context.Context, id string, r *JobLogV2Root, offset int64, data []byte) (bool, error) {
	if r.Deleted {
		return false, ErrJobLogDeleted
	}
	if offset+int64(len(data)) > r.Offset {
		return false, nil
	}
	if len(data) == 0 {
		return offset == r.Offset, nil
	}
	previous, err := s.readRootRange(ctx, id, r, offset, len(data))
	if err != nil {
		return false, err
	}
	if !bytes.Equal(previous, data) {
		return false, ErrConflict
	}
	return true, nil
}

func (s *JobLogRepository) Append(ctx context.Context, id string, offset int64, data []byte, final bool, maxBytes int64) (JobLogManifest, error) {
	if len(id) == 0 || len(id) > JobLogIDMaxBytes || offset < 0 || len(data) > JobLogChunkSize {
		return JobLogManifest{}, fmt.Errorf("invalid job log append")
	}
	if maxBytes <= 0 || maxBytes > JobLogAbsoluteMax || offset > maxBytes-int64(len(data)) {
		return JobLogManifest{}, ErrJobLogLimit
	}
	if s.atomic == nil {
		return JobLogManifest{}, ErrUnsupported
	}
	data = append([]byte(nil), data...)
	for retry := 0; retry < 8; retry++ {
		if err := ctx.Err(); err != nil {
			return JobLogManifest{}, err
		}
		r, format, err := s.root(ctx, id)
		absent := errors.Is(err, ErrNotFound)
		if err != nil && !absent {
			return JobLogManifest{}, err
		}
		if !absent && format == 1 || absent && s.opts.WriteFormat == 1 {
			return JobLogAppend(ctx, s.meta, id, offset, data, final, maxBytes)
		}
		if absent {
			r, err = s.newRoot()
			if err != nil {
				return JobLogManifest{}, err
			}
		}
		if r.Deleted {
			return r.view(), ErrJobLogDeleted
		}
		if offset < r.Offset {
			ok, err := s.committedReplay(ctx, id, r, offset, data)
			if err != nil {
				return r.view(), err
			}
			if !ok {
				return r.view(), ErrConflict
			}
			if final && offset+int64(len(data)) == r.Offset && !r.Final && r.Pending == nil {
				return s.seal(ctx, id, r, false, true)
			}
			return r.view(), nil
		}
		if offset != r.Offset || r.Final && len(data) > 0 {
			return r.view(), ErrConflict
		}
		if len(data) == 0 {
			return s.seal(ctx, id, r, absent, final)
		}
		plan, err := s.prepareAppend(ctx, id, r, data, final)
		if err != nil {
			return r.view(), err
		}
		if r.Pending == nil {
			err = s.atomic.Update(ctx, jobLogManifestKey(id), func(raw []byte) ([]byte, error) {
				current := r
				if len(raw) > 0 {
					var e error
					current, e = DecodeJobLogV2Root(raw)
					if e != nil {
						return nil, e
					}
				} else if !absent {
					return nil, ErrConflict
				}
				if current.Deleted {
					return nil, ErrJobLogDeleted
				}
				if current.Incarnation != r.Incarnation || current.CommitSeq != r.CommitSeq || current.Offset != r.Offset || current.IssuedSeq != r.IssuedSeq || current.Final || current.Pending != nil {
					return nil, ErrConflict
				}
				next := *current
				next.Pending = plan.pending
				next.IssuedSeq = plan.pending.UploadSeq
				return EncodeJobLogV2Root(&next)
			})
			if err != nil {
				current, readErr := s.readV2Root(ctx, id)
				if readErr != nil {
					if errors.Is(readErr, ErrJobLogFormat) {
						return r.view(), readErr
					}
					return r.view(), err
				}
				if current.Deleted {
					return current.view(), ErrJobLogDeleted
				}
				if current.Pending == nil || current.Pending.AttemptID != plan.pending.AttemptID {
					if errors.Is(err, ErrConflict) {
						continue
					}
					if ok, e := s.committedReplay(ctx, id, current, offset, data); e != nil {
						return current.view(), e
					} else if ok {
						return current.view(), nil
					}
					return current.view(), err
				}
			}
		}
		for _, object := range append(plan.chunks, plan.pages...) {
			if err = putLogObject(ctx, s.stores[r.StoreID], logObjectKey(r.Namespace, id, r.Incarnation, object.ref.ID), object); err != nil {
				return r.view(), err
			}
		}
		var result JobLogManifest
		err = s.atomic.Update(ctx, jobLogManifestKey(id), func(raw []byte) ([]byte, error) {
			current, e := DecodeJobLogV2Root(raw)
			if e != nil {
				return nil, e
			}
			if current.Deleted {
				return nil, ErrJobLogDeleted
			}
			p := current.Pending
			if current.Incarnation != r.Incarnation || p == nil || p.AttemptID != plan.pending.AttemptID || p.UploadSeq != plan.pending.UploadSeq || current.CommitSeq != plan.pending.BaseCommitSeq {
				return nil, ErrConflict
			}
			next := *current
			next.Pages = append([]JobLogObjectRef(nil), current.Pages...)
			for _, page := range p.Pages {
				if page.Index == len(next.Pages) {
					next.Pages = append(next.Pages, page.Object)
				} else if page.Index < len(next.Pages) {
					next.Pages[page.Index] = page.Object
				} else {
					return nil, fmt.Errorf("%w: page publication leaves a hole", ErrJobLogIntegrity)
				}
			}
			next.Offset = p.End
			next.Final = p.Final
			next.Pending = nil
			next.CommitSeq, e = nextLogSequence(current.CommitSeq)
			if e != nil {
				return nil, e
			}
			encoded, e := EncodeJobLogV2Root(&next)
			if e != nil {
				return nil, e
			}
			result = next.view()
			return encoded, nil
		})
		if err == nil {
			return result, nil
		}
		current, readErr := s.readV2Root(ctx, id)
		if errors.Is(readErr, ErrJobLogFormat) {
			return result, readErr
		}
		if readErr == nil {
			ok, e := s.committedReplay(ctx, id, current, offset, data)
			if e != nil {
				return current.view(), e
			}
			if ok {
				return current.view(), nil
			}
		}
		return result, err
	}
	return JobLogManifest{}, ErrConflict
}

func (s *JobLogRepository) seal(ctx context.Context, id string, r *JobLogV2Root, absent, final bool) (JobLogManifest, error) {
	if r.Pending != nil {
		return r.view(), ErrJobLogPending
	}
	var result JobLogManifest
	err := s.atomic.Update(ctx, jobLogManifestKey(id), func(raw []byte) ([]byte, error) {
		current := r
		if len(raw) > 0 {
			var e error
			current, e = DecodeJobLogV2Root(raw)
			if e != nil {
				return nil, e
			}
		} else if !absent {
			return nil, ErrConflict
		}
		if current.Deleted {
			return nil, ErrJobLogDeleted
		}
		if current.Incarnation != r.Incarnation || current.Offset != r.Offset || current.Pending != nil {
			return nil, ErrConflict
		}
		next := *current
		next.Final = current.Final || final
		if absent || next.Final != current.Final {
			var e error
			next.CommitSeq, e = nextLogSequence(current.CommitSeq)
			if e != nil {
				return nil, e
			}
		}
		encoded, e := EncodeJobLogV2Root(&next)
		if e != nil {
			return nil, e
		}
		result = next.view()
		return encoded, nil
	})
	if err == nil {
		return result, nil
	}
	current, readErr := s.readV2Root(ctx, id)
	if errors.Is(readErr, ErrJobLogFormat) {
		return result, readErr
	}
	if readErr == nil {
		if current.Deleted {
			return current.view(), ErrJobLogDeleted
		}
		if current.Incarnation == r.Incarnation && current.Offset == r.Offset && current.Pending == nil && (!final || current.Final) {
			return current.view(), nil
		}
	}
	return result, err
}

func (s *JobLogRepository) Delete(ctx context.Context, id string) error {
	r, format, err := s.root(ctx, id)
	absent := errors.Is(err, ErrNotFound)
	if err != nil && !absent {
		return err
	}
	if !absent && format != 2 || absent && s.opts.WriteFormat != 2 {
		return JobLogDelete(ctx, s.meta, id)
	}
	if s.atomic == nil {
		return ErrUnsupported
	}
	if absent {
		r, err = s.newRoot()
		if err != nil {
			return err
		}
	}
	err = s.atomic.Update(ctx, jobLogManifestKey(id), func(raw []byte) ([]byte, error) {
		current := r
		if len(raw) > 0 {
			var e error
			current, e = DecodeJobLogV2Root(raw)
			if e != nil {
				return nil, e
			}
		} else if !absent {
			return nil, ErrConflict
		}
		if current.Deleted {
			return raw, nil
		}
		next := *current
		next.Deleted = true
		next.Final = true
		next.Pending = nil
		next.Pages = nil
		var e error
		next.CommitSeq, e = nextLogSequence(current.CommitSeq)
		if e != nil {
			return nil, e
		}
		return EncodeJobLogV2Root(&next)
	})
	if err == nil {
		return nil
	}
	current, readErr := s.readV2Root(ctx, id)
	if errors.Is(readErr, ErrJobLogFormat) {
		return readErr
	}
	if readErr == nil && current.Deleted {
		return nil
	}
	return err
}

// RevokeExpiredPending changes authority only through the root CAS. A deadline
// by itself never authorizes object deletion. The local source may immediately
// retry from the unchanged committed offset using a new upload sequence.
func (s *JobLogRepository) RevokeExpiredPending(ctx context.Context, id string) (bool, error) {
	r, format, err := s.root(ctx, id)
	if err != nil {
		return false, err
	}
	if format != 2 || r.Pending == nil || r.Pending.Deadline > s.now().UnixNano() {
		return false, nil
	}
	if s.atomic == nil {
		return false, ErrUnsupported
	}
	attempt := r.Pending.AttemptID
	revoked := false
	err = s.atomic.Update(ctx, jobLogManifestKey(id), func(raw []byte) ([]byte, error) {
		current, e := DecodeJobLogV2Root(raw)
		if e != nil {
			return nil, e
		}
		revoked = false
		if current.Pending == nil || current.Pending.AttemptID != attempt {
			return raw, nil
		}
		next := *current
		next.Pending = nil
		next.CommitSeq, e = nextLogSequence(current.CommitSeq)
		if e != nil {
			return nil, e
		}
		encoded, e := EncodeJobLogV2Root(&next)
		if e != nil {
			return nil, e
		}
		revoked = true
		return encoded, nil
	})
	if err == nil {
		return revoked, nil
	}
	current, readErr := s.readV2Root(ctx, id)
	if errors.Is(readErr, ErrJobLogFormat) {
		return false, readErr
	}
	if readErr == nil && (current.Pending == nil || current.Pending.AttemptID != attempt) {
		return true, nil
	}
	return false, err
}
