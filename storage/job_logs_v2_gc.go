package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/soulteary/owlcron/blob"
)

type JobLogObject struct {
	StoreID string
	Key     string
	Size    int64
	SHA256  string
}

func logPublicObject(id string, r *JobLogV2Root, ref JobLogObjectRef) JobLogObject {
	return JobLogObject{StoreID: r.StoreID, Key: logObjectKey(r.Namespace, id, r.Incarnation, ref.ID), Size: ref.Size, SHA256: ref.SHA256}
}

// VisitCommittedObjects streams verified pages and chunks from one root.
// Administrative callers must stop writers and GC for a complete offline
// snapshot; a successful per-root visit is not a cross-store snapshot.
func (s *JobLogRepository) VisitCommittedObjects(ctx context.Context, id string, visit func(JobLogObject, []byte) error) error {
	r, format, err := s.root(ctx, id)
	if err != nil {
		return err
	}
	if format != 2 {
		return ErrJobLogFormat
	}
	if r.Pending != nil {
		return ErrJobLogPending
	}
	if visit == nil {
		return fmt.Errorf("object visitor is required")
	}
	seen := map[string]bool{}
	for index, ref := range r.Pages {
		page, raw, err := s.readPage(ctx, id, r, index)
		if err != nil {
			return err
		}
		if err = visit(logPublicObject(id, r, ref), raw); err != nil {
			return err
		}
		for _, chunk := range page.Chunks {
			if seen[chunk.ID] {
				return fmt.Errorf("%w: repeated chunk across index pages", ErrJobLogIntegrity)
			}
			seen[chunk.ID] = true
			o := logPublicObject(id, r, chunk)
			data, err := readLogObject(ctx, s.stores[r.StoreID], o.Key, chunk)
			if err != nil {
				return err
			}
			if err = visit(o, data); err != nil {
				return err
			}
		}
	}
	current, format, err := s.root(ctx, id)
	if err != nil {
		return err
	}
	if format != 2 || current.Incarnation != r.Incarnation || current.CommitSeq != r.CommitSeq || current.IssuedSeq != r.IssuedSeq || current.Pending != nil {
		return ErrConflict
	}
	return nil
}

type JobLogGCResult struct {
	NextCursor string
	Scanned    int
	Deleted    int
	Bytes      int64
	Errors     int
}

type logGCMark struct {
	root *JobLogV2Root
	ids  map[string]bool
}

func (s *JobLogRepository) markLog(ctx context.Context, id string) (*logGCMark, error) {
	for retry := 0; retry < 3; retry++ {
		if _, err := s.RevokeExpiredPending(ctx, id); err != nil {
			return nil, err
		}
		r, format, err := s.root(ctx, id)
		if err != nil {
			return nil, err
		}
		if format != 2 {
			return nil, ErrJobLogFormat
		}
		mark := &logGCMark{root: r, ids: make(map[string]bool)}
		var markErr error
		for index, ref := range r.Pages {
			mark.ids[ref.ID] = true
			page, _, err := s.readPage(ctx, id, r, index)
			if err != nil {
				markErr = err
				break
			}
			for _, chunk := range page.Chunks {
				if mark.ids[chunk.ID] {
					markErr = fmt.Errorf("%w: repeated chunk reference", ErrJobLogIntegrity)
					break
				}
				mark.ids[chunk.ID] = true
			}
			if markErr != nil {
				break
			}
		}
		if markErr == nil {
			if p := r.Pending; p != nil {
				for _, chunk := range p.Chunks {
					mark.ids[chunk.Object.ID] = true
				}
				for _, page := range p.Pages {
					mark.ids[page.Object.ID] = true
				}
			}
			return mark, nil
		}
		// Another collector can remove pages of an older root while we mark.
		// Retry only when the authoritative root actually moved. Never sweep a
		// partial mark or treat an unreadable root as an empty reference set.
		current, format, err := s.root(ctx, id)
		if err != nil {
			return nil, err
		}
		if format != 2 || current.Incarnation != r.Incarnation || current.CommitSeq == r.CommitSeq {
			return nil, markErr
		}
	}
	return nil, ErrConflict
}

// Collect scans one page of objects for one log. It is safe for concurrent
// collectors: the root-issued watermark protects future reservations and the
// complete Pending allocation protects uploads whose PUT has not begun yet.
func (s *JobLogRepository) Collect(ctx context.Context, id, cursor string, limit int) (JobLogGCResult, error) {
	r, format, err := s.root(ctx, id)
	if err != nil {
		return JobLogGCResult{}, err
	}
	if format != 2 {
		return JobLogGCResult{}, ErrJobLogFormat
	}
	prefix := logObjectKey(r.Namespace, id, r.Incarnation, "")
	return s.collectPage(ctx, r.StoreID, prefix, cursor, limit)
}

// CollectStore discovers owners from object keys, avoiding an unbounded
// metadata ListKeys call. A poisoned owner is skipped and reported while the
// page cursor still advances; later full inventory passes retry that owner.
func (s *JobLogRepository) CollectStore(ctx context.Context, storeID, cursor string, limit int) (JobLogGCResult, error) {
	if !validLogComponent(s.opts.Namespace) {
		return JobLogGCResult{}, fmt.Errorf("a log namespace is required for inventory")
	}
	return s.collectPage(ctx, storeID, s.opts.Namespace+"/logs/", cursor, limit)
}

func (s *JobLogRepository) collectPage(ctx context.Context, storeID, prefix, cursor string, limit int) (JobLogGCResult, error) {
	result := JobLogGCResult{NextCursor: cursor}
	if limit < 1 || limit > blob.MaxScanLimit {
		return result, fmt.Errorf("invalid garbage collection page limit")
	}
	store := s.stores[storeID]
	inventory, ok := store.(blob.Inventory)
	if !ok {
		return result, ErrUnsupported
	}
	page, err := inventory.Scan(ctx, prefix, cursor, limit)
	if err != nil {
		return result, err
	}
	if len(page.Objects) > limit {
		return result, fmt.Errorf("%w: oversized inventory page", ErrJobLogIntegrity)
	}
	result.NextCursor = page.NextCursor
	result.Scanned = len(page.Objects)
	owners := map[string][]blob.Info{}
	var failures []error
	for _, object := range page.Objects {
		namespace, id, _, _, err := parseLogObjectKey(object.Key)
		if err != nil || !strings.HasPrefix(object.Key, prefix) || namespace != s.opts.Namespace {
			result.Errors++
			failures = append(failures, fmt.Errorf("%w: inventory contains an invalid owner", ErrJobLogIntegrity))
			continue
		}
		owners[id] = append(owners[id], object)
	}
	cutoff := s.now().Add(-s.opts.GCGracePeriod)
	for id, objects := range owners {
		if err := ctx.Err(); err != nil {
			failures = append(failures, err)
			break
		}
		mark, err := s.markLog(ctx, id)
		if err != nil {
			result.Errors++
			failures = append(failures, fmt.Errorf("mark log owner: %w", err))
			continue
		}
		r := mark.root
		if r.StoreID != storeID {
			result.Errors++
			failures = append(failures, fmt.Errorf("%w: inventory store differs from owner", ErrJobLogIntegrity))
			continue
		}
		for _, object := range objects {
			if err := ctx.Err(); err != nil {
				failures = append(failures, err)
				break
			}
			namespace, _, incarnation, objectID, parseErr := parseLogObjectKey(object.Key)
			if parseErr != nil || namespace != r.Namespace || incarnation != r.Incarnation {
				result.Errors++
				failures = append(failures, fmt.Errorf("%w: inventory incarnation differs from permanent owner", ErrJobLogIntegrity))
				continue
			}
			seq, _, _ := parseLogObjectID(objectID)
			if seq > r.IssuedSeq || mark.ids[objectID] {
				continue
			}
			if s.opts.GCGracePeriod > 0 && (object.LastModified.IsZero() || object.LastModified.After(cutoff)) {
				continue
			}
			if err := store.Delete(ctx, object.Key); err != nil && !errors.Is(err, blob.ErrNotFound) {
				result.Errors++
				failures = append(failures, err)
				continue
			}
			result.Deleted++
			if object.Size > 0 && object.Size <= blob.MaxObjectSize {
				result.Bytes += object.Size
			}
		}
	}
	return result, errors.Join(failures...)
}
