package storage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/soulteary/owlcron/blob"
)

const SplitBackupFormat = "owlcron-split-backup-v2"

// Metadata is bounded independently of log size. Large legacy logs are emitted
// as 64 KiB fragments and converted to v2 during restore, never loaded at once.
const splitMetadataLimit = 16 << 20
const splitArchiveLineLimit = 24 << 20

type SplitBackupOptions struct {
	// Offline explicitly acknowledges that all writers, uploaders and GC have
	// stopped. Observable leases/active jobs are checked too; absence of a lease
	// alone cannot prove that a disconnected worker has stopped writing.
	Offline bool
	TempDir string
}

type SplitBackupResult struct {
	Records      int   `json:"records"`
	Objects      int   `json:"objects"`
	LegacyChunks int   `json:"legacy_chunks"`
	Bytes        int64 `json:"bytes"`
}

type splitArchiveRow struct {
	Kind      string             `json:"kind"`
	Format    string             `json:"format,omitempty"`
	Namespace string             `json:"namespace,omitempty"`
	Created   int64              `json:"created,omitempty"`
	Key       string             `json:"key,omitempty"`
	StoreID   string             `json:"store_id,omitempty"`
	Data      []byte             `json:"data,omitempty"`
	Expires   int64              `json:"expires,omitempty"`
	Size      int64              `json:"size,omitempty"`
	SHA256    string             `json:"sha256,omitempty"`
	Offset    int64              `json:"offset,omitempty"`
	Final     bool               `json:"final,omitempty"`
	Counts    *SplitBackupResult `json:"counts,omitempty"`
}

type splitArchiveWriter struct {
	w      io.Writer
	h      hash.Hash
	result SplitBackupResult
}

func (w *splitArchiveWriter) row(row splitArchiveRow, includeHash bool) error {
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > splitArchiveLineLimit {
		return fmt.Errorf("split backup row exceeds size limit")
	}
	n, err := w.w.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return err
	}
	if includeHash {
		_, _ = w.h.Write(data)
	}
	return nil
}

func checkOfflineSource(ctx context.Context, store Storage, offline bool) error {
	if !offline {
		return fmt.Errorf("offline operation requires explicit acknowledgement that all writers and GC are stopped")
	}
	if raw, err := store.Get(ctx, "global/master_lease"); err == nil {
		var lease struct {
			Owner   string `json:"owner"`
			Expires int64  `json:"expires"`
		}
		if err := json.Unmarshal(raw, &lease); err != nil {
			return fmt.Errorf("invalid leadership lease: %w", err)
		}
		if lease.Owner != "" && lease.Expires > time.Now().UnixNano() {
			return fmt.Errorf("source still has an active leadership lease")
		}
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	if raw, err := store.Get(ctx, "active_jobs"); err == nil {
		var jobs []json.RawMessage
		if err := json.Unmarshal(raw, &jobs); err != nil {
			return fmt.Errorf("invalid active job index: %w", err)
		}
		if len(jobs) != 0 {
			return fmt.Errorf("source still has active jobs; drain or terminate them before copying storage")
		}
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	return nil
}

func skipSplitRecord(key string) bool {
	return strings.HasPrefix(key, "_expire/") || key == "global/master_lease" ||
		key == "global/readiness_probe" || strings.HasPrefix(key, "split_migration/") ||
		key == "global/split_migration"
}

func manifestVersion(raw []byte) (int, error) {
	var version struct {
		Format int `json:"format"`
	}
	if err := json.Unmarshal(raw, &version); err != nil {
		return 0, err
	}
	if version.Format == 0 {
		return 1, nil
	}
	if version.Format != 1 && version.Format != 2 {
		return 0, fmt.Errorf("unsupported job log format %d", version.Format)
	}
	return version.Format, nil
}

// ExportSplitTo writes a self-contained stream. A missing/truncated final row
// invalidates the entire archive. It never changes the source namespace.
func ExportSplitTo(ctx context.Context, meta Storage, registry *BlobRegistry, out io.Writer, options SplitBackupOptions) (SplitBackupResult, error) {
	var zero SplitBackupResult
	if err := checkOfflineSource(ctx, meta, options.Offline); err != nil {
		return zero, err
	}
	layout, err := ValidateStorageLayout(ctx, meta, registry)
	if err != nil {
		return zero, err
	}
	repo, err := NewJobLogRepository(meta, registry.Stores(), JobLogRepositoryOptions{Namespace: layout.NamespaceID, WriteStore: registry.WriteStoreID(), WriteFormat: 2})
	if err != nil {
		return zero, err
	}
	w := &splitArchiveWriter{w: out, h: sha256.New()}
	if err := w.row(splitArchiveRow{Kind: "header", Format: SplitBackupFormat, Namespace: layout.NamespaceID, Created: time.Now().Unix()}, true); err != nil {
		return zero, err
	}
	err = walkBackupKeys(ctx, meta, func(key string) error {
		if skipSplitRecord(key) {
			return nil
		}
		if strings.HasPrefix(key, "job_logs:") {
			id := strings.TrimPrefix(key, "job_logs:")
			if _, err := meta.Get(ctx, JobLogManifestKey(id)); err == nil {
				return nil // an authoritative manifest supersedes the legacy body
			} else if !errors.Is(err, ErrNotFound) {
				return err
			}
			return exportLegacyLog(ctx, meta, id, w)
		}
		entry, err := readBackupEntry(ctx, meta, key)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(entry.Data) > splitMetadataLimit {
			return fmt.Errorf("metadata record %s exceeds %d bytes", key, splitMetadataLimit)
		}
		if id, isManifest, err := JobLogIDFromManifestKey(key); err != nil {
			return err
		} else if isManifest {
			version, err := manifestVersion(entry.Data)
			if err != nil {
				return err
			}
			if version == 2 {
				root, err := DecodeJobLogV2Root(entry.Data)
				if err != nil {
					return err
				}
				if entry.Expires != 0 || root.Namespace != layout.NamespaceID || root.Pending != nil || (!root.Final && !root.Deleted) {
					return fmt.Errorf("log %s is not a sealed, permanent root in the active namespace", id)
				}
				if !root.Deleted {
					if err := repo.VisitCommittedObjects(ctx, id, func(object JobLogObject, body []byte) error {
						if err := w.row(splitArchiveRow{Kind: "object", StoreID: object.StoreID, Key: object.Key, Size: object.Size, SHA256: object.SHA256, Data: body}, true); err != nil {
							return err
						}
						w.result.Objects++
						w.result.Bytes += int64(len(body))
						return nil
					}); err != nil {
						return fmt.Errorf("export log %s: %w", id, err)
					}
				}
			} else {
				var m JobLogManifest
				if err := decodeJobLogManifest(entry.Data, &m); err != nil {
					return err
				}
				if (m.PendingEnd != 0 || !m.Final) && !m.Deleted {
					return fmt.Errorf("log %s is not sealed; finish pending uploads before backup", id)
				}
				if err := verifyLegacyChunkedLog(ctx, meta, id, m); err != nil {
					return err
				}
			}
		}
		if err := w.row(splitArchiveRow{Kind: "record", Key: key, Data: entry.Data, Expires: entry.Expires}, true); err != nil {
			return err
		}
		w.result.Records++
		return nil
	})
	if err != nil {
		return w.result, err
	}
	if err := w.row(splitArchiveRow{Kind: "complete", SHA256: hex.EncodeToString(w.h.Sum(nil)), Counts: &w.result}, false); err != nil {
		return w.result, err
	}
	return w.result, nil
}

func verifyLegacyChunkedLog(ctx context.Context, meta Storage, id string, expected JobLogManifest) error {
	if expected.Deleted {
		return nil
	}
	for at := int64(0); at < expected.Offset; {
		limit := min(JobLogChunkSize, int(expected.Offset-at))
		data, state, err := JobLogRead(ctx, meta, id, at, limit)
		if err != nil {
			return fmt.Errorf("verify v1 log %s: %w", id, err)
		}
		if len(data) != limit || state.Offset != expected.Offset || !state.Final || state.PendingEnd != 0 || state.Deleted {
			return fmt.Errorf("v1 log %s changed or has incomplete chunks", id)
		}
		at += int64(len(data))
	}
	return nil
}

func exportLegacyLog(ctx context.Context, meta Storage, id string, w *splitArchiveWriter) error {
	deleted, err := isArchivedLogDeleted(ctx, meta, id)
	if err != nil || deleted {
		return err
	}
	if err := requirePermanentLegacyLog(ctx, meta, id); errors.Is(err, ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	if err := requireCompletedLegacyJob(ctx, meta, id); err != nil {
		return err
	}
	rangeStore, ok := meta.(RangeStorage)
	if !ok {
		return fmt.Errorf("bounded legacy log export: %w", ErrUnsupported)
	}
	var at int64
	for {
		data, total, err := rangeStore.GetRange(ctx, "job_logs:"+id, at, JobLogChunkSize)
		if err != nil {
			return err
		}
		if total < 0 || total > JobLogAbsoluteMax || at > total || len(data) > JobLogChunkSize || at+int64(len(data)) > total || (len(data) == 0 && at < total) {
			return fmt.Errorf("invalid bounded legacy log %s", id)
		}
		sum := sha256.Sum256(data)
		final := at+int64(len(data)) == total
		if err := w.row(splitArchiveRow{Kind: "legacy_log", Key: id, Offset: at, Size: total, SHA256: hex.EncodeToString(sum[:]), Data: data, Final: final}, true); err != nil {
			return err
		}
		w.result.LegacyChunks++
		w.result.Bytes += int64(len(data))
		at += int64(len(data))
		if final {
			return nil
		}
	}
}

func requireCompletedLegacyJob(ctx context.Context, meta Storage, id string) error {
	raw, err := meta.Get(ctx, "jobs:"+id)
	if err != nil {
		return fmt.Errorf("legacy log %s requires a completed canonical job: %w", id, err)
	}
	var job struct {
		Complete bool `json:"complete"`
	}
	if err := json.Unmarshal(raw, &job); err != nil || !job.Complete {
		return fmt.Errorf("legacy log %s is not a completed job", id)
	}
	return nil
}

func isArchivedLogDeleted(ctx context.Context, meta Storage, id string) (bool, error) {
	for _, key := range []string{"deleted_jobs:" + id, "deleted_job_logs:" + id} {
		exists, err := meta.Exists(ctx, key)
		if err != nil || exists {
			return exists, err
		}
	}
	return false, nil
}

func readArchiveLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(line)+len(part) > splitArchiveLineLimit {
			return nil, fmt.Errorf("split archive row exceeds size limit")
		}
		line = append(line, part...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err == io.EOF && len(line) != 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return line, err
	}
}

func decodeArchiveRow(line []byte) (splitArchiveRow, error) {
	var row splitArchiveRow
	d := json.NewDecoder(bytes.NewReader(line))
	d.DisallowUnknownFields()
	if err := d.Decode(&row); err != nil {
		return row, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return row, fmt.Errorf("multiple values in split archive row")
	}
	return row, nil
}

func putVerifiedBlob(ctx context.Context, store blob.Store, key string, data []byte, digest string) error {
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != digest {
		return fmt.Errorf("%w: archive object checksum", blob.ErrIntegrity)
	}
	err := store.Put(ctx, blob.Write{Key: key, Size: int64(len(data)), SHA256: sum}, bytes.NewReader(data))
	if err == nil {
		return nil
	}
	// A timeout may follow a committed write. Verify bytes rather than create a
	// second identity or delete an object whose outcome is not known.
	r, readErr := store.Open(ctx, key)
	if readErr != nil {
		return err
	}
	defer r.Close()
	got, readErr := io.ReadAll(io.LimitReader(r, int64(len(data))+1))
	if readErr == nil && bytes.Equal(got, data) {
		return nil
	}
	return err
}

func ensureEmptySplitTarget(ctx context.Context, meta Storage) error {
	return walkBackupKeys(ctx, meta, func(key string) error {
		if key == "global/readiness_probe" || strings.HasPrefix(key, "_expire/") {
			return nil
		}
		_, err := meta.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("restore requires an empty metadata namespace; found %s", key)
	})
}

// ImportSplitFrom never reuses the source physical namespace. Metadata is
// staged on disk until the terminal archive digest has been checked; the target
// remains restoring after any failure and is not usable by the daemon.
func ImportSplitFrom(ctx context.Context, meta Storage, registry *BlobRegistry, in io.Reader, options SplitBackupOptions) (SplitBackupResult, error) {
	var result SplitBackupResult
	if !options.Offline {
		return result, fmt.Errorf("split restore requires --offline")
	}
	reader := bufio.NewReaderSize(in, 64*1024)
	line, err := readArchiveLine(reader)
	if err != nil {
		return result, err
	}
	header, err := decodeArchiveRow(line)
	if err != nil || header.Kind != "header" || header.Format != SplitBackupFormat || len(header.Namespace) != 32 {
		return result, fmt.Errorf("invalid split backup header")
	}
	if _, err := hex.DecodeString(header.Namespace); err != nil {
		return result, fmt.Errorf("invalid source namespace")
	}
	if err := ensureEmptySplitTarget(ctx, meta); err != nil {
		return result, err
	}
	layout, err := InitializeStorageLayout(ctx, meta, registry, "restoring")
	if err != nil {
		return result, err
	}
	if layout.NamespaceID == header.Namespace {
		return result, fmt.Errorf("restore namespace must differ from source")
	}
	repo, err := NewJobLogRepository(meta, registry.Stores(), JobLogRepositoryOptions{Namespace: layout.NamespaceID, WriteStore: registry.WriteStoreID(), WriteFormat: 2})
	if err != nil {
		return result, err
	}
	dir, err := os.MkdirTemp(options.TempDir, "owlcron-split-restore-")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(dir)
	recordDir := filepath.Join(dir, "records")
	objectDir := filepath.Join(dir, "objects")
	if err := os.Mkdir(recordDir, 0700); err != nil {
		return result, err
	}
	if err := os.Mkdir(objectDir, 0700); err != nil {
		return result, err
	}
	archiveHash := sha256.New()
	_, _ = archiveHash.Write(line)
	complete := false
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		line, err := readArchiveLine(reader)
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return result, err
		}
		row, err := decodeArchiveRow(line)
		if err != nil {
			return result, err
		}
		if row.Kind == "complete" {
			if row.Counts == nil || *row.Counts != result || row.SHA256 != hex.EncodeToString(archiveHash.Sum(nil)) {
				return result, fmt.Errorf("split archive terminal checksum or counts mismatch")
			}
			if trailing, err := readArchiveLine(reader); err != io.EOF || len(trailing) != 0 {
				return result, fmt.Errorf("data after split archive terminal manifest")
			}
			complete = true
			break
		}
		_, _ = archiveHash.Write(line)
		switch row.Kind {
		case "record":
			if err := validateKey(row.Key); err != nil {
				return result, err
			}
			if len(row.Data) > splitMetadataLimit || skipSplitRecord(row.Key) || strings.HasPrefix(row.Key, "job_logs:") || row.Expires < 0 {
				return result, fmt.Errorf("invalid split metadata record %s", row.Key)
			}
			name := sha256.Sum256([]byte(row.Key))
			file, err := os.OpenFile(filepath.Join(recordDir, hex.EncodeToString(name[:])), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				return result, fmt.Errorf("duplicate or unwritable metadata record %s: %w", row.Key, err)
			}
			_, writeErr := file.Write(line)
			closeErr := file.Close()
			if writeErr != nil {
				return result, writeErr
			}
			if closeErr != nil {
				return result, closeErr
			}
			result.Records++
		case "object":
			if row.StoreID == "" || row.Size != int64(len(row.Data)) || row.Size < 0 || row.Size > blob.MaxObjectSize || !strings.HasPrefix(row.Key, header.Namespace+"/") {
				return result, fmt.Errorf("invalid archive object")
			}
			key, err := RebindJobLogObjectKey(row.Key, layout.NamespaceID)
			if err != nil {
				return result, err
			}
			objectHash := sha256.Sum256([]byte(key))
			marker, err := os.OpenFile(filepath.Join(objectDir, hex.EncodeToString(objectHash[:])), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				return result, fmt.Errorf("duplicate archive object identity: %w", err)
			}
			if err := marker.Close(); err != nil {
				return result, err
			}
			store, err := registry.Store(registry.WriteStoreID())
			if err != nil {
				return result, err
			}
			if err := putVerifiedBlob(ctx, store, key, row.Data, row.SHA256); err != nil {
				return result, err
			}
			result.Objects++
			result.Bytes += int64(len(row.Data))
		case "legacy_log":
			if row.Offset < 0 || row.Size < 0 || row.Size > JobLogAbsoluteMax || len(row.Data) > JobLogChunkSize || row.Offset+int64(len(row.Data)) > row.Size || row.Final != (row.Offset+int64(len(row.Data)) == row.Size) {
				return result, fmt.Errorf("invalid legacy log fragment")
			}
			sum := sha256.Sum256(row.Data)
			if hex.EncodeToString(sum[:]) != row.SHA256 {
				return result, fmt.Errorf("legacy log fragment checksum mismatch")
			}
			state, err := repo.Info(ctx, row.Key)
			if errors.Is(err, ErrNotFound) {
				if row.Offset != 0 {
					return result, fmt.Errorf("legacy log does not start at offset zero")
				}
			} else if err != nil || state.Final || state.Offset != row.Offset {
				return result, fmt.Errorf("out-of-order legacy log fragment")
			}
			if _, err := repo.Append(ctx, row.Key, row.Offset, row.Data, row.Final, JobLogAbsoluteMax); err != nil {
				return result, err
			}
			result.LegacyChunks++
			result.Bytes += int64(len(row.Data))
		default:
			return result, fmt.Errorf("unknown split archive row %q", row.Kind)
		}
	}
	if !complete {
		return result, io.ErrUnexpectedEOF
	}
	foundLayout := false
	err = walkStagedRecords(ctx, recordDir, func(row splitArchiveRow) error {
		if row.Key == StorageLayoutKey {
			sourceLayout, err := decodeStorageLayout(row.Data)
			if err != nil || sourceLayout.NamespaceID != header.Namespace || sourceLayout.Phase != "ready" || row.Expires != 0 {
				return fmt.Errorf("invalid archived layout")
			}
			foundLayout = true
			return nil
		}
		if id, isManifest, err := JobLogIDFromManifestKey(row.Key); err != nil {
			return err
		} else if isManifest {
			if _, err := meta.Get(ctx, row.Key); err == nil {
				return fmt.Errorf("duplicate legacy and manifest log %s", id)
			} else if !errors.Is(err, ErrNotFound) {
				return err
			}
			version, err := manifestVersion(row.Data)
			if err != nil {
				return err
			}
			if version == 2 {
				if row.Expires != 0 {
					return fmt.Errorf("v2 log root cannot expire")
				}
				root, err := DecodeJobLogV2Root(row.Data)
				if err != nil || root.Namespace != header.Namespace {
					return fmt.Errorf("invalid source log namespace")
				}
				row.Data, err = RebindJobLogV2Root(row.Data, layout.NamespaceID, registry.WriteStoreID())
				if err != nil {
					return err
				}
			}
		}
		return restoreBackupEntry(ctx, meta, row.Key, BackupEntry{Data: row.Data, Expires: row.Expires})
	})
	if err != nil {
		return result, err
	}
	if !foundLayout {
		return result, fmt.Errorf("archive is missing its source storage layout")
	}
	if _, err := verifySplitStorage(ctx, meta, registry, layout, true); err != nil {
		return result, err
	}
	if err := SetStorageLayoutPhase(ctx, meta, layout.NamespaceID, "restoring", "ready"); err != nil {
		current, readErr := LoadStorageLayout(ctx, meta)
		if readErr != nil || current.NamespaceID != layout.NamespaceID || current.Phase != "ready" {
			return result, err
		}
	}
	return result, nil
}

func walkStagedRecords(ctx context.Context, dir string, visit func(splitArchiveRow) error) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	for {
		names, err := d.Readdirnames(64)
		if err != nil && err != io.EOF {
			return err
		}
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				return err
			}
			row, err := decodeArchiveRow(data)
			if err != nil {
				return err
			}
			if err := visit(row); err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
	}
}

func restoreBackupEntry(ctx context.Context, meta Storage, key string, entry BackupEntry) error {
	if entry.Expires > 0 && entry.Expires <= time.Now().Unix() {
		return nil
	}
	data, err := backupRecordData(key, entry.Data)
	if err != nil {
		return err
	}
	if err := meta.Put(ctx, key, data); err != nil {
		return err
	}
	if entry.Expires > 0 {
		return meta.Expire(ctx, key, entry.Expires)
	}
	return nil
}

type SplitVerifyResult struct {
	Logs    int   `json:"logs"`
	Objects int   `json:"objects"`
	Bytes   int64 `json:"bytes"`
}

// PreflightStorageCopy validates observable quiescence and all readable log
// content. Operators still have to stop disconnected workers and GC explicitly.
func PreflightStorageCopy(ctx context.Context, meta Storage, registry *BlobRegistry, offline bool) error {
	if err := checkOfflineSource(ctx, meta, offline); err != nil {
		return err
	}
	if registry == nil {
		_, err := snapshotLegacySource(ctx, meta, nil)
		return err
	}
	layout, err := ValidateStorageLayout(ctx, meta, registry)
	if err != nil {
		return err
	}
	_, err = verifySplitStorage(ctx, meta, registry, layout, true)
	return err
}

func VerifySplitStorage(ctx context.Context, meta Storage, registry *BlobRegistry) (SplitVerifyResult, error) {
	layout, err := ValidateStorageLayout(ctx, meta, registry)
	if err != nil {
		return SplitVerifyResult{}, err
	}
	return verifySplitStorage(ctx, meta, registry, layout, false)
}

func verifySplitStorage(ctx context.Context, meta Storage, registry *BlobRegistry, layout StorageLayout, sealed bool) (SplitVerifyResult, error) {
	var result SplitVerifyResult
	repo, err := NewJobLogRepository(meta, registry.Stores(), JobLogRepositoryOptions{Namespace: layout.NamespaceID, WriteStore: registry.WriteStoreID(), WriteFormat: 2})
	if err != nil {
		return result, err
	}
	err = walkBackupKeys(ctx, meta, func(key string) error {
		id, isManifest, err := JobLogIDFromManifestKey(key)
		if err != nil || !isManifest {
			return err
		}
		entry, err := readBackupEntry(ctx, meta, key)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		raw := entry.Data
		version, err := manifestVersion(raw)
		if err != nil {
			return err
		}
		m, err := repo.Info(ctx, id)
		if err != nil {
			return err
		}
		if (m.PendingEnd != 0 || sealed && !m.Final) && !m.Deleted {
			return fmt.Errorf("log %s is not quiescent", id)
		}
		if version == 2 {
			root, err := DecodeJobLogV2Root(raw)
			if err != nil || root.Namespace != layout.NamespaceID || entry.Expires != 0 {
				return fmt.Errorf("invalid log namespace or expiration for %s", id)
			}
		}
		if m.Deleted {
			result.Logs++
			return nil
		}
		if version == 2 {
			if err := repo.VisitCommittedObjects(ctx, id, func(object JobLogObject, data []byte) error {
				result.Objects++
				result.Bytes += int64(len(data))
				return nil
			}); err != nil {
				return err
			}
		} else {
			for at := int64(0); at < m.Offset; {
				data, _, err := repo.Read(ctx, id, at, JobLogChunkSize)
				if err != nil || len(data) == 0 {
					return fmt.Errorf("invalid v1 log %s at %d: %w", id, at, err)
				}
				result.Bytes += int64(len(data))
				at += int64(len(data))
			}
		}
		result.Logs++
		return nil
	})
	return result, err
}
