package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const splitMigrationKey = "global/split_migration"

type SplitMigrationOptions struct {
	Offline bool
	Resume  bool
}

type SplitMigrationResult struct {
	MigrationID string `json:"migration_id"`
	Namespace   string `json:"namespace"`
	Records     int    `json:"records"`
	Logs        int    `json:"logs"`
	Bytes       int64  `json:"bytes"`
}

type splitMigrationState struct {
	Version           int                  `json:"version"`
	ID                string               `json:"id"`
	SourceFingerprint string               `json:"source_fingerprint"`
	TargetNamespace   string               `json:"target_namespace"`
	Complete          bool                 `json:"complete"`
	SnapshotComplete  bool                 `json:"snapshot_complete"`
	Result            SplitMigrationResult `json:"result"`
}

// Snapshot records bind resumable copies to exact bytes and absolute expiration.
// A disappeared key is accepted only after its snapshotted expiry has passed.
type migrationSourceRecord struct {
	Key     string `json:"key"`
	SHA256  string `json:"sha256"`
	Expires int64  `json:"expires,omitempty"`
}

func sourceSnapshotKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "split_migration/source/" + hex.EncodeToString(sum[:])
}

func sourceRecordDigest(ctx context.Context, source Storage, key string) (*migrationSourceRecord, error) {
	if skipSplitRecord(key) {
		return nil, nil
	}
	if key == StorageLayoutKey {
		return nil, fmt.Errorf("split-to-split moves use complete export/import")
	}
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%d:%s\x00", len(key), key)
	record := &migrationSourceRecord{Key: key}
	if strings.HasPrefix(key, "job_logs:") {
		id := strings.TrimPrefix(key, "job_logs:")
		if _, err := source.Get(ctx, JobLogManifestKey(id)); err == nil {
			return nil, nil
		} else if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		deleted, err := isArchivedLogDeleted(ctx, source, id)
		if err != nil || deleted {
			return nil, err
		}
		if err := requirePermanentLegacyLog(ctx, source, id); errors.Is(err, ErrNotFound) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		if err := requireCompletedLegacyJob(ctx, source, id); err != nil {
			return nil, err
		}
		r, ok := source.(RangeStorage)
		if !ok {
			return nil, ErrUnsupported
		}
		at, total := int64(0), int64(-1)
		for {
			data, size, err := r.GetRange(ctx, key, at, JobLogChunkSize)
			if errors.Is(err, ErrNotFound) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			if total < 0 {
				total = size
			}
			if size != total || total < 0 || total > JobLogAbsoluteMax || at+int64(len(data)) > total || len(data) > JobLogChunkSize || len(data) == 0 && at < total {
				return nil, fmt.Errorf("legacy log changed or has invalid bounds")
			}
			_, _ = h.Write(data)
			at += int64(len(data))
			if at == total {
				break
			}
		}
	} else {
		entry, err := readBackupEntry(ctx, source, key)
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if err := rejectSplitBackupRecord(key, entry.Data); err != nil {
			return nil, err
		}
		if strings.HasPrefix(key, "job_log_chunks/") && entry.Expires != 0 {
			return nil, fmt.Errorf("migration cannot make expiring v1 chunks permanent: %s", key)
		}
		if strings.HasPrefix(key, "job_log_manifest/") {
			if entry.Expires != 0 {
				return nil, fmt.Errorf("migration requires permanent log manifests: %s", key)
			}
			var m JobLogManifest
			if err := decodeJobLogManifest(entry.Data, &m); err != nil {
				return nil, err
			}
			if (m.PendingEnd != 0 || !m.Final) && !m.Deleted {
				return nil, fmt.Errorf("source log %s is not sealed", key)
			}
			id, _, err := JobLogIDFromManifestKey(key)
			if err != nil {
				return nil, err
			}
			if err := verifyLegacyChunkedLog(ctx, source, id, m); err != nil {
				return nil, err
			}
		}
		record.Expires = entry.Expires
		_, _ = fmt.Fprintf(h, "%d\x00", entry.Expires)
		_, _ = h.Write(entry.Data)
	}
	record.SHA256 = hex.EncodeToString(h.Sum(nil))
	return record, nil
}

type migrationFingerprint struct {
	sum   [sha256.Size]byte
	count uint64
}

func (f *migrationFingerprint) add(record migrationSourceRecord) error {
	digest, err := hex.DecodeString(record.SHA256)
	if err != nil || len(digest) != sha256.Size {
		return fmt.Errorf("invalid source snapshot digest")
	}
	carry := uint16(0)
	for i := len(f.sum) - 1; i >= 0; i-- {
		n := uint16(f.sum[i]) + uint16(digest[i]) + carry
		f.sum[i], carry = byte(n), n>>8
	}
	f.count++
	return nil
}
func (f *migrationFingerprint) String() string {
	return fmt.Sprintf("sha256-multiset-v1:%d:%x", f.count, f.sum)
}

// Order-independent aggregation permits streaming native range scans. This is
// change detection, not a signature or authentication of an untrusted source.
func snapshotLegacySource(ctx context.Context, source Storage, visit func(migrationSourceRecord) error) (string, error) {
	var fingerprint migrationFingerprint
	err := walkBackupKeys(ctx, source, func(key string) error {
		record, err := sourceRecordDigest(ctx, source, key)
		if err != nil || record == nil {
			return err
		}
		if visit != nil {
			if err := visit(*record); err != nil {
				return err
			}
		}
		return fingerprint.add(*record)
	})
	if err != nil {
		return "", err
	}
	return fingerprint.String(), nil
}

func validateMigrationSnapshot(ctx context.Context, source, target Storage, want string) error {
	var fingerprint migrationFingerprint
	err := walkBackupKeys(ctx, target, func(key string) error {
		if !strings.HasPrefix(key, "split_migration/source/") {
			return nil
		}
		raw, err := target.Get(ctx, key)
		if err != nil {
			return err
		}
		var saved migrationSourceRecord
		if err := json.Unmarshal(raw, &saved); err != nil || sourceSnapshotKey(saved.Key) != key {
			return fmt.Errorf("invalid migration source checkpoint")
		}
		current, err := sourceRecordDigest(ctx, source, saved.Key)
		if err != nil {
			return err
		}
		if current == nil {
			if saved.Expires <= 0 || saved.Expires > time.Now().Unix() {
				return fmt.Errorf("nonexpired source record disappeared: %s", saved.Key)
			}
			// The first attempt may have committed Put and crashed before Expire.
			// Do not let an already expired source record survive forever in the
			// target merely because it is absent from the next source inventory.
			if exists, err := target.Exists(ctx, saved.Key); err != nil {
				return err
			} else if exists {
				if err := target.Expire(ctx, saved.Key, saved.Expires); err != nil && !errors.Is(err, ErrNotFound) {
					return err
				}
			}
		} else if *current != saved {
			return fmt.Errorf("source record changed: %s", saved.Key)
		}
		return fingerprint.add(saved)
	})
	if err != nil {
		return err
	}
	if fingerprint.String() != want {
		return fmt.Errorf("source snapshot inventory is incomplete or changed")
	}
	return walkBackupKeys(ctx, source, func(key string) error {
		current, err := sourceRecordDigest(ctx, source, key)
		if err != nil || current == nil {
			return err
		}
		raw, err := target.Get(ctx, sourceSnapshotKey(key))
		if err != nil {
			return fmt.Errorf("unexpected source record %s: %w", key, err)
		}
		var saved migrationSourceRecord
		if json.Unmarshal(raw, &saved) != nil || saved != *current {
			return fmt.Errorf("source record changed: %s", key)
		}
		return nil
	})
}

// MigrateLegacyToSplit is an offline, resumable copy. It does not modify the
// source. Resume validates per-record digests and target bindings. Natural TTL
// expiry is accepted only against the recorded absolute expiry; nonexpired
// deletion, new records and changed content reject the resumed operation.
func MigrateLegacyToSplit(ctx context.Context, source, target Storage, registry *BlobRegistry, options SplitMigrationOptions) (SplitMigrationResult, error) {
	var result SplitMigrationResult
	if err := checkOfflineSource(ctx, source, options.Offline); err != nil {
		return result, err
	}
	if err := checkOfflineSource(ctx, target, options.Offline); err != nil {
		return result, err
	}
	var fingerprint string
	var err error
	var snapshotFile *os.File
	defer func() {
		if snapshotFile != nil {
			name := snapshotFile.Name()
			_ = snapshotFile.Close()
			_ = os.Remove(name)
		}
	}()
	var state splitMigrationState
	var layout StorageLayout
	if options.Resume {
		raw, err := target.Get(ctx, splitMigrationKey)
		if errors.Is(err, ErrNotFound) {
			// A crash between namespace initialization and the first checkpoint
			// is resumable only while no application data has been copied.
			if err := migrationTargetHasOnlyLayout(ctx, target); err != nil {
				return result, err
			}
			layout, err = InitializeStorageLayout(ctx, target, registry, "migrating")
			if err != nil {
				return result, err
			}
			snapshotFile, fingerprint, err = captureMigrationSnapshot(ctx, source)
			if err != nil {
				return result, err
			}
			state, err = newMigrationState(fingerprint, layout.NamespaceID)
			if err != nil {
				return result, err
			}
			if err := saveMigrationState(ctx, target, state); err != nil {
				return result, err
			}
			raw, err = json.Marshal(state)
		}
		if err != nil {
			return result, fmt.Errorf("no migration checkpoint: %w", err)
		}
		if err := json.Unmarshal(raw, &state); err != nil || state.Version != 1 || state.ID == "" || state.SourceFingerprint == "" {
			return result, fmt.Errorf("migration source no longer matches its checkpoint")
		}
		fingerprint = state.SourceFingerprint
		layout, err = LoadStorageLayout(ctx, target)
		if err != nil || layout.NamespaceID != state.TargetNamespace {
			return result, fmt.Errorf("migration target namespace does not match its checkpoint")
		}
		if layout.Phase == "ready" && state.Complete {
			if _, err := ValidateStorageLayout(ctx, target, registry); err != nil {
				return result, err
			}
			return state.Result, nil
		}
		layout, err = ValidateStorageLayoutPhase(ctx, target, registry, "migrating")
		if err != nil {
			return result, err
		}
		if state.SnapshotComplete {
			if err := validateMigrationSnapshot(ctx, source, target, fingerprint); err != nil {
				return result, err
			}
		}
	} else {
		snapshotFile, fingerprint, err = captureMigrationSnapshot(ctx, source)
		if err != nil {
			return result, err
		}
		if err := ensureEmptySplitTarget(ctx, target); err != nil {
			return result, err
		}
		layout, err = InitializeStorageLayout(ctx, target, registry, "migrating")
		if err != nil {
			return result, err
		}
		state, err = newMigrationState(fingerprint, layout.NamespaceID)
		if err != nil {
			return result, err
		}
		if err := saveMigrationState(ctx, target, state); err != nil {
			return result, err
		}
	}
	if !state.SnapshotComplete {
		// Capture once before creating the namespace. Entries expiring during
		// remote initialization still have their original recorded deadlines.
		if snapshotFile == nil {
			if err := validateMigrationSnapshot(ctx, source, target, state.SourceFingerprint); err == nil {
				state.SnapshotComplete = true
			} else {
				var got string
				snapshotFile, got, err = captureMigrationSnapshot(ctx, source)
				if err != nil {
					return result, err
				}
				if got != state.SourceFingerprint {
					return result, fmt.Errorf("source changed before its migration snapshot completed; use a fresh target")
				}
			}
		}
		if !state.SnapshotComplete {
			if _, err := snapshotFile.Seek(0, io.SeekStart); err != nil {
				return result, err
			}
			decoder := json.NewDecoder(snapshotFile)
			for {
				var record migrationSourceRecord
				if err := decoder.Decode(&record); err == io.EOF {
					break
				} else if err != nil {
					return result, err
				}
				raw, err := json.Marshal(record)
				if err != nil {
					return result, err
				}
				if err := target.Put(ctx, sourceSnapshotKey(record.Key), raw); err != nil {
					return result, err
				}
			}
			if err := validateMigrationSnapshot(ctx, source, target, state.SourceFingerprint); err != nil {
				return result, err
			}
		}
		state.SnapshotComplete = true
		if err := saveMigrationState(ctx, target, state); err != nil {
			return result, err
		}
	}
	result.MigrationID, result.Namespace = state.ID, layout.NamespaceID
	repo, err := NewJobLogRepository(target, registry.Stores(), JobLogRepositoryOptions{Namespace: layout.NamespaceID, WriteStore: registry.WriteStoreID(), WriteFormat: 2})
	if err != nil {
		return result, err
	}
	err = walkBackupKeys(ctx, source, func(key string) error {
		if skipSplitRecord(key) || strings.HasPrefix(key, "job_log_chunks/") {
			return nil
		}
		if id, isManifest, err := JobLogIDFromManifestKey(key); err != nil {
			return err
		} else if isManifest {
			raw, err := source.Get(ctx, key)
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			var m JobLogManifest
			if err := decodeJobLogManifest(raw, &m); err != nil {
				return err
			}
			if err := migrateOneLog(ctx, source, target, repo, id, m.Offset, m.Deleted, false, state.ID); err != nil {
				return err
			}
			result.Logs++
			result.Bytes += m.Offset
			return nil
		}
		if strings.HasPrefix(key, "job_logs:") {
			id := strings.TrimPrefix(key, "job_logs:")
			if _, err := source.Get(ctx, JobLogManifestKey(id)); err == nil {
				return nil
			} else if !errors.Is(err, ErrNotFound) {
				return err
			}
			deleted, err := isArchivedLogDeleted(ctx, source, id)
			if err != nil {
				return err
			}
			var total int64
			if !deleted {
				r, ok := source.(RangeStorage)
				if !ok {
					return ErrUnsupported
				}
				_, total, err = r.GetRange(ctx, key, 0, 0)
				if err != nil {
					return err
				}
			}
			if err := migrateOneLog(ctx, source, target, repo, id, total, deleted, true, state.ID); err != nil {
				return err
			}
			result.Logs++
			result.Bytes += total
			return nil
		}
		entry, err := readBackupEntry(ctx, source, key)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(entry.Data) > splitMetadataLimit {
			return fmt.Errorf("metadata record %s is too large", key)
		}
		// The copied record is its own durable checkpoint. Recheck bytes and TTL
		// instead of trusting a progress counter after a lost acknowledgment.
		current, readErr := readBackupEntry(ctx, target, key)
		if readErr != nil && !errors.Is(readErr, ErrNotFound) {
			return readErr
		}
		if readErr != nil || !bytes.Equal(current.Data, entry.Data) || current.Expires != entry.Expires {
			if err := restoreBackupEntry(ctx, target, key, entry); err != nil {
				return err
			}
		}
		result.Records++
		return nil
	})
	if err != nil {
		return result, err
	}
	if err := validateMigrationSnapshot(ctx, source, target, fingerprint); err != nil {
		return result, err
	}
	if _, err := verifySplitStorage(ctx, target, registry, layout, true); err != nil {
		return result, err
	}
	state.Complete = true
	state.Result = result
	if err := saveMigrationState(ctx, target, state); err != nil {
		return result, err
	}
	if err := SetStorageLayoutPhase(ctx, target, layout.NamespaceID, "migrating", "ready"); err != nil {
		current, readErr := LoadStorageLayout(ctx, target)
		if readErr != nil || current.NamespaceID != layout.NamespaceID || current.Phase != "ready" {
			return result, err
		}
	}
	return result, nil
}

func captureMigrationSnapshot(ctx context.Context, source Storage) (*os.File, string, error) {
	file, err := os.CreateTemp("", "owlcron-migration-source-")
	if err != nil {
		return nil, "", err
	}
	encoder := json.NewEncoder(file)
	fingerprint, err := snapshotLegacySource(ctx, source, func(record migrationSourceRecord) error { return encoder.Encode(record) })
	if err != nil {
		name := file.Name()
		_ = file.Close()
		_ = os.Remove(name)
		return nil, "", err
	}
	return file, fingerprint, nil
}

func migrationTargetHasOnlyLayout(ctx context.Context, target Storage) error {
	return walkBackupKeys(ctx, target, func(key string) error {
		if key == StorageLayoutKey || key == "global/readiness_probe" || strings.HasPrefix(key, "_expire/") {
			return nil
		}
		if _, err := target.Get(ctx, key); errors.Is(err, ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		return fmt.Errorf("migration checkpoint is missing in a populated target")
	})
}

func newMigrationState(fingerprint, namespace string) (splitMigrationState, error) {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return splitMigrationState{}, err
	}
	return splitMigrationState{Version: 1, ID: hex.EncodeToString(id), SourceFingerprint: fingerprint, TargetNamespace: namespace}, nil
}

func saveMigrationState(ctx context.Context, target Storage, state splitMigrationState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	atomic, ok := target.(AtomicStorage)
	if !ok {
		return ErrUnsupported
	}
	if err := atomic.Update(ctx, splitMigrationKey, func(raw []byte) ([]byte, error) {
		if len(raw) > 0 {
			var current splitMigrationState
			if err := json.Unmarshal(raw, &current); err != nil {
				return nil, err
			}
			if current.Version != state.Version || current.ID != state.ID || current.SourceFingerprint != state.SourceFingerprint || current.TargetNamespace != state.TargetNamespace || current.Complete && !state.Complete || current.SnapshotComplete && !state.SnapshotComplete {
				return nil, fmt.Errorf("migration checkpoint is owned by another operation or has advanced: %w", ErrConflict)
			}
		}
		return data, nil
	}); err != nil {
		got, readErr := target.Get(ctx, splitMigrationKey)
		if readErr != nil || !bytes.Equal(got, data) {
			return err
		}
	}
	return nil
}

func migrateOneLog(ctx context.Context, source, target Storage, repo *JobLogRepository, id string, size int64, deleted, legacy bool, migrationID string) error {
	if size < 0 || size > JobLogAbsoluteMax {
		return ErrJobLogLimit
	}
	canonicalDeleted, err := isArchivedLogDeleted(ctx, source, id)
	if err != nil {
		return err
	}
	deleted = deleted || canonicalDeleted
	checkpointHash := sha256.Sum256([]byte(id))
	checkpointKey := "split_migration/logs/" + hex.EncodeToString(checkpointHash[:])
	if deleted {
		if err := repo.Delete(ctx, id); err != nil {
			return err
		}
		return saveLogMigrationCheckpoint(ctx, target, checkpointKey, migrationID, id, 0, "deleted")
	}
	read := func(at int64, limit int) ([]byte, error) {
		if legacy {
			r, ok := source.(RangeStorage)
			if !ok {
				return nil, ErrUnsupported
			}
			data, total, err := r.GetRange(ctx, "job_logs:"+id, at, limit)
			if err == nil && total != size {
				err = fmt.Errorf("source log changed size")
			}
			return data, err
		}
		data, m, err := JobLogRead(ctx, source, id, at, limit)
		if err == nil && (m.Offset != size || !m.Final || m.PendingEnd != 0 || m.Deleted) {
			err = fmt.Errorf("source log changed during migration")
		}
		return data, err
	}
	m, err := repo.Info(ctx, id)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if m.Deleted || m.Offset > size || m.PendingEnd > size {
		return fmt.Errorf("target log conflicts with source")
	}
	// Verify already copied bytes before resuming; a committed offset alone is
	// not evidence that the previous attempt used the same source data.
	for at := int64(0); at < m.Offset; {
		limit := min(JobLogChunkSize, int(m.Offset-at))
		want, err := read(at, limit)
		if err != nil {
			return err
		}
		got, _, err := repo.Read(ctx, id, at, limit)
		if err != nil || len(want) == 0 || !bytes.Equal(want, got) {
			return fmt.Errorf("target log prefix does not match source")
		}
		at += int64(len(want))
	}
	for m.Offset < size {
		limit := min(JobLogChunkSize, int(size-m.Offset))
		if m.PendingEnd != 0 {
			limit = int(m.PendingEnd - m.Offset)
		}
		data, err := read(m.Offset, limit)
		if err != nil {
			return err
		}
		if len(data) != limit {
			return io.ErrUnexpectedEOF
		}
		m, err = repo.Append(ctx, id, m.Offset, data, m.Offset+int64(len(data)) == size, JobLogAbsoluteMax)
		if err != nil {
			return err
		}
	}
	if !m.Final {
		if _, err := repo.Append(ctx, id, size, nil, true, JobLogAbsoluteMax); err != nil {
			return err
		}
	}
	h := sha256.New()
	for at := int64(0); at < size; {
		want, err := read(at, min(JobLogChunkSize, int(size-at)))
		if err != nil {
			return err
		}
		got, _, err := repo.Read(ctx, id, at, len(want))
		if err != nil || len(want) == 0 || !bytes.Equal(want, got) {
			return fmt.Errorf("copied log failed verification")
		}
		_, _ = h.Write(want)
		at += int64(len(want))
	}
	return saveLogMigrationCheckpoint(ctx, target, checkpointKey, migrationID, id, size, hex.EncodeToString(h.Sum(nil)))
}

func saveLogMigrationCheckpoint(ctx context.Context, target Storage, key, migrationID, id string, size int64, digest string) error {
	data, err := json.Marshal(struct {
		MigrationID string `json:"migration_id"`
		ID          string `json:"id"`
		Size        int64  `json:"size"`
		SHA256      string `json:"sha256"`
		VerifiedAt  int64  `json:"verified_at"`
	}{migrationID, id, size, digest, time.Now().Unix()})
	if err != nil {
		return err
	}
	return target.Put(ctx, key, data)
}
