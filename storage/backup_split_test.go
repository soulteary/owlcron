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
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

type splitBackupFixture struct {
	meta     *FilesystemStorage
	registry *BlobRegistry
	repo     *JobLogRepository
	layout   StorageLayout
	blobDir  string
}

func newSplitBackupFixture(t *testing.T, initialized bool) *splitBackupFixture {
	t.Helper()
	meta, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	blobDir := t.TempDir()
	registry, err := NewBlobRegistry(&types.BlobStorageConfig{WriteStore: "logs", Stores: map[string]types.BlobStoreConfig{"logs": {Engine: "Filesystem", BaseDir: blobDir}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	f := &splitBackupFixture{meta: meta, registry: registry, blobDir: blobDir}
	if initialized {
		f.layout, err = InitializeStorageLayout(context.Background(), meta, registry, "ready")
		if err != nil {
			t.Fatal(err)
		}
		f.repo, err = NewJobLogRepository(meta, registry.Stores(), JobLogRepositoryOptions{Namespace: f.layout.NamespaceID, WriteStore: "logs", WriteFormat: 2})
		if err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func appendSplitTestLog(t *testing.T, f *splitBackupFixture, id string, data []byte) {
	t.Helper()
	ctx := context.Background()
	for at := 0; at < len(data); {
		end := min(at+JobLogChunkSize, len(data))
		if _, err := f.repo.Append(ctx, id, int64(at), data[at:end], end == len(data), JobLogAbsoluteMax); err != nil {
			t.Fatal(err)
		}
		at = end
	}
	if len(data) == 0 {
		if _, err := f.repo.Append(ctx, id, 0, nil, true, JobLogAbsoluteMax); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.meta.Put(ctx, "jobs:"+id, []byte(fmt.Sprintf(`{"id":%q,"complete":true}`, id))); err != nil {
		t.Fatal(err)
	}
}

func exportSplitFixture(t *testing.T, f *splitBackupFixture) []byte {
	t.Helper()
	var out bytes.Buffer
	if _, err := ExportSplitTo(context.Background(), f.meta, f.registry, &out, SplitBackupOptions{Offline: true}); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestSplitBackupRestoreWithoutOriginalObjects(t *testing.T) {
	ctx := context.Background()
	source := newSplitBackupFixture(t, true)
	payload := bytes.Repeat([]byte("bounded log\x00"), JobLogChunkSize/3)
	appendSplitTestLog(t, source, "finished", payload)
	appendSplitTestLog(t, source, "empty", nil)
	if err := source.repo.Delete(ctx, "deleted"); err != nil {
		t.Fatal(err)
	}
	if err := source.meta.Put(ctx, "deleted_jobs:deleted", []byte(`{"deleted":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := source.meta.Put(ctx, "session:one", []byte("session")); err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Unix() + 3600
	if err := source.meta.Expire(ctx, "session:one", expires); err != nil {
		t.Fatal(err)
	}
	if err := source.meta.Put(ctx, "global/state", []byte(`{"cursors":{"event":42},"_lease_owner":"old"}`)); err != nil {
		t.Fatal(err)
	}
	if err := source.meta.Put(ctx, "global/master_lease", []byte(`{"owner":"old","expires":1}`)); err != nil {
		t.Fatal(err)
	}
	archive := exportSplitFixture(t, source)
	if bytes.Contains(archive, []byte(`"key":"global/master_lease"`)) {
		t.Fatal("transient lease exported")
	}
	if err := os.RemoveAll(source.blobDir); err != nil {
		t.Fatal(err)
	}
	target := newSplitBackupFixture(t, false)
	result, err := ImportSplitFrom(ctx, target.meta, target.registry, bytes.NewReader(archive), SplitBackupOptions{Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Objects < 2 {
		t.Fatal("archive did not contain immutable content")
	}
	layout, err := ValidateStorageLayout(ctx, target.meta, target.registry)
	if err != nil || layout.NamespaceID == source.layout.NamespaceID {
		t.Fatalf("restore reused source identity: %+v %v", layout, err)
	}
	repo, err := NewJobLogRepository(target.meta, target.registry.Stores(), JobLogRepositoryOptions{Namespace: layout.NamespaceID, WriteStore: "logs", WriteFormat: 2})
	if err != nil {
		t.Fatal(err)
	}
	got, m, err := repo.Read(ctx, "finished", 0, len(payload))
	if err != nil || !bytes.Equal(got, payload) || !m.Final {
		t.Fatalf("independent restore failed: %d %+v %v", len(got), m, err)
	}
	if m, err := repo.Info(ctx, "deleted"); err != nil || !m.Deleted {
		t.Fatal("lost permanent deletion fence")
	}
	if expiry, err := target.meta.Get(ctx, "_expire/session:one"); err != nil || string(expiry) != fmt.Sprint(expires) {
		t.Fatalf("TTL changed: %s %v", expiry, err)
	}
	if raw, err := target.meta.Get(ctx, "global/state"); err != nil || bytes.Contains(raw, []byte("_lease_owner")) {
		t.Fatal("restored retired lease owner")
	}
	if _, err := VerifySplitStorage(ctx, target.meta, target.registry); err != nil {
		t.Fatal(err)
	}
}

func rewriteSplitArchive(t *testing.T, data []byte, edit func([]splitArchiveRow) []splitArchiveRow) []byte {
	t.Helper()
	var rows []splitArchiveRow
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		row, err := decodeArchiveRow(line)
		if err != nil {
			t.Fatal(err)
		}
		if row.Kind != "complete" {
			rows = append(rows, row)
		}
	}
	rows = edit(rows)
	var out bytes.Buffer
	w := &splitArchiveWriter{w: &out, h: sha256.New()}
	for _, row := range rows {
		if err := w.row(row, true); err != nil {
			t.Fatal(err)
		}
		switch row.Kind {
		case "record":
			w.result.Records++
		case "object":
			w.result.Objects++
			w.result.Bytes += int64(len(row.Data))
		case "legacy_log":
			w.result.LegacyChunks++
			w.result.Bytes += int64(len(row.Data))
		}
	}
	if err := w.row(splitArchiveRow{Kind: "complete", SHA256: hex.EncodeToString(w.h.Sum(nil)), Counts: &w.result}, false); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestSplitBackupCorruptionNeverActivatesTarget(t *testing.T) {
	source := newSplitBackupFixture(t, true)
	appendSplitTestLog(t, source, "job", []byte("important log"))
	archive := exportSplitFixture(t, source)
	cases := map[string][]byte{
		"truncated": archive[:len(archive)-15],
		"trailing":  append(append([]byte(nil), archive...), []byte("{}\n")...),
		"wrong object hash": rewriteSplitArchive(t, archive, func(rows []splitArchiveRow) []splitArchiveRow {
			for i := range rows {
				if rows[i].Kind == "object" {
					rows[i].SHA256 = strings.Repeat("0", 64)
					break
				}
			}
			return rows
		}),
		"missing object": rewriteSplitArchive(t, archive, func(rows []splitArchiveRow) []splitArchiveRow {
			for i := range rows {
				if rows[i].Kind == "object" {
					return append(rows[:i], rows[i+1:]...)
				}
			}
			return rows
		}),
		"duplicate object": rewriteSplitArchive(t, archive, func(rows []splitArchiveRow) []splitArchiveRow {
			for _, row := range rows {
				if row.Kind == "object" {
					return append(rows, row)
				}
			}
			return rows
		}),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			target := newSplitBackupFixture(t, false)
			if _, err := ImportSplitFrom(context.Background(), target.meta, target.registry, bytes.NewReader(data), SplitBackupOptions{Offline: true}); err == nil {
				t.Fatal("corrupt archive accepted")
			}
			layout, err := LoadStorageLayout(context.Background(), target.meta)
			if err == nil && layout.Phase == "ready" {
				t.Fatal("failed restore made target ready")
			}
		})
	}
}

func TestSplitRestorePreservesExpiredTTLAndRejectsNonemptyTarget(t *testing.T) {
	ctx := context.Background()
	source := newSplitBackupFixture(t, true)
	if err := source.meta.Put(ctx, "session:expired", []byte("old")); err != nil {
		t.Fatal(err)
	}
	archive := rewriteSplitArchive(t, exportSplitFixture(t, source), func(rows []splitArchiveRow) []splitArchiveRow {
		for i := range rows {
			if rows[i].Key == "session:expired" {
				rows[i].Expires = time.Now().Unix() - 1
			}
		}
		return rows
	})
	target := newSplitBackupFixture(t, false)
	if _, err := ImportSplitFrom(ctx, target.meta, target.registry, bytes.NewReader(archive), SplitBackupOptions{Offline: true}); err != nil {
		t.Fatal(err)
	}
	if exists, err := target.meta.Exists(ctx, "session:expired"); err != nil || exists {
		t.Fatal("expired record revived")
	}
	if _, err := ImportSplitFrom(ctx, target.meta, target.registry, bytes.NewReader(archive), SplitBackupOptions{Offline: true}); err == nil {
		t.Fatal("restore overwrote existing namespace")
	}
}

func TestSplitBackupLegacyLogsAreBoundedAndRestoredAsV2(t *testing.T) {
	ctx := context.Background()
	source := newSplitBackupFixture(t, true)
	payload := bytes.Repeat([]byte("legacy\n"), JobLogChunkSize)
	if err := source.meta.Put(ctx, "job_logs:legacy", payload); err != nil {
		t.Fatal(err)
	}
	if err := source.meta.Put(ctx, "jobs:legacy", []byte(`{"id":"legacy","complete":true}`)); err != nil {
		t.Fatal(err)
	}
	archive := exportSplitFixture(t, source)
	target := newSplitBackupFixture(t, false)
	result, err := ImportSplitFrom(ctx, target.meta, target.registry, bytes.NewReader(archive), SplitBackupOptions{Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.LegacyChunks <= 1 {
		t.Fatal("legacy log was not streamed as fragments")
	}
	layout, _ := LoadStorageLayout(ctx, target.meta)
	repo, _ := NewJobLogRepository(target.meta, target.registry.Stores(), JobLogRepositoryOptions{Namespace: layout.NamespaceID, WriteStore: "logs", WriteFormat: 2})
	got, _, err := repo.Read(ctx, "legacy", 0, len(payload))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("legacy restored incorrectly: %v", err)
	}
	if _, err := target.meta.Get(ctx, "job_logs:legacy"); !errors.Is(err, ErrNotFound) {
		t.Fatal("legacy large object restored into metadata")
	}
}

func TestSplitBackupRejectsLiveAndUnsealedSources(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"missing offline", "lease", "active job", "unsealed"} {
		t.Run(mode, func(t *testing.T) {
			f := newSplitBackupFixture(t, true)
			switch mode {
			case "lease":
				_ = f.meta.Put(ctx, "global/master_lease", []byte(fmt.Sprintf(`{"owner":"running","expires":%d}`, time.Now().Add(time.Hour).UnixNano())))
			case "active job":
				_ = f.meta.Put(ctx, "active_jobs", []byte(`[{"id":"running"}]`))
			case "unsealed":
				_, err := f.repo.Append(ctx, "job", 0, []byte("tail"), false, JobLogAbsoluteMax)
				if err != nil {
					t.Fatal(err)
				}
			}
			var out bytes.Buffer
			if _, err := ExportSplitTo(ctx, f.meta, f.registry, &out, SplitBackupOptions{Offline: mode != "missing offline"}); err == nil {
				t.Fatal("unsafe source accepted")
			}
		})
	}
}

func TestLegacyBackupCannotSilentlyOmitSplitObjects(t *testing.T) {
	ctx := context.Background()
	f := newSplitBackupFixture(t, true)
	appendSplitTestLog(t, f, "job", []byte("content"))
	if _, err := Export(ctx, f.meta); !errors.Is(err, ErrSplitBackupRequired) {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if _, err := ExportTo(ctx, f.meta, &out); !errors.Is(err, ErrSplitBackupRequired) || json.Valid(out.Bytes()) {
		t.Fatalf("split export completed a metadata-only archive: %v", err)
	}
	if err := Import(ctx, f.meta, &Backup{Format: "owlcron-storage-v1", Records: map[string]BackupEntry{}}); !errors.Is(err, ErrSplitBackupRequired) {
		t.Fatal(err)
	}
	raw, _ := f.meta.Get(ctx, JobLogManifestKey("job"))
	target, _ := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err := Import(ctx, target, &Backup{Format: "owlcron-storage-v1", Records: map[string]BackupEntry{JobLogManifestKey("job"): {Data: raw}}}); !errors.Is(err, ErrSplitBackupRequired) {
		t.Fatal("accepted dangling v2 metadata import")
	}
	// A missing layout is corruption, not permission to silently omit blobs.
	if err := f.meta.Delete(ctx, StorageLayoutKey); err != nil {
		t.Fatal(err)
	}
	if _, err := Export(ctx, f.meta); !errors.Is(err, ErrSplitBackupRequired) {
		t.Fatal("unbound split roots were exported as v1")
	}
}

func TestSplitArchiveRejectsUnterminatedRows(t *testing.T) {
	_, err := readArchiveLine(bufioReaderForTest("{}"))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}

func bufioReaderForTest(data string) *bufio.Reader { return bufio.NewReader(strings.NewReader(data)) }

func TestSplitFilesystemInventoryRejectsSymlinks(t *testing.T) {
	meta, _ := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err := os.Symlink(t.TempDir(), filepath.Join(meta.baseDir, "unsafe")); err != nil {
		t.Skip(err)
	}
	if err := walkBackupKeys(context.Background(), meta, func(string) error { return nil }); err == nil {
		t.Fatal("followed an unsafe inventory entry")
	}
}

func TestSplitConversionRejectsExpiringLegacyPayload(t *testing.T) {
	ctx := context.Background()
	for _, split := range []bool{false, true} {
		t.Run(fmt.Sprint(split), func(t *testing.T) {
			source := newSplitBackupFixture(t, split)
			if err := source.meta.Put(ctx, "jobs:legacy", []byte(`{"id":"legacy","complete":true}`)); err != nil {
				t.Fatal(err)
			}
			if err := source.meta.Put(ctx, "job_logs:legacy", []byte("expiring log")); err != nil {
				t.Fatal(err)
			}
			if err := source.meta.Expire(ctx, "job_logs:legacy", time.Now().Unix()+300); err != nil {
				t.Fatal(err)
			}
			if split {
				var out bytes.Buffer
				if _, err := ExportSplitTo(ctx, source.meta, source.registry, &out, SplitBackupOptions{Offline: true}); err == nil {
					t.Fatal("expiring payload was made permanent by backup")
				}
			} else {
				target := newSplitBackupFixture(t, false)
				if _, err := MigrateLegacyToSplit(ctx, source.meta, target.meta, target.registry, SplitMigrationOptions{Offline: true}); err == nil {
					t.Fatal("expiring payload was made permanent by migration")
				}
				if _, err := LoadStorageLayout(ctx, target.meta); !errors.Is(err, ErrNotFound) {
					t.Fatal("rejected conversion initialized target")
				}
			}
		})
	}
}

func TestSplitFilesystemInventoryVisitsMultiplePages(t *testing.T) {
	ctx := context.Background()
	meta, _ := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	for i := 0; i < 300; i++ {
		if err := meta.Put(ctx, fmt.Sprintf("records/%04d", i), []byte("body")); err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	if err := walkBackupKeys(ctx, meta, func(key string) error { _, err := meta.Get(ctx, key); count++; return err }); err != nil {
		t.Fatal(err)
	}
	if count != 300 {
		t.Fatalf("paged inventory lost records: %d", count)
	}
}

func TestSplitExportRejectsMissingV1Chunk(t *testing.T) {
	ctx := context.Background()
	source := newSplitBackupFixture(t, true)
	if _, err := JobLogAppend(ctx, source.meta, "v1", 0, []byte("must exist"), true, JobLogAbsoluteMax); err != nil {
		t.Fatal(err)
	}
	if err := source.meta.Delete(ctx, jobLogPrefix("v1")+fmt.Sprintf("%020d", 0)); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if _, err := ExportSplitTo(ctx, source.meta, source.registry, &out, SplitBackupOptions{Offline: true}); err == nil {
		t.Fatal("incomplete v1 log exported successfully")
	}
	if bytes.Contains(out.Bytes(), []byte(`"kind":"complete"`)) {
		t.Fatal("failed export wrote terminal manifest")
	}
}

func TestSplitLegacyS3ExpiryInspectionIsBounded(t *testing.T) {
	for _, expires := range []int64{0, time.Now().Unix() + 3600, time.Now().Unix() - 1} {
		t.Run(fmt.Sprint(expires), func(t *testing.T) {
			doc, _ := json.Marshal(remoteDocument{Data: bytes.Repeat([]byte("payload"), 100000), Revision: "revision", Expires: expires})
			readBytes := 0
			transport := s3SDKRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				headers := http.Header{"Etag": []string{`"revision"`}, "Last-Modified": []string{time.Unix(0, 0).UTC().Format(http.TimeFormat)}}
				if req.Method == http.MethodHead {
					headers.Set("Content-Length", fmt.Sprint(len(doc)))
					return &http.Response{StatusCode: 200, Header: headers, ContentLength: int64(len(doc)), Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
				}
				if req.Method != http.MethodGet || req.Header.Get("If-Match") != `"revision"` {
					return nil, fmt.Errorf("unexpected metadata inspection request")
				}
				var start, end int
				if _, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= len(doc) || end < start || end-start+1 > 4096 {
					return nil, fmt.Errorf("unbounded expiry lookup")
				}
				readBytes += end - start + 1
				headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(doc)))
				headers.Set("Content-Length", fmt.Sprint(end-start+1))
				return &http.Response{StatusCode: 206, Header: headers, ContentLength: int64(end - start + 1), Body: io.NopCloser(bytes.NewReader(doc[start : end+1])), Request: req}, nil
			})
			store := &S3Storage{client: newS3SDKTestClient(t, transport), bucket: "test", prefix: "tenant"}
			err := requirePermanentLegacyLog(context.Background(), store, "large")
			if expires == 0 && err != nil || expires != 0 && err == nil {
				t.Fatalf("expiry=%d err=%v", expires, err)
			}
			if expires > 0 && expires <= time.Now().Unix() && !errors.Is(err, ErrNotFound) {
				t.Fatal("expired log not omitted")
			}
			if readBytes > 4105 {
				t.Fatalf("read %d bytes to inspect expiry", readBytes)
			}
		})
	}
}

func TestSplitS3MigrationPreservesCapabilityRecords(t *testing.T) {
	for _, namespace := range []string{"tenant", ""} {
		t.Run(namespace, func(t *testing.T) {
			ctx := context.Background()
			namespacePrefix := ""
			if namespace != "" {
				namespacePrefix = namespace + "/"
			}
			records := map[string][]byte{
				"capability/foo": []byte("probe"),
				"capability/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": bytes.Repeat([]byte("application data"), 4096),
			}
			objects := map[string][]byte{
				namespacePrefix + "capability/00000000000000000000000000000000": []byte("probe"),
				namespacePrefix + "capability/11111111111111111111111111111111": []byte("claim-0"),
				namespacePrefix + "capability/22222222222222222222222222222222": []byte("must not replace"),
				namespacePrefix + s3CapabilityPrefix + "private":                []byte("probe"),
			}
			for key, data := range records {
				raw, err := json.Marshal(remoteDocument{Data: data, Revision: "original"})
				if err != nil {
					t.Fatal(err)
				}
				objects[namespacePrefix+key] = raw
			}
			source := prefixS3Store(t, namespace, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") == "2" {
					w.Header().Set("Content-Type", "application/xml")
					fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`)
					for key := range objects {
						if strings.HasPrefix(key, r.URL.Query().Get("prefix")) {
							fmt.Fprintf(w, `<Contents><Key>%s</Key></Contents>`, key)
						}
					}
					fmt.Fprint(w, `</ListBucketResult>`)
					return
				}
				key := strings.TrimPrefix(r.URL.Path, "/bucket/")
				if strings.HasPrefix(key, namespacePrefix+s3CapabilityPrefix) {
					t.Error("private probe payload was fetched")
				}
				data, ok := objects[key]
				if !ok {
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusNotFound)
					fmt.Fprint(w, `<Error><Code>NoSuchKey</Code></Error>`)
					return
				}
				w.Header().Set("ETag", `"original"`)
				w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				w.Write(data)
			}))
			target := newSplitBackupFixture(t, false)
			result, err := MigrateLegacyToSplit(ctx, source, target.meta, target.registry, SplitMigrationOptions{Offline: true})
			if err != nil {
				t.Fatal(err)
			}
			if result.Records != len(records) {
				t.Fatalf("migration copied %d application records, want %d", result.Records, len(records))
			}
			for key, want := range records {
				got, err := target.meta.Get(ctx, key)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("migration lost application record %s: %v", key, err)
				}
			}
		})
	}
}

func TestSplitS3InventoryDoesNotReadLegacyLogBodies(t *testing.T) {
	source := prefixS3Store(t, "tenant", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") != "2" {
			t.Errorf("inventory fetched a legacy log body: %s", r.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>tenant/job_logs:large</Key></Contents></ListBucketResult>`)
	}))
	visited := 0
	if err := walkBackupKeys(context.Background(), source, func(key string) error {
		if key != "job_logs:large" {
			t.Fatalf("unexpected key: %s", key)
		}
		visited++
		return nil
	}); err != nil || visited != 1 {
		t.Fatalf("legacy inventory: count=%d error=%v", visited, err)
	}
}
