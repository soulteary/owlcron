package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	minio "github.com/soulteary/otterio-sdk/v7"
	"github.com/soulteary/otterio-sdk/v7/pkg/credentials"
)

type prefixTestTransport struct{ handler http.Handler }

func (t prefixTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	t.handler.ServeHTTP(recorder, r)
	response := recorder.Result()
	response.Request = r
	return response, nil
}

func prefixS3Store(t *testing.T, prefix string, handler http.Handler) *S3Storage {
	t.Helper()
	client, err := minio.New("storage.invalid", &minio.Options{
		Creds:        credentials.NewStaticV4("test", "test", ""),
		Secure:       false,
		Region:       "us-east-1",
		BucketLookup: minio.BucketLookupPath,
		Transport:    prefixTestTransport{handler: handler},
		MaxRetries:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &S3Storage{client: client, bucket: "bucket", prefix: prefix}
	store.remoteStorage = &remoteStorage{kv: store}
	return store
}

func TestFilesystemListKeysPrefixSkipsOtherNamespacesAndExpired(t *testing.T) {
	store, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, key := range []string{"events:live", "events:expired", "job_logs:large", "logs/completed/old", "events/nested"} {
		if err := store.Put(ctx, key, []byte(`{"id":"test"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Expire(ctx, "events:expired", time.Now().Add(-time.Hour).Unix()); err != nil {
		t.Fatal(err)
	}
	// Unrelated unreadable/corrupt expiry and a symlink in a history subtree
	// must never be reached while repairing the events namespace.
	if err := store.Put(ctx, "_expire/job_logs:large", []byte("bad expiry")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/does-not-exist", filepath.Join(store.baseDir, "logs", "completed", "bad.json")); err != nil {
		t.Fatal(err)
	}
	keys, err := store.ListKeysPrefix(ctx, "events:")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keys, []string{"events:live"}) {
		t.Fatalf("keys: %v", keys)
	}
	keys, err = store.ListKeysPrefix(ctx, "events/")
	if err != nil || !reflect.DeepEqual(keys, []string{"events/nested"}) {
		t.Fatalf("nested keys: %v, %v", keys, err)
	}
	if _, err := store.ListKeysPrefix(ctx, "../"); err == nil {
		t.Fatal("unsafe prefix accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.ListKeysPrefix(canceled, "events:"); err == nil {
		t.Fatal("cancellation ignored")
	}
}

func TestS3ListKeysPrefixUsesServerPrefixAndPaginates(t *testing.T) {
	for _, namespace := range []string{"tenant", ""} {
		t.Run(namespace, func(t *testing.T) {
			objectPrefix := "events:"
			if namespace != "" {
				objectPrefix = namespace + "/" + objectPrefix
			}
			lists := 0
			gets := []string{}
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") == "2" {
					lists++
					if got := r.URL.Query().Get("prefix"); got != objectPrefix {
						t.Errorf("server prefix %q, want %q", got, objectPrefix)
					}
					if r.URL.Query().Get("delimiter") != "" || r.URL.Query().Get("fetch-owner") == "true" {
						t.Errorf("listing must include nested keys without requesting owner metadata: %s", r.URL.RawQuery)
					}
					w.Header().Set("Content-Type", "application/xml")
					if lists == 1 {
						fmt.Fprintf(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>page2</NextContinuationToken><Contents><Key>%slive</Key></Contents><Contents><Key>%sexpired</Key></Contents></ListBucketResult>`, objectPrefix, objectPrefix)
					} else {
						if r.URL.Query().Get("continuation-token") != "page2" {
							t.Error("missing continuation token")
						}
						fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>%sdeleted</Key></Contents><Contents><Key>%slast</Key></Contents></ListBucketResult>`, objectPrefix, objectPrefix)
					}
					return
				}
				gets = append(gets, r.URL.Path)
				w.Header().Set("ETag", `"version"`)
				w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
				doc := remoteDocument{Data: []byte(`{"id":"test"}`), Revision: "1"}
				switch r.URL.Path {
				case "/bucket/" + objectPrefix + "expired":
					doc.Expires = time.Now().Add(-time.Hour).Unix()
				case "/bucket/" + objectPrefix + "deleted":
					doc.Deleted = true
				case "/bucket/" + objectPrefix + "live", "/bucket/" + objectPrefix + "last":
				default:
					t.Errorf("unrelated object read: %s", r.URL.Path)
				}
				data, err := json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				w.Write(data)
			})
			store := prefixS3Store(t, namespace, handler)
			keys, err := store.ListKeysPrefix(context.Background(), "events:")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(keys, []string{"events:last", "events:live"}) {
				t.Fatalf("keys: %v", keys)
			}
			if lists != 2 || len(gets) != 4 {
				t.Fatalf("list calls %d, get calls %v", lists, gets)
			}
		})
	}
}

func TestS3CapabilityRecordsRemainInListingAndBackup(t *testing.T) {
	for _, namespace := range []string{"tenant", ""} {
		t.Run(namespace, func(t *testing.T) {
			namespacePrefix := ""
			if namespace != "" {
				namespacePrefix = namespace + "/"
			}
			records := map[string][]byte{
				"capability/foo": []byte("probe"),
				"capability/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": bytes.Repeat([]byte("large application record"), 4096),
			}
			objects := map[string][]byte{
				namespacePrefix + "capability/00000000000000000000000000000000": []byte("probe"),
				namespacePrefix + "capability/11111111111111111111111111111111": []byte("claim-0"),
				namespacePrefix + "capability/22222222222222222222222222222222": []byte("claim-7"),
				namespacePrefix + "capability/33333333333333333333333333333333": []byte("must not overwrite"),
				namespacePrefix + "capability/44444444444444444444444444444444": []byte("must not replace"),
				namespacePrefix + s3CapabilityPrefix + "private":                []byte("claim-7"),
			}
			for key, data := range records {
				raw, err := json.Marshal(remoteDocument{Data: data, Revision: "preexisting"})
				if err != nil {
					t.Fatal(err)
				}
				objects[namespacePrefix+key] = raw
			}
			reads := map[string]int{}
			store := prefixS3Store(t, namespace, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				reads[key]++
				if strings.HasPrefix(key, namespacePrefix+s3CapabilityPrefix) {
					t.Error("private probe payload was fetched")
				}
				data, exists := objects[key]
				if !exists {
					t.Errorf("unexpected object request: %s", r.URL)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.Header().Set("ETag", `"version"`)
				w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				w.Write(data)
			}))
			want := []string{"capability/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "capability/foo"}
			for _, prefix := range []string{"", "capability/"} {
				clear(reads)
				keys, err := store.ListKeysPrefix(context.Background(), prefix)
				if err != nil || !reflect.DeepEqual(keys, want) {
					t.Fatalf("prefix %q lost application records: %v %v", prefix, keys, err)
				}
				for key := range records {
					if reads[namespacePrefix+key] != 1 {
						t.Fatalf("enumeration fetched application payload %q %d times", key, reads[namespacePrefix+key])
					}
				}
			}
			backup, err := Export(context.Background(), store)
			if err != nil || len(backup.Records) != len(records) {
				t.Fatalf("probe filtering lost backup records: %#v %v", backup, err)
			}
			for key, data := range records {
				if !bytes.Equal(backup.Records[key].Data, data) {
					t.Fatalf("backup changed %q", key)
				}
			}
		})
	}
}

func TestS3CapabilityFilterDoesNotHideCorruptApplicationRecords(t *testing.T) {
	for _, key := range []string{
		"capability/foo",
		"capability/0000000000000000000000000000000",
		"capability/000000000000000000000000000000000",
		"capability/gggggggggggggggggggggggggggggggg",
		"capability/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	} {
		t.Run(key, func(t *testing.T) {
			store := prefixS3Store(t, "tenant", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") == "2" {
					w.Header().Set("Content-Type", "application/xml")
					fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>tenant/%s</Key></Contents></ListBucketResult>`, key)
					return
				}
				w.Header().Set("ETag", `"version"`)
				w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
				w.Header().Set("Content-Length", "5")
				fmt.Fprint(w, "probe")
			}))
			if _, err := store.ListKeys(context.Background()); err == nil || !strings.Contains(err.Error(), "decode storage document") {
				t.Fatalf("corrupt application key %q silently hidden: %v", key, err)
			}
		})
	}
	for _, data := range []string{"not a probe", "claim-8", "claim-00"} {
		if s3LegacyCapabilityProbe("capability/00000000000000000000000000000000", []byte(data)) {
			t.Fatalf("non-probe payload %q hidden solely because of its key", data)
		}
	}
}

func TestS3ListingCancellationNeverReturnsPartialSuccess(t *testing.T) {
	for _, afterPage := range []bool{false, true} {
		t.Run(fmt.Sprint(afterPage), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			requests := 0
			store := prefixS3Store(t, "tenant", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.URL.Query().Get("list-type") != "2" {
					t.Errorf("unexpected request: %s", r.URL)
				}
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>page2</NextContinuationToken></ListBucketResult>`)
				cancel()
			}))
			if !afterPage {
				cancel()
			}
			keys, err := store.ListKeys(ctx)
			if !errors.Is(err, context.Canceled) || keys != nil {
				t.Fatalf("canceled enumeration returned keys=%v err=%v", keys, err)
			}
			if afterPage && requests != 1 || !afterPage && requests != 0 {
				t.Fatalf("unexpected requests after cancellation: %d", requests)
			}
		})
	}
}

func TestS3ListingLaterPageFailureNeverReturnsPartialSuccess(t *testing.T) {
	lists := 0
	store := prefixS3Store(t, "tenant", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			lists++
			w.Header().Set("Content-Type", "application/xml")
			if lists == 1 {
				fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>page2</NextContinuationToken><Contents><Key>tenant/events:live</Key></Contents></ListBucketResult>`)
				return
			}
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `<Error><Code>AccessDenied</Code></Error>`)
			return
		}
		data, _ := json.Marshal(remoteDocument{Data: []byte(`{"id":"test"}`), Revision: "1"})
		w.Header().Set("ETag", `"version"`)
		w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		w.Write(data)
	}))
	keys, err := store.ListKeys(context.Background())
	if err == nil || keys != nil || lists != 2 {
		t.Fatalf("failed enumeration returned keys=%v err=%v list calls=%d", keys, err, lists)
	}
	if minio.ToErrorResponse(err).Code != "AccessDenied" {
		t.Fatalf("listing error changed: %v", err)
	}
}
