package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type prefixTestTransport struct{ handler http.Handler }

func (t prefixTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	t.handler.ServeHTTP(recorder, r)
	return recorder.Result(), nil
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
				if err := json.NewEncoder(w).Encode(doc); err != nil {
					t.Error(err)
				}
			})
			store, err := NewS3Storage(&StorageConfig{Options: map[string]interface{}{"bucket": "bucket", "access_key": "test", "secret_key": "test", "endpoint": "http://storage.invalid", "prefix": namespace}})
			if err != nil {
				t.Fatal(err)
			}
			store.prefix = namespace
			store.client = s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), HTTPClient: &http.Client{Transport: prefixTestTransport{handler: handler}}}, func(o *s3.Options) {
				o.BaseEndpoint = aws.String("http://storage.invalid")
				o.UsePathStyle = true
				o.RetryMaxAttempts = 1
			})
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
