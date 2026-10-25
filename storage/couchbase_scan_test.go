package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
)

func couchbaseScanToken(t *testing.T, partition, uuid, sequence uint64) *gocb.MutationToken {
	t.Helper()
	state := gocb.NewMutationState()
	raw := fmt.Sprintf(`{"bucket":{"%d":[%d,"%d"]}}`, partition, sequence, uuid)
	if err := json.Unmarshal([]byte(raw), state); err != nil {
		t.Fatal(err)
	}
	// The SDK exposes token construction only through its test/internal bridge.
	token := state.Internal().Tokens()[0]
	return &token
}

func TestCouchbaseScanBarrierTracksConcurrentMutationsWithoutGrowth(t *testing.T) {
	c := &CouchbaseStorage{}
	var wg sync.WaitGroup
	for i := uint64(1); i <= 128; i++ {
		token := couchbaseScanToken(t, (i-1)%8, 1, i)
		wg.Add(1)
		go func() { defer wg.Done(); c.rememberMutation(token) }()
	}
	wg.Wait()
	if len(c.mutations) != 8 {
		t.Fatalf("mutation state grew per operation: %d", len(c.mutations))
	}
	for partition, token := range c.mutations {
		if want := uint64(121) + partition; token.SequenceNumber() != want {
			t.Fatalf("out-of-order response moved partition %d backward: %d want %d", partition, token.SequenceNumber(), want)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snapshot := c.scanOptions(ctx)
	before, err := json.Marshal(snapshot.ConsistentWith)
	if err != nil {
		t.Fatal(err)
	}
	c.rememberMutation(couchbaseScanToken(t, 0, 2, 1))
	c.rememberMutation(nil)
	after, err := json.Marshal(snapshot.ConsistentWith)
	if err != nil || string(before) != string(after) || snapshot.Context != ctx || !snapshot.IDsOnly {
		t.Fatalf("concurrent mutation changed in-flight scan options: %s -> %s, %v", before, after, err)
	}
	if token := c.mutations[0]; token.PartitionUUID() != 2 || token.SequenceNumber() != 1 || len(c.mutations) != 8 {
		t.Fatalf("new partition generation retained an invalid barrier: %+v", token)
	}
}

func TestCouchbaseRealStorageImmediateInventory(t *testing.T) {
	conn := os.Getenv("OWLCRON_TEST_COUCHBASE_CONNECTION")
	if conn == "" {
		t.Skip("set OWLCRON_TEST_COUCHBASE_CONNECTION to run real Couchbase test")
	}
	for _, mode := range []string{"all_keys", "prefix_keys", "backup_walk", "export"} {
		t.Run(mode, func(t *testing.T) {
			store, err := NewCouchbaseStorage(&StorageConfig{Engine: "Couchbase", Options: map[string]interface{}{
				"connection_string": conn, "bucket": "owlcron-integration", "username": "Administrator", "password": "owlcron-test-secret",
				"prefix": fmt.Sprintf("scan-barrier-%d-%s:", time.Now().UnixNano(), mode),
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			want := make([]string, 64)
			for i := range want {
				want[i] = fmt.Sprintf("records/%03d", i)
				if err := store.Put(ctx, want[i], []byte(want[i])); err != nil {
					t.Fatal(err)
				}
			}
			// No sleep/retry: an acknowledged restore must be immediately
			// exportable, even while the server's disk flusher is catching up.
			var got []string
			switch mode {
			case "all_keys":
				got, err = store.ListKeys(ctx)
			case "prefix_keys":
				got, err = store.ListKeysPrefix(ctx, "records/")
			case "backup_walk":
				err = walkBackupKeys(ctx, store, func(key string) error { got = append(got, key); return nil })
			case "export":
				var backup *Backup
				backup, err = Export(ctx, store)
				if err == nil {
					for key, entry := range backup.Records {
						if string(entry.Data) != key {
							t.Fatalf("backup changed record %s", key)
						}
						got = append(got, key)
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("immediate inventory omitted acknowledged keys: got %d want %d\n%v", len(got), len(want), got)
			}
		})
	}
}
