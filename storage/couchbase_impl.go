package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
)

type CouchbaseStorage struct {
	*remoteStorage
	cluster    *gocb.Cluster
	collection *gocb.Collection
	prefix     string
	durability gocb.DurabilityLevel
	mutationMu sync.Mutex
	mutations  map[uint64]gocb.MutationToken
}

func NewCouchbaseStorage(config *StorageConfig) (*CouchbaseStorage, error) {
	opts := config.Options
	if value, exists := opts["durability"]; exists {
		if _, ok := value.(string); !ok {
			return nil, fmt.Errorf("Couchbase durability must be a string")
		}
	}
	durability, err := couchbaseDurability(optionString(opts, "durability", ""))
	if err != nil {
		return nil, err
	}
	conn := optionString(opts, "connection_string", "")
	bucketName := optionString(opts, "bucket", "")
	username := optionString(opts, "username", os.Getenv("COUCHBASE_USERNAME"))
	password := optionString(opts, "password", os.Getenv("COUCHBASE_PASSWORD"))
	if conn == "" || bucketName == "" || username == "" || password == "" {
		return nil, fmt.Errorf("Couchbase storage requires connection_string, bucket, username and password")
	}
	cluster, err := gocb.Connect(conn, gocb.ClusterOptions{Authenticator: gocb.PasswordAuthenticator{Username: username, Password: password}})
	if err != nil {
		return nil, err
	}
	bucket := cluster.Bucket(bucketName)
	if err = bucket.WaitUntilReady(15*time.Second, nil); err != nil {
		cluster.Close(nil)
		return nil, err
	}
	collection := bucket.Scope(optionString(opts, "scope", "_default")).Collection(optionString(opts, "collection", "_default"))
	store := &CouchbaseStorage{cluster: cluster, collection: collection, prefix: optionString(opts, "prefix", "owlcron:"), durability: durability}
	store.remoteStorage = &remoteStorage{kv: store}
	return store, nil
}
func (c *CouchbaseStorage) read(ctx context.Context, key string) ([]byte, string, error) {
	out, err := c.collection.Get(c.prefix+key, &gocb.GetOptions{Context: ctx, Transcoder: gocb.NewRawBinaryTranscoder()})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	var data []byte
	if err = out.Content(&data); err != nil {
		return nil, "", err
	}
	return data, strconv.FormatUint(uint64(out.Cas()), 10), nil
}
func (c *CouchbaseStorage) compareAndSwap(ctx context.Context, key string, data []byte, version string) error {
	var err error
	var result *gocb.MutationResult
	if version == "" {
		result, err = c.collection.Insert(c.prefix+key, data, &gocb.InsertOptions{Context: ctx, Transcoder: gocb.NewRawBinaryTranscoder(), DurabilityLevel: c.durability})
	} else {
		cas, e := strconv.ParseUint(version, 10, 64)
		if e != nil {
			return e
		}
		result, err = c.collection.Replace(c.prefix+key, data, &gocb.ReplaceOptions{Context: ctx, Cas: gocb.Cas(cas), Transcoder: gocb.NewRawBinaryTranscoder(), DurabilityLevel: c.durability})
	}
	if errors.Is(err, gocb.ErrCasMismatch) || errors.Is(err, gocb.ErrDocumentExists) || errors.Is(err, gocb.ErrDocumentNotFound) {
		return ErrConflict
	}
	if err == nil && result != nil {
		c.rememberMutation(result.MutationToken())
	}
	return err
}

func (c *CouchbaseStorage) rememberMutation(token *gocb.MutationToken) {
	if token == nil || token.BucketName() == "" {
		return
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if c.mutations == nil {
		c.mutations = make(map[uint64]gocb.MutationToken)
	}
	previous, found := c.mutations[token.PartitionID()]
	if !found || previous.PartitionUUID() != token.PartitionUUID() || previous.SequenceNumber() < token.SequenceNumber() {
		c.mutations[token.PartitionID()] = *token
	}
}

func (c *CouchbaseStorage) scanOptions(ctx context.Context) *gocb.ScanOptions {
	// Range scans read persisted snapshots. Without a mutation barrier, an
	// immediate backup after a successful restore can silently omit new keys.
	// Keep only the newest token per vBucket, so long-lived nodes have bounded
	// bookkeeping and concurrent writes never mutate an in-flight scan's state.
	c.mutationMu.Lock()
	tokens := make([]gocb.MutationToken, 0, len(c.mutations))
	for _, token := range c.mutations {
		tokens = append(tokens, token)
	}
	c.mutationMu.Unlock()
	return &gocb.ScanOptions{Context: ctx, IDsOnly: true, ConsistentWith: gocb.NewMutationState(tokens...)}
}

func (c *CouchbaseStorage) Close() error { return c.cluster.Close(nil) }

func couchbaseDurability(value string) (gocb.DurabilityLevel, error) {
	switch value {
	case "":
		return gocb.DurabilityLevelUnknown, nil
	case "none":
		return gocb.DurabilityLevelNone, nil
	case "majority":
		return gocb.DurabilityLevelMajority, nil
	case "majority_and_persist_to_active":
		return gocb.DurabilityLevelMajorityAndPersistOnMaster, nil
	case "persist_to_majority":
		return gocb.DurabilityLevelPersistToMajority, nil
	default:
		return gocb.DurabilityLevelNone, fmt.Errorf("invalid Couchbase durability %q", value)
	}
}
