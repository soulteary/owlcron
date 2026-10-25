package storage

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	minio "github.com/soulteary/otterio-sdk/v7"
	"github.com/soulteary/otterio-sdk/v7/pkg/credentials"
)

type blockedS3ProbeTransport struct {
	entered chan struct{}
	once    sync.Once
}

func (transport *blockedS3ProbeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.once.Do(func() { close(transport.entered) })
	<-request.Context().Done()
	return nil, request.Context().Err()
}

// No service is needed: this tests cancellation while another caller owns the
// first capability check, independently of SDK/network retry timing.
func TestS3ConditionalProbeWaitHonorsCallerDeadline(t *testing.T) {
	transport := &blockedS3ProbeTransport{entered: make(chan struct{})}
	client, err := minio.New("https://storage.invalid", &minio.Options{
		Region: "us-east-1", Creds: credentials.NewStaticV4("test", "test", ""),
		Secure: true, Transport: transport, BucketLookup: minio.BucketLookupPath, MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Literal construction is supported by the gate's lazy initialization.
	store := &S3Storage{client: client, bucket: "test"}
	ownerContext, stopOwner := context.WithCancel(context.Background())
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- store.ensureConditionalWrites(ownerContext) }()
	defer func() {
		stopOwner()
		select {
		case <-ownerDone:
		case <-time.After(time.Second):
			t.Error("probe owner leaked")
		}
	}()
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		t.Fatal("owner did not enter probe")
	}
	waiterContext, stopWaiter := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopWaiter()
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- store.ensureConditionalWrites(waiterContext) }()
	select {
	case err := <-waiterDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiter lost its deadline: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("short deadline blocked behind another caller's network probe")
	}
}
