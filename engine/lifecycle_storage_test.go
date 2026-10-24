package engine

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/soulteary/owlcron/storage"
)

var errLifecycleStorage = errors.New("injected lifecycle storage error")
var errStorageClose = errors.New("injected storage close error")

type lifecycleStorage struct {
	storage.Storage
	mu                sync.Mutex
	rosterReads       int
	failRosterAt      int
	failStateWrite    bool
	failOwnershipRead bool
	closeErr          error
	closeCount        int
}

func (s *lifecycleStorage) Get(ctx context.Context, key string) ([]byte, error) {
	if s.failOwnershipRead && key == "global/master_lease" {
		return nil, errLifecycleStorage
	}
	return s.Storage.Get(ctx, key)
}
func (s *lifecycleStorage) ListGet(ctx context.Context, key string, offset, limit int) ([]interface{}, error) {
	if key == "servers" {
		s.mu.Lock()
		s.rosterReads++
		fail := s.rosterReads == s.failRosterAt
		s.mu.Unlock()
		if fail {
			return nil, errLifecycleStorage
		}
	}
	return s.Storage.ListGet(ctx, key, offset, limit)
}
func (s *lifecycleStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if s.failStateWrite && key == "global/state" {
		return errLifecycleStorage
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}
func (s *lifecycleStorage) Close() error {
	s.mu.Lock()
	s.closeCount++
	s.mu.Unlock()
	return s.closeErr
}
func TestStartupRosterReadFailureCannotBecomeStandaloneMaster(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		e := newSchedulingTestEngine(t, "failed-roster")
		e.storage = &lifecycleStorage{Storage: e.storage, failRosterAt: failAt}
		err := e.Startup()
		if !errors.Is(err, errLifecycleStorage) || e.IsMaster() || e.GetMulti().Eligible {
			t.Fatalf("roster read %d failed open: %v %+v", failAt, err, e.GetMulti())
		}
	}
}
func TestServerRosterRejectsMalformedRecordsAndKeepsDisabledFiltering(t *testing.T) {
	for _, record := range []interface{}{map[string]interface{}{"ip": "127.0.0.1"}, map[string]interface{}{"hostname": 42}, "invalid"} {
		e := newSchedulingTestEngine(t, "invalid-roster")
		if err := e.storage.ListPush(context.Background(), "servers", record); err != nil {
			t.Fatal(err)
		}
		if err := e.checkMasterEligibility(); err == nil || e.GetMulti().Eligible {
			t.Fatalf("malformed record became empty standalone roster: %v", record)
		}
		if err := e.comm.LoadServersFromStorage(); err == nil {
			t.Fatal("malformed peer roster was silently ignored")
		}
	}
	e := newSchedulingTestEngine(t, "valid-roster")
	for _, record := range []interface{}{map[string]interface{}{"hostname": "disabled", "disabled": 1}, map[string]interface{}{"hostname": "enabled", "master": 0, "disabled": 0}} {
		if err := e.storage.ListPush(context.Background(), "servers", record); err != nil {
			t.Fatal(err)
		}
	}
	servers, err := e.comm.loadServerList()
	if err != nil || len(servers) != 1 || servers[0] != "enabled" {
		t.Fatalf("legacy disabled filtering failed: %v %v", servers, err)
	}
	e.storage = &lifecycleStorage{Storage: e.storage, failRosterAt: 1}
	if err := e.comm.LoadServersFromStorage(); !errors.Is(err, errLifecycleStorage) {
		t.Fatalf("peer storage outage hidden: %v", err)
	}
}
func TestShutdownReturnsPersistentStorageErrorsAndStillCloses(t *testing.T) {
	for _, ownershipReadFailure := range []bool{false, true} {
		e := newSchedulingTestEngine(t, "shutdown-errors")
		e.goMaster()
		store := &lifecycleStorage{Storage: e.storage, failStateWrite: !ownershipReadFailure, failOwnershipRead: ownershipReadFailure, closeErr: errStorageClose}
		e.storage = store
		for repeat := 0; repeat < 2; repeat++ {
			err := e.Shutdown()
			if !errors.Is(err, errLifecycleStorage) || !errors.Is(err, errStorageClose) {
				t.Fatalf("shutdown failure was hidden: %v", err)
			}
		}
		if store.closeCount != 1 {
			t.Fatalf("storage closed %d times", store.closeCount)
		}
		select {
		case <-e.ShutdownChan():
		default:
			t.Fatal("failed storage prevented shutdown completion")
		}
	}
}
