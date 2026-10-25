package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/soulteary/owlcron/storage"
)

type notificationReadOnlyStore struct{ storage.Storage }

type notificationAtomicOnlyStore struct{ storage.Storage }

func (notificationAtomicOnlyStore) Update(context.Context, string, func([]byte) ([]byte, error)) error {
	return nil
}

type notificationListOnlyStore struct{ storage.Storage }

func (notificationListOnlyStore) ListKeysPrefix(context.Context, string) ([]string, error) {
	return nil, nil
}

func TestNotificationStoreAdapterRequiresAtomicUpdateAndEnumeration(t *testing.T) {
	for name, backend := range map[string]storage.Storage{
		"nil":              nil,
		"read only":        notificationReadOnlyStore{},
		"no enumeration":   notificationAtomicOnlyStore{},
		"no atomic update": notificationListOnlyStore{},
	} {
		t.Run(name, func(t *testing.T) {
			store, err := newNotificationStore(backend)
			if store != nil || !errors.Is(err, storage.ErrUnsupported) {
				t.Fatalf("store=%v err=%v", store, err)
			}
		})
	}

	backend := notificationTestStore(t)
	store, err := newNotificationStore(backend)
	if err != nil || any(store) != backend {
		t.Fatalf("adapter replaced the backend: store=%v err=%v", store, err)
	}
}

func TestNotificationQueueRejectsMissingDependencies(t *testing.T) {
	deliver := func(context.Context, *Notification) error { return nil }
	if _, err := NewNotificationQueue(nil, NotificationQueueConfig{}, deliver); !errors.Is(err, storage.ErrUnsupported) {
		t.Fatalf("nil store: %v", err)
	}
	store, err := newNotificationStore(notificationTestStore(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewNotificationQueue(store, NotificationQueueConfig{}, nil); err == nil {
		t.Fatal("nil delivery function accepted")
	}
}
