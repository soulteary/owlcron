package engine

import "github.com/soulteary/owlcron/storage"

// newNotificationStore adapts the application's optional storage capabilities
// at initialization. The queue itself only accepts its three-method contract.
func newNotificationStore(store storage.Storage) (NotificationStore, error) {
	queueStore, ok := store.(NotificationStore)
	if !ok {
		return nil, storage.ErrUnsupported
	}
	return queueStore, nil
}

var (
	_ NotificationStore = (*storage.FilesystemStorage)(nil)
	_ NotificationStore = (*storage.S3Storage)(nil)
	_ NotificationStore = (*storage.CouchbaseStorage)(nil)
)
