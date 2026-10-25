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
	"maps"
	"regexp"

	"github.com/soulteary/owlcron/blob"
)

const StorageLayoutKey = "global/storage_layout"
const StorageLayoutVersion = 2

var ErrStorageLayoutNotReady = errors.New("storage layout is not ready")
var namespacePattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

type StorageLayout struct {
	Version     int               `json:"version"`
	NamespaceID string            `json:"namespace_id"`
	Phase       string            `json:"phase"`
	Bindings    map[string]string `json:"bindings"`
	TargetPhase string            `json:"target_phase,omitempty"`
}

func decodeStorageLayout(data []byte) (StorageLayout, error) {
	var layout StorageLayout
	if len(data) == 0 || len(data) > 16*1024 {
		return layout, fmt.Errorf("invalid storage layout size")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&layout); err != nil {
		return layout, fmt.Errorf("decode storage layout: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return layout, fmt.Errorf("storage layout contains trailing data")
	}
	if layout.Version != StorageLayoutVersion || !namespacePattern.MatchString(layout.NamespaceID) || len(layout.Bindings) == 0 || len(layout.Bindings) > 16 {
		return layout, fmt.Errorf("unsupported or invalid storage layout")
	}
	if !validLayoutPhase(layout.Phase) || (layout.Phase == "initializing" && !validTargetPhase(layout.TargetPhase)) || (layout.Phase != "initializing" && layout.TargetPhase != "") {
		return layout, fmt.Errorf("invalid storage layout phase")
	}
	for id, binding := range layout.Bindings {
		decoded, err := hex.DecodeString(binding)
		if !storeIDPattern.MatchString(id) || err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != binding {
			return layout, fmt.Errorf("invalid storage layout binding")
		}
	}
	return layout, nil
}
func validTargetPhase(phase string) bool {
	return phase == "ready" || phase == "migrating" || phase == "restoring"
}
func validLayoutPhase(phase string) bool { return phase == "initializing" || validTargetPhase(phase) }

func LoadStorageLayout(ctx context.Context, meta Storage) (StorageLayout, error) {
	data, err := meta.Get(ctx, StorageLayoutKey)
	if err != nil {
		return StorageLayout{}, err
	}
	return decodeStorageLayout(data)
}

func ValidateStorageLayout(ctx context.Context, meta Storage, registry *BlobRegistry) (StorageLayout, error) {
	return ValidateStorageLayoutPhase(ctx, meta, registry, "ready")
}
func ValidateStorageLayoutPhase(ctx context.Context, meta Storage, registry *BlobRegistry, phase string) (StorageLayout, error) {
	layout, err := LoadStorageLayout(ctx, meta)
	if err != nil {
		return layout, err
	}
	if layout.Phase != phase {
		return layout, fmt.Errorf("%w: phase is %s", ErrStorageLayoutNotReady, layout.Phase)
	}
	if registry == nil || !maps.Equal(layout.Bindings, registry.bindings) {
		return layout, fmt.Errorf("blob store bindings differ from the initialized layout")
	}
	if err := registry.Readiness(ctx, layout.NamespaceID); err != nil {
		return layout, err
	}
	return layout, nil
}

// InitializeStorageLayout is an explicit offline operation. New daemons must
// validate a ready layout before their first mutation. This marker cannot fence
// legacy binaries; operators must isolate old writers and use a fresh target.
// A failed initializing phase is resumable with the identical target bindings.
func InitializeStorageLayout(ctx context.Context, meta Storage, registry *BlobRegistry, phase string) (StorageLayout, error) {
	if registry == nil || !validTargetPhase(phase) {
		return StorageLayout{}, fmt.Errorf("invalid layout initialization")
	}
	atomic, ok := meta.(AtomicStorage)
	if !ok {
		return StorageLayout{}, ErrUnsupported
	}
	existing, err := LoadStorageLayout(ctx, meta)
	if err == nil && existing.Phase != "initializing" {
		return ValidateStorageLayoutPhase(ctx, meta, registry, phase)
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return StorageLayout{}, err
	}
	if errors.Is(err, ErrNotFound) {
		lister, ok := meta.(KeyLister)
		if !ok {
			return StorageLayout{}, ErrUnsupported
		}
		keys, e := lister.ListKeys(ctx)
		if e != nil {
			return StorageLayout{}, e
		}
		for _, key := range keys {
			if key != StorageLayoutKey && key != "global/readiness_probe" {
				return StorageLayout{}, fmt.Errorf("layout initialization requires an empty metadata target")
			}
		}
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return StorageLayout{}, err
	}
	proposed := StorageLayout{Version: StorageLayoutVersion, NamespaceID: hex.EncodeToString(nonce[:]), Phase: "initializing", TargetPhase: phase, Bindings: maps.Clone(registry.bindings)}
	chosen := proposed
	err = atomic.Update(ctx, StorageLayoutKey, func(raw []byte) ([]byte, error) {
		chosen = proposed
		if len(raw) > 0 {
			current, e := decodeStorageLayout(raw)
			if e != nil {
				return nil, e
			}
			if current.Phase != "initializing" || current.TargetPhase != phase || !maps.Equal(current.Bindings, registry.bindings) {
				return nil, fmt.Errorf("existing storage layout conflicts with initialization")
			}
			chosen = current
		}
		return json.Marshal(chosen)
	})
	if err != nil {
		return StorageLayout{}, err
	}
	for id, store := range registry.stores {
		if err := probeBlobStore(ctx, store, chosen.NamespaceID); err != nil {
			return chosen, fmt.Errorf("probe blob store %s: %w", id, err)
		}
		key, marker := registry.marker(chosen.NamespaceID, id)
		body, e := store.Open(ctx, key)
		if e == nil {
			data, readErr := io.ReadAll(io.LimitReader(body, int64(len(marker))+1))
			closeErr := body.Close()
			if readErr != nil {
				return chosen, readErr
			}
			if closeErr != nil {
				return chosen, closeErr
			}
			if !bytes.Equal(data, marker) {
				return chosen, fmt.Errorf("%w: existing namespace marker differs", blob.ErrIntegrity)
			}
		} else if errors.Is(e, blob.ErrNotFound) {
			if e = store.Put(ctx, blob.Write{Key: key, Size: int64(len(marker)), SHA256: sha256.Sum256(marker)}, bytes.NewReader(marker)); e != nil {
				return chosen, e
			}
		} else {
			return chosen, e
		}
	}
	if err := registry.Readiness(ctx, chosen.NamespaceID); err != nil {
		return chosen, err
	}
	if err := SetStorageLayoutPhase(ctx, meta, chosen.NamespaceID, "initializing", phase); err != nil {
		return chosen, err
	}
	return LoadStorageLayout(ctx, meta)
}

func SetStorageLayoutPhase(ctx context.Context, meta Storage, namespace, from, to string) error {
	if !namespacePattern.MatchString(namespace) || !validLayoutPhase(from) || !validTargetPhase(to) {
		return fmt.Errorf("invalid layout phase transition")
	}
	atomic, ok := meta.(AtomicStorage)
	if !ok {
		return ErrUnsupported
	}
	return atomic.Update(ctx, StorageLayoutKey, func(raw []byte) ([]byte, error) {
		layout, err := decodeStorageLayout(raw)
		if err != nil {
			return nil, err
		}
		if layout.NamespaceID != namespace {
			return nil, ErrConflict
		}
		if layout.Phase == to {
			return raw, nil
		}
		if layout.Phase != from || (from == "initializing" && layout.TargetPhase != to) {
			return nil, ErrConflict
		}
		layout.Phase = to
		layout.TargetPhase = ""
		return json.Marshal(layout)
	})
}

func probeBlobStore(ctx context.Context, store blob.Store, namespace string) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	key := namespace + "/probe/" + hex.EncodeToString(nonce[:])
	data := []byte("owlcron immutable blob capability v2\n")
	if err := store.Put(ctx, blob.Write{Key: key, Size: int64(len(data)), SHA256: sha256.Sum256(data)}, bytes.NewReader(data)); err != nil {
		return err
	}
	info, err := store.Stat(ctx, key)
	if err != nil {
		return err
	}
	if info.Size != int64(len(data)) {
		return fmt.Errorf("%w: probe HEAD length", blob.ErrIntegrity)
	}
	body, err := store.Open(ctx, key)
	if err != nil {
		return err
	}
	got, err := io.ReadAll(io.LimitReader(body, int64(len(data))+1))
	closeErr := body.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if !bytes.Equal(got, data) {
		return fmt.Errorf("%w: probe GET content", blob.ErrIntegrity)
	}
	return store.Delete(ctx, key)
}
