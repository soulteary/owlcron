package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/soulteary/owlcron/types"
)

func processCompletionSpoolPaths(logDir, id string) []string {
	return []string{filepath.Join(logDir, "jobs", id+".completion-wal"), filepath.Join(logDir, "jobs", id+".completed.json")}
}

// Observe the on-disk contract independently of the daemon. Only the newest
// valid sealed slot proves finalization; a protocol report can precede exit.
func readProcessCompletionSpool(logDir, id string) (*types.Job, error) {
	paths := processCompletionSpoolPaths(logDir, id)
	raw, err := os.ReadFile(paths[0])
	if errors.Is(err, os.ErrNotExist) {
		raw, err = os.ReadFile(paths[1])
		if err != nil {
			return nil, err
		}
		var job types.Job
		if err := json.Unmarshal(raw, &job); err != nil {
			return nil, err
		}
		if job.ID != id || !job.Complete {
			return nil, fmt.Errorf("invalid legacy completion spool")
		}
		return &job, nil
	}
	if err != nil {
		return nil, err
	}
	const headerSize = 64
	if len(raw)%2 != 0 || len(raw) < 2*(headerSize+1) {
		return nil, fmt.Errorf("invalid completion WAL layout")
	}
	slotSize := len(raw) / 2
	var newest uint64
	var latest struct {
		Phase string     `json:"phase"`
		Job   *types.Job `json:"job"`
	}
	for slot := range 2 {
		data := raw[slot*slotSize : (slot+1)*slotSize]
		header := data[:headerSize]
		if string(header[:8]) != "OWLCOMP\x01" {
			continue
		}
		generation := binary.BigEndian.Uint64(header[8:16])
		length := binary.BigEndian.Uint64(header[16:24])
		if generation == 0 || generation <= newest || length == 0 || length > uint64(slotSize-headerSize) {
			continue
		}
		payload := data[headerSize : headerSize+int(length)]
		hash := sha256.New()
		_, _ = hash.Write(header[:24])
		_, _ = hash.Write(payload)
		if string(hash.Sum(nil)) != string(header[24:56]) {
			continue
		}
		var checkpoint struct {
			Phase string     `json:"phase"`
			Job   *types.Job `json:"job"`
		}
		if json.Unmarshal(payload, &checkpoint) != nil || checkpoint.Job == nil || checkpoint.Job.ID != id {
			continue
		}
		if checkpoint.Phase != "active" && checkpoint.Phase != "reported" && checkpoint.Phase != "sealed" {
			continue
		}
		newest, latest = generation, checkpoint
	}
	if newest == 0 || latest.Phase != "sealed" || !latest.Job.Complete {
		return nil, fmt.Errorf("completion WAL has no sealed finalization")
	}
	return latest.Job, nil
}

func processCompletionSpoolsRemoved(logDir, id string) bool {
	for _, path := range processCompletionSpoolPaths(logDir, id) {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func TestProcessCompletionSpoolObserverRequiresNewestSealedSlot(t *testing.T) {
	dir := t.TempDir()
	paths := processCompletionSpoolPaths(dir, "one")
	if err := os.MkdirAll(filepath.Dir(paths[0]), 0700); err != nil {
		t.Fatal(err)
	}
	writeSlot := func(slot []byte, generation uint64, phase string) {
		payload := []byte(fmt.Sprintf(`{"phase":%q,"job":{"id":"one","complete":true,"code":0}}`, phase))
		copy(slot, "OWLCOMP\x01")
		binary.BigEndian.PutUint64(slot[8:16], generation)
		binary.BigEndian.PutUint64(slot[16:24], uint64(len(payload)))
		copy(slot[64:], payload)
		hash := sha256.New()
		_, _ = hash.Write(slot[:24])
		_, _ = hash.Write(payload)
		copy(slot[24:56], hash.Sum(nil))
	}
	raw := make([]byte, 1024)
	writeSlot(raw[:512], 1, "sealed")
	writeSlot(raw[512:], 2, "reported")
	if err := os.WriteFile(paths[0], raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readProcessCompletionSpool(dir, "one"); err == nil {
		t.Fatal("observer treated an older sealed slot as current finalization")
	}
	// A torn inactive write may be ignored only after its checksum fails.
	raw[512+24] ^= 1
	if err := os.WriteFile(paths[0], raw, 0600); err != nil {
		t.Fatal(err)
	}
	if job, err := readProcessCompletionSpool(dir, "one"); err != nil || job.Code != 0 {
		t.Fatalf("observer lost the surviving checked slot: %+v %v", job, err)
	}
	if err := os.Remove(paths[0]); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths[1], []byte(`{"id":"one","complete":true,"code":0}`), 0600); err != nil {
		t.Fatal(err)
	}
	if job, err := readProcessCompletionSpool(dir, "one"); err != nil || job.ID != "one" {
		t.Fatalf("observer lost legacy spool compatibility: %+v %v", job, err)
	}
	if processCompletionSpoolsRemoved(dir, "one") {
		t.Fatal("observer ignored a remaining legacy spool")
	}
}
