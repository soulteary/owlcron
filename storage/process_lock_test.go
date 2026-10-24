package storage

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
)

func TestFilesystemProcessWriter(t *testing.T) {
	base := os.Getenv("OWLCRON_FS_CHILD_BASE")
	if base == "" {
		t.Skip("subprocess helper")
	}
	store, err := NewFilesystemStorage(&StorageConfig{BaseDir: base})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		if err := store.ListPush(context.Background(), "process_writes", fmt.Sprintf("%s-%d", os.Getenv("OWLCRON_FS_CHILD_ID"), i)); err != nil {
			t.Fatal(err)
		}
	}
}
func TestFilesystemAtomicWritesAcrossProcesses(t *testing.T) {
	base := t.TempDir()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			command := exec.Command(os.Args[0], "-test.run=^TestFilesystemProcessWriter$", "-test.count=1")
			command.Env = append(os.Environ(), "OWLCRON_FS_CHILD_BASE="+base, fmt.Sprintf("OWLCRON_FS_CHILD_ID=%d", i))
			if output, err := command.CombinedOutput(); err != nil {
				t.Errorf("writer process: %v %s", err, output)
			}
		}(i)
	}
	wg.Wait()
	store, err := NewFilesystemStorage(&StorageConfig{BaseDir: base})
	if err != nil {
		t.Fatal(err)
	}
	items, err := store.ListGet(context.Background(), "process_writes", 0, 0)
	if err != nil || len(items) != 100 {
		t.Fatalf("cross-process lost writes: count=%d error=%v", len(items), err)
	}
	unique := map[string]bool{}
	for _, item := range items {
		unique[item.(string)] = true
	}
	if len(unique) != 100 {
		t.Fatal("duplicate process writes")
	}
}
