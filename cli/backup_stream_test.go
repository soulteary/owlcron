package cli

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestStreamingBackupFailurePreservesDestination(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.json")
	if err := os.WriteFile(path, []byte("original backup"), 0600); err != nil {
		t.Fatal(err)
	}
	err := writeBackupFile(path, func(w io.Writer) error { _, _ = io.WriteString(w, "partial"); return io.ErrUnexpectedEOF })
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "original backup" {
		t.Fatal("failed export replaced destination")
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatal("temporary export left behind")
	}
}
