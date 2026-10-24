package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestReadOnlyCLIHasNoStorageOrExecutionSideEffects(t *testing.T) {
	export, err := filepath.Abs("../../migration/testdata/cronicle-export.txt")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	t.Chdir(dir)
	var output bytes.Buffer
	if err := run([]string{"--export", export}, &output); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("created runtime files: %v, %v", entries, err)
	}
	if !bytes.Contains(output.Bytes(), []byte(`"read_only": true`)) {
		t.Fatal("missing read-only report")
	}
	for _, args := range [][]string{nil, {"--export", "missing"}, {"--export", export, "--config", "missing"}, {"--export", export, "extra"}} {
		if err := run(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("invalid arguments accepted: %v", args)
		}
	}
	entries, _ = os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal("failed command created runtime files")
	}
}

func TestHelpDoesNotRequireInput(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"--help"}, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte("Read-only")) {
		t.Fatal("missing help")
	}
}
