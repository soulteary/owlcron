package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"
)

type exportFixture struct {
	Storage
	reads int
}

func (s *exportFixture) ListKeys(context.Context) ([]string, error) { return []string{"b", "a"}, nil }
func (s *exportFixture) Get(_ context.Context, key string) ([]byte, error) {
	if key == "a" || key == "b" {
		s.reads++
		return []byte("payload"), nil
	}
	return nil, ErrNotFound
}

type observingWriter struct {
	bytes.Buffer
	source   *exportFixture
	streamed bool
}

func (w *observingWriter) Write(p []byte) (int, error) {
	if w.source.reads == 1 && bytes.Contains(p, []byte("data")) {
		w.streamed = true
	}
	return w.Buffer.Write(p)
}

type failingExportWriter struct{}

func (failingExportWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestExportToStreamsExistingFormat(t *testing.T) {
	source := &exportFixture{}
	writer := &observingWriter{source: source}
	count, err := ExportTo(context.Background(), source, writer)
	if err != nil || count != 2 || !writer.streamed {
		t.Fatalf("not streamed: count=%d err=%v streamed=%v", count, err, writer.streamed)
	}
	var got Backup
	if err := json.Unmarshal(writer.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	expected, err := Export(context.Background(), &exportFixture{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Format != expected.Format || !reflect.DeepEqual(got.Records, expected.Records) {
		t.Fatal("stream changed backup format or records")
	}
	target, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := Import(context.Background(), target, &got); err != nil {
		t.Fatal(err)
	}
	if data, err := target.Get(context.Background(), "a"); err != nil || string(data) != "payload" {
		t.Fatalf("restore failed: %q %v", data, err)
	}
	if _, err := ExportTo(context.Background(), &exportFixture{}, failingExportWriter{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("lost writer error: %v", err)
	}
}
