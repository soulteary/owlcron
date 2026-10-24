package plugin

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/soulteary/owlcron/types"
)

func TestSyntheticCroniclePluginFixture(t *testing.T) {
	request, err := os.ReadFile("testdata/cronicle-job.json")
	if err != nil {
		t.Fatal(err)
	}
	var job types.Job
	if json.Unmarshal(request, &job) != nil || job.ID != "fixture-job" || job.Params["message"] != "synthetic fixture" {
		t.Fatal("stdin job fixture failed")
	}
	output, err := os.ReadFile("testdata/cronicle-output.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range []int{1, 7, 64, len(output)} {
		var log bytes.Buffer
		result := &PluginResult{}
		writer := &protocolWriter{log: &log, result: result}
		for start := 0; start < len(output); start += chunk {
			end := start + chunk
			if end > len(output) {
				end = len(output)
			}
			if _, err := writer.Write(output[start:end]); err != nil {
				t.Fatal(err)
			}
		}
		writer.flush()
		if !result.Complete || result.Code != 0 || result.Progress != 0.375 || result.ChainData["answer"] != float64(42) {
			t.Fatalf("protocol fixture failed in %d-byte chunks: %+v", chunk, result)
		}
		if log.String() != "fixture ordinary log\ntail after completion\n" {
			t.Fatalf("log fixture failed: %q", log.String())
		}
	}
}
