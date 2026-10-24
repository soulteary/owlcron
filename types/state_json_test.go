package types

import (
	"encoding/json"
	"testing"
)

func TestStateJSONCronicleCompatibility(t *testing.T) {
	for _, input := range []string{
		`{"enabled":1,"jobCodes":{"event":2},"stats":{"jobs_elapsed":0.625,"jobs_completed":2}}`,
		`{"enabled":true,"job_codes":{"event":2},"stats":{"jobs_elapsed":0.625,"jobs_completed":2}}`,
	} {
		var state State
		if err := json.Unmarshal([]byte(input), &state); err != nil {
			t.Fatal(err)
		}
		if !state.Enabled || state.JobCodes["event"] != 2 || state.Stats["jobs_elapsed"] != 0.625 {
			t.Fatalf("state compatibility: %+v", state)
		}
		raw, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		var wire map[string]interface{}
		_ = json.Unmarshal(raw, &wire)
		if wire["jobCodes"].(map[string]interface{})["event"] != float64(2) {
			t.Fatal("browser last-code selector missing")
		}
	}
}
