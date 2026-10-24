package types

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Cronicle sends timing:false for events that are run on demand.
func (e *Event) UnmarshalJSON(data []byte) error {
	type eventAlias Event
	decoded := struct {
		*eventAlias
		Timing json.RawMessage `json:"timing"`
	}{eventAlias: (*eventAlias)(e)}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	timing := bytes.TrimSpace(decoded.Timing)
	if len(timing) == 0 {
		return nil
	}
	if bytes.Equal(timing, []byte("false")) || bytes.Equal(timing, []byte("null")) {
		e.Timing = nil
		return nil
	}
	if timing[0] != '{' {
		return fmt.Errorf("timing must be an object, false, or null")
	}
	var parsed Timing
	if err := json.Unmarshal(timing, &parsed); err != nil {
		return err
	}
	e.Timing = &parsed
	return nil
}
