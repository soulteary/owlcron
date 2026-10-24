package types

import "encoding/json"

// MarshalJSON preserves the 0.9.102 browser and persisted-state contract.
func (s State) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Enabled     bool                        `json:"enabled"`
		Cursors     map[string]int64            `json:"cursors"`
		Stats       map[string]float64          `json:"stats"`
		FlaggedJobs map[string]map[string]int64 `json:"flagged_jobs"`
		Robins      map[string]int              `json:"robins"`
		JobCodes    map[string]int              `json:"jobCodes"`
	}{s.Enabled, s.Cursors, s.Stats, s.FlaggedJobs, s.Robins, s.JobCodes})
}

func (s *State) UnmarshalJSON(raw []byte) error {
	type plainState State
	wire := struct {
		*plainState
		Enabled  *FlexBool      `json:"enabled"`
		JobCodes map[string]int `json:"jobCodes"`
	}{plainState: (*plainState)(s)}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	if wire.Enabled != nil {
		s.Enabled = wire.Enabled.Bool()
	}
	if wire.JobCodes != nil {
		s.JobCodes = wire.JobCodes
	}
	return nil
}
