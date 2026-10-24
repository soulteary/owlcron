package types

import "encoding/json"

func (s *Server) UnmarshalJSON(raw []byte) error {
	type plain Server
	wire := struct {
		*plain
		Master   *FlexBool `json:"master"`
		Disabled *FlexBool `json:"disabled"`
	}{plain: (*plain)(s)}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	if wire.Master != nil {
		s.Master = wire.Master.Bool()
	}
	if wire.Disabled != nil {
		s.Disabled = wire.Disabled.Bool()
	}
	return nil
}
