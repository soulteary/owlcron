package types

import "encoding/json"

// Cronicle's browser submits master as 0/1 while Go clients commonly use bool.
func (s *ServerGroup) UnmarshalJSON(data []byte) error {
	type record ServerGroup
	alias := record(*s)
	var input struct {
		*record
		Master *FlexBool `json:"master"`
	}
	input.record = &alias
	if err := json.Unmarshal(data, &input); err != nil {
		return err
	}
	*s = ServerGroup(alias)
	if input.Master != nil {
		s.Master = input.Master.Bool()
	}
	return nil
}
