package types

import (
	"encoding/json"
	"fmt"
)

// Privileges preserves Cronicle's boolean and numeric (0/1) privilege format.
type Privileges map[string]bool

func (p *Privileges) UnmarshalJSON(data []byte) error {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	result := make(Privileges, len(values))
	for name, raw := range values {
		var value bool
		if err := json.Unmarshal(raw, &value); err == nil {
			result[name] = value
			continue
		}
		var number int
		if err := json.Unmarshal(raw, &number); err != nil || (number != 0 && number != 1) {
			return fmt.Errorf("privilege %q must be a boolean or 0/1", name)
		}
		result[name] = number == 1
	}
	*p = result
	return nil
}

// User 表示一个用户
type User struct {
	ID         string                 `json:"id"`
	Username   string                 `json:"username"`
	Email      string                 `json:"email"`
	Password   string                 `json:"password,omitempty"`
	Enabled    bool                   `json:"enabled"`
	Privileges Privileges             `json:"privileges"`
	Created    int64                  `json:"created"`
	Modified   int64                  `json:"modified"`
	Data       map[string]interface{} `json:"data,omitempty"`
}

// SafeMap returns a copy of the user without the password, suitable for API responses.
func (u *User) SafeMap() map[string]interface{} {
	return map[string]interface{}{
		"id":         u.ID,
		"username":   u.Username,
		"email":      u.Email,
		"enabled":    u.Enabled,
		"active":     u.Enabled,
		"full_name":  u.Data["full_name"],
		"privileges": u.Privileges,
		"created":    u.Created,
		"modified":   u.Modified,
	}
}

// APIKey 表示一个 API 密钥
type APIKey struct {
	ID         string                 `json:"id"`
	Title      string                 `json:"title"`
	Key        string                 `json:"key"`
	Enabled    bool                   `json:"enabled"`
	Privileges Privileges             `json:"privileges"`
	Created    int64                  `json:"created"`
	Modified   int64                  `json:"modified"`
	LastUsed   int64                  `json:"last_used,omitempty"`
	Data       map[string]interface{} `json:"data,omitempty"`
}

// Session 表示一个会话
type Session struct {
	ID      string                 `json:"id"`
	UserID  string                 `json:"user_id"`
	Expires int64                  `json:"expires"`
	Data    map[string]interface{} `json:"data,omitempty"`
}

// UnmarshalJSON accepts Cronicle's active:0/1 API-key field while retaining
// bool internally for authorization.
func (k *APIKey) UnmarshalJSON(data []byte) error {
	type plain APIKey
	var raw struct {
		*plain
		Enabled *FlexBool `json:"enabled"`
		Active  *FlexBool `json:"active"`
	}
	raw.plain = (*plain)(k)
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Enabled != nil {
		k.Enabled = raw.Enabled.Bool()
	} else if raw.Active != nil {
		k.Enabled = raw.Active.Bool()
	}
	return nil
}
func (k *APIKey) SafeMap() map[string]interface{} {
	return map[string]interface{}{"id": k.ID, "title": k.Title, "key": k.Key, "active": k.Enabled, "enabled": k.Enabled, "privileges": k.Privileges, "created": k.Created, "modified": k.Modified, "last_used": k.LastUsed}
}
